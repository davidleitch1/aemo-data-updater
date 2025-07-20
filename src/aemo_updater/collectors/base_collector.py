"""
Base collector class for all AEMO data collectors
Provides common functionality for downloading, processing, and storing data
"""

from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, List
import logging
from datetime import datetime, timedelta
import aiohttp
import asyncio
from io import BytesIO
import zipfile

from ..config import HTTP_HEADERS, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY


class BaseCollector(ABC):
    """Abstract base class for all NEMWEB data collectors"""
    
    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize base collector
        
        Args:
            name: Collector name (e.g., 'generation', 'price')
            config: Configuration dict from config.PARQUET_FILES
        """
        self.name = name
        self.config = config
        self.output_file = Path(config['path'])
        self.update_interval = config['update_interval']
        self.retention_days = config['retention_days']
        self.logger = logging.getLogger(f'aemo_updater.collectors.{name}')
        
        # Status tracking
        self.last_update_time: Optional[datetime] = None
        self.last_update_success: bool = False
        self.last_error: Optional[str] = None
        self.records_added: int = 0
        
        # Ensure output directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
    @abstractmethod
    async def get_latest_urls(self) -> List[str]:
        """Get URLs of latest data files to download"""
        pass
        
    @abstractmethod
    async def parse_data(self, content: bytes, filename: str) -> pd.DataFrame:
        """Parse downloaded data into DataFrame"""
        pass
        
    @abstractmethod
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate parsed data meets quality standards"""
        pass
        
    async def download_file(self, url: str) -> Optional[bytes]:
        """
        Download file from URL with retries
        
        Args:
            url: URL to download
            
        Returns:
            File content as bytes or None if failed
        """
        for attempt in range(MAX_RETRIES):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url, 
                        headers=HTTP_HEADERS,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    ) as response:
                        if response.status == 200:
                            content = await response.read()
                            self.logger.debug(f"Downloaded {len(content)} bytes from {url}")
                            return content
                        else:
                            self.logger.warning(f"HTTP {response.status} for {url}")
                            
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout downloading {url} (attempt {attempt + 1}/{MAX_RETRIES})")
            except Exception as e:
                self.logger.error(f"Error downloading {url}: {e}")
                
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY)
                
        return None
        
    def extract_zip_content(self, zip_content: bytes) -> Optional[str]:
        """
        Extract CSV content from ZIP file
        
        Args:
            zip_content: ZIP file as bytes
            
        Returns:
            CSV content as string or None if failed
        """
        try:
            with zipfile.ZipFile(BytesIO(zip_content)) as zf:
                # Find CSV file in ZIP
                csv_files = [f for f in zf.namelist() if f.endswith('.CSV') or f.endswith('.csv')]
                if not csv_files:
                    self.logger.error("No CSV file found in ZIP")
                    return None
                    
                # Read first CSV file
                with zf.open(csv_files[0]) as csv_file:
                    return csv_file.read().decode('utf-8')
                    
        except Exception as e:
            self.logger.error(f"Error extracting ZIP: {e}")
            return None
            
    def load_existing_data(self) -> pd.DataFrame:
        """Load existing data from parquet file"""
        if self.output_file.exists():
            try:
                df = pd.read_parquet(self.output_file)
                self.logger.debug(f"Loaded {len(df)} existing records from {self.output_file.name}")
                return df
            except Exception as e:
                self.logger.error(f"Error loading existing data: {e}")
                return pd.DataFrame()
        else:
            self.logger.info(f"No existing data file at {self.output_file}")
            return pd.DataFrame()
            
    def save_data(self, df: pd.DataFrame) -> bool:
        """
        Save DataFrame to parquet file
        
        Args:
            df: DataFrame to save
            
        Returns:
            True if successful
        """
        try:
            # Save to temporary file first
            temp_file = self.output_file.with_suffix('.tmp')
            df.to_parquet(temp_file, compression='snappy', index=False)
            
            # Atomic rename
            temp_file.replace(self.output_file)
            
            self.logger.info(f"Saved {len(df)} records to {self.output_file.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
            return False
            
    def apply_retention_policy(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data retention policy to DataFrame"""
        if 'settlementdate' not in df.columns:
            return df
            
        cutoff = datetime.now() - timedelta(days=self.retention_days)
        
        # Ensure settlementdate is datetime
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        
        original_count = len(df)
        df = df[df['settlementdate'] >= cutoff]
        removed = original_count - len(df)
        
        if removed > 0:
            self.logger.info(f"Removed {removed} records older than {cutoff.date()}")
            
        return df
        
    async def collect_once(self) -> bool:
        """
        Run one collection cycle
        
        Returns:
            True if successful
        """
        start_time = datetime.now()
        self.records_added = 0
        
        try:
            # Get URLs to download
            urls = await self.get_latest_urls()
            if not urls:
                self.logger.debug("No new data files to download")
                self.last_update_success = True
                return True
                
            # Load existing data
            existing_df = self.load_existing_data()
            
            # Download and parse each file
            new_data = []
            for url in urls:
                # Download
                content = await self.download_file(url)
                if content is None:
                    continue
                    
                # Parse
                filename = url.split('/')[-1]
                df = await self.parse_data(content, filename)
                if df is None or df.empty:
                    continue
                    
                # Validate
                if not self.validate_data(df):
                    self.logger.warning(f"Data validation failed for {filename}")
                    continue
                    
                new_data.append(df)
                
            if not new_data:
                self.logger.debug("No valid new data collected")
                self.last_update_success = True
                return True
                
            # Combine new data
            new_df = pd.concat(new_data, ignore_index=True)
            self.logger.info(f"Collected {len(new_df)} new records")
            
            # Merge with existing data
            if not existing_df.empty:
                # Identify unique columns for deduplication
                unique_cols = self.get_unique_columns()
                
                # If settlementdate exists, filter out overlapping date ranges
                # This prevents keeping both old and new data for the same time periods
                if 'settlementdate' in new_df.columns and 'settlementdate' in existing_df.columns:
                    # Get date range of new data
                    new_min_date = new_df['settlementdate'].min()
                    new_max_date = new_df['settlementdate'].max()
                    
                    # Filter out existing data in the date range of new data
                    existing_filtered = existing_df[
                        (existing_df['settlementdate'] < new_min_date) | 
                        (existing_df['settlementdate'] > new_max_date)
                    ]
                    
                    self.logger.debug(
                        f"Filtering existing data: removed {len(existing_df) - len(existing_filtered)} "
                        f"records in date range {new_min_date} to {new_max_date}"
                    )
                    
                    # Combine filtered existing with new data
                    combined_df = pd.concat([existing_filtered, new_df], ignore_index=True)
                else:
                    # No settlementdate column, use original logic
                    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                
                # Remove duplicates based on unique columns
                original_len = len(combined_df)
                combined_df = combined_df.drop_duplicates(subset=unique_cols, keep='last')
                duplicates = original_len - len(combined_df)
                
                if duplicates > 0:
                    self.logger.debug(f"Removed {duplicates} duplicate records")
                    
                self.records_added = len(combined_df) - len(existing_df)
            else:
                combined_df = new_df
                self.records_added = len(new_df)
                
            # Apply retention policy
            combined_df = self.apply_retention_policy(combined_df)
            
            # Sort by time
            if 'settlementdate' in combined_df.columns:
                combined_df = combined_df.sort_values('settlementdate')
                
            # Save data
            if self.save_data(combined_df):
                duration = (datetime.now() - start_time).total_seconds()
                self.logger.info(
                    f"Collection complete: {self.records_added} new records "
                    f"in {duration:.1f}s"
                )
                self.last_update_time = datetime.now()
                self.last_update_success = True
                self.last_error = None
                return True
            else:
                raise Exception("Failed to save data")
                
        except Exception as e:
            self.logger.error(f"Collection failed: {e}")
            self.last_update_success = False
            self.last_error = str(e)
            return False
            
    @abstractmethod
    def get_unique_columns(self) -> List[str]:
        """Get column names that uniquely identify a record"""
        pass
        
    def get_status(self) -> Dict[str, Any]:
        """Get current collector status"""
        # Check file info
        file_info = {}
        if self.output_file.exists():
            stat = self.output_file.stat()
            file_info = {
                'exists': True,
                'size_mb': stat.st_size / (1024 * 1024),
                'modified': datetime.fromtimestamp(stat.st_mtime),
            }
            
            # Get data info
            try:
                df = pd.read_parquet(self.output_file)
                if 'settlementdate' in df.columns:
                    file_info['records'] = len(df)
                    file_info['date_range'] = {
                        'start': df['settlementdate'].min(),
                        'end': df['settlementdate'].max(),
                    }
            except:
                pass
        else:
            file_info = {'exists': False}
            
        return {
            'name': self.name,
            'last_update': self.last_update_time,
            'last_success': self.last_update_success,
            'last_error': self.last_error,
            'records_added': self.records_added,
            'file_info': file_info,
        }