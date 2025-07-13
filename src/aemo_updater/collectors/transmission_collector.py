"""
Transmission flow data collector
Downloads DISPATCHINTERCONNECTORRES data from NEMWEB
Handles both CURRENT (real-time) and ARCHIVE (historical) data
"""

import pandas as pd
from datetime import datetime, timedelta
import re
from typing import List, Optional, Dict
import aiohttp
from bs4 import BeautifulSoup
import zipfile
from io import BytesIO

from .base_collector import BaseCollector
from ..config import NEMWEB_URLS, HTTP_HEADERS, REQUEST_TIMEOUT


class TransmissionCollector(BaseCollector):
    """Collects 5-minute transmission interconnector flow data"""
    
    def __init__(self, config: Dict):
        super().__init__('transmission', config)
        self.current_url = NEMWEB_URLS['transmission']['current']
        self.archive_url = NEMWEB_URLS['transmission']['archive']
        self.file_pattern = re.compile(NEMWEB_URLS['transmission']['file_pattern'])
        self.archive_pattern = re.compile(NEMWEB_URLS['transmission']['archive_pattern'])
        
        # Track last processed file to avoid reprocessing
        self.last_processed_file = None
        self._load_last_processed()
        
    def _load_last_processed(self):
        """Load last processed filename from existing data"""
        try:
            df = self.load_existing_data()
            if not df.empty and 'source_file' in df.columns:
                self.last_processed_file = df['source_file'].iloc[-1]
                self.logger.debug(f"Last processed file: {self.last_processed_file}")
        except:
            pass
            
    async def get_latest_urls(self) -> List[str]:
        """Get URLs of latest transmission data files"""
        urls = []
        
        # Check CURRENT directory first
        current_file = await self._find_latest_current_file()
        if current_file:
            urls.append(current_file)
            
        return urls
        
    async def _find_latest_current_file(self) -> Optional[str]:
        """Find latest file in CURRENT directory"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.current_url,
                    headers=HTTP_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as response:
                    if response.status != 200:
                        self.logger.warning(f"Failed to access CURRENT directory: HTTP {response.status}")
                        return None
                        
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all DISPATCHIS files
                    dispatch_files = []
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if self.file_pattern.match(href) and 'DISPATCHIS' in href:
                            dispatch_files.append(href)
                            
                    if not dispatch_files:
                        self.logger.debug("No DISPATCHIS files found in CURRENT")
                        return None
                        
                    # Sort and get latest
                    dispatch_files.sort()
                    latest_file = dispatch_files[-1]
                    
                    # Check if already processed
                    if latest_file == self.last_processed_file:
                        self.logger.debug(f"File {latest_file} already processed")
                        return None
                        
                    # Construct full URL
                    if latest_file.startswith('http'):
                        return latest_file
                    elif latest_file.startswith('/'):
                        return f"http://nemweb.com.au{latest_file}"
                    else:
                        return f"{self.current_url}{latest_file}"
                        
        except Exception as e:
            self.logger.error(f"Error accessing CURRENT directory: {e}")
            return None
            
    async def parse_data(self, content: bytes, filename: str) -> pd.DataFrame:
        """Parse transmission data from ZIP file"""
        try:
            # Extract CSV from ZIP
            csv_content = self.extract_zip_content(content)
            if not csv_content:
                return pd.DataFrame()
                
            # Parse DISPATCHINTERCONNECTORRES data
            data_rows = []
            lines = csv_content.strip().split('\n')
            
            for line in lines:
                if line.startswith('D,DISPATCH,INTERCONNECTORRES'):
                    fields = line.split(',')
                    if len(fields) >= 17:
                        data_rows.append({
                            'settlementdate': fields[4].strip('"'),
                            'interconnectorid': fields[6].strip('"'),
                            'meteredmwflow': float(fields[9].strip('"') or 0),
                            'mwflow': float(fields[10].strip('"') or 0),
                            'mwlosses': float(fields[11].strip('"') or 0),
                            'exportlimit': float(fields[15].strip('"') or 0),
                            'importlimit': float(fields[16].strip('"') or 0),
                            'source_file': filename,
                        })
                        
            if not data_rows:
                self.logger.warning(f"No transmission data found in {filename}")
                return pd.DataFrame()
                
            # Create DataFrame
            df = pd.DataFrame(data_rows)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            # Update last processed file
            self.last_processed_file = filename
            
            self.logger.info(f"Parsed {len(df)} transmission records from {filename}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing transmission data: {e}")
            return pd.DataFrame()
            
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate transmission data"""
        if df.empty:
            return False
            
        required_cols = ['settlementdate', 'interconnectorid', 'meteredmwflow']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns. Have: {df.columns.tolist()}")
            return False
            
        # Check for reasonable values
        if df['meteredmwflow'].isna().all():
            self.logger.error("All flow values are NaN")
            return False
            
        # Check interconnector IDs
        valid_interconnectors = [
            'NSW1-QLD1', 'QLD1-NSW1', 'N-Q-MNSP1',
            'VIC1-NSW1', 'NSW1-VIC1',
            'VIC1-SA1', 'SA1-VIC1', 'V-SA', 'V-S-MNSP1',
            'VIC1-TAS1', 'TAS1-VIC1', 'T-V-MNSP1'
        ]
        invalid = df[~df['interconnectorid'].isin(valid_interconnectors)]
        if not invalid.empty:
            self.logger.warning(f"Unknown interconnectors: {invalid['interconnectorid'].unique()}")
            
        return True
        
    def get_unique_columns(self) -> List[str]:
        """Transmission data is unique by timestamp + interconnector"""
        return ['settlementdate', 'interconnectorid']
        
    async def backfill_date_range(self, start_date: datetime, end_date: datetime) -> bool:
        """
        Backfill transmission data for a date range from ARCHIVE
        
        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            
        Returns:
            True if successful
        """
        self.logger.info(f"Starting transmission backfill for {start_date.date()} to {end_date.date()}")
        
        # Load existing data
        existing_df = self.load_existing_data()
        all_new_data = []
        
        # Process each day
        current_date = start_date
        while current_date <= end_date:
            try:
                # Check if we already have complete data for this day
                if not existing_df.empty:
                    day_data = existing_df[
                        existing_df['settlementdate'].dt.date == current_date.date()
                    ]
                    # Expect 288 intervals × 6+ interconnectors = 1700+ records
                    if len(day_data) >= 1700:
                        self.logger.debug(f"Skipping {current_date.date()} - already have {len(day_data)} records")
                        current_date += timedelta(days=1)
                        continue
                        
                # Download daily archive
                daily_data = await self._download_daily_archive(current_date)
                if daily_data is not None and not daily_data.empty:
                    all_new_data.append(daily_data)
                    self.logger.info(f"Collected {len(daily_data)} records for {current_date.date()}")
                else:
                    self.logger.warning(f"No data collected for {current_date.date()}")
                    
            except Exception as e:
                self.logger.error(f"Error processing {current_date.date()}: {e}")
                
            current_date += timedelta(days=1)
            
        # Merge all new data
        if not all_new_data:
            self.logger.warning("No new data collected during backfill")
            return False
            
        new_df = pd.concat(all_new_data, ignore_index=True)
        self.logger.info(f"Collected {len(new_df)} total records during backfill")
        
        # Merge with existing data
        if not existing_df.empty:
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=self.get_unique_columns(),
                keep='last'
            )
        else:
            combined_df = new_df
            
        # Sort and save
        combined_df = combined_df.sort_values('settlementdate')
        
        if self.save_data(combined_df):
            self.logger.info(f"Backfill complete. Total records: {len(combined_df)}")
            return True
        else:
            return False
            
    async def _download_daily_archive(self, date: datetime) -> Optional[pd.DataFrame]:
        """Download and process daily archive file"""
        date_str = date.strftime('%Y%m%d')
        filename = f"PUBLIC_DISPATCHIS_{date_str}.zip"
        url = f"{self.archive_url}{filename}"
        
        self.logger.debug(f"Downloading archive: {url}")
        
        # Download file
        content = await self.download_file(url)
        if content is None:
            return None
            
        # Process nested ZIP structure
        try:
            all_data = []
            
            with zipfile.ZipFile(BytesIO(content)) as daily_zip:
                # Get all 5-minute ZIP files
                nested_files = [f for f in daily_zip.namelist() if f.endswith('.zip')]
                
                if not nested_files:
                    self.logger.error(f"No nested ZIP files in {filename}")
                    return None
                    
                self.logger.debug(f"Processing {len(nested_files)} 5-minute files for {date.date()}")
                
                # Process each 5-minute file
                for nested_name in sorted(nested_files):
                    try:
                        with daily_zip.open(nested_name) as nested_file:
                            nested_content = nested_file.read()
                            
                        # Parse the 5-minute data
                        df = await self.parse_data(nested_content, nested_name)
                        if not df.empty:
                            # Filter to only include data for target date
                            df = df[df['settlementdate'].dt.date == date.date()]
                            if not df.empty:
                                all_data.append(df)
                                
                    except Exception as e:
                        self.logger.debug(f"Error processing {nested_name}: {e}")
                        continue
                        
            if not all_data:
                return None
                
            # Combine all 5-minute data
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(
                subset=['settlementdate', 'interconnectorid']
            )
            
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error processing archive {filename}: {e}")
            return None