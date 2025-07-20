#!/usr/bin/env python3
"""
AEMO Generation Data Collector - Simple Implementation
Downloads AEMO SCADA data and stores in efficient parquet format.
Adapted from working generation update code
"""

import os
import re
import csv
import zipfile
import pickle
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
from pathlib import Path
from typing import Optional, Dict, Any, List
import requests
from bs4 import BeautifulSoup

from ..config import get_config, get_logger, HTTP_HEADERS


class GenerationCollector:
    """
    AEMO Generation Data Collector
    Downloads and processes SCADA data from NEMWEB
    """
    
    def __init__(self):
        """Initialize the collector with configuration"""
        self.config = get_config()
        self.logger = get_logger(self.__class__.__name__)
        self.name = "generation"
        self.parquet_file = self.config.gen_output_file
        
        # URL for SCADA data
        self.base_url = "http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/"
        
        # HTTP session with required headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
        
        # Ensure the directory exists
        self.parquet_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Load gen_info for DUID mappings
        self.gen_info = self.load_gen_info()
        
        # Track unknown DUIDs
        self.unknown_duids = set()
        
        self.logger.info(f"Generation collector initialized - output: {self.parquet_file}")
        
    def load_gen_info(self) -> Optional[pd.DataFrame]:
        """Load gen_info.pkl for DUID to fuel/region mappings"""
        try:
            # Look for gen_info.pkl in various locations
            base_path = Path.home() / 'Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot'
            possible_paths = [
                self.parquet_file.parent / 'gen_info.pkl',
                base_path / 'genhist/gen_info.pkl',
                base_path / 'aemo-energy-dashboard/data/gen_info.pkl'
            ]
            
            for path in possible_paths:
                if path.exists():
                    self.logger.info(f"Loading gen_info from: {path}")
                    gen_info = pd.read_pickle(path)
                    self.logger.info(f"Loaded {len(gen_info)} DUID mappings")
                    return gen_info
            
            self.logger.warning("gen_info.pkl not found - DUID mapping will not be available")
            return None
            
        except Exception as e:
            self.logger.error(f"Error loading gen_info: {e}")
            return None
    
    def get_latest_urls(self) -> List[str]:
        """Get URLs for latest SCADA files"""
        try:
            # Get the main page
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            # Parse HTML to find files
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all ZIP file links
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'DISPATCHSCADA' in href:
                    zip_files.append(href)
            
            if not zip_files:
                self.logger.warning("No SCADA ZIP files found")
                return []
            
            # Sort to get the most recent
            zip_files.sort(reverse=True)
            latest_file = zip_files[0]
            
            # Construct proper URL
            if latest_file.startswith('/'):
                file_url = "http://nemweb.com.au" + latest_file
            else:
                file_url = self.base_url + latest_file
            
            self.logger.info(f"Found latest SCADA file: {latest_file}")
            return [file_url]
            
        except Exception as e:
            self.logger.error(f"Error getting SCADA URLs: {e}")
            return []
    
    def download_file(self, url: str) -> Optional[bytes]:
        """Download a file from URL"""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            return response.content
        except Exception as e:
            self.logger.error(f"Error downloading {url}: {e}")
            return None
    
    def parse_data(self, content: bytes) -> pd.DataFrame:
        """
        Parse AEMO SCADA CSV data
        Returns DataFrame with columns: settlementdate, duid, scadavalue
        """
        try:
            # Extract CSV from ZIP file
            with zipfile.ZipFile(BytesIO(content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    self.logger.error("No CSV file found in ZIP")
                    return pd.DataFrame()
                
                csv_content = zf.read(csv_files[0]).decode('utf-8')
            
            # Parse CSV content
            lines = csv_content.strip().split('\n')
            data_rows = []
            
            for line in lines:
                # Look for UNIT_SCADA data rows
                if line.startswith('D,DISPATCH,UNIT_SCADA'):
                    fields = line.split(',')
                    if len(fields) >= 7:  # Ensure we have all required fields
                        settlementdate = fields[4].strip('"')
                        duid = fields[5].strip('"')
                        scadavalue = fields[6].strip('"')
                        
                        try:
                            scadavalue = float(scadavalue)
                        except ValueError:
                            continue  # Skip invalid numeric values
                        
                        data_rows.append({
                            'settlementdate': settlementdate,
                            'duid': duid,
                            'scadavalue': scadavalue
                        })
                        
                        # Track unknown DUIDs
                        if self.gen_info is not None and duid not in self.gen_info['DUID'].values:
                            self.unknown_duids.add(duid)
            
            if not data_rows:
                self.logger.warning("No valid UNIT_SCADA records extracted")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data_rows)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            self.logger.info(f"Extracted {len(df)} SCADA records")
            
            # Log sample of data
            unique_duids = df['duid'].nunique()
            settlement_time = df['settlementdate'].iloc[0]
            self.logger.info(f"  Settlement time: {settlement_time}")
            self.logger.info(f"  Unique DUIDs: {unique_duids}")
            
            # Log unknown DUIDs if any
            if self.unknown_duids:
                self.logger.warning(f"Found {len(self.unknown_duids)} unknown DUIDs")
                for duid in list(self.unknown_duids)[:5]:  # Show first 5
                    self.logger.warning(f"  Unknown DUID: {duid}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error parsing SCADA data: {e}")
            return pd.DataFrame()
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate generation data"""
        if df.empty:
            return False
        
        # Check required columns
        required_cols = ['settlementdate', 'duid', 'scadavalue']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns. Found: {df.columns.tolist()}")
            return False
        
        # Validate data types and ranges
        try:
            # scadavalue should be numeric
            if not pd.api.types.is_numeric_dtype(df['scadavalue']):
                self.logger.error("scadavalue column is not numeric")
                return False
            
            # Check for reasonable generation ranges (MW)
            if df['scadavalue'].min() < -100:  # Some negative values are ok (auxiliary load)
                self.logger.warning(f"Very negative generation: {df['scadavalue'].min():.2f} MW")
            
            if df['scadavalue'].max() > 5000:  # Largest units are ~750MW
                self.logger.warning(f"Very high generation: {df['scadavalue'].max():.2f} MW")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False
    
    def load_historical_data(self) -> pd.DataFrame:
        """Load the historical generation data from parquet file"""
        try:
            if self.parquet_file.exists():
                df = pd.read_parquet(self.parquet_file)
                self.logger.info(f"Loaded historical data: {len(df)} records, latest: {df['settlementdate'].max()}")
                return df
            else:
                # Check for legacy pickle file
                pkl_file = self.parquet_file.with_suffix('.pkl')
                if pkl_file.exists():
                    self.logger.info(f"Found legacy pickle file: {pkl_file}")
                    df = pd.read_pickle(pkl_file)
                    
                    # Migrate to parquet
                    self.logger.info("Migrating to parquet format...")
                    df.to_parquet(self.parquet_file, compression='snappy', index=False)
                    
                    # Compare sizes
                    pkl_size = pkl_file.stat().st_size / (1024*1024)
                    parquet_size = self.parquet_file.stat().st_size / (1024*1024)
                    savings = ((pkl_size - parquet_size) / pkl_size) * 100
                    
                    self.logger.info(f"Migration complete: {pkl_size:.2f}MB -> {parquet_size:.2f}MB ({savings:.1f}% savings)")
                    
                    # Backup pickle file
                    backup_name = pkl_file.with_name(pkl_file.stem + '_backup.pkl')
                    pkl_file.rename(backup_name)
                    self.logger.info(f"Backup created: {backup_name}")
                    
                    return df
                else:
                    self.logger.info("No historical data file found, starting fresh")
                    return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
        except Exception as e:
            self.logger.error(f"Error loading historical data: {e}")
            return pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
    
    def save_data(self, df: pd.DataFrame):
        """Save the data to parquet file"""
        try:
            df.to_parquet(self.parquet_file, compression='snappy', index=False)
            file_size = self.parquet_file.stat().st_size / (1024*1024)
            self.logger.info(f"Saved {len(df)} records to {self.parquet_file} ({file_size:.2f} MB)")
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
    
    def update_data(self) -> bool:
        """Main update method - download and process new data"""
        self.logger.info("Checking for new generation data...")
        
        # Load existing data
        historical_df = self.load_historical_data()
        latest_timestamp = historical_df['settlementdate'].max() if not historical_df.empty else pd.Timestamp.min
        
        # Get latest file URL
        urls = self.get_latest_urls()
        if not urls:
            self.logger.warning("No URLs found")
            return False
        
        # Download and parse
        content = self.download_file(urls[0])
        if not content:
            return False
        
        new_df = self.parse_data(content)
        if new_df.empty:
            return False
        
        # Validate
        if not self.validate_data(new_df):
            return False
        
        # Check if we have new data
        new_timestamp = new_df['settlementdate'].max()
        if new_timestamp <= latest_timestamp:
            self.logger.info("No new generation data - data not newer than existing")
            return False
        
        # Filter for only newer records
        newer_records = new_df[new_df['settlementdate'] > latest_timestamp]
        if newer_records.empty:
            self.logger.info("No new records found")
            return False
        
        # Log new data summary
        self.logger.info(f"New generation data found for {newer_records['settlementdate'].iloc[0]}:")
        self.logger.info(f"  Records: {len(newer_records)}")
        self.logger.info(f"  DUIDs: {newer_records['duid'].nunique()}")
        self.logger.info(f"  Total MW: {newer_records['scadavalue'].sum():.1f}")
        
        # Combine and sort
        updated_df = pd.concat([historical_df, newer_records], ignore_index=True)
        updated_df = updated_df.sort_values('settlementdate').reset_index(drop=True)
        
        # Save
        self.save_data(updated_df)
        
        self.logger.info(f"Added {len(newer_records)} new records. Total: {len(updated_df)}")
        return True
    
    def update(self) -> bool:
        """Alias for update_data to match expected interface"""
        return self.update_data()
    
    def get_summary(self) -> Dict[str, Any]:
        """Get collector summary for status reporting"""
        try:
            df = self.load_historical_data()
            latest = df['settlementdate'].max() if len(df) > 0 else None
            return {
                'records': len(df),
                'latest': latest.strftime('%Y-%m-%d %H:%M') if latest else 'No data',
                'duids': df['duid'].nunique() if len(df) > 0 else 0
            }
        except Exception as e:
            return {'error': str(e)}
    
    def check_integrity(self) -> Dict[str, Any]:
        """Check data integrity"""
        df = self.load_historical_data()
        
        if df.empty:
            return {
                "status": "No data",
                "issues": ["No data found in parquet file"]
            }
        
        issues = []
        
        # Check for gaps
        df_sorted = df.sort_values('settlementdate')
        
        # Group by timestamp to check completeness
        records_per_time = df_sorted.groupby('settlementdate').size()
        
        # Check for timestamps with too few records (should have ~300+ DUIDs)
        low_count = records_per_time[records_per_time < 200]
        if len(low_count) > 0:
            issues.append(f"Found {len(low_count)} timestamps with < 200 DUIDs")
        
        # Check for time gaps
        unique_times = df_sorted['settlementdate'].unique()
        time_series = pd.Series(unique_times).sort_values()
        time_diffs = time_series.diff()
        expected_interval = pd.Timedelta(minutes=5)
        
        gaps = time_diffs[time_diffs > expected_interval * 1.5]
        if len(gaps) > 0:
            issues.append(f"Found {len(gaps)} time gaps in data")
        
        # Check for duplicates
        duplicates = df.duplicated(subset=['settlementdate', 'duid'])
        if duplicates.any():
            issues.append(f"Found {duplicates.sum()} duplicate records")
        
        return {
            "status": "OK" if not issues else "Issues found",
            "issues": issues,
            "records": len(df),
            "unique_duids": df['duid'].nunique(),
            "date_range": f"{df_sorted['settlementdate'].min()} to {df_sorted['settlementdate'].max()}"
        }