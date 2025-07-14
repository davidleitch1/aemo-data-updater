#!/usr/bin/env python3
"""
AEMO Spot Price Collector - Simple Implementation
Downloads AEMO dispatch data and updates historical spot price file.
Adapted directly from working update_spot.py
"""

import os
import re
import csv
import zipfile
import pandas as pd
from datetime import datetime
from io import StringIO, BytesIO
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
import requests

from ..config import get_config, get_logger, HTTP_HEADERS

# Import Twilio price alerts
try:
    from .twilio_price_alerts import check_price_alerts
    TWILIO_ALERTS_AVAILABLE = True
except ImportError as e:
    TWILIO_ALERTS_AVAILABLE = False


class PriceCollector:
    """
    AEMO Spot Price Collector
    Downloads and processes dispatch price data from NEMWEB
    """
    
    def __init__(self):
        """Initialize the collector with configuration"""
        self.config = get_config()
        self.logger = get_logger(self.__class__.__name__)
        self.name = "price"
        self.parquet_file = self.config.spot_hist_file
        
        # Use URL from working update_spot.py (CLAUDE_UPDATER.md has wrong URL)
        self.base_url = "http://nemweb.com.au/Reports/Current/Dispatch_Reports/"
        
        # HTTP session with required headers
        self.session = requests.Session()
        # Use browser User-Agent to avoid 406 errors
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        })
        
        # Ensure the directory exists
        self.parquet_file.parent.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"Price collector initialized - output: {self.parquet_file}")
        
    def get_latest_urls(self) -> List[str]:
        """Get URLs for latest price files"""
        try:
            # Get the main page to find the latest file
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            # Look for PUBLIC_DISPATCH files - pattern from update_spot.py
            # Updated to match the HREF format in the HTML
            zip_pattern = r'HREF="[^"]*/?(PUBLIC_DISPATCH_\d{12}_\d{14}_LEGACY\.zip)"'
            matches = re.findall(zip_pattern, response.text)
            
            if not matches:
                self.logger.warning("No dispatch files found on NEMWEB")
                return []
            
            # Get the latest file
            latest_file = sorted(matches)[-1]
            file_url = self.base_url + latest_file
            
            self.logger.info(f"Found latest price file: {latest_file}")
            return [file_url]
            
        except Exception as e:
            self.logger.error(f"Error getting price URLs: {e}")
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
        Parse AEMO dispatch CSV data and extract regional price information.
        Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
        """
        try:
            # First extract CSV from ZIP file
            with zipfile.ZipFile(BytesIO(content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    self.logger.error("No CSV file found in ZIP")
                    return pd.DataFrame()
                
                csv_content = zf.read(csv_files[0]).decode('utf-8')
            
            # Parse CSV - logic from update_spot.py
            lines = csv_content.strip().split('\n')
            price_records = []
            
            for line in lines:
                if not line.strip():
                    continue
                
                # Split the line by commas, handling quoted fields properly
                reader = csv.reader(StringIO(line))
                try:
                    fields = next(reader)
                except:
                    continue
                
                # Look for DREGION data rows (marked with 'D' in first column)
                if len(fields) >= 9 and fields[0] == 'D' and fields[1] == 'DREGION':
                    try:
                        # Field positions from update_spot.py:
                        # 0: D, 1: DREGION, 2: empty, 3: 3, 4: settlement_date, 
                        # 5: runno, 6: regionid, 7: intervention, 8: rrp
                        settlement_date = pd.to_datetime(fields[4])
                        region = fields[6]
                        rrp = float(fields[8])
                        
                        price_records.append({
                            'SETTLEMENTDATE': settlement_date,
                            'REGIONID': region,
                            'RRP': rrp
                        })
                    except (ValueError, TypeError, IndexError) as e:
                        self.logger.debug(f"Error parsing row: {e}")
                        continue
            
            if not price_records:
                self.logger.warning("No valid price records extracted")
                return pd.DataFrame()
            
            # Convert to DataFrame and set SETTLEMENTDATE as index (as in update_spot.py)
            temp_df = pd.DataFrame(price_records)
            result_df = temp_df.set_index('SETTLEMENTDATE')
            
            self.logger.info(f"Extracted {len(result_df)} price records")
            
            # Log sample of data
            for idx, (settlement_date, row) in enumerate(result_df.iterrows()):
                if idx < 3:  # Just show first 3
                    self.logger.info(f"  Parsed: {row['REGIONID']} = ${row['RRP']:.2f}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error parsing price data: {e}")
            return pd.DataFrame()
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate price data"""
        if df.empty:
            return False
        
        # Check required columns
        required_cols = ['REGIONID', 'RRP']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns. Found: {df.columns.tolist()}")
            return False
        
        # Validate data types and ranges
        try:
            # RRP should be numeric
            if not pd.api.types.is_numeric_dtype(df['RRP']):
                self.logger.error("RRP column is not numeric")
                return False
            
            # Check for reasonable price ranges (can be negative but not too extreme)
            if df['RRP'].min() < -1000 or df['RRP'].max() > 20000:
                self.logger.warning(f"Unusual price range: ${df['RRP'].min():.2f} to ${df['RRP'].max():.2f}")
            
            # Check regions are valid
            valid_regions = {'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}
            invalid_regions = set(df['REGIONID'].unique()) - valid_regions
            if invalid_regions:
                self.logger.warning(f"Unknown regions: {invalid_regions}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False
    
    def load_historical_data(self) -> pd.DataFrame:
        """Load the historical spot price data from parquet file"""
        try:
            if self.parquet_file.exists():
                df = pd.read_parquet(self.parquet_file)
                
                # Verify index is SETTLEMENTDATE
                if df.index.name != 'SETTLEMENTDATE':
                    if 'SETTLEMENTDATE' in df.columns:
                        df = df.set_index('SETTLEMENTDATE')
                        self.logger.info("Converted SETTLEMENTDATE column to index")
                
                self.logger.info(f"Loaded historical data: {len(df)} records, latest: {df.index.max()}")
                return df
            else:
                self.logger.info("No historical data file found, starting fresh")
                return pd.DataFrame(columns=['REGIONID', 'RRP'], 
                                  index=pd.DatetimeIndex([], name='SETTLEMENTDATE'))
        except Exception as e:
            self.logger.error(f"Error loading historical data: {e}")
            return pd.DataFrame(columns=['REGIONID', 'RRP'], 
                              index=pd.DatetimeIndex([], name='SETTLEMENTDATE'))
    
    def save_data(self, df: pd.DataFrame):
        """Save the data to parquet file"""
        try:
            df.to_parquet(self.parquet_file, compression='snappy', index=True)
            self.logger.info(f"Saved {len(df)} records to {self.parquet_file}")
        except Exception as e:
            self.logger.error(f"Error saving data: {e}")
    
    def update_data(self) -> bool:
        """Main update method - download and process new data"""
        self.logger.info("Checking for new spot price data...")
        
        # Load existing data
        historical_df = self.load_historical_data()
        latest_timestamp = historical_df.index.max() if not historical_df.empty else pd.Timestamp.min
        
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
        
        # Check if we have new data
        new_timestamp = new_df.index.max()
        if new_timestamp <= latest_timestamp:
            self.logger.info("No new prices - data not newer than existing")
            return False
        
        # Filter for only newer records
        newer_records = new_df[new_df.index > latest_timestamp]
        if newer_records.empty:
            self.logger.info("No new records found")
            return False
        
        # CHECK FOR PRICE ALERTS before logging (exactly as in update_spot.py)
        if TWILIO_ALERTS_AVAILABLE:
            try:
                check_price_alerts(newer_records)
            except Exception as e:
                self.logger.error(f"Error checking price alerts: {e}")
        else:
            self.logger.debug("Twilio price alerts not available")
        
        # Log new prices
        self.logger.info(f"New prices found for {newer_records.index[0]}:")
        for idx, (_, row) in enumerate(newer_records.iterrows()):
            if idx < 5:  # Show first 5
                self.logger.info(f"  {row['REGIONID']}: ${row['RRP']:.2f}")
        
        # Combine and deduplicate
        updated_df = pd.concat([historical_df, newer_records])
        updated_df = updated_df.reset_index()
        updated_df = updated_df.drop_duplicates(subset=['SETTLEMENTDATE', 'REGIONID'], keep='last')
        updated_df = updated_df.set_index('SETTLEMENTDATE').sort_index()
        
        # Save
        self.save_data(updated_df)
        
        self.logger.info(f"Added {len(newer_records)} new records. Total: {len(updated_df)}")
        return True
    
    def update(self) -> bool:
        """Alias for update_data to match expected interface"""
        return self.update_data()
    
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
        df_sorted = df.reset_index().sort_values('SETTLEMENTDATE')
        df_sorted['time_diff'] = df_sorted.groupby('REGIONID')['SETTLEMENTDATE'].diff()
        expected_interval = pd.Timedelta(minutes=5)
        
        gaps = df_sorted[df_sorted['time_diff'] > expected_interval * 1.5]
        if len(gaps) > 0:
            issues.append(f"Found {len(gaps)} time gaps in data")
        
        # Check regions
        expected_regions = {'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}
        actual_regions = set(df['REGIONID'].unique())
        missing_regions = expected_regions - actual_regions
        if missing_regions:
            issues.append(f"Missing regions: {missing_regions}")
        
        return {
            "status": "OK" if not issues else "Issues found",
            "issues": issues,
            "records": len(df),
            "date_range": f"{df.index.min()} to {df.index.max()}"
        }