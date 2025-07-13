#!/usr/bin/env python3
"""
AEMO Spot Price Collector
Downloads AEMO dispatch data and updates historical spot price file.
"""

import os
import time
import requests
import zipfile
import pandas as pd
from datetime import datetime
import tempfile
from io import StringIO
import csv
import re
from pathlib import Path
from typing import Optional, Dict, Any, Tuple

from .base_collector import BaseCollector
from ..config import get_config, get_logger

logger = get_logger(__name__)


class PriceCollector(BaseCollector):
    """
    AEMO Spot Price Collector
    Downloads and processes dispatch price data from AEMO website
    """
    
    def __init__(self):
        """Initialize the collector with configuration"""
        super().__init__()
        self.base_url = "http://nemweb.com.au/Reports/Current/Dispatch_Reports/"
        self.parquet_file_path = self.config.spot_hist_file
        
        # Ensure the directory exists
        self.parquet_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data
        self.historical_df = self.load_historical_data()
        
    def get_latest_dispatch_file(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Download the latest dispatch file from AEMO website.
        Returns the CSV content as a string and filename, or None if failed.
        """
        try:
            # Get the main page to find the latest file
            response = requests.get(self.base_url, timeout=30, headers=self.headers)
            response.raise_for_status()
            
            # Look for PUBLIC_DISPATCH files - they follow pattern: PUBLIC_DISPATCH_YYYYMMDDHHMM_*.zip
            zip_pattern = r'PUBLIC_DISPATCH_\d{12}_\d{14}_LEGACY\.zip'
            matches = re.findall(zip_pattern, response.text)
            
            if not matches:
                logger.error("No dispatch files found on AEMO website")
                return None, None
                
            # Get the latest file (they should be in chronological order)
            latest_file = sorted(matches)[-1]
            file_url = self.base_url + latest_file
            
            logger.info(f"Downloading: {latest_file}")
            
            # Download the zip file
            zip_response = requests.get(file_url, timeout=60, headers=self.headers)
            zip_response.raise_for_status()
            
            # Create a temporary file for the zip
            with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip_file:
                temp_zip_path = temp_zip_file.name
                temp_zip_file.write(zip_response.content)
                temp_zip_file.flush()
                
                try:
                    # Extract the CSV from the zip file
                    with zipfile.ZipFile(temp_zip_path, 'r') as zip_file:
                        # Find the CSV file inside (should be similar name but .CSV)
                        csv_name = latest_file.replace('.zip', '.CSV')
                        if csv_name not in zip_file.namelist():
                            # Try without _LEGACY suffix
                            csv_name = latest_file.replace('_LEGACY.zip', '.CSV')
                        
                        if csv_name in zip_file.namelist():
                            csv_content = zip_file.read(csv_name).decode('utf-8')
                            return csv_content, latest_file
                        else:
                            logger.error(f"CSV file not found in zip. Available files: {zip_file.namelist()}")
                            return None, None
                            
                finally:
                    # Clean up: delete the temporary zip file
                    try:
                        os.unlink(temp_zip_path)
                        logger.debug(f"Deleted temporary file: {temp_zip_path}")
                    except OSError as e:
                        logger.warning(f"Could not delete temporary file {temp_zip_path}: {e}")
                        
        except Exception as e:
            logger.error(f"Error downloading dispatch file: {e}")
            return None, None

    def parse_dispatch_data(self, csv_content: str) -> pd.DataFrame:
        """
        Parse AEMO dispatch CSV data and extract regional price information.
        Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
        """
        try:
            # AEMO CSV files have variable number of columns per row
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
                        # Field positions:
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
                        logger.debug(f"Error parsing row: {e}")
                        continue
            
            if not price_records:
                logger.warning("No valid price records extracted")
                return pd.DataFrame()
                
            # Convert to DataFrame and set SETTLEMENTDATE as index
            temp_df = pd.DataFrame(price_records)
            result_df = temp_df.set_index('SETTLEMENTDATE')
            
            settlement_time = result_df.index[0]
            logger.info(f"Extracted {len(result_df)} price records for settlement time: {settlement_time}")
            
            # Show the extracted data for verification
            for settlement_date, row in result_df.iterrows():
                logger.info(f"  Parsed: {row['REGIONID']} = ${row['RRP']:.5f}")
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error parsing dispatch data: {e}")
            return pd.DataFrame()

    def load_historical_data(self) -> pd.DataFrame:
        """
        Load the historical spot price data from parquet file.
        Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
        """
        try:
            if self.parquet_file_path.exists():
                # Load parquet file
                df = pd.read_parquet(self.parquet_file_path)
                
                # Debug: check the structure
                logger.info(f"Existing parquet file columns: {list(df.columns)}")
                logger.info(f"Index name: {df.index.name}")
                
                # Verify index is SETTLEMENTDATE
                if df.index.name != 'SETTLEMENTDATE':
                    if 'SETTLEMENTDATE' in df.columns:
                        df = df.set_index('SETTLEMENTDATE')
                        logger.info("Converted SETTLEMENTDATE column to index")
                    else:
                        logger.error("Cannot find SETTLEMENTDATE in columns or index")
                        return pd.DataFrame(columns=['REGIONID', 'RRP'])
                
                # Ensure we have the expected columns
                expected_cols = ['REGIONID', 'RRP']
                missing_cols = [col for col in expected_cols if col not in df.columns]
                
                if missing_cols:
                    logger.error(f"Missing expected columns: {missing_cols}")
                    return pd.DataFrame(columns=expected_cols)
                
                logger.info(f"Loaded historical data: {len(df)} records, latest: {df.index.max()}")
                return df
            else:
                logger.info("No historical data file found, starting fresh")
                # Create empty DataFrame with SETTLEMENTDATE as index
                return pd.DataFrame(columns=['REGIONID', 'RRP'], 
                                  index=pd.DatetimeIndex([], name='SETTLEMENTDATE'))
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            return pd.DataFrame(columns=['REGIONID', 'RRP'], 
                              index=pd.DatetimeIndex([], name='SETTLEMENTDATE'))

    def save_historical_data(self, df: pd.DataFrame):
        """Save the historical data to parquet file with compression."""
        try:
            # Save with snappy compression for better performance
            df.to_parquet(self.parquet_file_path, compression='snappy', index=True)
            logger.info(f"Saved {len(df)} records to {self.parquet_file_path}")
            
        except Exception as e:
            logger.error(f"Error saving historical data: {e}")

    def get_latest_timestamp(self, df: pd.DataFrame) -> pd.Timestamp:
        """Get the latest settlement date from the historical data."""
        if df.empty:
            return pd.Timestamp.min
        return df.index.max()

    def update(self) -> bool:
        """Check for new data and update the historical file."""
        logger.info("Checking for new spot price data...")
        
        # Get current latest timestamp
        latest_timestamp = self.get_latest_timestamp(self.historical_df)
        
        # Download latest dispatch file
        csv_content, filename = self.get_latest_dispatch_file()
        if csv_content is None:
            logger.warning("Failed to download latest dispatch file")
            return False
        
        # Parse the new data
        new_df = self.parse_dispatch_data(csv_content)
        
        if new_df.empty:
            logger.warning("No new data to process")
            return False
        
        # Check if new data is more recent than our latest
        new_timestamp = new_df.index.max()
        
        if new_timestamp <= latest_timestamp:
            logger.info("No new prices - latest data is not newer than existing records")
            return False
        
        # Filter for only newer records
        newer_records = new_df[new_df.index > latest_timestamp]
        
        if newer_records.empty:
            logger.info("No new prices - no records newer than existing data")
            return False
        
        # Log the new prices found
        settlement_time = newer_records.index[0]
        logger.info(f"New prices found for {settlement_time}:")
        for settlement_date, row in newer_records.iterrows():
            logger.info(f"  {row['REGIONID']}: ${row['RRP']:.2f}")
        
        # Combine historical data with new records
        updated_df = pd.concat([self.historical_df, newer_records])
        
        # Sort by index and remove duplicate region-timestamp combinations
        updated_df = updated_df.reset_index()
        updated_df = updated_df.drop_duplicates(subset=['SETTLEMENTDATE', 'REGIONID'], keep='last')
        updated_df = updated_df.set_index('SETTLEMENTDATE').sort_index()
        
        # Save updated data
        self.save_historical_data(updated_df)
        self.historical_df = updated_df
        
        logger.info(f"Added {len(newer_records)} new records. Total records: {len(updated_df)}")
        
        return True
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of the current data"""
        if self.historical_df.empty:
            return {
                "status": "No data",
                "records": 0,
                "latest_update": None,
                "file_size_mb": 0
            }
        
        latest_update = self.historical_df.index.max()
        oldest_record = self.historical_df.index.min()
        unique_regions = self.historical_df['REGIONID'].nunique()
        file_size = self.parquet_file_path.stat().st_size / (1024*1024) if self.parquet_file_path.exists() else 0
        
        # Get latest prices
        latest_prices = self.historical_df[self.historical_df.index == latest_update]
        price_summary = {}
        for _, row in latest_prices.iterrows():
            price_summary[row['REGIONID']] = f"${row['RRP']:.2f}"
        
        return {
            "status": "OK",
            "records": len(self.historical_df),
            "latest_update": latest_update,
            "oldest_record": oldest_record,
            "unique_regions": unique_regions,
            "file_size_mb": round(file_size, 2),
            "date_range": f"{oldest_record} to {latest_update}",
            "latest_prices": price_summary
        }
    
    def check_integrity(self) -> Dict[str, Any]:
        """Check data integrity"""
        if self.historical_df.empty:
            return {
                "status": "No data",
                "issues": ["No data found in parquet file"]
            }
        
        issues = []
        
        # Check for gaps in data
        df_sorted = self.historical_df.reset_index().sort_values('SETTLEMENTDATE')
        df_sorted['time_diff'] = df_sorted.groupby('REGIONID')['SETTLEMENTDATE'].diff()
        expected_interval = pd.Timedelta(minutes=5)
        
        # Find gaps greater than expected interval
        gaps = df_sorted[df_sorted['time_diff'] > expected_interval * 1.5]
        if len(gaps) > 0:
            issues.append(f"Found {len(gaps)} time gaps in data")
        
        # Check for duplicates
        duplicates = self.historical_df.reset_index().duplicated(subset=['SETTLEMENTDATE', 'REGIONID'])
        if duplicates.any():
            issues.append(f"Found {duplicates.sum()} duplicate records")
        
        # Check for missing regions
        expected_regions = {'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}
        actual_regions = set(self.historical_df['REGIONID'].unique())
        missing_regions = expected_regions - actual_regions
        if missing_regions:
            issues.append(f"Missing regions: {missing_regions}")
        
        return {
            "status": "OK" if not issues else "Issues found",
            "issues": issues,
            "records": len(self.historical_df),
            "date_range": f"{self.historical_df.index.min()} to {self.historical_df.index.max()}"
        }
    
    def backfill(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> bool:
        """
        Backfill historical price data - not typically available for price data
        as AEMO only keeps recent files in CURRENT directory
        """
        logger.warning("Price data backfill not implemented - historical dispatch data not readily available")
        return False


def main():
    """Main function to run the price collector"""
    logger.info("AEMO Spot Price Collector starting...")
    
    # Create collector instance
    collector = PriceCollector()
    
    try:
        # Run single update
        collector.update()
    except KeyboardInterrupt:
        logger.info("Collector stopped by user")
    except Exception as e:
        logger.error(f"Collector error: {e}")


if __name__ == "__main__":
    main()