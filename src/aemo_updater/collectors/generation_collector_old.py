#!/usr/bin/env python3
"""
AEMO Generation Data Collector
Downloads AEMO SCADA data and stores in efficient parquet format.
"""

import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime
import re
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any

from .base_collector import BaseCollector
from ..config import get_config, get_logger

logger = get_logger(__name__)


class GenerationCollector(BaseCollector):
    """
    AEMO Generation Data Collector
    Downloads and processes SCADA data from AEMO website
    """
    
    def __init__(self):
        """Initialize the collector with configuration"""
        super().__init__()
        self.base_url = "http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/"
        self.gen_output_file = self.config.gen_output_file
        self.gen_output_backup = self.gen_output_file.with_suffix('.pkl')  # For migration
        self.last_processed_file = None
        
        # Ensure the directory exists
        self.gen_output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Initialize or load existing DataFrame
        self.gen_output = self.load_or_create_dataframe()
        
    def load_or_create_dataframe(self):
        """Load existing gen_output DataFrame or create new one"""
        # Try parquet first
        if self.gen_output_file.exists():
            try:
                df = pd.read_parquet(self.gen_output_file)
                logger.info(f"Loaded existing gen_output.parquet with {len(df)} records")
                return df
            except Exception as e:
                logger.error(f"Error loading parquet file: {e}")
        
        # Fall back to pickle file if parquet doesn't exist
        if self.gen_output_backup.exists():
            try:
                df = pd.read_pickle(self.gen_output_backup)
                logger.info(f"Loaded existing gen_output.pkl with {len(df)} records")
                logger.info("Converting to parquet format...")
                
                # Save as parquet and remove pickle
                df.to_parquet(self.gen_output_file, compression='snappy', index=False)
                
                # Get file sizes for comparison
                pkl_size = self.gen_output_backup.stat().st_size / (1024*1024)
                parquet_size = self.gen_output_file.stat().st_size / (1024*1024)
                savings = ((pkl_size - parquet_size) / pkl_size) * 100
                
                logger.info(f"Migration complete: {pkl_size:.2f}MB -> {parquet_size:.2f}MB ({savings:.1f}% savings)")
                
                # Keep backup for safety
                backup_name = self.gen_output_backup.with_name(self.gen_output_backup.stem + '_backup.pkl')
                self.gen_output_backup.rename(backup_name)
                logger.info(f"Backup created: {backup_name}")
                
                return df
            except Exception as e:
                logger.error(f"Error loading existing pickle file: {e}")
                
        # Create new DataFrame with proper structure
        df = pd.DataFrame(columns=['settlementdate', 'duid', 'scadavalue'])
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        logger.info("Created new gen_output DataFrame")
        return df
    
    def get_latest_file_url(self):
        """Get the URL of the most recent SCADA file"""
        try:
            response = requests.get(self.base_url, timeout=30, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all ZIP file links
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'DISPATCHSCADA' in href:
                    zip_files.append(href)
            
            if not zip_files:
                logger.warning("No SCADA ZIP files found")
                return None
                
            # Sort to get the most recent (AEMO files have timestamp in filename)
            zip_files.sort(reverse=True)
            latest_file = zip_files[0]
            
            # Extract timestamp from filename for logging
            timestamp_match = re.search(r'(\d{12})', latest_file)
            if timestamp_match:
                timestamp = timestamp_match.group(1)
                logger.info(f"Latest file: {latest_file} (timestamp: {timestamp})")
            
            # Construct proper URL - href already contains the full path from root
            if latest_file.startswith('/'):
                file_url = "http://nemweb.com.au" + latest_file
            else:
                file_url = self.base_url + latest_file
                
            return file_url
            
        except Exception as e:
            logger.error(f"Error getting latest file URL: {e}")
            return None
    
    def download_and_parse_file(self, file_url):
        """Download and parse SCADA ZIP file"""
        try:
            response = requests.get(file_url, timeout=60, headers=self.headers)
            response.raise_for_status()
            
            # Extract ZIP file
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                # Get the first (and usually only) CSV file in the ZIP
                csv_files = [name for name in zip_file.namelist() if name.endswith('.CSV')]
                
                if not csv_files:
                    logger.error("No CSV files found in ZIP archive")
                    return None
                    
                csv_filename = csv_files[0]
                logger.info(f"Extracting and parsing: {csv_filename}")
                
                # Read CSV content from ZIP
                with zip_file.open(csv_filename) as csv_file:
                    csv_content = csv_file.read().decode('utf-8')
            
            # Parse CSV content
            lines = csv_content.strip().split('\n')
            
            # Find data lines (start with 'D')
            data_rows = []
            for line in lines:
                if line.startswith('D,DISPATCH,UNIT_SCADA'):
                    # Split CSV line and extract required fields
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
            
            if data_rows:
                df = pd.DataFrame(data_rows)
                df['settlementdate'] = pd.to_datetime(df['settlementdate'])
                logger.info(f"Parsed {len(df)} records from {file_url}")
                return df
            else:
                logger.warning("No valid data rows found in file")
                return None
                
        except Exception as e:
            logger.error(f"Error downloading/parsing file {file_url}: {e}")
            return None
    
    def is_new_data(self, new_df):
        """Check if the new data contains records not already in gen_output"""
        if self.gen_output.empty:
            return True
            
        if new_df is None or new_df.empty:
            return False
            
        # Get the latest timestamp in existing data
        latest_existing = self.gen_output['settlementdate'].max()
        
        # Check if new data has more recent timestamps
        latest_new = new_df['settlementdate'].max()
        
        is_new = latest_new > latest_existing
        logger.info(f"Latest existing: {latest_existing}, Latest new: {latest_new}, Is new: {is_new}")
        
        return is_new
    
    def add_new_data(self, new_df):
        """Add new data to gen_output DataFrame and save"""
        if new_df is None or new_df.empty:
            return
            
        try:
            # If gen_output is empty, just use the new data
            if self.gen_output.empty:
                self.gen_output = new_df.copy()
            else:
                # Filter out any duplicate records based on settlementdate and duid
                latest_existing = self.gen_output['settlementdate'].max()
                truly_new = new_df[new_df['settlementdate'] > latest_existing]
                
                if not truly_new.empty:
                    self.gen_output = pd.concat([self.gen_output, truly_new], ignore_index=True)
                    logger.info(f"Added {len(truly_new)} new records")
                else:
                    logger.info("No truly new records to add")
                    return
            
            # Sort by settlement date
            self.gen_output = self.gen_output.sort_values('settlementdate').reset_index(drop=True)
            
            # Save to parquet file with compression
            self.gen_output.to_parquet(self.gen_output_file, compression='snappy', index=False)
            
            # Get file size for logging
            file_size = self.gen_output_file.stat().st_size / (1024*1024)
            logger.info(f"Saved gen_output.parquet with {len(self.gen_output)} total records ({file_size:.2f}MB)")
            
        except Exception as e:
            logger.error(f"Error adding new data: {e}")
    
    def update(self) -> bool:
        """Run a single update cycle"""
        try:
            # Get latest file URL
            latest_url = self.get_latest_file_url()
            
            if latest_url and latest_url != self.last_processed_file:
                logger.info(f"Processing new file: {latest_url}")
                
                # Download and parse
                new_data = self.download_and_parse_file(latest_url)
                
                # Check if it's truly new data
                if self.is_new_data(new_data):
                    self.add_new_data(new_data)
                    self.last_processed_file = latest_url
                    logger.info(f"Updated data summary: {self.get_summary()}")
                    return True
                else:
                    logger.info("File exists but contains no new data")
                    return False
                    
            else:
                logger.info("No new files found")
                return False
                
        except Exception as e:
            logger.error(f"Error in update cycle: {e}")
            return False
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics of the current data"""
        if self.gen_output.empty:
            return {
                "status": "No data",
                "records": 0,
                "latest_update": None,
                "file_size_mb": 0
            }
        
        total_records = len(self.gen_output)
        latest_update = self.gen_output['settlementdate'].max()
        oldest_record = self.gen_output['settlementdate'].min()
        unique_duids = self.gen_output['duid'].nunique()
        file_size = self.gen_output_file.stat().st_size / (1024*1024) if self.gen_output_file.exists() else 0
        
        return {
            "status": "OK",
            "records": total_records,
            "latest_update": latest_update,
            "oldest_record": oldest_record,
            "unique_duids": unique_duids,
            "file_size_mb": round(file_size, 2),
            "date_range": f"{oldest_record} to {latest_update}"
        }
    
    def check_integrity(self) -> Dict[str, Any]:
        """Check data integrity"""
        if self.gen_output.empty:
            return {
                "status": "No data",
                "issues": ["No data found in parquet file"]
            }
        
        issues = []
        
        # Check for gaps in data
        df_sorted = self.gen_output.sort_values('settlementdate')
        time_diffs = df_sorted['settlementdate'].diff()
        expected_interval = pd.Timedelta(minutes=5)
        
        # Find gaps greater than expected interval
        gaps = time_diffs[time_diffs > expected_interval * 1.5]
        if len(gaps) > 0:
            issues.append(f"Found {len(gaps)} time gaps in data")
        
        # Check for duplicates
        duplicates = self.gen_output.duplicated(subset=['settlementdate', 'duid'])
        if duplicates.any():
            issues.append(f"Found {duplicates.sum()} duplicate records")
        
        return {
            "status": "OK" if not issues else "Issues found",
            "issues": issues,
            "records": len(self.gen_output),
            "date_range": f"{df_sorted['settlementdate'].min()} to {df_sorted['settlementdate'].max()}"
        }
    
    def backfill(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> bool:
        """
        Backfill historical data - not typically available for generation data
        as AEMO only keeps recent files in CURRENT directory
        """
        logger.warning("Generation data backfill not implemented - historical data not readily available")
        return False


def migrate_existing_pickle_to_parquet(config):
    """
    Standalone function to migrate existing pickle file to parquet
    """
    pkl_file = Path(config.gen_output_file).with_suffix('.pkl')
    parquet_file = Path(config.gen_output_file)
    
    if not pkl_file.exists():
        logger.info(f"No pickle file found at: {pkl_file}")
        return False
    
    if parquet_file.exists():
        logger.info(f"Parquet file already exists at: {parquet_file}")
        return False
    
    try:
        logger.info(f"Loading pickle file: {pkl_file}")
        df = pd.read_pickle(pkl_file)
        
        pkl_size = pkl_file.stat().st_size / (1024*1024)
        logger.info(f"Loaded {len(df)} records ({pkl_size:.2f}MB)")
        
        logger.info(f"Saving as parquet: {parquet_file}")
        parquet_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(parquet_file, compression='snappy', index=False)
        
        parquet_size = parquet_file.stat().st_size / (1024*1024)
        savings = ((pkl_size - parquet_size) / pkl_size) * 100
        
        logger.info(f"Migration complete: {pkl_size:.2f}MB -> {parquet_size:.2f}MB ({savings:.1f}% savings)")
        
        # Create backup
        backup_file = pkl_file.with_name(pkl_file.stem + '_backup.pkl')
        pkl_file.rename(backup_file)
        logger.info(f"Original file backed up as: {backup_file}")
        
        return True
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return False


def main():
    """Main function to run the generation data collector"""
    logger.info("AEMO Generation Data Collector starting...")
    
    # Create collector instance
    collector = GenerationCollector()
    
    try:
        # Run single update
        collector.update()
    except KeyboardInterrupt:
        logger.info("Collector stopped by user")
    except Exception as e:
        logger.error(f"Collector error: {e}")


if __name__ == "__main__":
    main()