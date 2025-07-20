#!/usr/bin/env python3
"""
AEMO 30-Minute Trading Interval Data Collector
Collects trading prices, regional summaries, interconnector flows, and generation data
"""

import os
import requests
import zipfile
import io
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import logging

from ..config import get_logger

logger = get_logger(__name__)


class TradingCollector:
    """Collector for 30-minute trading interval data"""
    
    BASE_URL = "https://nemweb.com.au/Reports/Archive/TradingIS/"
    CURRENT_URL = "https://nemweb.com.au/Reports/Current/TradingIS_Reports/"
    
    # Table mappings to output files
    TABLES = {
        "TRADINGPRICE": "trading_price.parquet",
        "TRADINGREGIONSUM": "trading_regionsum.parquet", 
        "TRADINGINTERCONNECT": "trading_interconnect.parquet",
        "TRADINGGENUNITS": "trading_genunits.parquet",
    }
    
    def __init__(self, config=None):
        """Initialize the trading collector"""
        
        # Set up output directory using iCloud path
        self.base_dir = Path.home() / "Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot"
        self.output_dir = self.base_dir / "trading_data"
        self.output_dir.mkdir(exist_ok=True)
        
        # Headers required for NEMWEB
        self.headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        
        logger.info(f"Trading collector initialized. Output directory: {self.output_dir}")
    
    async def collect_once(self) -> bool:
        """Collect latest trading data (async wrapper for compatibility)"""
        return self.fetch_latest_trading_data()
    
    def update(self) -> bool:
        """Update trading data (sync method for service compatibility)"""
        return self.fetch_latest_trading_data()
    
    def download_and_extract_zip(self, url: str) -> Dict[str, pd.DataFrame]:
        """Download ZIP file and extract CSV contents"""
        try:
            logger.info(f"Downloading: {url}")
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            dataframes = {}
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for name in z.namelist():
                    if name.endswith(".CSV"):
                        logger.debug(f"Extracting: {name}")
                        try:
                            df = pd.read_csv(z.open(name))
                            dataframes[name] = df
                        except Exception as e:
                            logger.error(f"Failed to parse CSV {name}: {e}")
            
            logger.info(f"Successfully extracted {len(dataframes)} CSV files from {url}")
            return dataframes
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to download {url}: {e}")
            return {}
        except Exception as e:
            logger.error(f"Failed to process {url}: {e}")
            return {}
    
    def process_csv_data(self, csvs: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Process CSV data and append to parquet files"""
        stats = {}
        
        for csv_name, df in csvs.items():
            # Extract table type from CSV name
            table_type = None
            for table_key in self.TABLES.keys():
                if csv_name.upper().startswith(table_key):
                    table_type = table_key
                    break
            
            if table_type:
                target_file = self.output_dir / self.TABLES[table_type]
                
                try:
                    # Convert SETTLEMENTDATE to datetime if present
                    if 'SETTLEMENTDATE' in df.columns:
                        df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
                    
                    # Handle append or create
                    if target_file.exists():
                        # Read existing data
                        existing_df = pd.read_parquet(target_file)
                        
                        # Remove duplicates based on key columns
                        if table_type == "TRADINGPRICE":
                            key_cols = ['SETTLEMENTDATE', 'REGIONID']
                        elif table_type == "TRADINGREGIONSUM":
                            key_cols = ['SETTLEMENTDATE', 'REGIONID']
                        elif table_type == "TRADINGINTERCONNECT":
                            key_cols = ['SETTLEMENTDATE', 'INTERCONNECTORID']
                        elif table_type == "TRADINGGENUNITS":
                            key_cols = ['SETTLEMENTDATE', 'DUID']
                        else:
                            key_cols = ['SETTLEMENTDATE']
                        
                        # Combine and remove duplicates
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=key_cols, keep='last')
                        combined_df = combined_df.sort_values('SETTLEMENTDATE')
                        
                        # Write back
                        combined_df.to_parquet(target_file, engine='pyarrow', 
                                              compression='snappy', index=False)
                        
                        new_records = len(combined_df) - len(existing_df)
                        stats[table_type] = new_records
                        logger.info(f"Added {new_records} new records to {target_file.name}")
                    else:
                        # Create new file
                        df.to_parquet(target_file, engine='pyarrow', 
                                     compression='snappy', index=False)
                        stats[table_type] = len(df)
                        logger.info(f"Created {target_file.name} with {len(df)} records")
                        
                except Exception as e:
                    logger.error(f"Failed to process {table_type}: {e}")
                    stats[table_type] = 0
        
        return stats
    
    def fetch_historical_trading_data(self, start_date: str = "2020-01-01", 
                                    end_date: Optional[str] = None) -> None:
        """Fetch historical trading data for a date range"""
        if end_date is None:
            end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        total_days = (end - current).days + 1
        processed_days = 0
        
        logger.info(f"Starting historical download from {start_date} to {end_date} ({total_days} days)")
        
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            url = f"{self.BASE_URL}PUBLIC_TRADINGIS_{date_str}0000.zip"
            
            csvs = self.download_and_extract_zip(url)
            if csvs:
                stats = self.process_csv_data(csvs)
                processed_days += 1
                
                if processed_days % 10 == 0:
                    logger.info(f"Progress: {processed_days}/{total_days} days processed")
            
            current += timedelta(days=1)
        
        logger.info(f"Historical download complete. Processed {processed_days}/{total_days} days")
    
    def get_latest_processed_time(self) -> Optional[datetime]:
        """Get the latest timestamp from all trading data files"""
        latest = None
        
        for table_name, file_name in self.TABLES.items():
            file_path = self.output_dir / file_name
            if file_path.exists():
                try:
                    df = pd.read_parquet(file_path, columns=['SETTLEMENTDATE'])
                    if not df.empty:
                        file_latest = df['SETTLEMENTDATE'].max()
                        if latest is None or file_latest > latest:
                            latest = file_latest
                except Exception as e:
                    logger.error(f"Error reading {file_name}: {e}")
        
        return latest

    def list_current_trading_files(self, from_time: Optional[datetime] = None) -> List[str]:
        """List available trading files from current directory"""
        try:
            from bs4 import BeautifulSoup
            
            response = requests.get(self.CURRENT_URL, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            files = []
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                if 'PUBLIC_TRADINGIS' in href and href.endswith('.zip'):
                    # Extract timestamp from filename
                    # Format: PUBLIC_TRADINGIS_YYYYMMDDHHMM_*.zip
                    try:
                        # Extract just the filename
                        filename = href.split('/')[-1]
                        parts = filename.split('_')
                        if len(parts) >= 3 and parts[0] == 'PUBLIC' and parts[1] == 'TRADINGIS':
                            timestamp_str = parts[2][:12]  # YYYYMMDDHHMM
                            file_time = datetime.strptime(timestamp_str, "%Y%m%d%H%M")
                            
                            # Only include files for 30-minute intervals (00 or 30)
                            if file_time.minute in [0, 30]:
                                if from_time is None or file_time > from_time:
                                    files.append((file_time, href))
                    except Exception as e:
                        logger.debug(f"Could not parse timestamp from {href}: {e}")
            
            # Sort by timestamp
            files.sort(key=lambda x: x[0])
            return [f[1] for f in files]
            
        except Exception as e:
            logger.error(f"Failed to list current trading files: {e}")
            return []

    def fetch_latest_trading_data(self) -> bool:
        """Fetch the latest trading data from current TradingIS reports"""
        # Get latest processed timestamp
        latest_processed = self.get_latest_processed_time()
        
        if latest_processed:
            logger.info(f"Latest processed data: {latest_processed}")
            # Look for files newer than latest processed
            from_time = latest_processed
        else:
            logger.info("No existing data, fetching all available files")
            # If no data, get files from last 24 hours
            from_time = datetime.now() - timedelta(hours=24)
        
        # List available files
        available_files = self.list_current_trading_files(from_time)
        
        if not available_files:
            logger.info("No new trading files available")
            return False
        
        logger.info(f"Found {len(available_files)} new trading files to process")
        
        # Process each file
        total_new_records = 0
        processed_files = 0
        
        for file_href in available_files:
            # Construct full URL
            if file_href.startswith('http'):
                url = file_href
            elif file_href.startswith('/'):
                url = f"https://nemweb.com.au{file_href}"
            else:
                url = f"{self.CURRENT_URL}{file_href}"
            
            # Parse the CSV data (not download as the CSV contains MMS format)
            csvs = self.download_and_parse_mms_file(url)
            
            if csvs:
                stats = self.process_trading_tables(csvs)
                file_new_records = sum(stats.values())
                total_new_records += file_new_records
                processed_files += 1
                
                if processed_files % 10 == 0:
                    logger.info(f"Progress: {processed_files}/{len(available_files)} files processed")
        
        logger.info(f"Trading data update complete: {processed_files} files, {total_new_records} new records")
        return total_new_records > 0

    def download_and_parse_mms_file(self, url: str) -> Dict[str, pd.DataFrame]:
        """Download and parse MMS format file"""
        try:
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            dataframes = {}
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                for name in z.namelist():
                    if name.endswith('.CSV'):
                        with z.open(name) as f:
                            content = f.read().decode('utf-8')
                            tables = self.parse_mms_content(content)
                            dataframes.update(tables)
            
            return dataframes
            
        except Exception as e:
            logger.error(f"Failed to download/parse {url}: {e}")
            return {}

    def parse_mms_content(self, content: str) -> Dict[str, pd.DataFrame]:
        """Parse MMS format CSV content"""
        tables = {}
        lines = content.strip().split('\n')
        
        current_table = None
        current_headers = None
        current_data = []
        
        for line in lines:
            if not line.strip():
                continue
                
            parts = line.split(',')
            if len(parts) < 3:
                continue
                
            row_type = parts[0]
            
            if row_type == 'I':
                # Information/header row
                # Save previous table if exists
                if current_table and current_data:
                    df = pd.DataFrame(current_data, columns=current_headers)
                    tables[current_table] = df
                
                # Start new table
                current_table = parts[2]  # Table name
                current_headers = [h.strip('"') for h in parts[4:]]  # Column names
                current_data = []
                
            elif row_type == 'D' and current_table:
                # Data row
                data_values = [v.strip('"') for v in parts[4:]]
                if len(data_values) == len(current_headers):
                    current_data.append(data_values)
        
        # Save last table
        if current_table and current_data:
            df = pd.DataFrame(current_data, columns=current_headers)
            tables[current_table] = df
        
        return tables

    def process_trading_tables(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Process parsed trading tables and save to appropriate files"""
        stats = {}
        
        # Map MMS table names to our file structure
        table_mapping = {
            'PRICE': ('TRADINGPRICE', 'trading_price.parquet'),
            'REGIONSUM': ('TRADINGREGIONSUM', 'trading_regionsum.parquet'),
            'INTERCONNECTORRES': ('TRADINGINTERCONNECT', 'trading_interconnect.parquet'),
        }
        
        for mms_table, (file_key, file_name) in table_mapping.items():
            if mms_table in tables:
                df = tables[mms_table]
                target_file = self.output_dir / file_name
                
                try:
                    # Convert SETTLEMENTDATE to datetime
                    if 'SETTLEMENTDATE' in df.columns:
                        df['SETTLEMENTDATE'] = pd.to_datetime(df['SETTLEMENTDATE'])
                    
                    # Handle append or create
                    if target_file.exists():
                        existing_df = pd.read_parquet(target_file)
                        
                        # Define key columns for deduplication
                        if file_key == "TRADINGPRICE":
                            key_cols = ['SETTLEMENTDATE', 'REGIONID']
                        elif file_key == "TRADINGINTERCONNECT":
                            key_cols = ['SETTLEMENTDATE', 'INTERCONNECTORID']
                        else:
                            key_cols = ['SETTLEMENTDATE']
                        
                        # Combine and deduplicate
                        combined_df = pd.concat([existing_df, df], ignore_index=True)
                        combined_df = combined_df.drop_duplicates(subset=key_cols, keep='last')
                        combined_df = combined_df.sort_values('SETTLEMENTDATE')
                        
                        # Save
                        combined_df.to_parquet(target_file, engine='pyarrow', 
                                              compression='snappy', index=False)
                        
                        new_records = len(combined_df) - len(existing_df)
                        stats[file_key] = new_records
                    else:
                        # Create new file
                        df.to_parquet(target_file, engine='pyarrow', 
                                     compression='snappy', index=False)
                        stats[file_key] = len(df)
                        
                except Exception as e:
                    logger.error(f"Failed to process {file_key}: {e}")
                    stats[file_key] = 0
        
        return stats
    
    def get_status(self) -> Dict:
        """Get collector status"""
        status = {
            'name': 'Trading Data (30-min)',
            'last_update': None,
            'record_count': {},
            'file_sizes': {},
            'data_freshness': 'Unknown'
        }
        
        # Check each parquet file
        for table_name, file_name in self.TABLES.items():
            file_path = self.output_dir / file_name
            
            if file_path.exists():
                # Get file stats
                stats = file_path.stat()
                status['file_sizes'][table_name] = f"{stats.st_size / 1024 / 1024:.1f} MB"
                
                # Get record count and latest timestamp
                try:
                    df = pd.read_parquet(file_path, columns=['SETTLEMENTDATE'])
                    status['record_count'][table_name] = len(df)
                    
                    if not df.empty:
                        latest = df['SETTLEMENTDATE'].max()
                        if status['last_update'] is None or latest > status['last_update']:
                            status['last_update'] = latest
                except Exception as e:
                    logger.error(f"Error reading {file_name}: {e}")
                    status['record_count'][table_name] = 'Error'
        
        # Calculate data freshness
        if status['last_update']:
            age = datetime.now() - pd.Timestamp(status['last_update']).to_pydatetime()
            hours_old = age.total_seconds() / 3600
            
            if hours_old < 1:
                status['data_freshness'] = 'Current'
            elif hours_old < 24:
                status['data_freshness'] = f'{hours_old:.1f} hours old'
            else:
                status['data_freshness'] = f'{hours_old / 24:.1f} days old'
        
        return status
    
    async def repair_data(self, start_time: datetime, end_time: datetime) -> bool:
        """Repair missing data for a time range"""
        # For trading data, we need to download full days
        start_date = start_time.date()
        end_date = end_time.date()
        
        current = start_date
        success = True
        
        while current <= end_date:
            date_str = current.strftime("%Y%m%d")
            url = f"{self.BASE_URL}PUBLIC_TRADINGIS_{date_str}0000.zip"
            
            csvs = self.download_and_extract_zip(url)
            if csvs:
                self.process_csv_data(csvs)
            else:
                success = False
                logger.error(f"Failed to repair data for {current}")
            
            current += timedelta(days=1)
        
        return success


# Standalone functions for testing
def test_historical_download():
    """Test downloading historical data for a short period"""
    collector = TradingCollector()
    # Test with just a few days
    collector.fetch_historical_trading_data("2025-01-01", "2025-01-05")


def test_latest_download():
    """Test downloading latest trading data"""
    collector = TradingCollector()
    success = collector.fetch_latest_trading_data()
    print(f"Latest download {'succeeded' if success else 'failed'}")
    
    # Show status
    status = collector.get_status()
    print("\nCollector Status:")
    for key, value in status.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    # Run tests
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--historical":
        test_historical_download()
    else:
        test_latest_download()