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
    
    def get_summary(self) -> Dict[str, Any]:
        """Get collector summary for status reporting"""
        try:
            df = self.load_historical_data()
            latest = df.index.max() if len(df) > 0 else None
            return {
                'records': len(df),
                'latest': latest.strftime('%Y-%m-%d %H:%M') if latest else 'No data',
                'regions': df['REGIONID'].nunique() if len(df) > 0 else 0
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
    
    def backfill_missing_data(self, missing_times: List[datetime]) -> bool:
        """Download and integrate missing price data for specific timestamps"""
        if not missing_times:
            return True
            
        self.logger.info(f"Starting backfill for {len(missing_times)} missing price intervals")
        
        try:
            # Load existing data
            historical_df = self.load_historical_data()
            new_records = []
            
            # Group missing times by date to minimize downloads
            dates_needed = {}
            for dt in missing_times:
                date_key = dt.strftime('%Y%m%d')
                if date_key not in dates_needed:
                    dates_needed[date_key] = []
                dates_needed[date_key].append(dt)
            
            # Download data for each date
            for date_str, times_for_date in dates_needed.items():
                self.logger.info(f"Downloading data for {date_str}: {len(times_for_date)} intervals")
                
                # Try to find and download data for this date
                date_records = self._download_date_data(date_str, times_for_date)
                if date_records:
                    new_records.extend(date_records)
                    self.logger.info(f"Found {len(date_records)} records for {date_str}")
                else:
                    self.logger.warning(f"No data found for {date_str}")
            
            if new_records:
                # Convert to DataFrame and integrate
                new_df = pd.DataFrame(new_records)
                new_df['SETTLEMENTDATE'] = pd.to_datetime(new_df['SETTLEMENTDATE'])
                new_df = new_df.set_index('SETTLEMENTDATE').sort_index()
                
                # Combine with existing data
                combined_df = pd.concat([historical_df, new_df])
                combined_df = combined_df.reset_index()
                combined_df = combined_df.drop_duplicates(subset=['SETTLEMENTDATE', 'REGIONID'], keep='last')
                combined_df = combined_df.set_index('SETTLEMENTDATE').sort_index()
                
                # Save updated data
                self.save_data(combined_df)
                
                self.logger.info(f"Backfill complete: added {len(new_records)} records")
                return True
            else:
                self.logger.warning("No new data found during backfill")
                return False
                
        except Exception as e:
            self.logger.error(f"Backfill failed: {e}")
            return False
    
    def _download_date_data(self, date_str: str, target_times: List[datetime]) -> List[Dict]:
        """Download price data for a specific date"""
        try:
            # Try current directory first
            self.logger.info(f"Trying CURRENT directory for {date_str}")
            records = self._try_current_directory(date_str, target_times)
            if records:
                return records
            
            # Try archive directory if current didn't work
            self.logger.info(f"Trying ARCHIVE directory for {date_str}")
            records = self._try_archive_directory(date_str, target_times)
            if records:
                return records
                
            self.logger.warning(f"No data found in CURRENT or ARCHIVE for {date_str}")
            return []
            
        except Exception as e:
            self.logger.error(f"Error downloading data for {date_str}: {e}")
            return []
    
    def _try_current_directory(self, date_str: str, target_times: List[datetime]) -> List[Dict]:
        """Try to find data in CURRENT directory - process ALL matching files"""
        try:
            # Use the SAME URL as real-time collection (case matters!)
            base_url = "http://nemweb.com.au/Reports/Current/Dispatch_Reports/"
            response = requests.get(base_url, headers=HTTP_HEADERS, timeout=30)
            if response.status_code != 200:
                return []
                
            # Find ALL dispatch files for this date
            # Files can be either PUBLIC_DISPATCH_YYYYMMDD* or PUBLIC_DISPATCHIS_YYYYMMDD*
            patterns = [
                f'PUBLIC_DISPATCH_{date_str}[^"]*\\.zip',
                f'PUBLIC_DISPATCHIS_{date_str}[^"]*\\.zip'
            ]
            
            all_files = []
            for pattern in patterns:
                files = re.findall(pattern, response.text)
                all_files.extend(files)
            
            # Remove duplicates
            all_files = list(set(all_files))
            self.logger.info(f"Found {len(all_files)} dispatch files in CURRENT for {date_str}")
            
            if not all_files:
                return []
            
            # Convert target times to set for efficient lookup
            target_times_set = {dt.replace(second=0, microsecond=0) for dt in target_times}
            all_records = []
            files_processed = 0
            
            # Process ALL files to find matching timestamps
            # Note: We need to check all files as timestamp in filename doesn't match data inside
            for i, filename in enumerate(all_files):
                # Note: Filename timestamp doesn't match data timestamp inside
                # e.g., PUBLIC_DISPATCH_202507141430_20250714142516_LEGACY.zip
                # was created at 14:25:16 but contains data for 14:30:00
                self.logger.debug(f"Checking file: {filename}")
                
                # Download and process this file
                file_url = f"{base_url}{filename}"
                try:
                    records = self._extract_price_data_from_url(file_url)
                    self.logger.debug(f"Extracted {len(records)} records from {filename}")
                    if records:
                        # Filter to only the timestamps we need
                        filtered_records = self._filter_records_by_time(records, target_times)
                        if filtered_records:
                            all_records.extend(filtered_records)
                            self.logger.info(f"Found {len(filtered_records)} records in {filename}")
                            files_processed += 1
                            
                            # Check if we've found all needed timestamps
                            found_times = {pd.to_datetime(r['SETTLEMENTDATE']).replace(second=0, microsecond=0) 
                                         for r in all_records}
                            missing_count = len(target_times_set - found_times)
                            if missing_count == 0:
                                self.logger.info(f"Found all {len(target_times_set)} target timestamps after processing {files_processed} files")
                                return all_records
                            elif files_processed % 10 == 0:
                                self.logger.info(f"Progress: processed {files_processed} files, found {len(all_records)} records, still missing {missing_count} timestamps")
                                
                except Exception as e:
                    self.logger.debug(f"Failed to process {filename}: {e}")
                    continue
            
            if all_records:
                self.logger.info(f"Found {len(all_records)} total records from {files_processed} CURRENT files")
                return all_records
            else:
                self.logger.warning(f"No matching records found after checking {len(all_files)} files")
                return []
            
        except Exception as e:
            self.logger.debug(f"Error checking CURRENT directory: {e}")
            return []
    
    def _try_archive_directory(self, date_str: str, target_times: List[datetime]) -> List[Dict]:
        """Try to find data in ARCHIVE directory - use Dispatch_Reports not DispatchIS"""
        try:
            # Price data is in Dispatch_Reports archive, not DispatchIS_Reports!
            archive_url = "https://www.nemweb.com.au/REPORTS/ARCHIVE/Dispatch_Reports/"
            daily_filename = f"PUBLIC_DISPATCH_{date_str}.zip"
            daily_file_url = f"{archive_url}{daily_filename}"
            
            self.logger.info(f"Trying working archive pattern: {daily_file_url}")
            
            try:
                response = requests.get(daily_file_url, headers=HTTP_HEADERS, timeout=30)
                self.logger.info(f"Archive response: HTTP {response.status_code}")
                
                if response.status_code == 200:
                    self.logger.info(f"Found daily archive: {daily_filename} ({len(response.content)} bytes)")
                    records = self._extract_from_daily_archive(response.content, target_times)
                    if records:
                        return records
                    else:
                        self.logger.warning("Daily archive found but no matching records extracted")
                else:
                    self.logger.warning(f"Daily archive not found: HTTP {response.status_code}")
                        
            except Exception as e:
                self.logger.error(f"Error accessing {daily_file_url}: {e}")
                    
            return []
            
        except Exception as e:
            self.logger.error(f"Error checking ARCHIVE directory: {e}")
            return []
    
    def _extract_from_daily_archive(self, daily_zip_content: bytes, target_times: List[datetime]) -> List[Dict]:
        """Extract data from daily archive with nested ZIP structure"""
        try:
            all_records = []
            target_times_set = {dt.replace(second=0, microsecond=0) for dt in target_times}
            
            with zipfile.ZipFile(BytesIO(daily_zip_content)) as daily_zip:
                # List all nested ZIP files (should be 288 for a full day)
                nested_zips = [f for f in daily_zip.namelist() if f.endswith('.zip')]
                self.logger.info(f"Daily archive contains {len(nested_zips)} nested 5-minute ZIP files")
                
                for nested_zip_name in nested_zips:
                    try:
                        # Extract timestamp from nested ZIP filename
                        # Format: PUBLIC_DISPATCH_YYYYMMDDHHMM_*.zip (not DISPATCHIS)
                        timestamp_match = re.search(r'(\d{12})', nested_zip_name)
                        if not timestamp_match:
                            continue
                            
                        timestamp_str = timestamp_match.group(1)
                        file_datetime = datetime.strptime(timestamp_str, '%Y%m%d%H%M')
                        file_datetime = file_datetime.replace(second=0, microsecond=0)
                        
                        # Only process if this timestamp is in our target list
                        if file_datetime not in target_times_set:
                            continue
                            
                        self.logger.debug(f"Processing nested ZIP for {file_datetime}: {nested_zip_name}")
                        
                        # Extract and process the nested ZIP
                        with daily_zip.open(nested_zip_name) as nested_file:
                            with zipfile.ZipFile(nested_file) as minute_zip:
                                csv_files = [f for f in minute_zip.namelist() if f.endswith('.CSV')]
                                if csv_files:
                                    with minute_zip.open(csv_files[0]) as csv_file:
                                        content = csv_file.read().decode('utf-8')
                                        records = self._parse_dispatch_csv(content)
                                        
                                        # Filter records to this specific timestamp
                                        filtered_records = []
                                        for record in records:
                                            record_time = pd.to_datetime(record['SETTLEMENTDATE']).replace(second=0, microsecond=0)
                                            if record_time == file_datetime:
                                                filtered_records.append(record)
                                        
                                        if filtered_records:
                                            all_records.extend(filtered_records)
                                            self.logger.debug(f"Found {len(filtered_records)} price records for {file_datetime}")
                        
                    except Exception as e:
                        self.logger.debug(f"Error processing nested ZIP {nested_zip_name}: {e}")
                        continue
            
            if all_records:
                self.logger.info(f"Extracted {len(all_records)} total records from daily archive")
                return all_records
            else:
                self.logger.warning("No matching records found in daily archive")
                return []
                
        except Exception as e:
            self.logger.error(f"Error extracting from daily archive: {e}")
            return []
    
    def _parse_dispatch_csv(self, content: str) -> List[Dict]:
        """Parse DISPATCH CSV content to extract DREGION price data"""
        try:
            records = []
            lines = content.split('\n')
            
            for line in lines:
                if not line.strip():
                    continue
                    
                # Parse CSV line properly
                try:
                    reader = csv.reader([line])
                    fields = next(reader)
                except:
                    continue
                
                # Price data is in DREGION table (same as real-time update)
                # D,DREGION,,3,"2025/07/14 16:35:00",1,NSW1,0,176.80386,0,176.80386,0,0,...
                # Index: 0=D, 1=DREGION, 2=empty, 3=3, 4=SETTLEMENTDATE, 5=RUNNO, 6=REGIONID, 7=INTERVENTION, 8=RRP
                if len(fields) >= 9 and fields[0] == 'D' and fields[1] == 'DREGION':
                    try:
                        record = {
                            'REGIONID': fields[6],
                            'SETTLEMENTDATE': fields[4].strip('"'),  # Remove quotes
                            'RRP': float(fields[8])
                        }
                        records.append(record)
                        self.logger.debug(f"Parsed DREGION price record: {record}")
                    except (ValueError, IndexError) as e:
                        self.logger.debug(f"Error parsing DREGION row: {e}")
                        continue
            
            if not records:
                self.logger.debug("No DREGION price rows found")
            else:
                self.logger.debug(f"Found {len(records)} DREGION price records")
            
            return records
            
        except Exception as e:
            self.logger.error(f"Error parsing CSV: {e}")
            return []
    
    def _filter_records_by_time(self, records: List[Dict], target_times: List[datetime]) -> List[Dict]:
        """Filter records to only include target timestamps"""
        try:
            filtered_records = []
            target_times_set = {dt.replace(second=0, microsecond=0) for dt in target_times}
            
            for record in records:
                record_time = pd.to_datetime(record['SETTLEMENTDATE']).replace(second=0, microsecond=0)
                if record_time in target_times_set:
                    filtered_records.append(record)
            
            return filtered_records
            
        except Exception as e:
            self.logger.debug(f"Error filtering records: {e}")
            return []
    
    def _extract_price_data_from_url(self, url: str) -> List[Dict]:
        """Extract price data from a specific ZIP URL"""
        try:
            response = requests.get(url, headers=HTTP_HEADERS, timeout=30)
            if response.status_code != 200:
                self.logger.debug(f"HTTP {response.status_code} for {url}")
                return []
                
            # Process ZIP file
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                self.logger.debug(f"ZIP contains: {[f.filename for f in zip_file.filelist]}")
                
                for file_info in zip_file.filelist:
                    if file_info.filename.endswith('.CSV'):
                        self.logger.debug(f"Processing CSV: {file_info.filename}")
                        
                        with zip_file.open(file_info.filename) as csvfile:
                            content = csvfile.read().decode('utf-8')
                            
                            # Parse using the same logic for both archive and current files
                            records = self._parse_dispatch_csv(content)
                            
                            if records:
                                self.logger.info(f"Extracted {len(records)} records from {file_info.filename}")
                                return records
                            else:
                                self.logger.debug(f"No valid records found in {file_info.filename}")
                                
            return []
            
        except Exception as e:
            self.logger.error(f"Error extracting data from {url}: {e}")
            return []