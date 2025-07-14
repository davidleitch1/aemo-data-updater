"""
Rooftop solar data collector
Downloads distributed PV data and converts 30-minute intervals to 5-minute data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from typing import List, Optional, Dict
import aiohttp
from bs4 import BeautifulSoup
import zipfile
from io import BytesIO

from .base_collector import BaseCollector
from ..config import NEMWEB_URLS, HTTP_HEADERS, REQUEST_TIMEOUT


class RooftopCollector(BaseCollector):
    """Collects 30-minute rooftop solar data and converts to 5-minute intervals"""
    
    def __init__(self, config: Dict):
        super().__init__('rooftop', config)
        self.current_url = NEMWEB_URLS['rooftop']['current']
        self.archive_url = NEMWEB_URLS['rooftop']['archive']
        self.file_pattern = re.compile(NEMWEB_URLS['rooftop']['file_pattern'])
        
        # Track last processed file to avoid reprocessing
        self.last_processed_file = None
        self._load_last_processed()
        
    def _load_last_processed(self):
        """Load last processed filename from existing data"""
        try:
            df = self.load_existing_data()
            if not df.empty and 'source_file' in df.columns:
                # Get unique source files from the last day
                recent_df = df[df['settlementdate'] >= df['settlementdate'].max() - timedelta(days=1)]
                if not recent_df.empty:
                    source_files = recent_df['source_file'].dropna().unique()
                    if len(source_files) > 0:
                        self.last_processed_file = sorted(source_files)[-1]
                        self.logger.debug(f"Last processed file: {self.last_processed_file}")
        except:
            pass
            
    async def get_latest_urls(self) -> List[str]:
        """Get URLs of latest rooftop solar data files"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.current_url,
                    headers=HTTP_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as response:
                    if response.status != 200:
                        self.logger.warning(f"Failed to access rooftop directory: HTTP {response.status}")
                        return []
                        
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Find all rooftop PV files
                    rooftop_files = []
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        if self.file_pattern.match(href) and 'ROOFTOP_PV_ACTUAL_MEASUREMENT' in href:
                            rooftop_files.append(href)
                            
                    if not rooftop_files:
                        self.logger.debug("No rooftop PV files found")
                        return []
                        
                    # Sort and get latest (rooftop files have timestamp in name)
                    rooftop_files.sort()
                    
                    # Get the latest few files to ensure we don't miss data
                    latest_files = rooftop_files[-3:]  # Last 3 files
                    
                    # Filter out already processed files
                    new_files = []
                    for file in latest_files:
                        if self.last_processed_file and file <= self.last_processed_file:
                            continue
                        
                        # Construct full URL
                        if file.startswith('http'):
                            url = file
                        elif file.startswith('/'):
                            url = f"http://nemweb.com.au{file}"
                        else:
                            url = f"{self.current_url}{file}"
                        
                        new_files.append(url)
                    
                    self.logger.info(f"Found {len(new_files)} new rooftop files to process")
                    return new_files
                        
        except Exception as e:
            self.logger.error(f"Error accessing rooftop directory: {e}")
            return []
            
    async def parse_data(self, content: bytes, filename: str) -> pd.DataFrame:
        """Parse rooftop solar data from ZIP file and convert to 5-minute intervals"""
        try:
            # Extract 30-minute data from ZIP
            df_30min = self._extract_30min_data(content)
            if df_30min.empty:
                return pd.DataFrame()
                
            self.logger.info(f"Extracted {len(df_30min)} 30-minute records from {filename}")
            
            # Convert to 5-minute intervals
            df_5min = self._convert_30min_to_5min(df_30min)
            
            # Add source file info
            df_5min['source_file'] = filename
            
            self.logger.info(f"Converted to {len(df_5min)} 5-minute records")
            return df_5min
            
        except Exception as e:
            self.logger.error(f"Error parsing rooftop data: {e}")
            return pd.DataFrame()
            
    def _extract_30min_data(self, zip_content: bytes) -> pd.DataFrame:
        """Extract 30-minute data from ZIP file"""
        try:
            with zipfile.ZipFile(BytesIO(zip_content)) as zip_file:
                # Find CSV file
                csv_files = [f for f in zip_file.namelist() if f.lower().endswith('.csv')]
                if not csv_files:
                    self.logger.error("No CSV files found in ZIP")
                    return pd.DataFrame()
                    
                # Read CSV content
                with zip_file.open(csv_files[0]) as csv_file:
                    csv_content = csv_file.read().decode('utf-8')
                    
            # Parse ROOFTOP data
            data_rows = []
            lines = csv_content.strip().split('\n')
            
            for line in lines:
                if line.startswith('D,ROOFTOP,ACTUAL'):
                    fields = line.split(',')
                    if len(fields) >= 8:
                        data_rows.append({
                            'settlementdate': fields[4].strip('"'),
                            'regionid': fields[5].strip('"'),
                            'powermw': float(fields[6].strip('"') or 0)
                        })
                        
            if not data_rows:
                self.logger.warning("No rooftop data found in CSV")
                return pd.DataFrame()
                
            # Create DataFrame and pivot
            df = pd.DataFrame(data_rows)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'])
            
            # Pivot to get regions as columns
            pivot_df = df.pivot_table(
                index='settlementdate',
                columns='regionid',
                values='powermw',
                aggfunc='first'
            ).fillna(0)
            
            # Reset index to make settlementdate a column
            pivot_df = pivot_df.reset_index()
            
            return pivot_df
            
        except Exception as e:
            self.logger.error(f"Error extracting 30min data: {e}")
            return pd.DataFrame()
            
    def _convert_30min_to_5min(self, df_30min: pd.DataFrame) -> pd.DataFrame:
        """
        Convert 30-minute data to 5-minute intervals using weighted interpolation
        
        CRITICAL ALGORITHM:
        - Each 30-min period creates 6 x 5-min periods
        - Weighted average: ((6-j)*current + j*next) / 6
        - Where j = 0 to 5 for each 5-minute interval
        """
        if df_30min.empty:
            return pd.DataFrame()
            
        # Sort by time
        df_30min = df_30min.sort_values('settlementdate')
        
        # Get region columns
        region_columns = [col for col in df_30min.columns if col != 'settlementdate']
        
        # Create 5-minute records
        five_min_records = []
        
        for i in range(len(df_30min)):
            current_row = df_30min.iloc[i]
            current_time = current_row['settlementdate']
            
            # Get next row if available
            has_next = i < len(df_30min) - 1
            if has_next:
                next_row = df_30min.iloc[i + 1]
            
            # Generate 6 x 5-minute periods
            for j in range(6):
                five_min_time = current_time + timedelta(minutes=j*5)
                record = {'settlementdate': five_min_time}
                
                # Calculate values for each region
                for region in region_columns:
                    current_value = current_row[region]
                    
                    if has_next:
                        next_value = next_row[region]
                        # Weighted interpolation
                        value = ((6 - j) * current_value + j * next_value) / 6
                    else:
                        # Last period - use current value
                        value = current_value
                    
                    record[region] = value
                
                five_min_records.append(record)
        
        df_5min = pd.DataFrame(five_min_records)
        
        # Remove duplicates that might occur at boundaries
        df_5min = df_5min.drop_duplicates(subset=['settlementdate'])
        
        return df_5min
        
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate rooftop solar data"""
        if df.empty:
            return False
            
        # Check required columns
        if 'settlementdate' not in df.columns:
            self.logger.error("Missing settlementdate column")
            return False
            
        # Check for at least one region
        region_columns = [col for col in df.columns if col in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']]
        if not region_columns:
            self.logger.error("No region columns found")
            return False
            
        # Check for reasonable values (rooftop solar should be >= 0)
        for region in region_columns:
            if (df[region] < 0).any():
                self.logger.warning(f"Negative values found in {region}")
                return False
                
        # Check time intervals (should be 5 minutes)
        time_diffs = df['settlementdate'].diff().dropna()
        if not time_diffs.empty:
            expected_interval = timedelta(minutes=5)
            # Allow some tolerance for the check
            invalid_intervals = time_diffs[
                (time_diffs < expected_interval * 0.9) | 
                (time_diffs > expected_interval * 1.1)
            ]
            if len(invalid_intervals) > len(time_diffs) * 0.1:  # More than 10% invalid
                self.logger.warning(f"Too many invalid time intervals: {len(invalid_intervals)}/{len(time_diffs)}")
                
        return True
        
    def get_unique_columns(self) -> List[str]:
        """Rooftop data is unique by timestamp only"""
        return ['settlementdate']
        
    async def backfill_date_range(self, start_date: datetime, end_date: datetime) -> bool:
        """
        Backfill rooftop solar data for a date range from ARCHIVE
        
        Archive files are weekly, starting on Thursdays
        """
        self.logger.info(f"Starting rooftop backfill for {start_date.date()} to {end_date.date()}")
        
        # Load existing data
        existing_df = self.load_existing_data()
        all_new_data = []
        
        # Calculate which weekly archives we need
        current_date = start_date
        archive_dates = set()
        
        while current_date <= end_date:
            # Find the Thursday of this week (archive files are weekly from Thursday)
            days_since_thursday = (current_date.weekday() - 3) % 7
            thursday = current_date - timedelta(days=days_since_thursday)
            archive_dates.add(thursday.strftime('%Y%m%d'))
            current_date += timedelta(days=7)
            
        self.logger.info(f"Need archives for weeks starting: {sorted(archive_dates)}")
        
        # Download each archive
        for archive_date in sorted(archive_dates):
            try:
                # Skip if we already have data for this period
                if not existing_df.empty:
                    week_start = datetime.strptime(archive_date, '%Y%m%d')
                    week_end = week_start + timedelta(days=7)
                    existing_week = existing_df[
                        (existing_df['settlementdate'] >= week_start) &
                        (existing_df['settlementdate'] < week_end)
                    ]
                    # Expect ~2016 records per week (7 days * 288 intervals)
                    if len(existing_week) >= 2000:
                        self.logger.debug(f"Skipping week {archive_date} - already have {len(existing_week)} records")
                        continue
                
                # Download weekly archive
                archive_data = await self._download_weekly_archive(archive_date)
                if archive_data is not None and not archive_data.empty:
                    # Filter to requested date range
                    archive_data = archive_data[
                        (archive_data['settlementdate'] >= start_date) &
                        (archive_data['settlementdate'] <= end_date)
                    ]
                    if not archive_data.empty:
                        all_new_data.append(archive_data)
                        self.logger.info(f"Collected {len(archive_data)} records for week {archive_date}")
                        
            except Exception as e:
                self.logger.error(f"Error processing week {archive_date}: {e}")
                
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
            
    async def _download_weekly_archive(self, archive_date: str) -> Optional[pd.DataFrame]:
        """Download and process weekly archive file"""
        filename = f"PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_{archive_date}.zip"
        url = f"{self.archive_url}{filename}"
        
        self.logger.debug(f"Downloading archive: {url}")
        
        # Download file
        content = await self.download_file(url)
        if content is None:
            return None
            
        # Process nested ZIP structure
        try:
            all_data = []
            
            with zipfile.ZipFile(BytesIO(content)) as weekly_zip:
                # Get all nested ZIP files
                nested_files = [f for f in weekly_zip.namelist() if f.endswith('.zip')]
                
                if not nested_files:
                    self.logger.error(f"No nested ZIP files in {filename}")
                    return None
                    
                self.logger.debug(f"Processing {len(nested_files)} nested files for week {archive_date}")
                
                # Process each nested file
                for nested_name in sorted(nested_files):
                    try:
                        with weekly_zip.open(nested_name) as nested_file:
                            nested_content = nested_file.read()
                            
                        # Parse the nested data
                        df = await self.parse_data(nested_content, nested_name)
                        if not df.empty:
                            all_data.append(df)
                            
                    except Exception as e:
                        self.logger.debug(f"Error processing {nested_name}: {e}")
                        continue
                        
            if not all_data:
                return None
                
            # Combine all data
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate'])
            
            return combined_df
            
        except Exception as e:
            self.logger.error(f"Error processing archive {filename}: {e}")
            return None