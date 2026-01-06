"""
Operational demand data collector
Downloads half-hourly operational demand data from AEMO
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


class DemandCollector(BaseCollector):
    """Collects 30-minute operational demand data"""

    def __init__(self, config: Dict):
        super().__init__('demand', config)
        self.current_url = NEMWEB_URLS['demand']['current']
        self.archive_url = NEMWEB_URLS['demand']['archive']
        self.file_pattern = re.compile(NEMWEB_URLS['demand']['file_pattern'])

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
        """Get URLs of latest operational demand data files"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.current_url,
                    headers=HTTP_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                ) as response:
                    if response.status != 200:
                        self.logger.warning(f"Failed to access demand directory: HTTP {response.status}")
                        return []

                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')

                    # Find all demand files
                    demand_files = []
                    for link in soup.find_all('a', href=True):
                        href = link['href']
                        # Extract filename from href (may contain full path)
                        filename = href.split('/')[-1]
                        if self.file_pattern.match(filename):
                            demand_files.append(href)

                    if not demand_files:
                        self.logger.debug("No demand files found")
                        return []

                    # Sort and get latest (files have timestamp in name)
                    demand_files.sort()

                    # Get the latest few files to ensure we don't miss data
                    latest_files = demand_files[-3:]  # Last 3 files

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

                    self.logger.info(f"Found {len(new_files)} new demand files to process")
                    return new_files

        except Exception as e:
            self.logger.error(f"Error accessing demand directory: {e}")
            return []

    async def parse_data(self, content: bytes, filename: str) -> pd.DataFrame:
        """Parse operational demand data from ZIP file"""
        try:
            # Extract CSV from ZIP
            with zipfile.ZipFile(BytesIO(content)) as zf:
                # Find CSV file
                csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
                if not csv_files:
                    self.logger.error(f"No CSV file found in {filename}")
                    return pd.DataFrame()

                # Read CSV content
                with zf.open(csv_files[0]) as f:
                    csv_content = f.read().decode('utf-8', errors='ignore')

            # Parse AEMO CSV format
            df = self._parse_aemo_csv(csv_content)

            if df.empty:
                self.logger.warning(f"No demand data found in {filename}")
                return pd.DataFrame()

            # Add source file info
            df['source_file'] = filename

            self.logger.info(f"Parsed {len(df)} demand records from {filename}")
            return df

        except Exception as e:
            self.logger.error(f"Error parsing demand data: {e}")
            return pd.DataFrame()

    def _parse_aemo_csv(self, csv_content: str) -> pd.DataFrame:
        """
        Parse AEMO CSV format for operational demand

        Format:
        C,... - Header line (metadata)
        I,... - Column definition line
        D,... - Data rows

        Data line format for OPERATIONAL_DEMAND:
        D,OPERATIONAL_DEMAND,ACTUAL,3,REGIONID,INTERVAL_DATETIME,OPERATIONAL_DEMAND,...
        Example: D,OPERATIONAL_DEMAND,ACTUAL,3,NSW1,"2024/09/29 00:00:00",7416,...
        """
        lines = csv_content.strip().split('\n')

        # Find column headers (I line) and data rows (D lines)
        header_line = None
        data_lines = []

        for line in lines:
            if line.startswith('I,'):
                header_line = line
            elif line.startswith('D,'):
                data_lines.append(line)

        if not header_line or not data_lines:
            return pd.DataFrame()

        # Parse header
        headers = header_line.split(',')

        # Parse data
        data_rows = []
        for line in data_lines:
            values = line.split(',')
            # Remove quotes from values
            values = [v.strip('"') for v in values]
            data_rows.append(values)

        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers)

        # Extract relevant columns - use column positions to avoid duplicate name issues
        # Format: D,OPERATIONAL_DEMAND,ACTUAL,3,REGIONID,INTERVAL_DATETIME,OPERATIONAL_DEMAND,...
        # Positions: 0=D, 1=table, 2=type, 3=version, 4=REGIONID, 5=INTERVAL_DATETIME, 6=OPERATIONAL_DEMAND
        try:
            df = df.iloc[:, [4, 5, 6]].copy()
            df.columns = ['regionid', 'settlementdate', 'demand']
        except IndexError:
            self.logger.error("Unexpected CSV format - cannot extract columns")
            return pd.DataFrame()

        # Convert types
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df['demand'] = pd.to_numeric(df['demand'], errors='coerce')

        # Remove any rows with missing data
        df = df.dropna()

        # Reorder columns
        df = df[['settlementdate', 'regionid', 'demand']]

        return df

    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate operational demand data"""
        if df.empty:
            return False

        # Check required columns
        required_cols = ['settlementdate', 'regionid', 'demand']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns. Found: {df.columns}")
            return False

        # Check for at least one region
        expected_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        found_regions = df['regionid'].unique()
        if len(found_regions) == 0:
            self.logger.error("No regions found in data")
            return False

        # Expect all 5 regions
        missing_regions = set(expected_regions) - set(found_regions)
        if missing_regions:
            self.logger.warning(f"Missing regions: {missing_regions}")

        # Check for reasonable values (demand can be negative in SA due to high renewables)
        # But values should be within reasonable bounds
        for region in found_regions:
            region_data = df[df['regionid'] == region]['demand']
            if region_data.min() < -1000 or region_data.max() > 20000:
                self.logger.warning(
                    f"Unusual demand values for {region}: "
                    f"min={region_data.min():.0f} MW, max={region_data.max():.0f} MW"
                )

        # Check time intervals (should be 30 minutes for half-hourly data)
        time_sorted = df.sort_values(['regionid', 'settlementdate'])
        for region in found_regions:
            region_times = time_sorted[time_sorted['regionid'] == region]['settlementdate']
            time_diffs = region_times.diff().dropna()
            if not time_diffs.empty:
                expected_interval = timedelta(minutes=30)
                # Allow some tolerance
                invalid_intervals = time_diffs[
                    (time_diffs < expected_interval * 0.9) |
                    (time_diffs > expected_interval * 1.5)
                ]
                if len(invalid_intervals) > len(time_diffs) * 0.1:  # More than 10% invalid
                    self.logger.warning(
                        f"Too many invalid time intervals in {region}: "
                        f"{len(invalid_intervals)}/{len(time_diffs)}"
                    )

        return True

    def get_unique_columns(self) -> List[str]:
        """Demand data is unique by timestamp and region"""
        return ['settlementdate', 'regionid']

    async def backfill_date_range(self, start_date: datetime, end_date: datetime) -> bool:
        """
        Backfill operational demand data for a date range from ARCHIVE

        Archive files are typically daily
        """
        self.logger.info(f"Starting demand backfill for {start_date.date()} to {end_date.date()}")

        # Load existing data
        existing_df = self.load_existing_data()
        all_new_data = []

        # Process each day
        current_date = start_date
        while current_date <= end_date:
            try:
                # Skip if we already have data for this day
                if not existing_df.empty:
                    day_start = current_date.replace(hour=0, minute=0, second=0)
                    day_end = day_start + timedelta(days=1)
                    existing_day = existing_df[
                        (existing_df['settlementdate'] >= day_start) &
                        (existing_df['settlementdate'] < day_end)
                    ]
                    # Expect ~240 records per day (48 intervals * 5 regions)
                    if len(existing_day) >= 200:
                        self.logger.debug(
                            f"Skipping {current_date.date()} - already have {len(existing_day)} records"
                        )
                        current_date += timedelta(days=1)
                        continue

                # Download daily data
                daily_data = await self._download_daily_archive(current_date)
                if daily_data is not None and not daily_data.empty:
                    all_new_data.append(daily_data)
                    self.logger.info(
                        f"Collected {len(daily_data)} records for {current_date.date()}"
                    )

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
        combined_df = combined_df.sort_values(['settlementdate', 'regionid'])

        if self.save_data(combined_df):
            self.logger.info(f"Backfill complete. Total records: {len(combined_df)}")
            return True
        else:
            return False

    async def _download_daily_archive(self, date: datetime) -> Optional[pd.DataFrame]:
        """Download and process daily archive file"""
        # Archive files use format: PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_YYYYMMDD.zip
        short_date = date.strftime('%Y%m%d')

        possible_filenames = [
            f"PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_{short_date}.zip",
        ]

        for filename in possible_filenames:
            url = f"{self.archive_url}{filename}"

            self.logger.debug(f"Trying archive: {url}")

            # Download file
            content = await self.download_file(url)
            if content is not None:
                # Parse the data
                df = await self.parse_data(content, filename)
                if not df.empty:
                    return df

        return None
