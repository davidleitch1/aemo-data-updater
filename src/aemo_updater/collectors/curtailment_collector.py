#!/usr/bin/env python3
"""
AEMO Curtailment Data Collector
Downloads wind/solar curtailment data from AEMO DISPATCH UNIT_SOLUTION reports
Extracts AVAILABILITY, TOTALCLEARED, and SEMIDISPATCHCAP for curtailment analysis
"""

import re
import zipfile
import pandas as pd
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Optional, Dict, Any, List
from bs4 import BeautifulSoup

from .base_collector import BaseCollector
from ..config import get_logger

# Target wind/solar DUIDs based on patterns
WIND_SOLAR_PATTERNS = re.compile(r'(WF|SF|SOLAR|WIND|PV)', re.IGNORECASE)


class CurtailmentCollector(BaseCollector):
    """
    AEMO Curtailment Data Collector
    Downloads and processes DISPATCH UNIT_SOLUTION data for wind/solar curtailment
    """

    def __init__(self, config: Dict[str, Any]):
        """Initialize the curtailment collector"""
        super().__init__('curtailment', config)
        self.logger = get_logger(self.__class__.__name__)

        # URL for next day dispatch reports (CURRENT directory)
        # These files contain UNIT_SOLUTION data with AVAILABILITY, TOTALCLEARED, SEMIDISPATCHCAP
        self.base_url = "https://www.nemweb.com.au/Reports/Current/Next_Day_Dispatch/"

        self.logger.info(f"Curtailment collector initialized - output: {self.output_file}")

    async def get_latest_urls(self) -> List[str]:
        """Get URLs for latest dispatch reports"""
        try:
            # Download the directory listing
            content = await self.download_file(self.base_url)
            if content is None:
                return []

            # Parse HTML
            soup = BeautifulSoup(content, 'html.parser')

            # Find all ZIP file links with NEXT_DAY_DISPATCH pattern
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and 'PUBLIC_NEXT_DAY_DISPATCH' in href:
                    zip_files.append(href)

            if not zip_files:
                self.logger.warning("No dispatch ZIP files found")
                return []

            # Sort to get the most recent
            zip_files.sort(reverse=True)
            latest_file = zip_files[0]

            # Construct full URL
            if latest_file.startswith('/'):
                file_url = "http://nemweb.com.au" + latest_file
            else:
                file_url = self.base_url + latest_file

            self.logger.info(f"Found latest dispatch file: {latest_file}")
            return [file_url]

        except Exception as e:
            self.logger.error(f"Error getting dispatch URLs: {e}")
            return []

    async def parse_data(self, content: bytes, filename: str) -> pd.DataFrame:
        """
        Parse AEMO DISPATCH UNIT_SOLUTION CSV data
        Extracts AVAILABILITY, TOTALCLEARED, SEMIDISPATCHCAP for wind/solar units

        Returns DataFrame with columns:
        - settlementdate: datetime
        - duid: str
        - availability: float (MW)
        - totalcleared: float (MW)
        - semidispatchcap: int (0 or 1)
        - curtailment: float (MW)
        """
        records = []

        try:
            # Extract CSV from ZIP
            csv_content = self.extract_zip_content(content)
            if csv_content is None:
                return pd.DataFrame()

            # Parse CSV line by line
            for line in csv_content.split('\n'):
                # Look for UNIT_SOLUTION data rows
                if not line.startswith('D,DISPATCH,UNIT_SOLUTION'):
                    continue

                parts = line.split(',')
                if len(parts) <= 60:
                    continue

                # Extract DUID (column 7, index 6)
                duid = parts[6].strip('"')

                # Filter to wind/solar units only
                if not WIND_SOLAR_PATTERNS.search(duid):
                    continue

                try:
                    # Extract key fields
                    settlementdate = parts[4].strip('"')  # Column 5
                    totalcleared = float(parts[14]) if parts[14] else 0.0  # Column 15
                    availability = float(parts[36]) if parts[36] else 0.0  # Column 37
                    semidispatchcap = int(parts[59]) if parts[59] else 0  # Column 60

                    # Calculate curtailment with solar night filter
                    curtailment = 0.0
                    if semidispatchcap == 1:
                        # For solar, only calculate when availability > 1 MW (exclude night)
                        if 'SF' in duid or 'SOLAR' in duid.upper():
                            if availability > 1.0:
                                curtailment = availability - totalcleared
                        else:
                            # For wind, always calculate when semidispatchcap = 1
                            curtailment = availability - totalcleared

                    # Ensure curtailment is not negative
                    curtailment = max(0.0, curtailment)

                    records.append({
                        'settlementdate': settlementdate,
                        'duid': duid,
                        'availability': availability,
                        'totalcleared': totalcleared,
                        'semidispatchcap': semidispatchcap,
                        'curtailment': curtailment
                    })

                except (ValueError, IndexError) as e:
                    # Skip invalid rows
                    continue

            if not records:
                self.logger.warning("No valid UNIT_SOLUTION records extracted")
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(records)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'], format='%Y/%m/%d %H:%M:%S')

            self.logger.info(f"Extracted {len(df)} curtailment records from {filename}")
            self.logger.info(f"  Settlement time: {df['settlementdate'].iloc[0]}")
            self.logger.info(f"  Unique DUIDs: {df['duid'].nunique()}")
            self.logger.info(f"  Total curtailment: {df['curtailment'].sum():.1f} MW")

            return df

        except Exception as e:
            self.logger.error(f"Error parsing curtailment data: {e}")
            return pd.DataFrame()

    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate curtailment data"""
        if df.empty:
            return False

        # Check required columns
        required_cols = ['settlementdate', 'duid', 'availability', 'totalcleared',
                        'semidispatchcap', 'curtailment']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns. Found: {df.columns.tolist()}")
            return False

        # Validate data types and ranges
        try:
            # Check numeric columns
            for col in ['availability', 'totalcleared', 'curtailment']:
                if not pd.api.types.is_numeric_dtype(df[col]):
                    self.logger.error(f"{col} column is not numeric")
                    return False

            # Check for reasonable ranges (MW)
            if df['availability'].min() < 0:
                self.logger.warning(f"Negative availability: {df['availability'].min():.2f} MW")

            if df['availability'].max() > 2000:  # Largest wind/solar farms ~500MW
                self.logger.warning(f"Very high availability: {df['availability'].max():.2f} MW")

            if df['curtailment'].min() < 0:
                self.logger.error(f"Negative curtailment found: {df['curtailment'].min():.2f} MW")
                return False

            # Check DUID count (should be ~150 wind/solar units)
            duid_count = df['duid'].nunique()
            if duid_count < 50:
                self.logger.warning(f"Low DUID count: {duid_count} (expected ~150)")

            return True

        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False

    def get_unique_columns(self) -> List[str]:
        """Get column names that uniquely identify a curtailment record"""
        return ['settlementdate', 'duid']

    def get_summary(self) -> Dict[str, Any]:
        """Get collector summary for status reporting"""
        try:
            df = self.load_existing_data()
            if df.empty:
                return {
                    'records': 0,
                    'latest': 'No data',
                    'duids': 0,
                    'total_curtailment_mw': 0
                }

            latest = df['settlementdate'].max()
            total_curtailment = df['curtailment'].sum()

            return {
                'records': len(df),
                'latest': latest.strftime('%Y-%m-%d %H:%M') if latest else 'No data',
                'duids': df['duid'].nunique(),
                'total_curtailment_mw': f"{total_curtailment:.1f}"
            }
        except Exception as e:
            return {'error': str(e)}

    def check_integrity(self) -> Dict[str, Any]:
        """Check curtailment data integrity"""
        df = self.load_existing_data()

        if df.empty:
            return {
                "status": "No data",
                "issues": ["No curtailment data found in parquet file"]
            }

        issues = []

        # Check for negative curtailment
        negative_curtailment = df[df['curtailment'] < 0]
        if len(negative_curtailment) > 0:
            issues.append(f"Found {len(negative_curtailment)} records with negative curtailment")

        # Check for time gaps
        df_sorted = df.sort_values('settlementdate')
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

        # Check DUID count
        duid_count = df['duid'].nunique()
        if duid_count < 100:
            issues.append(f"Low DUID count: {duid_count} (expected ~150)")

        return {
            "status": "OK" if not issues else "Issues found",
            "issues": issues,
            "records": len(df),
            "unique_duids": duid_count,
            "date_range": f"{df_sorted['settlementdate'].min()} to {df_sorted['settlementdate'].max()}",
            "total_curtailment_mw": f"{df['curtailment'].sum():.1f}"
        }