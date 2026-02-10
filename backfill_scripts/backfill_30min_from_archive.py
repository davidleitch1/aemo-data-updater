#!/usr/bin/env python3
"""
Backfill 30-minute Data from AEMO Archive

Handles AEMO TradingIS Archive files which are WEEKLY ZIPs containing nested ZIPs.
Each nested ZIP contains one CSV file with 5-minute data.
Aggregates to 30-minute intervals.

Usage:
    python backfill_30min_from_archive.py --start "2025-10-28 02:00" --end "2025-10-28 08:00" --type all
    python backfill_30min_from_archive.py --start "2025-11-16 06:00" --end "2025-11-16 14:00" --type prices --test
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sys
import argparse
import requests
import zipfile
import io
import re

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ThirtyMinuteArchiveBackfillTool:
    """Tool for backfilling 30-minute data from AEMO TradingIS Archive"""

    def __init__(self, collector: UnifiedAEMOCollector):
        """Initialize backfill tool with collector"""
        self.collector = collector
        self.archive_url = 'http://nemweb.com.au/Reports/ARCHIVE/TradingIS_Reports/'
        self.headers = {'User-Agent': 'AEMO Dashboard Data Collector'}

    def find_weekly_archive(self, target_date: datetime) -> str:
        """Find the weekly archive file containing the target date"""
        # List available archives
        response = requests.get(self.archive_url, headers=self.headers, timeout=30)
        response.raise_for_status()

        # Parse archive listing
        # Format: PUBLIC_TRADINGIS_20251026_20251101.zip
        pattern = r'PUBLIC_TRADINGIS_(\d{8})_(\d{8})\.zip'

        for match in re.finditer(pattern, response.text):
            start_str, end_str = match.groups()
            archive_start = pd.to_datetime(start_str, format='%Y%m%d')
            archive_end = pd.to_datetime(end_str, format='%Y%m%d')

            # Check if target date falls within this archive
            if archive_start <= target_date <= archive_end:
                return f"PUBLIC_TRADINGIS_{start_str}_{end_str}.zip"

        raise ValueError(f"No archive found containing {target_date}")

    def download_weekly_archive(self, filename: str) -> bytes:
        """Download weekly archive ZIP"""
        url = f"{self.archive_url}{filename}"
        logger.info(f"Downloading {filename} ({url})")

        response = requests.get(url, headers=self.headers, timeout=300)
        response.raise_for_status()
        logger.info(f"Downloaded {len(response.content):,} bytes")
        return response.content

    def extract_files_for_period(self, archive_content: bytes, start: datetime, end: datetime) -> list:
        """
        Extract nested ZIPs for specific time period from weekly archive.

        Returns list of (timestamp, csv_content) tuples
        """
        logger.info(f"Extracting files for period {start} to {end}")

        extracted_data = []

        with zipfile.ZipFile(io.BytesIO(archive_content)) as outer_zip:
            # Get all nested ZIP files
            nested_zips = [f for f in outer_zip.namelist() if f.endswith('.zip')]
            logger.info(f"Archive contains {len(nested_zips)} nested ZIPs")

            for nested_zip_name in nested_zips:
                try:
                    # Extract timestamp from filename
                    # Format: PUBLIC_TRADINGIS_202510280030_0000000485200850.zip
                    parts = nested_zip_name.split('_')
                    if len(parts) < 3:
                        continue
                    timestamp_str = parts[2][:12]  # YYYYMMDDHHMM
                    file_time = pd.to_datetime(timestamp_str, format='%Y%m%d%H%M')

                    # Check if within our period (with buffer for 30-min aggregation)
                    # Need files 25 minutes before start to 5 minutes after end
                    buffer_start = start - timedelta(minutes=30)
                    buffer_end = end + timedelta(minutes=5)

                    if buffer_start <= file_time <= buffer_end:
                        # Extract the nested ZIP
                        nested_zip_content = outer_zip.read(nested_zip_name)

                        # Open nested ZIP and extract CSV
                        with zipfile.ZipFile(io.BytesIO(nested_zip_content)) as inner_zip:
                            csv_files = [f for f in inner_zip.namelist() if f.endswith('.CSV') or f.endswith('.csv')]

                            if csv_files:
                                csv_content = inner_zip.read(csv_files[0])
                                extracted_data.append((file_time, csv_content))

                except Exception as e:
                    logger.debug(f"Error extracting {nested_zip_name}: {e}")
                    continue

        extracted_data.sort(key=lambda x: x[0])
        logger.info(f"Extracted {len(extracted_data)} files for period")
        return extracted_data

    def parse_price_data(self, csv_content: bytes) -> pd.DataFrame:
        """Parse PRICE table from MMS CSV"""
        df = self.collector.parse_mms_csv(csv_content, 'PRICE')

        if df.empty or 'SETTLEMENTDATE' not in df.columns:
            return pd.DataFrame()

        price_df = pd.DataFrame()
        price_df['settlementdate'] = pd.to_datetime(
            df['SETTLEMENTDATE'].str.strip('"'),
            format='%Y/%m/%d %H:%M:%S'
        )

        if 'REGIONID' in df.columns and 'RRP' in df.columns:
            price_df['regionid'] = df['REGIONID'].str.strip()
            price_df['rrp'] = pd.to_numeric(df['RRP'], errors='coerce')

            # Filter to main regions
            main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
            price_df = price_df[price_df['regionid'].isin(main_regions)]

        return price_df

    def parse_transmission_data(self, csv_content: bytes) -> pd.DataFrame:
        """Parse INTERCONNECTORRES table from MMS CSV"""
        df = self.collector.parse_mms_csv(csv_content, 'INTERCONNECTORRES')

        if df.empty or 'SETTLEMENTDATE' not in df.columns:
            return pd.DataFrame()

        trans_df = pd.DataFrame()
        trans_df['settlementdate'] = pd.to_datetime(
            df['SETTLEMENTDATE'].str.strip('"'),
            format='%Y/%m/%d %H:%M:%S'
        )

        if 'INTERCONNECTORID' in df.columns and 'METEREDMWFLOW' in df.columns:
            trans_df['interconnectorid'] = df['INTERCONNECTORID'].str.strip()
            trans_df['meteredmwflow'] = pd.to_numeric(df['METEREDMWFLOW'], errors='coerce')
            trans_df = trans_df[trans_df['meteredmwflow'].notna()]

        return trans_df

    def aggregate_to_30min(self, df_5min: pd.DataFrame, group_cols: list, value_col: str) -> pd.DataFrame:
        """
        Aggregate 5-minute data to 30-minute intervals.

        AEMO convention: 30-min timestamps represent the END of the interval.
        12:30:00 = average of 12:05, 12:10, 12:15, 12:20, 12:25, 12:30
        """
        if df_5min.empty:
            return pd.DataFrame()

        # Find 30-minute endpoints
        unique_times = df_5min['settlementdate'].unique()
        endpoints = [t for t in unique_times if pd.Timestamp(t).minute in [0, 30]]

        if not endpoints:
            logger.warning("No 30-minute endpoints found in data")
            return pd.DataFrame()

        logger.info(f"Aggregating {len(df_5min)} 5-min records to {len(endpoints)} 30-min endpoints")

        aggregated_records = []

        for endpoint in sorted(endpoints):
            endpoint = pd.Timestamp(endpoint)
            window_start = endpoint - pd.Timedelta(minutes=30)

            # Get all 5-minute records in this window
            window_data = df_5min[
                (df_5min['settlementdate'] > window_start) &
                (df_5min['settlementdate'] <= endpoint)
            ]

            # Group and aggregate
            for group_key, group_data in window_data.groupby(group_cols):
                if len(group_data) > 0:
                    avg_value = group_data[value_col].mean()

                    record = {'settlementdate': endpoint, value_col: avg_value}
                    if isinstance(group_key, tuple):
                        for i, col in enumerate(group_cols):
                            record[col] = group_key[i]
                    else:
                        record[group_cols[0]] = group_key

                    aggregated_records.append(record)

        if aggregated_records:
            result = pd.DataFrame(aggregated_records)
            result = result.drop_duplicates(subset=['settlementdate'] + group_cols)
            result = result.sort_values(['settlementdate'] + group_cols)
            return result

        return pd.DataFrame()

    def backfill_data_type(self, start: datetime, end: datetime, data_type: str, test_only: bool = False) -> bool:
        """
        Backfill 30-minute data for a specific type and period.
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Backfilling {data_type}30 from {start} to {end}")
        logger.info(f"{'='*60}")

        # Find and download weekly archive(s)
        # May need multiple archives if period spans weeks
        dates_to_check = pd.date_range(start.date(), end.date(), freq='D')
        archive_files = set()

        for date in dates_to_check:
            try:
                archive = self.find_weekly_archive(date)
                archive_files.add(archive)
            except ValueError as e:
                logger.warning(f"No archive for {date}: {e}")

        if not archive_files:
            logger.error("No archives found for period")
            return False

        logger.info(f"Need {len(archive_files)} archive(s): {archive_files}")

        # Collect all 5-minute data
        all_5min_data = []

        for archive_file in archive_files:
            try:
                archive_content = self.download_weekly_archive(archive_file)
                extracted_files = self.extract_files_for_period(archive_content, start, end)

                for file_time, csv_content in extracted_files:
                    if data_type == 'prices':
                        df = self.parse_price_data(csv_content)
                    else:  # transmission
                        df = self.parse_transmission_data(csv_content)

                    if not df.empty:
                        all_5min_data.append(df)

            except Exception as e:
                logger.error(f"Error processing {archive_file}: {e}")
                continue

        if not all_5min_data:
            logger.error(f"No 5-minute data collected for {data_type}")
            return False

        # Combine and deduplicate 5-minute data
        combined_5min = pd.concat(all_5min_data, ignore_index=True)

        if data_type == 'prices':
            combined_5min = combined_5min.drop_duplicates(subset=['settlementdate', 'regionid'])
            group_cols = ['regionid']
            value_col = 'rrp'
            output_file = self.collector.output_files['prices30']
            key_columns = ['settlementdate', 'regionid']
        else:  # transmission
            combined_5min = combined_5min.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
            group_cols = ['interconnectorid']
            value_col = 'meteredmwflow'
            output_file = self.collector.output_files['transmission30']
            key_columns = ['settlementdate', 'interconnectorid']

        logger.info(f"Collected {len(combined_5min)} 5-min {data_type} records")

        # Aggregate to 30-minute
        aggregated_df = self.aggregate_to_30min(combined_5min, group_cols, value_col)

        if aggregated_df.empty:
            logger.error(f"No 30-minute data after aggregation")
            return False

        # Filter to requested period
        aggregated_df = aggregated_df[
            (aggregated_df['settlementdate'] >= start) &
            (aggregated_df['settlementdate'] <= end)
        ]

        logger.info(f"Aggregated to {len(aggregated_df)} 30-min records")

        if test_only:
            logger.info("TEST MODE - showing sample data:")
            print(aggregated_df.head(20))
            return True

        # Merge with existing data
        success = self.collector.merge_and_save(aggregated_df, output_file, key_columns)

        if success:
            logger.info(f"✓ Successfully backfilled {data_type}30")
        else:
            logger.error(f"✗ Failed to merge {data_type}30")

        return success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Backfill 30-minute data from AEMO TradingIS Archive',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill Oct 28 price gaps
  python backfill_30min_from_archive.py --start "2025-10-28 02:00" --end "2025-10-28 08:00" --type prices

  # Test mode
  python backfill_30min_from_archive.py --start "2025-11-16 06:00" --end "2025-11-16 14:00" --type all --test
        """
    )
    parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Start datetime (format: "YYYY-MM-DD HH:MM")'
    )
    parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='End datetime (format: "YYYY-MM-DD HH:MM")'
    )
    parser.add_argument(
        '--type',
        choices=['prices', 'transmission', 'all'],
        default='all',
        help='Type of data to backfill (default: all)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode - download and parse only, no merge'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = pd.to_datetime(args.start)
    end_date = pd.to_datetime(args.end)

    logger.info("="*60)
    logger.info("AEMO 30-Minute Archive Backfill Tool")
    logger.info("="*60)
    logger.info(f"Period: {start_date} to {end_date}")
    logger.info(f"Type: {args.type}")
    logger.info(f"Mode: {'TEST' if args.test else 'FULL BACKFILL'}")
    logger.info("="*60)

    # Initialize collector and backfill tool
    collector = UnifiedAEMOCollector()
    backfill_tool = ThirtyMinuteArchiveBackfillTool(collector)

    # Determine which data types to backfill
    if args.type == 'all':
        data_types = ['prices', 'transmission']
    else:
        data_types = [args.type]

    # Run backfill for each type
    results = {}
    for data_type in data_types:
        try:
            success = backfill_tool.backfill_data_type(
                start_date,
                end_date,
                data_type,
                test_only=args.test
            )
            results[data_type] = success
        except Exception as e:
            logger.error(f"Error backfilling {data_type}: {e}", exc_info=True)
            results[data_type] = False

    # Summary
    logger.info("\n" + "="*60)
    logger.info("BACKFILL SUMMARY")
    logger.info("="*60)
    for data_type, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{data_type}30        : {status}")
    logger.info("="*60)

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    exit(main())
