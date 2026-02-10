#!/usr/bin/env python3
"""
Backfill 5-minute Data Gaps

Backfills missing 5-minute data for prices, SCADA, and transmission.
Downloads files from NEMWeb Current directory, processes them,
and merges with existing parquet files.

Usage:
    python backfill_5min_gaps.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type all
    python backfill_5min_gaps.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices --test
    python backfill_5min_gaps.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type scada
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sys
import argparse
import tempfile
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FiveMinuteBackfillTool:
    """Tool for backfilling 5-minute data gaps"""

    def __init__(self, collector: UnifiedAEMOCollector):
        """Initialize backfill tool with collector"""
        self.collector = collector
        self.temp_dir = Path(tempfile.gettempdir()) / 'aemo_backfill'
        self.temp_dir.mkdir(exist_ok=True)

    def identify_files_for_period(self, start: datetime, end: datetime, data_type: str) -> list:
        """
        Identify NEMWeb files needed for the specified time period.
        Checks both Current and Archive directories.

        Args:
            start: Start datetime for gap
            end: End datetime for gap
            data_type: 'prices', 'scada', or 'transmission'

        Returns:
            List of filenames that cover the period
        """
        logger.info(f"Identifying {data_type} files for period {start} to {end}")

        # Map data types to URLs and patterns
        # Current URLs (last 1-2 days)
        current_urls = {
            'prices': {
                'url': 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/',
                'pattern': 'PUBLIC_DISPATCHIS_',
                'archive_url': 'http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/'
            },
            'scada': {
                'url': 'http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/',
                'pattern': 'PUBLIC_DISPATCHSCADA_',
                'archive_url': 'http://nemweb.com.au/Reports/ARCHIVE/Dispatch_SCADA/'
            },
            'transmission': {
                'url': 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/',
                'pattern': 'PUBLIC_DISPATCHIS_',
                'archive_url': 'http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/'
            }
        }

        config = current_urls.get(data_type)
        if not config:
            raise ValueError(f"Unknown data type: {data_type}")

        # Calculate days ago to determine if we need Archive
        days_ago = (datetime.now() - start).days

        # Try Current directory first (if within last 2 days)
        all_files = []
        source_url = None

        if days_ago <= 2:
            logger.info(f"Checking Current directory (gap is {days_ago} days ago)")
            all_files = self.collector.get_latest_files(config['url'], config['pattern'])
            source_url = config['url']

        # If not found in Current, try Archive
        if not all_files and days_ago > 1:
            logger.info(f"Checking Archive directory (gap is {days_ago} days ago)")

            # Archive has daily consolidated ZIP files: PUBLIC_DISPATCHIS_YYYYMMDD.zip
            # Each ZIP contains all 5-minute files for that day
            # We need to download and extract the daily ZIPs for our date range
            date_range = pd.date_range(start.date(), end.date(), freq='D')

            for date in date_range:
                date_str = date.strftime('%Y%m%d')
                # Daily archive ZIP filename
                daily_zip = f"{config['pattern']}{date_str}.zip"

                logger.info(f"Checking for {daily_zip}")

                # These are special - need to download and extract nested ZIPs
                all_files.append(('archive', config['archive_url'], daily_zip, date))

            source_url = 'archive'  # Flag that we're using archive
        else:
            # Convert current files to same format
            all_files = [(config['url'], f) for f in all_files]

        # Parse timestamps and filter to period
        relevant_files = []
        for url_and_file in all_files:
            try:
                # Handle tuple format (url, filename) or just filename
                if isinstance(url_and_file, tuple):
                    file_url, filename = url_and_file
                else:
                    file_url = config['url']
                    filename = url_and_file

                # Extract timestamp from filename
                # Format: PUBLIC_DISPATCHIS_202510091005_0000000... or PUBLIC_DISPATCHSCADA_...
                parts = filename.split('_')
                timestamp_str = parts[2] if len(parts) > 2 else None

                if timestamp_str:
                    file_time = pd.to_datetime(timestamp_str, format='%Y%m%d%H%M')
                    # Include files within the period (with 10-minute buffer on each side)
                    buffer_start = start - timedelta(minutes=10)
                    buffer_end = end + timedelta(minutes=10)

                    if buffer_start <= file_time <= buffer_end:
                        relevant_files.append((file_time, file_url, filename))
            except Exception as e:
                logger.debug(f"Could not parse timestamp from {filename}: {e}")
                continue

        # Sort by timestamp
        relevant_files.sort(key=lambda x: x[0])
        # Return list of (url, filename) tuples
        url_filename_pairs = [(f[1], f[2]) for f in relevant_files]

        logger.info(f"Found {len(url_filename_pairs)} files covering the period")
        if url_filename_pairs:
            logger.info(f"First file: {url_filename_pairs[0][1]}")
            logger.info(f"Last file: {url_filename_pairs[-1][1]}")

        return url_filename_pairs

    def test_download_file(self, data_type: str, url_filename: tuple) -> pd.DataFrame:
        """
        Download and parse one test file to verify structure.

        Args:
            data_type: 'prices', 'scada', or 'transmission'
            url_filename: Tuple of (url, filename)

        Returns:
            DataFrame with parsed data
        """
        url, filename = url_filename
        logger.info(f"Testing download of {filename} from {url}")

        # Map data types to table names
        table_names = {
            'prices': 'PRICE',
            'scada': 'UNIT_SCADA',
            'transmission': 'INTERCONNECTORRES'
        }

        table_name = table_names.get(data_type)
        if not table_name:
            raise ValueError(f"Unknown data type: {data_type}")

        # Download and parse
        df = self.collector.download_and_parse_file(
            url,
            filename,
            table_name
        )

        if not df.empty:
            logger.info(f"Test file downloaded successfully")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"Records: {len(df)}")
            logger.info(f"Sample:\n{df.head()}")
        else:
            logger.warning(f"Test file returned empty DataFrame")

        return df

    def download_all_files(self, data_type: str, url_filename_pairs: list) -> pd.DataFrame:
        """
        Download and parse all files for the period.

        Args:
            data_type: 'prices', 'scada', or 'transmission'
            url_filename_pairs: List of (url, filename) tuples to download

        Returns:
            Combined DataFrame with all data
        """
        logger.info(f"Downloading {len(url_filename_pairs)} files for {data_type}")

        # Map data types to table names and processors
        config = {
            'prices': {
                'table': 'PRICE',
                'processor': self._process_price_data
            },
            'scada': {
                'table': 'UNIT_SCADA',
                'processor': self._process_scada_data
            },
            'transmission': {
                'table': 'INTERCONNECTORRES',
                'processor': self._process_transmission_data
            }
        }.get(data_type)

        if not config:
            raise ValueError(f"Unknown data type: {data_type}")

        all_data = []
        for i, (url, filename) in enumerate(url_filename_pairs):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(url_filename_pairs)} files")

            # Small delay to avoid rate limiting
            time.sleep(0.1)

            # Download and parse
            df = self.collector.download_and_parse_file(
                url,
                filename,
                config['table']
            )

            if not df.empty:
                # Process data using type-specific processor
                processed_df = config['processor'](df)
                if not processed_df.empty:
                    all_data.append(processed_df)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            # Remove duplicates based on data type
            if data_type == 'prices':
                combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            elif data_type == 'scada':
                combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'duid'])
            elif data_type == 'transmission':
                combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'interconnectorid'])

            combined_df = combined_df.sort_values(combined_df.columns.tolist())
            logger.info(f"Downloaded and processed {len(combined_df)} records")
            return combined_df
        else:
            logger.warning("No data downloaded")
            return pd.DataFrame()

    def _process_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw price data into clean format"""
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

    def _process_scada_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw SCADA data into clean format"""
        if df.empty or 'SETTLEMENTDATE' not in df.columns:
            return pd.DataFrame()

        scada_df = pd.DataFrame()
        scada_df['settlementdate'] = pd.to_datetime(
            df['SETTLEMENTDATE'].str.strip('"'),
            format='%Y/%m/%d %H:%M:%S'
        )

        if 'DUID' in df.columns and 'SCADAVALUE' in df.columns:
            scada_df['duid'] = df['DUID'].str.strip()
            scada_df['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')

            # Filter out invalid values
            scada_df = scada_df[scada_df['scadavalue'].notna()]

        return scada_df

    def _process_transmission_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process raw transmission data into clean format"""
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

            # Filter out invalid values
            trans_df = trans_df[trans_df['meteredmwflow'].notna()]

        return trans_df

    def backfill_data_type(self, start: datetime, end: datetime, data_type: str, test_only: bool = False) -> bool:
        """
        Backfill data for a specific type and period.

        Args:
            start: Start datetime
            end: End datetime
            data_type: 'prices', 'scada', or 'transmission'
            test_only: If True, only test download without merging

        Returns:
            True if successful
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Backfilling {data_type} from {start} to {end}")
        logger.info(f"{'='*60}")

        # Step 1: Identify files
        filenames = self.identify_files_for_period(start, end, data_type)
        if not filenames:
            logger.warning(f"No files found for {data_type} in period")
            return False

        # Step 2: Test download one file
        test_df = self.test_download_file(data_type, filenames[0])
        if test_df.empty:
            logger.error(f"Test download failed for {data_type}")
            return False

        if test_only:
            logger.info("Test mode - stopping here")
            return True

        # Step 3: Download all files
        backfill_df = self.download_all_files(data_type, filenames)
        if backfill_df.empty:
            logger.error(f"No data downloaded for {data_type}")
            return False

        # Step 4: Save to temp parquet
        temp_file = self.temp_dir / f'{data_type}5_backfill_temp.parquet'
        backfill_df.to_parquet(temp_file, compression='snappy', index=False)
        logger.info(f"Saved {len(backfill_df)} records to {temp_file}")

        # Step 5: Merge with existing parquet
        output_file_map = {
            'prices': self.collector.output_files['prices5'],
            'scada': self.collector.output_files['scada5'],
            'transmission': self.collector.output_files['transmission5']
        }

        key_columns_map = {
            'prices': ['settlementdate', 'regionid'],
            'scada': ['settlementdate', 'duid'],
            'transmission': ['settlementdate', 'interconnectorid']
        }

        output_file = output_file_map[data_type]
        key_columns = key_columns_map[data_type]

        success = self.collector.merge_and_save(backfill_df, output_file, key_columns)

        if success:
            logger.info(f"✓ Successfully backfilled {data_type}")
        else:
            logger.error(f"✗ Failed to merge {data_type}")

        return success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Backfill 5-minute data gaps',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill all types for Oct 9-10 gaps
  python backfill_5min_gaps.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type all

  # Test mode - only download and check one file
  python backfill_5min_gaps.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices --test

  # Backfill only SCADA data
  python backfill_5min_gaps.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type scada
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
        choices=['prices', 'scada', 'transmission', 'all'],
        default='all',
        help='Type of data to backfill (default: all)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode - download one file only, no merge'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = pd.to_datetime(args.start)
    end_date = pd.to_datetime(args.end)

    logger.info("="*60)
    logger.info("AEMO 5-Minute Data Backfill Tool")
    logger.info("="*60)
    logger.info(f"Period: {start_date} to {end_date}")
    logger.info(f"Type: {args.type}")
    logger.info(f"Mode: {'TEST' if args.test else 'FULL BACKFILL'}")
    logger.info("="*60)

    # Initialize collector and backfill tool
    collector = UnifiedAEMOCollector()
    backfill_tool = FiveMinuteBackfillTool(collector)

    # Determine which data types to backfill
    if args.type == 'all':
        data_types = ['prices', 'scada', 'transmission']
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
            logger.error(f"Error backfilling {data_type}: {e}")
            results[data_type] = False

    # Summary
    logger.info("\n" + "="*60)
    logger.info("BACKFILL SUMMARY")
    logger.info("="*60)
    for data_type, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{data_type:15s}: {status}")
    logger.info("="*60)

    # Return success if all succeeded
    all_success = all(results.values())
    return 0 if all_success else 1


if __name__ == "__main__":
    exit(main())
