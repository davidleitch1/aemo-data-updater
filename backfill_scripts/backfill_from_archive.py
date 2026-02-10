#!/usr/bin/env python3
"""
Backfill from AEMO Archive (Nested ZIP Handler)

Handles AEMO Archive files which are daily ZIPs containing nested ZIPs.
Each nested ZIP contains one CSV file with 5-minute data.

Usage:
    python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices
    python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type all --test
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


class ArchiveBackfillTool:
    """Tool for backfilling from AEMO Archive nested ZIPs"""

    def __init__(self, collector: UnifiedAEMOCollector):
        """Initialize backfill tool with collector"""
        self.collector = collector

        # Archive URLs
        self.archive_urls = {
            'prices': 'http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/',
            'scada': 'http://nemweb.com.au/Reports/ARCHIVE/Dispatch_SCADA/',
            'transmission': 'http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/'
        }

        # File patterns
        self.patterns = {
            'prices': 'PUBLIC_DISPATCHIS',
            'scada': 'PUBLIC_DISPATCHSCADA',
            'transmission': 'PUBLIC_DISPATCHIS'
        }

        # Table names in CSV files
        self.table_names = {
            'prices': 'PRICE',
            'scada': 'UNIT_SCADA',
            'transmission': 'INTERCONNECTORRES'
        }

    def download_daily_archive(self, data_type: str, date: datetime) -> bytes:
        """
        Download daily archive ZIP for specific date.

        Args:
            data_type: 'prices', 'scada', or 'transmission'
            date: Date to download

        Returns:
            ZIP file content as bytes
        """
        date_str = date.strftime('%Y%m%d')
        filename = f"{self.patterns[data_type]}_{date_str}.zip"
        url = f"{self.archive_urls[data_type]}{filename}"

        logger.info(f"Downloading {filename} ({url})")

        try:
            response = requests.get(url, headers={'User-Agent': 'AEMO Dashboard'}, timeout=300)
            response.raise_for_status()
            logger.info(f"Downloaded {len(response.content):,} bytes")
            return response.content
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.error(f"Archive file not found: {filename}")
                logger.error(f"This may mean the date is too recent or data not yet archived")
            raise

    def extract_files_for_period(self, archive_content: bytes, start: datetime, end: datetime, data_type: str) -> list:
        """
        Extract nested ZIPs for specific time period from daily archive.

        Args:
            archive_content: Daily archive ZIP content
            start: Start datetime
            end: End datetime
            data_type: Data type

        Returns:
            List of (timestamp, csv_content) tuples
        """
        logger.info(f"Extracting files for period {start} to {end}")

        extracted_data = []

        with zipfile.ZipFile(io.BytesIO(archive_content)) as outer_zip:
            # Get all nested ZIP files
            nested_zips = [f for f in outer_zip.namelist() if f.endswith('.zip')]
            logger.info(f"Archive contains {len(nested_zips)} nested ZIPs")

            # Filter to time period
            for nested_zip_name in nested_zips:
                try:
                    # Extract timestamp from filename
                    # Format: PUBLIC_DISPATCHIS_202510091005_0000000484092485.zip
                    parts = nested_zip_name.split('_')
                    timestamp_str = parts[2][:12]  # YYYYMMDDHHMM
                    file_time = pd.to_datetime(timestamp_str, format='%Y%m%d%H%M')

                    # AEMO files are named with END timestamp but contain data for 5-min BEFORE
                    # E.g., file "1010" has SETTLEMENTDATE 10:05
                    # So to get data for 10:00-13:00, we need files 1005-1305
                    file_start = start + timedelta(minutes=5)
                    file_end = end + timedelta(minutes=5)

                    if file_start <= file_time <= file_end:
                        # Extract the nested ZIP
                        nested_zip_content = outer_zip.read(nested_zip_name)

                        # Open nested ZIP and extract CSV
                        with zipfile.ZipFile(io.BytesIO(nested_zip_content)) as inner_zip:
                            csv_files = [f for f in inner_zip.namelist() if f.endswith('.CSV') or f.endswith('.csv')]

                            if csv_files:
                                csv_content = inner_zip.read(csv_files[0])
                                extracted_data.append((file_time, csv_content))
                                logger.debug(f"Extracted {file_time}: {len(csv_content):,} bytes")

                except Exception as e:
                    logger.warning(f"Error extracting {nested_zip_name}: {e}")
                    continue

        logger.info(f"Extracted {len(extracted_data)} files for period")
        return extracted_data

    def parse_csv_data(self, csv_content: bytes, table_name: str) -> pd.DataFrame:
        """
        Parse MMS format CSV content.

        Args:
            csv_content: CSV file content as bytes
            table_name: Table name to extract

        Returns:
            DataFrame with parsed data
        """
        return self.collector.parse_mms_csv(csv_content, table_name)

    def process_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
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

    def process_scada_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
            scada_df = scada_df[scada_df['scadavalue'].notna()]

        return scada_df

    def process_transmission_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
            trans_df = trans_df[trans_df['meteredmwflow'].notna()]

        return trans_df

    def backfill_data_type(self, start: datetime, end: datetime, data_type: str, test_only: bool = False) -> bool:
        """
        Backfill data for a specific type and period from Archive.

        Args:
            start: Start datetime
            end: End datetime
            data_type: 'prices', 'scada', or 'transmission'
            test_only: If True, only download and parse without merging

        Returns:
            True if successful
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Backfilling {data_type} from {start} to {end}")
        logger.info(f"{'='*60}")

        # Determine which daily archives we need
        date_range = pd.date_range(start.date(), end.date(), freq='D')
        logger.info(f"Need archives for {len(date_range)} day(s): {[d.strftime('%Y-%m-%d') for d in date_range]}")

        all_data = []

        # Process each day's archive
        for date in date_range:
            try:
                # Step 1: Download daily archive
                archive_content = self.download_daily_archive(data_type, date)

                # Step 2: Extract files for our time period
                extracted_files = self.extract_files_for_period(archive_content, start, end, data_type)

                if not extracted_files:
                    logger.warning(f"No files extracted for {date.strftime('%Y-%m-%d')}")
                    continue

                # Step 3: Parse CSV data
                processor_map = {
                    'prices': self.process_price_data,
                    'scada': self.process_scada_data,
                    'transmission': self.process_transmission_data
                }

                processor = processor_map[data_type]
                table_name = self.table_names[data_type]

                for file_time, csv_content in extracted_files:
                    df = self.parse_csv_data(csv_content, table_name)
                    if not df.empty:
                        processed_df = processor(df)
                        if not processed_df.empty:
                            all_data.append(processed_df)

                logger.info(f"Processed {len(extracted_files)} files from {date.strftime('%Y-%m-%d')}")

            except Exception as e:
                logger.error(f"Error processing {date.strftime('%Y-%m-%d')}: {e}")
                continue

        if not all_data:
            logger.error(f"No data collected for {data_type}")
            return False

        # Step 4: Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)

        # Remove duplicates
        key_columns_map = {
            'prices': ['settlementdate', 'regionid'],
            'scada': ['settlementdate', 'duid'],
            'transmission': ['settlementdate', 'interconnectorid']
        }
        key_columns = key_columns_map[data_type]
        combined_df = combined_df.drop_duplicates(subset=key_columns)
        combined_df = combined_df.sort_values(key_columns)

        logger.info(f"Combined data: {len(combined_df)} records")

        if test_only:
            logger.info("TEST MODE - showing sample data:")
            print(combined_df.head(10))
            return True

        # Step 5: Merge with existing parquet
        output_file_map = {
            'prices': self.collector.output_files['prices5'],
            'scada': self.collector.output_files['scada5'],
            'transmission': self.collector.output_files['transmission5']
        }

        output_file = output_file_map[data_type]
        success = self.collector.merge_and_save(combined_df, output_file, key_columns)

        if success:
            logger.info(f"✓ Successfully backfilled {data_type}")
        else:
            logger.error(f"✗ Failed to merge {data_type}")

        return success


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Backfill from AEMO Archive (nested ZIP handler)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill Oct 9 price gaps
  python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices

  # Test mode - download and parse but don't merge
  python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices --test

  # Backfill all data types
  python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type all
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
        help='Test mode - download and parse only, no merge'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = pd.to_datetime(args.start)
    end_date = pd.to_datetime(args.end)

    logger.info("="*60)
    logger.info("AEMO Archive Backfill Tool (Nested ZIP Handler)")
    logger.info("="*60)
    logger.info(f"Period: {start_date} to {end_date}")
    logger.info(f"Type: {args.type}")
    logger.info(f"Mode: {'TEST' if args.test else 'FULL BACKFILL'}")
    logger.info("="*60)

    # Initialize collector and backfill tool
    collector = UnifiedAEMOCollector()
    backfill_tool = ArchiveBackfillTool(collector)

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
            logger.error(f"Error backfilling {data_type}: {e}", exc_info=True)
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
