#!/usr/bin/env python3
"""
Backfill Regional Curtailment Data from AEMO Archive

Downloads DispatchIS files from AEMO Archive and extracts DISPATCHREGIONSUM table
to calculate regional curtailment based on UIGF (Unconstrained Intermittent Generation Forecast).

Curtailment = UIGF - CLEAREDMW

This creates a fresh curtailment_regional5.parquet file with the schema:
    settlementdate, regionid, solar_uigf, solar_cleared, solar_curtailment,
    wind_uigf, wind_cleared, wind_curtailment, total_curtailment

Usage:
    python backfill_regional_curtailment.py --start "2025-07-01" --end "2026-01-05"
    python backfill_regional_curtailment.py --start "2025-07-01" --end "2026-01-05" --test
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


class RegionalCurtailmentBackfiller:
    """Backfill regional curtailment data from AEMO DispatchIS Archive"""

    def __init__(self, data_path: Path):
        """Initialize backfiller with data path"""
        self.data_path = data_path
        self.archive_url = 'http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/'
        self.headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        self.main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']

    def get_archive_files_for_date(self, date: datetime) -> str:
        """Get the archive filename for a specific date"""
        date_str = date.strftime('%Y%m%d')
        return f"PUBLIC_DISPATCHIS_{date_str}.zip"

    def download_daily_archive(self, date: datetime) -> bytes:
        """Download daily archive ZIP for specific date"""
        filename = self.get_archive_files_for_date(date)
        url = f"{self.archive_url}{filename}"

        logger.debug(f"Downloading {filename}")

        try:
            response = requests.get(url, headers=self.headers, timeout=300)
            response.raise_for_status()
            logger.debug(f"Downloaded {len(response.content):,} bytes")
            return response.content
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Archive file not found: {filename}")
            raise

    def parse_mms_csv(self, content: bytes, table_name: str) -> pd.DataFrame:
        """Parse MMS format CSV content for specific table"""
        try:
            lines = content.decode('utf-8', errors='ignore').strip().split('\n')

            header_row = None
            data_rows = []

            for line in lines:
                if not line.strip():
                    continue

                parts = line.split(',')

                if parts[0] == 'C':  # Comment row
                    continue
                elif parts[0] == 'I' and len(parts) > 2:
                    if parts[2] == table_name:
                        header_row = parts
                elif parts[0] == 'D' and len(parts) > 2:
                    if parts[2] == table_name:
                        data_rows.append(parts[1:])

            if header_row and data_rows:
                columns = header_row[4:]
                columns = [col.strip().replace('\r', '') for col in columns]

                df = pd.DataFrame(data_rows, columns=['ROW_TYPE', 'DATA_TYPE', 'VERSION'] + columns)
                df = df.drop(['ROW_TYPE', 'DATA_TYPE', 'VERSION'], axis=1)

                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error parsing MMS CSV for {table_name}: {e}")
            return pd.DataFrame()

    def extract_curtailment_from_archive(self, archive_content: bytes) -> pd.DataFrame:
        """Extract regional curtailment data from daily archive"""
        all_data = []

        try:
            with zipfile.ZipFile(io.BytesIO(archive_content)) as outer_zip:
                # Get all nested ZIP files
                nested_zips = [f for f in outer_zip.namelist() if f.endswith('.zip')]

                for nested_zip_name in nested_zips:
                    try:
                        # Extract the nested ZIP
                        nested_zip_content = outer_zip.read(nested_zip_name)

                        # Open nested ZIP and extract CSV
                        with zipfile.ZipFile(io.BytesIO(nested_zip_content)) as inner_zip:
                            csv_files = [f for f in inner_zip.namelist() if f.endswith('.CSV') or f.endswith('.csv')]

                            if csv_files:
                                csv_content = inner_zip.read(csv_files[0])
                                df = self.parse_mms_csv(csv_content, 'REGIONSUM')

                                if not df.empty and 'SETTLEMENTDATE' in df.columns:
                                    curtail_df = self.process_dispatchregionsum(df)
                                    if not curtail_df.empty:
                                        all_data.append(curtail_df)

                    except Exception as e:
                        logger.debug(f"Error processing {nested_zip_name}: {e}")
                        continue

        except Exception as e:
            logger.error(f"Error processing archive: {e}")
            return pd.DataFrame()

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined = combined.drop_duplicates(subset=['settlementdate', 'regionid'])
            return combined

        return pd.DataFrame()

    def process_dispatchregionsum(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process DISPATCHREGIONSUM data into curtailment format"""
        if df.empty or 'SETTLEMENTDATE' not in df.columns:
            return pd.DataFrame()

        curtail_df = pd.DataFrame()
        curtail_df['settlementdate'] = pd.to_datetime(
            df['SETTLEMENTDATE'].str.strip('"'),
            format='%Y/%m/%d %H:%M:%S'
        )

        if 'REGIONID' not in df.columns:
            return pd.DataFrame()

        curtail_df['regionid'] = df['REGIONID'].str.strip()

        # Extract UIGF and cleared values with fallbacks
        curtail_df['solar_uigf'] = pd.to_numeric(
            df.get('SS_SOLAR_UIGF', pd.Series([0] * len(df))), errors='coerce'
        ).fillna(0)

        curtail_df['solar_cleared'] = pd.to_numeric(
            df.get('SS_SOLAR_CLEAREDMW', pd.Series([0] * len(df))), errors='coerce'
        ).fillna(0)

        curtail_df['wind_uigf'] = pd.to_numeric(
            df.get('SS_WIND_UIGF', pd.Series([0] * len(df))), errors='coerce'
        ).fillna(0)

        curtail_df['wind_cleared'] = pd.to_numeric(
            df.get('SS_WIND_CLEAREDMW', pd.Series([0] * len(df))), errors='coerce'
        ).fillna(0)

        # Calculate curtailment (UIGF - Cleared, minimum 0)
        curtail_df['solar_curtailment'] = (curtail_df['solar_uigf'] - curtail_df['solar_cleared']).clip(lower=0)
        curtail_df['wind_curtailment'] = (curtail_df['wind_uigf'] - curtail_df['wind_cleared']).clip(lower=0)
        curtail_df['total_curtailment'] = curtail_df['solar_curtailment'] + curtail_df['wind_curtailment']

        # Filter to main regions
        curtail_df = curtail_df[curtail_df['regionid'].isin(self.main_regions)]

        return curtail_df

    def backfill_date_range(self, start_date: datetime, end_date: datetime, test_only: bool = False) -> pd.DataFrame:
        """Backfill curtailment data for a date range"""
        logger.info(f"Backfilling regional curtailment from {start_date.date()} to {end_date.date()}")

        all_data = []
        current_date = start_date
        days_processed = 0
        days_failed = 0

        while current_date <= end_date:
            try:
                logger.info(f"Processing {current_date.date()}...")

                archive_content = self.download_daily_archive(current_date)
                df = self.extract_curtailment_from_archive(archive_content)

                if not df.empty:
                    all_data.append(df)
                    logger.info(f"  Extracted {len(df)} records")
                    days_processed += 1
                else:
                    logger.warning(f"  No curtailment data found for {current_date.date()}")
                    days_failed += 1

                # Brief pause to be nice to the server
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error processing {current_date.date()}: {e}")
                days_failed += 1

            current_date += timedelta(days=1)

        logger.info(f"Processed {days_processed} days successfully, {days_failed} days failed")

        if not all_data:
            logger.error("No data collected")
            return pd.DataFrame()

        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
        combined_df = combined_df.sort_values(['settlementdate', 'regionid'])

        logger.info(f"Total records: {len(combined_df)}")
        logger.info(f"Date range: {combined_df['settlementdate'].min()} to {combined_df['settlementdate'].max()}")

        if test_only:
            logger.info("TEST MODE - showing sample data:")
            print("\nFirst 10 records:")
            print(combined_df.head(10))
            print("\nLast 10 records:")
            print(combined_df.tail(10))
            print(f"\nTotal curtailment summary:")
            print(combined_df.groupby('regionid')['total_curtailment'].sum())
            return combined_df

        return combined_df


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Backfill regional curtailment data from AEMO Archive',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill from July 1 2025 to today
  python backfill_regional_curtailment.py --start "2025-07-01" --end "2026-01-05"

  # Test mode - download and parse but don't save
  python backfill_regional_curtailment.py --start "2025-07-01" --end "2025-07-05" --test
        """
    )
    parser.add_argument(
        '--start',
        type=str,
        required=True,
        help='Start date (format: "YYYY-MM-DD")'
    )
    parser.add_argument(
        '--end',
        type=str,
        required=True,
        help='End date (format: "YYYY-MM-DD")'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode - download and parse only, no save'
    )
    parser.add_argument(
        '--data-path',
        type=str,
        default='/Volumes/davidleitch/aemo_production/data',
        help='Path to data directory'
    )

    args = parser.parse_args()

    # Parse dates
    start_date = pd.to_datetime(args.start)
    end_date = pd.to_datetime(args.end)
    data_path = Path(args.data_path)

    logger.info("=" * 60)
    logger.info("Regional Curtailment Backfill Tool")
    logger.info("=" * 60)
    logger.info(f"Period: {start_date.date()} to {end_date.date()}")
    logger.info(f"Data path: {data_path}")
    logger.info(f"Mode: {'TEST' if args.test else 'FULL BACKFILL'}")
    logger.info("=" * 60)

    # Initialize backfiller
    backfiller = RegionalCurtailmentBackfiller(data_path)

    # Run backfill
    start_time = time.time()
    result_df = backfiller.backfill_date_range(start_date, end_date, test_only=args.test)
    elapsed = time.time() - start_time

    if result_df.empty:
        logger.error("No data collected")
        return 1

    if not args.test:
        # Save to parquet (create fresh file)
        output_file = data_path / 'curtailment_regional5.parquet'

        logger.info(f"Saving {len(result_df)} records to {output_file}")
        result_df.to_parquet(output_file, compression='snappy', index=False)
        logger.info(f"Saved successfully")

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("BACKFILL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total records: {len(result_df):,}")
    logger.info(f"Unique timestamps: {result_df['settlementdate'].nunique():,}")
    logger.info(f"Date range: {result_df['settlementdate'].min()} to {result_df['settlementdate'].max()}")
    logger.info(f"Execution time: {elapsed:.1f} seconds")

    # Curtailment summary by region
    logger.info("\nTotal curtailment by region (MW-intervals):")
    for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
        region_data = result_df[result_df['regionid'] == region]
        solar = region_data['solar_curtailment'].sum()
        wind = region_data['wind_curtailment'].sum()
        total = region_data['total_curtailment'].sum()
        logger.info(f"  {region}: Solar={solar:,.0f} Wind={wind:,.0f} Total={total:,.0f}")

    logger.info("=" * 60)

    return 0


if __name__ == "__main__":
    exit(main())
