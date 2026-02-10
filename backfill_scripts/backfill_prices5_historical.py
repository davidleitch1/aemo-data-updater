#!/usr/bin/env python3
"""
Backfill prices5 from MMSDM Historical Archive

Downloads monthly DISPATCHPRICE files from AEMO MMSDM archive
and builds complete historical prices5.parquet file.

Usage:
    python backfill_prices5_historical.py --start-year 2022 --start-month 1 --end-year 2024 --end-month 6
    python backfill_prices5_historical.py --start-year 2022 --start-month 1 --end-year 2024 --end-month 6 --test
"""

import pandas as pd
import sys
sys.path.insert(0, '../src')

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector
import requests
import zipfile
import io
from datetime import datetime
import argparse
from pathlib import Path

class MMSDMPriceBackfill:
    """Backfill prices5 from MMSDM archive"""

    def __init__(self):
        self.collector = UnifiedAEMOCollector()
        self.base_url = "http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"

    def get_monthly_url(self, year: int, month: int) -> str:
        """Generate URL for monthly MMSDM file"""
        return (f"{self.base_url}/{year}/MMSDM_{year}_{month:02d}/"
                f"MMSDM_Historical_Data_SQLLoader/DATA/"
                f"PUBLIC_DVD_DISPATCHPRICE_{year}{month:02d}010000.zip")

    def download_month(self, year: int, month: int) -> bytes:
        """Download monthly MMSDM file"""
        url = self.get_monthly_url(year, month)
        print(f"  Downloading {year}-{month:02d}...", end=' ', flush=True)

        try:
            response = requests.get(url, headers={'User-Agent': 'AEMO Dashboard'}, timeout=300)
            response.raise_for_status()
            size_mb = len(response.content) / (1024 * 1024)
            print(f"✓ ({size_mb:.1f} MB)")
            return response.content
        except Exception as e:
            print(f"✗ Error: {e}")
            raise

    def parse_month(self, zip_content: bytes, year: int, month: int) -> pd.DataFrame:
        """Parse monthly MMSDM ZIP file"""
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            # Extract the CSV file (should be only one)
            csv_files = [f for f in z.namelist() if f.endswith('.CSV')]
            if not csv_files:
                raise ValueError(f"No CSV file found in {year}-{month:02d} archive")

            csv_content = z.read(csv_files[0])

        # Parse using unified collector
        df = self.collector.parse_mms_csv(csv_content, 'PRICE')

        if df.empty or 'SETTLEMENTDATE' not in df.columns:
            raise ValueError(f"No PRICE data found in {year}-{month:02d}")

        # Clean and format like prices5
        price_df = pd.DataFrame()
        price_df['settlementdate'] = pd.to_datetime(
            df['SETTLEMENTDATE'].str.strip('"'),
            format='%Y/%m/%d %H:%M:%S'
        )
        price_df['regionid'] = df['REGIONID'].str.strip()
        price_df['rrp'] = pd.to_numeric(df['RRP'], errors='coerce')

        # Filter to main regions
        main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
        price_df = price_df[price_df['regionid'].isin(main_regions)]

        print(f"    Parsed {len(price_df):,} records ({price_df['settlementdate'].min()} to {price_df['settlementdate'].max()})")

        return price_df

    def backfill(self, start_year: int, start_month: int, end_year: int, end_month: int, test_only: bool = False):
        """
        Backfill prices5 data from MMSDM archive

        Args:
            start_year: Starting year
            start_month: Starting month (1-12)
            end_year: Ending year
            end_month: Ending month (1-12)
            test_only: If True, only process first 3 months
        """
        print("="*70)
        print("MMSDM Historical Price Backfill")
        print("="*70)
        print(f"Period: {start_year}-{start_month:02d} to {end_year}-{end_month:02d}")
        print(f"Mode: {'TEST (first 3 months only)' if test_only else 'FULL BACKFILL'}")
        print("="*70)

        # Generate list of (year, month) tuples
        months = []
        current_year = start_year
        current_month = start_month

        while (current_year < end_year) or (current_year == end_year and current_month <= end_month):
            months.append((current_year, current_month))
            current_month += 1
            if current_month > 12:
                current_month = 1
                current_year += 1

        if test_only:
            months = months[:3]
            print(f"TEST MODE: Processing only first 3 months: {months}")

        print(f"\nTotal months to process: {len(months)}")
        print()

        all_data = []

        for i, (year, month) in enumerate(months, 1):
            print(f"[{i}/{len(months)}] Processing {year}-{month:02d}:")

            try:
                # Download
                zip_content = self.download_month(year, month)

                # Parse
                month_df = self.parse_month(zip_content, year, month)

                all_data.append(month_df)

            except Exception as e:
                print(f"  ✗ Failed to process {year}-{month:02d}: {e}")
                continue

        if not all_data:
            print("\n✗ No data collected!")
            return False

        # Combine all months
        print("\nCombining all data...")
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
        combined_df = combined_df.sort_values(['settlementdate', 'regionid'])

        print(f"Total records: {len(combined_df):,}")
        print(f"Date range: {combined_df['settlementdate'].min()} to {combined_df['settlementdate'].max()}")
        print(f"Regions: {sorted(combined_df['regionid'].unique())}")

        # Save to temp file
        output_file = Path("/tmp/prices5_historical_backfill.parquet")
        print(f"\nSaving to {output_file}...")
        combined_df.to_parquet(output_file, compression='snappy', index=False)

        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        print(f"✓ Saved: {file_size_mb:.1f} MB")

        print("\n" + "="*70)
        print("BACKFILL COMPLETE")
        print("="*70)
        print(f"Output file: {output_file}")
        print(f"Records: {len(combined_df):,}")
        print(f"Date range: {combined_df['settlementdate'].min()} to {combined_df['settlementdate'].max()}")
        print("\nNext step: Merge with existing prices5.parquet")
        print("="*70)

        return True


def main():
    parser = argparse.ArgumentParser(
        description='Backfill prices5 from MMSDM historical archive',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill Jan 2022 to Jun 2024
  python backfill_prices5_historical.py --start-year 2022 --start-month 1 --end-year 2024 --end-month 6

  # Test with first 3 months only
  python backfill_prices5_historical.py --start-year 2022 --start-month 1 --end-year 2024 --end-month 6 --test
        """
    )
    parser.add_argument('--start-year', type=int, required=True, help='Start year')
    parser.add_argument('--start-month', type=int, required=True, help='Start month (1-12)')
    parser.add_argument('--end-year', type=int, required=True, help='End year')
    parser.add_argument('--end-month', type=int, required=True, help='End month (1-12)')
    parser.add_argument('--test', action='store_true', help='Test mode - process only first 3 months')

    args = parser.parse_args()

    backfill = MMSDMPriceBackfill()
    success = backfill.backfill(args.start_year, args.start_month, args.end_year, args.end_month, args.test)

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
