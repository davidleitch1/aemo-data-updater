#!/usr/bin/env python3
"""
Backfill DUID-level curtailment data from July 1, 2025 onwards.

Uses UIGF (Unconstrained Intermittent Generation Forecast) from Next_Day_Dispatch
UNIT_SOLUTION table. UIGF is what a plant COULD generate, TOTALCLEARED is what was dispatched.
Curtailment = UIGF - TOTALCLEARED (clipped to 0)

Monthly archives: http://nemweb.com.au/Reports/ARCHIVE/Next_Day_Dispatch/
Current files: http://nemweb.com.au/Reports/Current/Next_Day_Dispatch/
"""

import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import time
import os
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DATA_PATH = Path(os.getenv('AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data'))
OUTPUT_FILE = DATA_PATH / 'curtailment_duid5.parquet'
HEADERS = {'User-Agent': 'AEMO Dashboard Data Collector'}

ARCHIVE_URL = 'http://nemweb.com.au/Reports/ARCHIVE/Next_Day_Dispatch/'
CURRENT_URL = 'http://nemweb.com.au/Reports/Current/Next_Day_Dispatch/'


def parse_unit_solution_for_uigf(content: bytes) -> pd.DataFrame:
    """
    Parse UNIT_SOLUTION records from CSV content, extracting UIGF data.

    Only includes records where UIGF > 0 (semi-scheduled renewables).
    """
    try:
        lines = content.decode('utf-8', errors='ignore').split('\n')

        all_data = []
        for line in lines:
            if not line.startswith('D,DISPATCH,UNIT_SOLUTION'):
                continue

            parts = line.split(',')
            if len(parts) <= 68:  # Need at least 69 columns for UIGF
                continue

            try:
                duid = parts[6].strip('"')
                settlementdate = parts[4].strip('"')
                totalcleared = float(parts[14]) if parts[14] else 0.0
                uigf = float(parts[68]) if parts[68] else 0.0

                # Only include records with UIGF > 0 (semi-scheduled renewables)
                if uigf > 0:
                    curtailment = max(0.0, uigf - totalcleared)

                    all_data.append({
                        'settlementdate': settlementdate,
                        'duid': duid,
                        'uigf': uigf,
                        'totalcleared': totalcleared,
                        'curtailment': curtailment
                    })
            except (ValueError, IndexError):
                continue

        if all_data:
            df = pd.DataFrame(all_data)
            df['settlementdate'] = pd.to_datetime(df['settlementdate'], format='%Y/%m/%d %H:%M:%S')
            return df

        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error parsing content: {e}")
        return pd.DataFrame()


def process_archive_file(url: str, filename: str) -> pd.DataFrame:
    """Download and process a monthly archive file."""
    try:
        file_url = f"{url}{filename}"
        logger.info(f"Downloading {filename} (~200MB, this may take a moment)...")

        response = requests.get(file_url, headers=HEADERS, timeout=300, stream=True)
        response.raise_for_status()

        content = response.content
        logger.info(f"Downloaded {len(content) / 1024 / 1024:.1f} MB, extracting...")

        all_data = []

        # Process the outer ZIP which contains daily ZIPs
        with zipfile.ZipFile(io.BytesIO(content)) as outer_zip:
            daily_zips = [f for f in outer_zip.namelist() if f.endswith('.zip')]
            logger.info(f"Found {len(daily_zips)} daily files in archive")

            for i, daily_zip_name in enumerate(sorted(daily_zips)):
                if (i + 1) % 5 == 0:
                    logger.info(f"Processing {i + 1}/{len(daily_zips)} daily files...")

                try:
                    # Extract the daily ZIP
                    daily_zip_content = outer_zip.read(daily_zip_name)

                    with zipfile.ZipFile(io.BytesIO(daily_zip_content)) as daily_zip:
                        csv_files = [f for f in daily_zip.namelist() if f.lower().endswith('.csv')]

                        for csv_file in csv_files:
                            csv_content = daily_zip.read(csv_file)
                            df = parse_unit_solution_for_uigf(csv_content)

                            if not df.empty:
                                all_data.append(df)

                except Exception as e:
                    logger.warning(f"Error processing {daily_zip_name}: {e}")
                    continue

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined = combined.drop_duplicates(subset=['settlementdate', 'duid'])
            combined = combined.sort_values(['settlementdate', 'duid'])
            return combined

        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error downloading {filename}: {e}")
        return pd.DataFrame()


def process_current_files(start_date: datetime) -> pd.DataFrame:
    """Download and process current (daily) files from a start date."""
    try:
        # Get list of available files
        logger.info(f"Getting file list from Current folder...")
        response = requests.get(CURRENT_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

        files = []
        for link in soup.find_all('a'):
            filename = link.text.strip()
            if 'PUBLIC_NEXT_DAY_DISPATCH_' in filename and filename.endswith('.zip'):
                # Extract date from filename
                try:
                    date_str = filename.replace('PUBLIC_NEXT_DAY_DISPATCH_', '').replace('.zip', '')[:8]
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    if file_date >= start_date:
                        files.append(filename)
                except:
                    continue

        logger.info(f"Found {len(files)} files to process from {start_date.date()}")

        all_data = []
        for i, filename in enumerate(sorted(files)):
            if (i + 1) % 10 == 0:
                logger.info(f"Processing {i + 1}/{len(files)} current files...")

            try:
                file_url = f"{CURRENT_URL}{filename}"
                response = requests.get(file_url, headers=HEADERS, timeout=60)
                response.raise_for_status()

                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_files = [f for f in z.namelist() if f.lower().endswith('.csv')]

                    for csv_file in csv_files:
                        csv_content = z.read(csv_file)
                        df = parse_unit_solution_for_uigf(csv_content)

                        if not df.empty:
                            all_data.append(df)

            except Exception as e:
                logger.warning(f"Error processing {filename}: {e}")
                continue

        if all_data:
            combined = pd.concat(all_data, ignore_index=True)
            combined = combined.drop_duplicates(subset=['settlementdate', 'duid'])
            combined = combined.sort_values(['settlementdate', 'duid'])
            return combined

        return pd.DataFrame()

    except Exception as e:
        logger.error(f"Error processing current files: {e}")
        return pd.DataFrame()


def main():
    logger.info("=" * 60)
    logger.info("DUID Curtailment Backfill - Starting")
    logger.info("=" * 60)
    logger.info(f"Output file: {OUTPUT_FILE}")

    all_data = []

    # Monthly archives for July-November 2025
    archive_files = [
        'PUBLIC_NEXT_DAY_DISPATCH_20250701.zip',  # July 2025 data
        'PUBLIC_NEXT_DAY_DISPATCH_20250801.zip',  # August 2025 data
        'PUBLIC_NEXT_DAY_DISPATCH_20250901.zip',  # September 2025 data
        'PUBLIC_NEXT_DAY_DISPATCH_20251001.zip',  # October 2025 data
        'PUBLIC_NEXT_DAY_DISPATCH_20251101.zip',  # November 2025 data
    ]

    for archive_file in archive_files:
        logger.info(f"\nProcessing {archive_file}...")
        df = process_archive_file(ARCHIVE_URL, archive_file)

        if not df.empty:
            logger.info(f"Extracted {len(df):,} records, {df['duid'].nunique()} DUIDs")
            logger.info(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            all_data.append(df)
        else:
            logger.warning(f"No data extracted from {archive_file}")

        # Small delay between large downloads
        time.sleep(2)

    # December 2025 from current files
    logger.info("\nProcessing December 2025 from current files...")
    december_start = datetime(2025, 12, 1)
    current_df = process_current_files(december_start)

    if not current_df.empty:
        logger.info(f"Extracted {len(current_df):,} records from current files")
        all_data.append(current_df)

    # Combine all data
    if all_data:
        logger.info("\nCombining all data...")
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'duid'])
        combined_df = combined_df.sort_values(['settlementdate', 'duid'])

        # Filter to July 1, 2025 onwards (UIGF data starts here)
        july_start = datetime(2025, 7, 1)
        combined_df = combined_df[combined_df['settlementdate'] >= july_start]

        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total records: {len(combined_df):,}")
        logger.info(f"Unique DUIDs: {combined_df['duid'].nunique()}")
        logger.info(f"Date range: {combined_df['settlementdate'].min()} to {combined_df['settlementdate'].max()}")
        logger.info(f"Total curtailment: {combined_df['curtailment'].sum():,.0f} MW-intervals")
        logger.info(f"Total curtailment (MWh): {combined_df['curtailment'].sum() / 12:,.0f} MWh")

        # Show top curtailed DUIDs
        top_curtailed = combined_df.groupby('duid')['curtailment'].sum().sort_values(ascending=False).head(10)
        logger.info("\nTop 10 most curtailed DUIDs (total MW-intervals):")
        for duid, curtailment in top_curtailed.items():
            logger.info(f"  {duid}: {curtailment:,.0f}")

        # Save to parquet
        logger.info(f"\nSaving to {OUTPUT_FILE}...")
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        combined_df.to_parquet(OUTPUT_FILE, compression='snappy', index=False)
        logger.info("Done!")

        # Verify saved file
        verify_df = pd.read_parquet(OUTPUT_FILE)
        logger.info(f"Verified: {len(verify_df):,} records saved")

    else:
        logger.error("No data collected!")


if __name__ == "__main__":
    main()
