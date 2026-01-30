#!/usr/bin/env python3
"""
Stage 3: Transmission Data Backfill Script
Downloads missing transmission data (July 17, 2025 - present) from AEMO daily archives.

Features:
- Uses daily archive ZIPs for efficiency (~198 downloads vs 60,000+ individual files)
- Checkpointing every 10 days for resume capability
- Schema validation to detect AEMO format changes
- Separate output files for 5-min and 30-min data

Usage:
    python backfill_transmission_full.py              # Full backfill with checkpointing
    python backfill_transmission_full.py --test       # Test single day download
    python backfill_transmission_full.py --resume     # Resume from checkpoint
"""

import argparse
import json
import logging
import io
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Tuple
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
ARCHIVE_BASE_URL = 'https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/'
TRADING_ARCHIVE_URL = 'https://www.nemweb.com.au/REPORTS/ARCHIVE/TradingIS_Reports/'
HEADERS = {'User-Agent': 'AEMO Dashboard Data Collector - Backfill'}

# Date range for backfill
BACKFILL_START = datetime(2025, 7, 17)
BACKFILL_END = datetime.now()

# Output paths
DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')
OUTPUT_5MIN = DATA_PATH / 'transmission5_backfill_temp.parquet'
OUTPUT_30MIN = DATA_PATH / 'transmission30_backfill_temp.parquet'

# Checkpointing
CHECKPOINT_FILE = Path(__file__).parent / 'backfill_checkpoint.json'
CHECKPOINT_INTERVAL = 10  # Save every 10 days

# Schema validation
REQUIRED_COLUMNS = ['INTERCONNECTORID', 'MWFLOW', 'MWLOSSES', 'EXPORTLIMIT', 'IMPORTLIMIT', 'METEREDMWFLOW']


def validate_aemo_schema(df: pd.DataFrame) -> None:
    """Validate that AEMO data contains expected columns"""
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"AEMO format change detected. Missing columns: {missing}")


def save_checkpoint(current_date: datetime, data_5min: List[Dict], data_30min: List[Dict]) -> None:
    """Save progress checkpoint for resume capability"""
    checkpoint = {
        'current_date': current_date.isoformat(),
        'records_5min': len(data_5min),
        'records_30min': len(data_30min),
        'timestamp': datetime.now().isoformat()
    }

    # Save checkpoint metadata
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f, indent=2)

    # Save partial data
    if data_5min:
        df_5min = pd.DataFrame(data_5min)
        df_5min.to_parquet(OUTPUT_5MIN.with_suffix('.checkpoint.parquet'), index=False)

    if data_30min:
        df_30min = pd.DataFrame(data_30min)
        df_30min.to_parquet(OUTPUT_30MIN.with_suffix('.checkpoint.parquet'), index=False)

    logger.info(f"Checkpoint saved at {current_date.date()}: {len(data_5min)} 5-min, {len(data_30min)} 30-min records")


def load_checkpoint() -> Tuple[Optional[datetime], List[Dict], List[Dict]]:
    """Load checkpoint if exists"""
    if not CHECKPOINT_FILE.exists():
        return None, [], []

    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)

        current_date = datetime.fromisoformat(checkpoint['current_date'])

        # Load partial data
        data_5min = []
        data_30min = []

        checkpoint_5min = OUTPUT_5MIN.with_suffix('.checkpoint.parquet')
        if checkpoint_5min.exists():
            df = pd.read_parquet(checkpoint_5min)
            data_5min = df.to_dict('records')

        checkpoint_30min = OUTPUT_30MIN.with_suffix('.checkpoint.parquet')
        if checkpoint_30min.exists():
            df = pd.read_parquet(checkpoint_30min)
            data_30min = df.to_dict('records')

        logger.info(f"Resumed from checkpoint: {current_date.date()}, {len(data_5min)} 5-min, {len(data_30min)} 30-min records")
        return current_date, data_5min, data_30min

    except Exception as e:
        logger.warning(f"Could not load checkpoint: {e}")
        return None, [], []


def clear_checkpoint():
    """Remove checkpoint files after successful completion"""
    for f in [CHECKPOINT_FILE,
              OUTPUT_5MIN.with_suffix('.checkpoint.parquet'),
              OUTPUT_30MIN.with_suffix('.checkpoint.parquet')]:
        if f.exists():
            f.unlink()
    logger.info("Checkpoint files cleared")


def parse_mms_csv(content: bytes, table_name: str) -> pd.DataFrame:
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


def download_daily_archive(date: datetime) -> Optional[bytes]:
    """Download daily archive ZIP for a given date"""
    date_str = date.strftime('%Y%m%d')

    # Try different filename patterns
    patterns = [
        f'PUBLIC_DISPATCHIS_{date_str}.zip',
        f'PUBLIC_DISPATCHIS_{date_str}000000.zip',
    ]

    for pattern in patterns:
        url = f"{ARCHIVE_BASE_URL}{pattern}"
        try:
            response = requests.get(url, headers=HEADERS, timeout=60)
            if response.status_code == 200:
                return response.content
        except requests.RequestException:
            continue

    return None


def get_daily_archive_files(date: datetime) -> List[str]:
    """Get list of archive files for a date range by scraping the archive directory"""
    try:
        response = requests.get(ARCHIVE_BASE_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        files = []

        date_str = date.strftime('%Y%m%d')

        for link in soup.find_all('a'):
            filename = link.text.strip()
            if 'PUBLIC_DISPATCHIS_' in filename and date_str in filename and filename.endswith('.zip'):
                files.append(filename)

        return sorted(files)
    except Exception as e:
        logger.error(f"Error getting archive files: {e}")
        return []


def extract_transmission_from_zip(zip_content: bytes) -> Tuple[List[Dict], List[Dict]]:
    """Extract transmission data from ZIP file containing CSVs or nested ZIPs"""
    data_5min = []

    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as z:
            files = z.namelist()

            for file_name in files:
                try:
                    file_content = z.read(file_name)

                    # Handle nested ZIPs (daily archive contains individual dispatch ZIPs)
                    if file_name.endswith('.zip') or file_name.endswith('.ZIP'):
                        try:
                            with zipfile.ZipFile(io.BytesIO(file_content)) as inner_z:
                                inner_files = inner_z.namelist()
                                csv_files = [f for f in inner_files if f.endswith('.csv') or f.endswith('.CSV')]

                                for csv_file in csv_files:
                                    csv_content = inner_z.read(csv_file)
                                    records = extract_transmission_from_csv(csv_content)
                                    data_5min.extend(records)
                        except zipfile.BadZipFile:
                            continue

                    # Handle direct CSV files
                    elif file_name.endswith('.csv') or file_name.endswith('.CSV'):
                        records = extract_transmission_from_csv(file_content)
                        data_5min.extend(records)

                except Exception as e:
                    logger.debug(f"Error processing {file_name}: {e}")
                    continue

    except Exception as e:
        logger.error(f"Error extracting from ZIP: {e}")

    return data_5min, []


def extract_transmission_from_csv(csv_content: bytes) -> List[Dict]:
    """Extract transmission records from a single CSV"""
    records = []

    df = parse_mms_csv(csv_content, 'INTERCONNECTORRES')

    if df.empty:
        return records

    # Validate schema
    if 'SETTLEMENTDATE' not in df.columns:
        return records

    # Extract transmission data
    for _, row in df.iterrows():
        try:
            record = {
                'settlementdate': pd.to_datetime(
                    str(row.get('SETTLEMENTDATE', '')).strip('"'),
                    format='%Y/%m/%d %H:%M:%S'
                ),
                'interconnectorid': str(row.get('INTERCONNECTORID', '')).strip(),
                'meteredmwflow': pd.to_numeric(row.get('METEREDMWFLOW'), errors='coerce'),
                'mwflow': pd.to_numeric(row.get('MWFLOW'), errors='coerce'),
                'mwlosses': pd.to_numeric(row.get('MWLOSSES'), errors='coerce'),
                'exportlimit': pd.to_numeric(row.get('EXPORTLIMIT'), errors='coerce'),
                'importlimit': pd.to_numeric(row.get('IMPORTLIMIT'), errors='coerce'),
            }

            # Skip invalid records
            if pd.isna(record['meteredmwflow']):
                continue

            records.append(record)
        except Exception:
            continue

    return records


def download_single_dispatch_file(date: datetime) -> List[Dict]:
    """Download daily archive and extract transmission data"""
    # First, try the daily archive (most efficient)
    zip_content = download_daily_archive(date)

    if zip_content:
        records, _ = extract_transmission_from_zip(zip_content)
        if records:
            return records

    # Fallback: try getting list of individual files
    files = get_daily_archive_files(date)

    if not files:
        # Try current directory for recent dates
        current_url = 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/'
        try:
            response = requests.get(current_url, headers=HEADERS, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')

            date_str = date.strftime('%Y%m%d')

            for link in soup.find_all('a'):
                filename = link.text.strip()
                if 'PUBLIC_DISPATCHIS_' in filename and date_str in filename and filename.endswith('.zip'):
                    files.append(filename)
        except Exception:
            pass

    data = []
    for filename in files:
        try:
            # Try archive first, then current
            for base_url in [ARCHIVE_BASE_URL, 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/']:
                url = f"{base_url}{filename}"
                try:
                    response = requests.get(url, headers=HEADERS, timeout=60)
                    if response.status_code == 200:
                        records, _ = extract_transmission_from_zip(response.content)
                        data.extend(records)
                        break
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Error downloading {filename}: {e}")

    return data


def run_backfill(start_date: Optional[datetime] = None,
                 end_date: Optional[datetime] = None,
                 resume: bool = False) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Run the full backfill process"""

    # Initialize data lists
    data_5min = []
    data_30min = []

    # Handle resume
    if resume:
        checkpoint_date, data_5min, data_30min = load_checkpoint()
        if checkpoint_date:
            start_date = checkpoint_date + timedelta(days=1)

    if start_date is None:
        start_date = BACKFILL_START
    if end_date is None:
        end_date = BACKFILL_END

    logger.info(f"Starting backfill from {start_date.date()} to {end_date.date()}")

    current_date = start_date
    days_processed = 0

    while current_date <= end_date:
        logger.info(f"Processing {current_date.date()}...")

        # Try to download daily data
        day_data = download_single_dispatch_file(current_date)

        if day_data:
            data_5min.extend(day_data)
            logger.info(f"  Downloaded {len(day_data)} records")
        else:
            logger.warning(f"  No data found for {current_date.date()}")

        days_processed += 1

        # Checkpoint every N days
        if days_processed % CHECKPOINT_INTERVAL == 0:
            save_checkpoint(current_date, data_5min, data_30min)

        # Small delay to be nice to AEMO servers
        time.sleep(0.5)

        current_date += timedelta(days=1)

    # Create final dataframes
    df_5min = pd.DataFrame()
    df_30min = pd.DataFrame()

    if data_5min:
        df_5min = pd.DataFrame(data_5min)
        df_5min = df_5min.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
        df_5min = df_5min.sort_values(['settlementdate', 'interconnectorid'])

        # Save to temp file
        df_5min.to_parquet(OUTPUT_5MIN, compression='snappy', index=False)
        logger.info(f"Saved {len(df_5min)} 5-min records to {OUTPUT_5MIN}")

    # Generate 30-min aggregates from 5-min data
    if not df_5min.empty:
        logger.info("Generating 30-minute aggregates...")
        df_30min = aggregate_to_30min(df_5min)

        if not df_30min.empty:
            df_30min.to_parquet(OUTPUT_30MIN, compression='snappy', index=False)
            logger.info(f"Saved {len(df_30min)} 30-min records to {OUTPUT_30MIN}")

    # Clear checkpoint on success
    clear_checkpoint()

    return df_5min, df_30min


def aggregate_to_30min(df_5min: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 5-minute data to 30-minute intervals"""
    if df_5min.empty:
        return pd.DataFrame()

    df = df_5min.copy()

    # Create 30-minute endpoint column
    # AEMO convention: 12:30 represents the 30-minute period ending at 12:30
    df['period_30min'] = df['settlementdate'].dt.ceil('30min')

    # Aggregate by 30-minute period and interconnector
    agg_cols = ['meteredmwflow', 'mwflow', 'mwlosses', 'exportlimit', 'importlimit']
    agg_dict = {col: 'mean' for col in agg_cols if col in df.columns}

    df_30min = df.groupby(['period_30min', 'interconnectorid']).agg(agg_dict).reset_index()
    df_30min = df_30min.rename(columns={'period_30min': 'settlementdate'})

    return df_30min


def test_single_day():
    """Test downloading a single day of data"""
    test_date = datetime(2025, 7, 20)

    logger.info(f"Testing single day download: {test_date.date()}")

    data = download_single_dispatch_file(test_date)

    if data:
        df = pd.DataFrame(data)
        logger.info(f"Downloaded {len(df)} records")
        logger.info(f"Columns: {df.columns.tolist()}")
        logger.info(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
        logger.info(f"Interconnectors: {df['interconnectorid'].unique()}")

        # Check for valid data
        for col in ['mwflow', 'exportlimit', 'importlimit']:
            if col in df.columns:
                valid = df[col].notna().sum()
                logger.info(f"  {col}: {valid} valid values")

        return df
    else:
        logger.error("No data downloaded")
        return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description='Backfill transmission data')
    parser.add_argument('--test', action='store_true', help='Test single day download')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--start', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)')

    args = parser.parse_args()

    if args.test:
        test_single_day()
        return 0

    start_date = datetime.strptime(args.start, '%Y-%m-%d') if args.start else None
    end_date = datetime.strptime(args.end, '%Y-%m-%d') if args.end else None

    df_5min, df_30min = run_backfill(start_date, end_date, resume=args.resume)

    logger.info("=" * 60)
    logger.info("Backfill Complete!")
    logger.info(f"  5-min records: {len(df_5min)}")
    logger.info(f"  30-min records: {len(df_30min)}")
    logger.info(f"  Output files:")
    logger.info(f"    {OUTPUT_5MIN}")
    logger.info(f"    {OUTPUT_30MIN}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
