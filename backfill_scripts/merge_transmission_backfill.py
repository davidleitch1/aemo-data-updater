#!/usr/bin/env python3
"""
Stage 5: Merge Transmission Backfill
Safely merges backfilled transmission data into production files.

Pre-requisites:
1. Backfill files exist:
   - transmission5_backfill_temp.parquet
   - transmission30_backfill_temp.parquet
2. Production service is stopped
3. Backups have been created

Usage:
    python merge_transmission_backfill.py --dry-run    # Preview merge without writing
    python merge_transmission_backfill.py              # Execute merge
    python merge_transmission_backfill.py --verify     # Verify merge was successful
"""

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path
from typing import Tuple
import pandas as pd
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')
BACKUP_PATH = DATA_PATH / 'backups'

# Files
PRODUCTION_5MIN = DATA_PATH / 'transmission5.parquet'
PRODUCTION_30MIN = DATA_PATH / 'transmission30.parquet'
BACKFILL_5MIN = DATA_PATH / 'transmission5_backfill_temp.parquet'
BACKFILL_30MIN = DATA_PATH / 'transmission30_backfill_temp.parquet'

# Cutoff date for data with NaN values
CUTOFF_DATE = pd.Timestamp('2025-07-17')


def create_backup(file_path: Path) -> Path:
    """Create timestamped backup of a file"""
    if not file_path.exists():
        raise FileNotFoundError(f"Cannot backup: {file_path} not found")

    # Ensure backup directory exists
    BACKUP_PATH.mkdir(parents=True, exist_ok=True)

    # Create timestamped backup name
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = f"{file_path.stem}_backup_{timestamp}.parquet"
    backup_path = BACKUP_PATH / backup_name

    # Copy file
    shutil.copy2(file_path, backup_path)

    # Verify backup
    orig_size = file_path.stat().st_size
    backup_size = backup_path.stat().st_size

    if orig_size != backup_size:
        raise RuntimeError(f"Backup size mismatch: {orig_size} != {backup_size}")

    logger.info(f"Backup created: {backup_path} ({backup_size:,} bytes)")
    return backup_path


def merge_5min_data(dry_run: bool = False) -> Tuple[int, int, int]:
    """
    Merge 5-minute backfill data into production.

    Strategy:
    1. Keep existing data BEFORE July 17, 2025 (has valid values)
    2. Replace data from July 17, 2025 onwards with backfill data
    3. Deduplicate on (settlementdate, interconnectorid)

    Returns:
        Tuple of (records_before_merge, records_after_merge, net_change)
    """
    logger.info("=" * 60)
    logger.info("Merging 5-minute transmission data...")
    logger.info("=" * 60)

    # Load existing data
    if not PRODUCTION_5MIN.exists():
        raise FileNotFoundError(f"Production file not found: {PRODUCTION_5MIN}")

    existing_df = pd.read_parquet(PRODUCTION_5MIN)
    logger.info(f"Existing 5-min: {len(existing_df):,} records")
    logger.info(f"  Date range: {existing_df['settlementdate'].min()} to {existing_df['settlementdate'].max()}")

    # Load backfill data
    if not BACKFILL_5MIN.exists():
        raise FileNotFoundError(f"Backfill file not found: {BACKFILL_5MIN}")

    backfill_df = pd.read_parquet(BACKFILL_5MIN)
    logger.info(f"Backfill 5-min: {len(backfill_df):,} records")
    logger.info(f"  Date range: {backfill_df['settlementdate'].min()} to {backfill_df['settlementdate'].max()}")

    # Normalize timestamps (remove microseconds)
    existing_df['settlementdate'] = existing_df['settlementdate'].dt.floor('s')
    backfill_df['settlementdate'] = backfill_df['settlementdate'].dt.floor('s')

    # Normalize interconnector IDs (strip whitespace)
    existing_df['interconnectorid'] = existing_df['interconnectorid'].str.strip()
    backfill_df['interconnectorid'] = backfill_df['interconnectorid'].str.strip()

    # Keep existing data BEFORE cutoff (has valid values)
    existing_before = existing_df[existing_df['settlementdate'] < CUTOFF_DATE]
    logger.info(f"Keeping {len(existing_before):,} existing records before {CUTOFF_DATE.date()}")

    # Combine: existing before cutoff + all backfill data
    combined = pd.concat([existing_before, backfill_df], ignore_index=True)

    # Deduplicate (prefer later entry = backfill data)
    before_dedup = len(combined)
    combined = combined.drop_duplicates(
        subset=['settlementdate', 'interconnectorid'],
        keep='last'
    )
    after_dedup = len(combined)
    logger.info(f"Deduplication: {before_dedup:,} -> {after_dedup:,} ({before_dedup - after_dedup:,} removed)")

    # Sort
    combined = combined.sort_values(['settlementdate', 'interconnectorid'])

    # Ensure column order matches original
    combined = combined[existing_df.columns]

    # Statistics
    records_before = len(existing_df)
    records_after = len(combined)
    net_change = records_after - records_before

    logger.info(f"Merge result: {records_before:,} -> {records_after:,} ({net_change:+,} records)")

    # Check mwflow validity after merge
    recent = combined[combined['settlementdate'] >= CUTOFF_DATE]
    nan_count = recent['mwflow'].isna().sum()
    nan_ratio = nan_count / len(recent) if len(recent) > 0 else 0
    logger.info(f"Post-merge mwflow NaN (after {CUTOFF_DATE.date()}): {nan_count:,}/{len(recent):,} ({nan_ratio:.1%})")

    if not dry_run:
        # Create backup first
        backup_path = create_backup(PRODUCTION_5MIN)

        # Save merged data
        combined.to_parquet(PRODUCTION_5MIN, compression='snappy', index=False)
        logger.info(f"Saved merged data to {PRODUCTION_5MIN}")
    else:
        logger.info("[DRY RUN] Would save merged data")

    return records_before, records_after, net_change


def merge_30min_data(dry_run: bool = False) -> Tuple[int, int, int]:
    """
    Merge 30-minute backfill data into production.
    Similar strategy to 5-minute merge.
    """
    logger.info("=" * 60)
    logger.info("Merging 30-minute transmission data...")
    logger.info("=" * 60)

    # Load existing data
    if not PRODUCTION_30MIN.exists():
        raise FileNotFoundError(f"Production file not found: {PRODUCTION_30MIN}")

    existing_df = pd.read_parquet(PRODUCTION_30MIN)
    logger.info(f"Existing 30-min: {len(existing_df):,} records")

    # Load backfill data
    if not BACKFILL_30MIN.exists():
        logger.warning(f"No 30-min backfill file found: {BACKFILL_30MIN}")
        return len(existing_df), len(existing_df), 0

    backfill_df = pd.read_parquet(BACKFILL_30MIN)
    logger.info(f"Backfill 30-min: {len(backfill_df):,} records")

    # Normalize
    existing_df['settlementdate'] = existing_df['settlementdate'].dt.floor('s')
    backfill_df['settlementdate'] = backfill_df['settlementdate'].dt.floor('s')
    existing_df['interconnectorid'] = existing_df['interconnectorid'].str.strip()
    backfill_df['interconnectorid'] = backfill_df['interconnectorid'].str.strip()

    # Keep existing before cutoff
    existing_before = existing_df[existing_df['settlementdate'] < CUTOFF_DATE]

    # Combine
    combined = pd.concat([existing_before, backfill_df], ignore_index=True)

    # Deduplicate
    combined = combined.drop_duplicates(
        subset=['settlementdate', 'interconnectorid'],
        keep='last'
    )

    # Sort
    combined = combined.sort_values(['settlementdate', 'interconnectorid'])

    # Ensure column order - handle potentially different columns
    # Keep all columns from both, prioritizing existing order
    all_cols = list(existing_df.columns)
    for col in backfill_df.columns:
        if col not in all_cols:
            all_cols.append(col)

    # Only include columns that exist in combined
    final_cols = [c for c in all_cols if c in combined.columns]
    combined = combined[final_cols]

    # Statistics
    records_before = len(existing_df)
    records_after = len(combined)
    net_change = records_after - records_before

    logger.info(f"Merge result: {records_before:,} -> {records_after:,} ({net_change:+,} records)")

    if not dry_run:
        backup_path = create_backup(PRODUCTION_30MIN)
        combined.to_parquet(PRODUCTION_30MIN, compression='snappy', index=False)
        logger.info(f"Saved merged data to {PRODUCTION_30MIN}")
    else:
        logger.info("[DRY RUN] Would save merged data")

    return records_before, records_after, net_change


def verify_merge():
    """Verify that the merge was successful"""
    logger.info("=" * 60)
    logger.info("Verifying merge...")
    logger.info("=" * 60)

    errors = []

    # Check 5-min data
    if PRODUCTION_5MIN.exists():
        df = pd.read_parquet(PRODUCTION_5MIN)
        logger.info(f"5-min file: {len(df):,} records")

        # Check mwflow after July 17
        recent = df[df['settlementdate'] >= CUTOFF_DATE]
        nan_count = recent['mwflow'].isna().sum()
        nan_ratio = nan_count / len(recent) if len(recent) > 0 else 0

        logger.info(f"  mwflow NaN after {CUTOFF_DATE.date()}: {nan_count:,}/{len(recent):,} ({nan_ratio:.1%})")

        if nan_ratio > 0.01:
            errors.append(f"5-min mwflow has {nan_ratio:.1%} NaN values after cutoff")
    else:
        errors.append("5-min production file not found")

    # Check 30-min data
    if PRODUCTION_30MIN.exists():
        df = pd.read_parquet(PRODUCTION_30MIN)
        logger.info(f"30-min file: {len(df):,} records")

        if 'mwflow' in df.columns:
            recent = df[df['settlementdate'] >= CUTOFF_DATE]
            nan_count = recent['mwflow'].isna().sum()
            nan_ratio = nan_count / len(recent) if len(recent) > 0 else 0
            logger.info(f"  mwflow NaN after {CUTOFF_DATE.date()}: {nan_count:,}/{len(recent):,} ({nan_ratio:.1%})")

            if nan_ratio > 0.01:
                errors.append(f"30-min mwflow has {nan_ratio:.1%} NaN values after cutoff")
    else:
        errors.append("30-min production file not found")

    if errors:
        logger.error("Verification FAILED:")
        for e in errors:
            logger.error(f"  - {e}")
        return False
    else:
        logger.info("Verification PASSED")
        return True


def main():
    parser = argparse.ArgumentParser(description='Merge transmission backfill data')
    parser.add_argument('--dry-run', action='store_true', help='Preview merge without writing')
    parser.add_argument('--verify', action='store_true', help='Verify merge was successful')
    parser.add_argument('--5min-only', action='store_true', help='Only merge 5-min data')
    parser.add_argument('--30min-only', action='store_true', help='Only merge 30-min data')

    args = parser.parse_args()

    if args.verify:
        success = verify_merge()
        return 0 if success else 1

    # Pre-flight checks
    logger.info("Pre-flight checks...")

    if not BACKFILL_5MIN.exists() and not args.__dict__.get('30min_only'):
        logger.error(f"Backfill file not found: {BACKFILL_5MIN}")
        logger.error("Run the backfill script first: python backfill_transmission_full.py")
        return 1

    if args.dry_run:
        logger.info("DRY RUN MODE - No files will be modified")

    # Execute merge
    try:
        if not args.__dict__.get('30min_only'):
            merge_5min_data(dry_run=args.dry_run)

        if not args.__dict__.get('5min_only'):
            merge_30min_data(dry_run=args.dry_run)

        logger.info("=" * 60)
        logger.info("Merge complete!")
        logger.info("=" * 60)

        if not args.dry_run:
            logger.info("Next steps:")
            logger.info("  1. Run verification: python merge_transmission_backfill.py --verify")
            logger.info("  2. Restart production service")
            logger.info("  3. Clean up temp files")

        return 0

    except Exception as e:
        logger.error(f"Merge failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
