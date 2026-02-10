#!/usr/bin/env python3
"""
Merge misplaced demand data from wrong location into correct file

The demand data from Oct 1-22 was saved to the wrong location due to a
configuration error. This script merges that data into the correct file.
"""

import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Merge misplaced demand data"""

    logger.info("="*60)
    logger.info("MERGE MISPLACED DEMAND DATA")
    logger.info("="*60)

    # Paths (using /Volumes/davidleitch for development MacBook accessing production over network)
    correct_path = Path('/Volumes/davidleitch/aemo_production/data/demand30.parquet')
    wrong_path = Path('/Volumes/davidleitch/aemo_production/data/aemo-energy-dashboard/data/demand30.parquet')

    # Check if wrong file exists
    if not wrong_path.exists():
        logger.error(f"Wrong path file doesn't exist: {wrong_path}")
        return 1

    # Load misplaced data
    logger.info(f"\nLoading misplaced data from: {wrong_path}")
    misplaced_df = pd.read_parquet(wrong_path)
    logger.info(f"  Records: {len(misplaced_df):,}")
    logger.info(f"  Date range: {misplaced_df['settlementdate'].min()} to {misplaced_df['settlementdate'].max()}")
    logger.info(f"  Regions: {', '.join(sorted(misplaced_df['regionid'].unique()))}")

    # Load current correct data if it exists
    if correct_path.exists():
        logger.info(f"\nLoading existing data from: {correct_path}")
        current_df = pd.read_parquet(correct_path)
        logger.info(f"  Records: {len(current_df):,}")
        logger.info(f"  Date range: {current_df['settlementdate'].min()} to {current_df['settlementdate'].max()}")

        # Merge data
        logger.info(f"\nMerging data...")
        combined_df = pd.concat([current_df, misplaced_df], ignore_index=True)
    else:
        logger.info(f"\nNo existing data at correct location")
        combined_df = misplaced_df

    # Remove duplicates
    before_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
    combined_df = combined_df.sort_values(['settlementdate', 'regionid'])
    after_count = len(combined_df)

    logger.info(f"  Before dedup: {before_count:,} records")
    logger.info(f"  After dedup: {after_count:,} records")
    logger.info(f"  Duplicates removed: {before_count - after_count:,}")

    # Save to correct location
    logger.info(f"\nSaving to: {correct_path}")
    correct_path.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(correct_path, compression='snappy', index=False)

    logger.info(f"\n✓ Merge successful!")
    logger.info(f"\nFinal data:")
    logger.info(f"  Total records: {len(combined_df):,}")
    logger.info(f"  Date range: {combined_df['settlementdate'].min()} to {combined_df['settlementdate'].max()}")
    logger.info(f"  Regions: {', '.join(sorted(combined_df['regionid'].unique()))}")

    # Show gap analysis
    logger.info(f"\n Gap Analysis:")
    # Expected: 48 intervals/day * 5 regions * days = records
    date_range = (combined_df['settlementdate'].max() - combined_df['settlementdate'].min()).days + 1
    expected_records = date_range * 48 * 5
    actual_records = len(combined_df)
    logger.info(f"  Days covered: {date_range}")
    logger.info(f"  Expected records (approx): {expected_records:,}")
    logger.info(f"  Actual records: {actual_records:,}")
    logger.info(f"  Coverage: {actual_records/expected_records*100:.1f}%")

    logger.info(f"\nNote: The unified collector will continue to add new data as it runs.")
    logger.info(f"Any remaining gaps will be filled over time (3 files per 4.5 min cycle).")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
