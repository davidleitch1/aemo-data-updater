#!/usr/bin/env python3
"""
Fast demand backfill - Process ALL files from Current directory

The unified collector limits to 3 files per run (designed for incremental updates).
This script processes all available files to backfill quickly.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector
import logging
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Fast backfill - process ALL demand files"""

    logger.info("="*60)
    logger.info("FAST DEMAND BACKFILL")
    logger.info("="*60)

    # Initialize collector
    collector = UnifiedAEMOCollector()

    url = collector.current_urls['demand']
    files = collector.get_latest_files(url, 'PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_')

    logger.info(f"Found {len(files)} total demand files in Current directory")

    # Check existing data to determine what we need
    demand_file = collector.output_files['demand30']

    if demand_file.exists():
        existing_df = pd.read_parquet(demand_file)
        logger.info(f"Existing data: {len(existing_df)} records")
        logger.info(f"  Latest: {existing_df['settlementdate'].max()}")
    else:
        existing_df = pd.DataFrame()
        logger.info("No existing data file")

    # Process files in batches
    batch_size = 100
    all_new_data = []

    for i in range(0, len(files), batch_size):
        batch = files[i:i+batch_size]
        logger.info(f"\nProcessing batch {i//batch_size + 1} ({len(batch)} files)...")

        batch_data = []
        for filename in batch:
            try:
                import requests
                import zipfile
                import io

                # Download the file
                file_url = f"{url}{filename}"
                response = requests.get(file_url, headers=collector.headers, timeout=60)
                response.raise_for_status()

                # Extract CSV from ZIP
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
                    if not csv_files:
                        continue

                    with zf.open(csv_files[0]) as f:
                        csv_content = f.read().decode('utf-8', errors='ignore')

                # Parse using collector's demand parser
                demand_df = collector._parse_demand_csv(csv_content)

                if not demand_df.empty:
                    batch_data.append(demand_df)

            except Exception as e:
                logger.debug(f"Error processing {filename}: {e}")
                continue

        if batch_data:
            batch_combined = pd.concat(batch_data, ignore_index=True)
            all_new_data.append(batch_combined)
            logger.info(f"  Collected {len(batch_combined)} records from this batch")

    if not all_new_data:
        logger.warning("No new data collected")
        return 1

    # Combine all new data
    new_df = pd.concat(all_new_data, ignore_index=True)
    new_df = new_df.drop_duplicates(subset=['settlementdate', 'regionid'])
    new_df = new_df.sort_values(['settlementdate', 'regionid'])

    logger.info(f"\nTotal new data collected: {len(new_df)} records")
    logger.info(f"  Date range: {new_df['settlementdate'].min()} to {new_df['settlementdate'].max()}")

    # Merge and save
    success = collector.merge_and_save(
        new_df,
        demand_file,
        ['settlementdate', 'regionid']
    )

    if success:
        logger.info("\n✓ Backfill successful!")

        # Show final state
        final_df = pd.read_parquet(demand_file)
        logger.info(f"\nFinal state:")
        logger.info(f"  Total records: {len(final_df):,}")
        logger.info(f"  Date range: {final_df['settlementdate'].min()} to {final_df['settlementdate'].max()}")
        logger.info(f"  Regions: {', '.join(sorted(final_df['regionid'].unique()))}")

        return 0
    else:
        logger.error("\n✗ Failed to save data")
        return 1


if __name__ == "__main__":
    sys.exit(main())
