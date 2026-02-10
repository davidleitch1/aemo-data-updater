#!/usr/bin/env python3
"""
Backfill October demand data using Current directory files

Since demand archive files are nested ZIPs (complex), and October data
is recent enough to still be in the Current directory, we'll just use
the unified collector's demand method to process all available files.
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
    """Backfill demand data from current directory"""

    logger.info("="*60)
    logger.info("DEMAND BACKFILL - October 2025")
    logger.info("="*60)
    logger.info("Strategy: Process ALL files from Current directory")
    logger.info("This will collect all available 30-min demand data")
    logger.info("="*60)

    # Initialize collector
    collector = UnifiedAEMOCollector()

    # Check existing data
    demand_file = collector.output_files['demand30']
    logger.info(f"\nDemand file: {demand_file}")

    if demand_file.exists():
        df = pd.read_parquet(demand_file)
        logger.info(f"Existing records: {len(df):,}")
        logger.info(f"Latest: {df['settlementdate'].max()}")
        logger.info(f"Earliest: {df['settlementdate'].min()}")
    else:
        logger.info("No existing demand file")

    # Collect demand data (will process all files in Current directory)
    logger.info("\nCollecting demand data from Current directory...")

    try:
        demand_df = collector.collect_30min_demand()

        if not demand_df.empty:
            logger.info(f"✓ Collected {len(demand_df)} new records")
            logger.info(f"  Date range: {demand_df['settlementdate'].min()} to {demand_df['settlementdate'].max()}")

            # Merge and save
            success = collector.merge_and_save(
                demand_df,
                collector.output_files['demand30'],
                ['settlementdate', 'regionid']
            )

            if success:
                logger.info("\n✓ Backfill successful!")

                # Show final state
                df = pd.read_parquet(demand_file)
                logger.info(f"\nFinal state:")
                logger.info(f"  Total records: {len(df):,}")
                logger.info(f"  Latest: {df['settlementdate'].max()}")
                logger.info(f"  Earliest: {df['settlementdate'].min()}")
                logger.info(f"  Regions: {', '.join(sorted(df['regionid'].unique()))}")

                return 0
            else:
                logger.error("\n✗ Failed to save data")
                return 1
        else:
            logger.warning("\n⚠️  No new data collected")
            logger.info("This may mean all files have already been processed")
            return 0

    except Exception as e:
        logger.error(f"\n✗ Error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
