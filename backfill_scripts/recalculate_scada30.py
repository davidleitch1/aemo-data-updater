#!/usr/bin/env python3
"""
Recalculate scada30 for specific periods from scada5 data

Usage:
    python recalculate_scada30.py --start "2025-10-28 02:00" --end "2025-10-28 08:00"
    python recalculate_scada30.py --start "2025-11-16 06:00" --end "2025-11-16 14:00"
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sys
import argparse
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


def recalculate_scada30_for_period(collector: UnifiedAEMOCollector, start: datetime, end: datetime, test_only: bool = False) -> bool:
    """
    Recalculate scada30 for a specific period using existing scada5 data.

    AEMO convention: 30-min timestamps represent the END of the interval.
    12:30:00 = average of 12:05, 12:10, 12:15, 12:20, 12:25, 12:30
    """
    logger.info(f"Recalculating scada30 from {start} to {end}")

    # Read scada5 data
    scada5_file = collector.output_files['scada5']
    if not scada5_file.exists():
        logger.error("scada5.parquet not found")
        return False

    scada5_df = pd.read_parquet(scada5_file)
    logger.info(f"Loaded {len(scada5_df):,} scada5 records")

    # Filter to period (with buffer for aggregation)
    buffer_start = start - timedelta(minutes=30)
    buffer_end = end + timedelta(minutes=5)

    period_data = scada5_df[
        (scada5_df['settlementdate'] >= buffer_start) &
        (scada5_df['settlementdate'] <= buffer_end)
    ].copy()

    if period_data.empty:
        logger.warning(f"No scada5 data found for period {start} to {end}")
        return False

    logger.info(f"Found {len(period_data):,} scada5 records in period")

    # Find 30-minute endpoints in the period
    unique_times = period_data['settlementdate'].unique()
    endpoints = [t for t in unique_times if pd.Timestamp(t).minute in [0, 30]]

    # Filter to requested period
    endpoints = [t for t in endpoints if start <= pd.Timestamp(t) <= end]

    if not endpoints:
        logger.warning("No 30-minute endpoints found in data")
        return False

    logger.info(f"Found {len(endpoints)} 30-minute endpoints to process")

    # Aggregate for each endpoint
    aggregated_records = []

    for endpoint in sorted(endpoints):
        endpoint = pd.Timestamp(endpoint)
        window_start = endpoint - pd.Timedelta(minutes=30)

        # Get all 5-minute records in this window
        window_mask = (
            (scada5_df['settlementdate'] > window_start) &
            (scada5_df['settlementdate'] <= endpoint)
        )
        window_data = scada5_df[window_mask]

        # Aggregate by DUID
        for duid, duid_data in window_data.groupby('duid'):
            if len(duid_data) > 0:
                avg_value = duid_data['scadavalue'].mean()
                aggregated_records.append({
                    'settlementdate': endpoint,
                    'duid': duid,
                    'scadavalue': avg_value
                })

    if not aggregated_records:
        logger.warning("No records aggregated")
        return False

    # Create DataFrame
    result_df = pd.DataFrame(aggregated_records)
    result_df = result_df.drop_duplicates(subset=['settlementdate', 'duid'])
    result_df = result_df.sort_values(['settlementdate', 'duid'])

    logger.info(f"Aggregated {len(result_df):,} scada30 records")

    if test_only:
        logger.info("TEST MODE - showing sample data:")
        print(result_df.head(20))
        print(f"\nUnique timestamps: {result_df['settlementdate'].nunique()}")
        print(f"Unique DUIDs: {result_df['duid'].nunique()}")
        return True

    # Merge with existing scada30
    success = collector.merge_and_save(
        result_df,
        collector.output_files['scada30'],
        ['settlementdate', 'duid']
    )

    if success:
        logger.info(f"✓ Successfully recalculated scada30")
    else:
        logger.error(f"✗ Failed to merge scada30")

    return success


def main():
    parser = argparse.ArgumentParser(
        description='Recalculate scada30 for specific periods from scada5 data'
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
        '--test',
        action='store_true',
        help='Test mode - calculate but do not save'
    )

    args = parser.parse_args()

    start_date = pd.to_datetime(args.start)
    end_date = pd.to_datetime(args.end)

    logger.info("="*60)
    logger.info("SCADA30 Recalculation Tool")
    logger.info("="*60)
    logger.info(f"Period: {start_date} to {end_date}")
    logger.info(f"Mode: {'TEST' if args.test else 'FULL'}")
    logger.info("="*60)

    start_time = time.time()
    collector = UnifiedAEMOCollector()
    success = recalculate_scada30_for_period(collector, start_date, end_date, args.test)
    elapsed = time.time() - start_time
    logger.info(f"Total execution time: {elapsed:.1f} seconds")

    return 0 if success else 1


if __name__ == "__main__":
    exit(main())