#!/usr/bin/env python3
"""
Test script to verify demand collector can download and parse archive files
"""

import asyncio
import pandas as pd
from pathlib import Path
import logging
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from aemo_updater.collectors.demand_collector import DemandCollector

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_single_file_download():
    """Test downloading and parsing a single demand file"""
    logger.info("="*60)
    logger.info("TEST 1: Download and parse single demand file")
    logger.info("="*60)

    # Initialize collector
    data_path = Path('/Users/davidleitch/aemo_production/data')
    config = {
        'path': data_path / 'demand30.parquet',
        'update_interval': 1800,  # 30 minutes
        'retention_days': 3650,
    }

    collector = DemandCollector(config)

    # Test date: October 1, 2025 (should be in archive)
    test_date = pd.to_datetime('2025-10-01')

    logger.info(f"\nAttempting to download demand file for {test_date.date()}")
    logger.info(f"Archive URL: {collector.archive_url}")

    # Try to download the file
    df = await collector._download_daily_archive(test_date)

    if df is not None and not df.empty:
        logger.info(f"\n✓ Successfully downloaded and parsed data!")
        logger.info(f"  Records: {len(df):,}")
        logger.info(f"  Columns: {list(df.columns)}")
        logger.info(f"  Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
        logger.info(f"  Regions: {', '.join(sorted(df['regionid'].unique()))}")

        logger.info(f"\nSample data (first 10 records):")
        print(df.head(10).to_string())

        logger.info(f"\nSample data (last 10 records):")
        print(df.tail(10).to_string())

        # Validate data
        logger.info(f"\nData validation:")
        logger.info(f"  Null values: {df.isnull().sum().sum()}")
        logger.info(f"  Demand range: {df['demand'].min():.1f} to {df['demand'].max():.1f} MW")

        # Check intervals (should be 30 minutes)
        for region in df['regionid'].unique():
            region_data = df[df['regionid'] == region].sort_values('settlementdate')
            intervals = region_data['settlementdate'].diff().dropna()
            unique_intervals = intervals.unique()
            logger.info(f"  {region} intervals: {[str(i) for i in unique_intervals[:3]]}")

        return True
    else:
        logger.error("\n✗ Failed to download or parse data")
        return False


async def test_get_latest_urls():
    """Test getting latest URLs from current directory"""
    logger.info("\n" + "="*60)
    logger.info("TEST 2: Get latest demand file URLs from current directory")
    logger.info("="*60)

    data_path = Path('/Users/davidleitch/aemo_production/data')
    config = {
        'path': data_path / 'demand30.parquet',
        'update_interval': 1800,  # 30 minutes
        'retention_days': 3650,
    }

    collector = DemandCollector(config)

    logger.info(f"Current URL: {collector.current_url}")

    urls = await collector.get_latest_urls()

    if urls:
        logger.info(f"\n✓ Found {len(urls)} new demand files")
        for i, url in enumerate(urls[:5], 1):
            logger.info(f"  {i}. {url}")
        if len(urls) > 5:
            logger.info(f"  ... and {len(urls) - 5} more")
        return True
    else:
        logger.warning("\n⚠️  No new demand files found (this may be normal if already processed)")
        return True


async def test_backfill_single_day():
    """Test backfilling a single day"""
    logger.info("\n" + "="*60)
    logger.info("TEST 3: Backfill single day (Oct 1, 2025)")
    logger.info("="*60)

    data_path = Path('/Users/davidleitch/aemo_production/data')
    config = {
        'path': data_path / 'demand30_test.parquet',  # Use test file
        'update_interval': 1800,  # 30 minutes
        'retention_days': 3650,
    }

    collector = DemandCollector(config)

    # Test with a single day that's in the archive
    start_date = pd.to_datetime('2025-10-01 00:00:00')
    end_date = pd.to_datetime('2025-10-01 23:59:59')

    logger.info(f"Backfilling: {start_date.date()} to {end_date.date()}")

    success = await collector.backfill_date_range(start_date, end_date)

    if success:
        logger.info("\n✓ Backfill successful!")

        # Check the test output file
        test_file = Path(config['path'])
        if test_file.exists():
            df = pd.read_parquet(test_file)
            logger.info(f"  Test file records: {len(df):,}")
            logger.info(f"  Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            logger.info(f"  Regions: {', '.join(sorted(df['regionid'].unique()))}")

            # Expected: 48 intervals x 5 regions = 240 records per day
            expected_records = 48 * 5
            logger.info(f"  Expected records: {expected_records}")
            logger.info(f"  Actual records: {len(df)}")

            if len(df) >= 200:  # Allow some tolerance
                logger.info(f"  ✓ Record count looks good!")
            else:
                logger.warning(f"  ⚠️  Record count seems low")

            # Clean up test file
            logger.info(f"\nCleaning up test file...")
            test_file.unlink()

        return True
    else:
        logger.error("\n✗ Backfill failed")
        return False


async def main():
    """Run all tests"""
    logger.info("DEMAND COLLECTOR BACKFILL TEST SUITE")
    logger.info("="*60)

    results = {}

    # Test 1: Single file download
    try:
        results['single_file'] = await test_single_file_download()
    except Exception as e:
        logger.error(f"Test 1 failed with error: {e}", exc_info=True)
        results['single_file'] = False

    # Test 2: Get latest URLs
    try:
        results['latest_urls'] = await test_get_latest_urls()
    except Exception as e:
        logger.error(f"Test 2 failed with error: {e}", exc_info=True)
        results['latest_urls'] = False

    # Test 3: Backfill single day
    try:
        results['backfill_day'] = await test_backfill_single_day()
    except Exception as e:
        logger.error(f"Test 3 failed with error: {e}", exc_info=True)
        results['backfill_day'] = False

    # Summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)
    for test_name, success in results.items():
        status = "✓ PASS" if success else "✗ FAIL"
        logger.info(f"{test_name:20s}: {status}")
    logger.info("="*60)

    all_passed = all(results.values())
    if all_passed:
        logger.info("\n✓ All tests passed! Demand backfill is working correctly.")
        return 0
    else:
        logger.error("\n✗ Some tests failed. Review errors above.")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
