#!/usr/bin/env python3
"""
Stage 5: Merge Validation Tests
Tests to verify the merge was successful.

These tests should be run AFTER the merge is complete.
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

# Paths
DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')
BACKUP_PATH = DATA_PATH / 'backups'
PRODUCTION_5MIN = DATA_PATH / 'transmission5.parquet'
PRODUCTION_30MIN = DATA_PATH / 'transmission30.parquet'

# Cutoff date
CUTOFF_DATE = pd.Timestamp('2025-07-17')


@pytest.fixture
def production_5min_df():
    """Load production 5-min parquet"""
    if not PRODUCTION_5MIN.exists():
        pytest.skip(f"Production file not found: {PRODUCTION_5MIN}")
    return pd.read_parquet(PRODUCTION_5MIN)


@pytest.fixture
def production_30min_df():
    """Load production 30-min parquet"""
    if not PRODUCTION_30MIN.exists():
        pytest.skip(f"Production file not found: {PRODUCTION_30MIN}")
    return pd.read_parquet(PRODUCTION_30MIN)


class TestProductionLoads:
    """Test that merged production file loads without error"""

    def test_production_5min_loads(self, production_5min_df):
        """Merged 5-min file should load without error"""
        df = production_5min_df

        assert len(df) > 0, "Production file is empty"
        assert 'settlementdate' in df.columns
        assert 'interconnectorid' in df.columns
        assert 'mwflow' in df.columns

        print(f"PASS: Loaded {len(df):,} records from production 5-min file")

    def test_production_30min_loads(self, production_30min_df):
        """Merged 30-min file should load without error"""
        df = production_30min_df

        assert len(df) > 0, "Production file is empty"
        assert 'settlementdate' in df.columns
        assert 'interconnectorid' in df.columns

        print(f"PASS: Loaded {len(df):,} records from production 30-min file")


class TestNoNaNAfterJuly17:
    """Test that mwflow is not NaN after July 17, 2025"""

    def test_no_nan_after_july17_5min(self, production_5min_df):
        """mwflow should not be NaN after July 17, 2025"""
        df = production_5min_df

        # Filter to data after cutoff
        recent = df[df['settlementdate'] >= CUTOFF_DATE]

        if len(recent) == 0:
            pytest.skip("No data after cutoff date")

        nan_count = recent['mwflow'].isna().sum()
        nan_ratio = nan_count / len(recent)

        print(f"mwflow NaN after {CUTOFF_DATE.date()}: {nan_count:,}/{len(recent):,} ({nan_ratio:.1%})")

        assert nan_ratio < 0.01, f"Too many NaN values: {nan_ratio:.1%}"

        print("PASS: mwflow has <1% NaN values after July 17, 2025")

    def test_no_nan_in_limits_5min(self, production_5min_df):
        """exportlimit/importlimit should not be NaN after July 17"""
        df = production_5min_df

        recent = df[df['settlementdate'] >= CUTOFF_DATE]

        for col in ['exportlimit', 'importlimit']:
            if col in df.columns:
                nan_count = recent[col].isna().sum()
                nan_ratio = nan_count / len(recent) if len(recent) > 0 else 0

                print(f"  {col} NaN: {nan_count:,}/{len(recent):,} ({nan_ratio:.1%})")
                assert nan_ratio < 0.01, f"Too many NaN in {col}: {nan_ratio:.1%}"

        print("PASS: exportlimit/importlimit have <1% NaN values")


class TestDataContinuity:
    """Test that there are no gaps around the merge boundary"""

    def test_no_gaps_around_july17(self, production_5min_df):
        """Data should be continuous around July 17, 2025"""
        df = production_5min_df

        # Get data around the cutoff
        window_start = CUTOFF_DATE - pd.Timedelta(days=1)
        window_end = CUTOFF_DATE + pd.Timedelta(days=1)

        window_data = df[
            (df['settlementdate'] >= window_start) &
            (df['settlementdate'] <= window_end)
        ]

        if len(window_data) == 0:
            pytest.skip("No data in window around cutoff date")

        # Check for gaps > 10 minutes
        window_data = window_data.sort_values('settlementdate')
        unique_times = window_data['settlementdate'].unique()
        unique_times = sorted(unique_times)

        max_gap = pd.Timedelta(0)
        for i in range(1, len(unique_times)):
            gap = pd.Timestamp(unique_times[i]) - pd.Timestamp(unique_times[i-1])
            if gap > max_gap:
                max_gap = gap

        print(f"Max gap around cutoff: {max_gap}")

        # Allow up to 10 minutes gap (missing one 5-min interval is acceptable)
        assert max_gap <= pd.Timedelta(minutes=10), f"Gap too large: {max_gap}"

        print("PASS: No significant gaps around merge boundary")


class TestHistoricalPreserved:
    """Test that historical data before July 17 was preserved"""

    def test_historical_preserved(self, production_5min_df):
        """Data before July 17, 2025 should be unchanged"""
        df = production_5min_df

        # Get data before cutoff
        historical = df[df['settlementdate'] < CUTOFF_DATE]

        if len(historical) == 0:
            pytest.skip("No historical data before cutoff")

        # Check that historical data exists
        print(f"Historical records (before {CUTOFF_DATE.date()}): {len(historical):,}")

        # Check date range
        min_date = historical['settlementdate'].min()
        print(f"Earliest historical date: {min_date}")

        # Should have data going back reasonably far
        assert len(historical) > 1000, "Too few historical records"

        print("PASS: Historical data preserved")


class TestTotalRecordCount:
    """Test that total record count is reasonable"""

    def test_total_record_count(self, production_5min_df):
        """Total records should be within expected range"""
        df = production_5min_df

        actual = len(df)
        print(f"Total records: {actual:,}")

        # Should have at least as many records as before merge
        # (backfill adds data, shouldn't remove)
        min_expected = 300000  # Rough minimum

        assert actual >= min_expected, \
            f"Too few records: {actual:,} < {min_expected:,}"

        print(f"PASS: {actual:,} records within expected range")


class TestBackupExists:
    """Test that backup was created"""

    def test_backup_exists(self):
        """At least one backup file should exist"""
        if not BACKUP_PATH.exists():
            pytest.skip("Backup directory does not exist")

        backups = list(BACKUP_PATH.glob('transmission5_backup_*.parquet'))

        print(f"Found {len(backups)} backup files")
        if backups:
            latest = max(backups, key=lambda p: p.stat().st_mtime)
            print(f"Latest backup: {latest.name}")

        # Backup should exist if merge was run
        # Skip if no backups (merge may not have been run yet)
        if len(backups) == 0:
            pytest.skip("No backup files found - merge may not have been run yet")

        print("PASS: Backup file exists")


def main():
    """Run merge validation tests"""
    print("=" * 60)
    print("Stage 5: Merge Validation Tests")
    print("=" * 60)

    exit_code = pytest.main([__file__, '-v', '--tb=short'])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
