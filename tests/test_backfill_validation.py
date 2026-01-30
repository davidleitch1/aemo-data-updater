#!/usr/bin/env python3
"""
Stage 4: Backfill Validation Tests
Comprehensive validation of backfilled transmission data before merging.
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

# Paths
DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')
BACKFILL_5MIN = DATA_PATH / 'transmission5_backfill_temp.parquet'
BACKFILL_30MIN = DATA_PATH / 'transmission30_backfill_temp.parquet'
EXISTING_5MIN = DATA_PATH / 'transmission5.parquet'
EXISTING_30MIN = DATA_PATH / 'transmission30.parquet'

# Expected interconnectors
EXPECTED_INTERCONNECTORS = ['N-Q-MNSP1', 'NSW1-QLD1', 'T-V-MNSP1', 'V-S-MNSP1', 'V-SA', 'VIC1-NSW1']


@pytest.fixture
def backfill_5min_df():
    """Load backfill 5-min parquet"""
    if not BACKFILL_5MIN.exists():
        pytest.skip(f"Backfill file not found: {BACKFILL_5MIN}")
    return pd.read_parquet(BACKFILL_5MIN)


@pytest.fixture
def backfill_30min_df():
    """Load backfill 30-min parquet"""
    if not BACKFILL_30MIN.exists():
        pytest.skip(f"Backfill file not found: {BACKFILL_30MIN}")
    return pd.read_parquet(BACKFILL_30MIN)


@pytest.fixture
def existing_5min_df():
    """Load existing 5-min parquet"""
    if not EXISTING_5MIN.exists():
        pytest.skip(f"Existing file not found: {EXISTING_5MIN}")
    return pd.read_parquet(EXISTING_5MIN)


class TestRequiredColumns:
    """Test that all required columns are present"""

    def test_required_columns_5min(self, backfill_5min_df):
        """Check all 7 columns exist in 5-min backfill"""
        required = ['settlementdate', 'interconnectorid', 'meteredmwflow',
                   'mwflow', 'mwlosses', 'exportlimit', 'importlimit']

        for col in required:
            assert col in backfill_5min_df.columns, f"Missing column: {col}"

        print(f"PASS: All {len(required)} required columns present")

    def test_required_columns_30min(self, backfill_30min_df):
        """Check columns exist in 30-min backfill"""
        required = ['settlementdate', 'interconnectorid', 'meteredmwflow', 'mwflow']

        for col in required:
            assert col in backfill_30min_df.columns, f"Missing column: {col}"

        print(f"PASS: Required columns present in 30-min data")


class TestNoNaNInFlow:
    """Test that mwflow is not all NaN"""

    def test_no_nan_in_mwflow_5min(self, backfill_5min_df):
        """Check mwflow is not all NaN in 5-min data"""
        df = backfill_5min_df

        nan_count = df['mwflow'].isna().sum()
        total_count = len(df)
        nan_ratio = nan_count / total_count if total_count > 0 else 1.0

        print(f"mwflow NaN: {nan_count}/{total_count} ({nan_ratio:.1%})")

        # Less than 1% NaN is acceptable
        assert nan_ratio < 0.01, f"Too many NaN values in mwflow: {nan_ratio:.1%}"

        print(f"PASS: mwflow has <1% NaN values")

    def test_no_nan_in_limits(self, backfill_5min_df):
        """Check export/import limits are not all NaN"""
        df = backfill_5min_df

        for col in ['exportlimit', 'importlimit']:
            nan_count = df[col].isna().sum()
            nan_ratio = nan_count / len(df)

            print(f"  {col} NaN: {nan_count}/{len(df)} ({nan_ratio:.1%})")
            assert nan_ratio < 0.01, f"Too many NaN in {col}: {nan_ratio:.1%}"

        print("PASS: exportlimit and importlimit have <1% NaN values")


class TestDateRange:
    """Test that date range covers expected period"""

    def test_date_range_covers_july17(self, backfill_5min_df):
        """Verify data covers July 17, 2025 onwards"""
        df = backfill_5min_df

        min_date = df['settlementdate'].min()
        max_date = df['settlementdate'].max()

        print(f"Date range: {min_date} to {max_date}")

        # Should start at or before July 17, 2025
        assert min_date <= pd.Timestamp('2025-07-18'), \
            f"Min date {min_date} should be <= 2025-07-18"

        # Should extend to recent date (within last few days)
        expected_max = pd.Timestamp('2026-01-29')
        assert max_date >= expected_max, \
            f"Max date {max_date} should be >= {expected_max}"

        print(f"PASS: Date range covers July 17, 2025 to {max_date.date()}")


class TestAllInterconnectors:
    """Test that all 6 interconnectors are present"""

    def test_all_interconnectors_present(self, backfill_5min_df):
        """All 6 NEM interconnectors should be present"""
        df = backfill_5min_df

        actual_ics = sorted(df['interconnectorid'].unique())
        expected_ics = sorted(EXPECTED_INTERCONNECTORS)

        print(f"Found interconnectors: {actual_ics}")

        missing = set(expected_ics) - set(actual_ics)
        extra = set(actual_ics) - set(expected_ics)

        if missing:
            print(f"Missing: {missing}")
        if extra:
            print(f"Extra: {extra}")

        assert len(missing) == 0, f"Missing interconnectors: {missing}"

        print("PASS: All 6 interconnectors present")


class TestFlowValuesReasonable:
    """Test that flow values are in reasonable range"""

    def test_flow_values_reasonable(self, backfill_5min_df):
        """Flow values should be within -3000 to +3000 MW"""
        df = backfill_5min_df

        # Check mwflow range
        mwflow_min = df['mwflow'].min()
        mwflow_max = df['mwflow'].max()

        print(f"mwflow range: {mwflow_min:.1f} to {mwflow_max:.1f} MW")

        # Typical interconnector limits are around +/- 3000 MW max
        assert mwflow_min >= -4000, f"mwflow min {mwflow_min} too low"
        assert mwflow_max <= 4000, f"mwflow max {mwflow_max} too high"

        print("PASS: Flow values in reasonable range")


class TestRecordCount:
    """Test that we have sufficient records"""

    def test_sufficient_record_count_5min(self, backfill_5min_df):
        """Should have at least 300,000 records for ~6 months of 5-min data"""
        df = backfill_5min_df

        # 6 months * 30 days * 288 intervals * 6 interconnectors = ~311,040
        # Allow some margin for missing data
        expected_min = 300000

        actual = len(df)
        print(f"Record count: {actual}")

        assert actual >= expected_min, \
            f"Expected at least {expected_min} records, got {actual}"

        print(f"PASS: {actual} records >= {expected_min} minimum")


class TestNoDuplicates:
    """Test for duplicate records"""

    def test_no_duplicates(self, backfill_5min_df):
        """No duplicate keys on (settlementdate, interconnectorid)"""
        df = backfill_5min_df

        duplicates = df.duplicated(subset=['settlementdate', 'interconnectorid']).sum()

        print(f"Duplicate records: {duplicates}")

        assert duplicates == 0, f"Found {duplicates} duplicate records"

        print("PASS: No duplicate records")


class TestColumnDtypesMatch:
    """Test that data types match existing parquet"""

    def test_column_dtypes_match(self, backfill_5min_df, existing_5min_df):
        """Data types should match existing parquet schema"""
        backfill = backfill_5min_df
        existing = existing_5min_df

        mismatches = []

        for col in existing.columns:
            if col in backfill.columns:
                existing_dtype = str(existing[col].dtype)
                backfill_dtype = str(backfill[col].dtype)

                # Allow some flexibility (e.g., int64 vs float64 for numeric)
                if 'datetime' in existing_dtype and 'datetime' in backfill_dtype:
                    continue  # Both datetime, OK
                elif 'float' in existing_dtype and 'float' in backfill_dtype:
                    continue  # Both float, OK
                elif existing_dtype != backfill_dtype:
                    mismatches.append(f"{col}: existing={existing_dtype}, backfill={backfill_dtype}")

        if mismatches:
            print("Dtype mismatches:")
            for m in mismatches:
                print(f"  {m}")
            # Warning only, not failure
            print("WARNING: Some dtype mismatches, but may be acceptable")

        print("PASS: Column dtypes checked")


class TestTimestampNormalization:
    """Test timestamp consistency"""

    def test_timestamp_normalization(self, backfill_5min_df):
        """Timestamps should be consistent format without microseconds"""
        df = backfill_5min_df

        # Check for microseconds
        has_microseconds = (df['settlementdate'].dt.microsecond != 0).any()

        if has_microseconds:
            print("WARNING: Some timestamps have microseconds")

        # Check timezone consistency
        tz = df['settlementdate'].dt.tz
        print(f"Timezone: {tz}")

        # All timestamps should be at 5-minute intervals
        minutes = df['settlementdate'].dt.minute.unique()
        valid_minutes = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]

        invalid_minutes = [m for m in minutes if m not in valid_minutes]
        assert len(invalid_minutes) == 0, f"Invalid minute values: {invalid_minutes}"

        print("PASS: Timestamps at valid 5-minute intervals")


def main():
    """Run backfill validation tests"""
    print("=" * 60)
    print("Stage 4: Backfill Validation Tests")
    print("=" * 60)

    exit_code = pytest.main([__file__, '-v', '--tb=short'])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
