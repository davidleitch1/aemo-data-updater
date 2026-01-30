#!/usr/bin/env python3
"""
Stage 3: Backfill Download Tests
Tests for the backfill download script.
"""

import sys
from pathlib import Path
import pandas as pd
import pytest
import json
import tempfile
import requests

# Add the backfill_scripts directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'backfill_scripts'))


class TestArchiveAccess:
    """Test access to AEMO archive URLs"""

    def test_archive_url_valid(self):
        """Check that AEMO archive URL is accessible"""
        url = 'https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/'
        headers = {'User-Agent': 'AEMO Dashboard Data Collector - Test'}

        try:
            response = requests.get(url, headers=headers, timeout=30)
            assert response.status_code == 200, f"Archive URL returned {response.status_code}"
            assert len(response.text) > 0, "Archive page is empty"

            # Check that it contains ZIP files
            assert '.zip' in response.text.lower(), "No ZIP files found in archive listing"

            print(f"PASS: Archive URL accessible, page size: {len(response.text)} bytes")
        except requests.RequestException as e:
            pytest.fail(f"Could not access archive URL: {e}")


class TestSingleDayDownload:
    """Test downloading data for a single day"""

    def test_single_day_download(self):
        """Download one day (2025-07-20) and verify data"""
        from backfill_transmission_full import download_single_dispatch_file
        from datetime import datetime

        test_date = datetime(2025, 7, 20)
        data = download_single_dispatch_file(test_date)

        if not data:
            pytest.skip("No data available for test date - archive may not be available")

        df = pd.DataFrame(data)

        # Should have reasonable number of records (288 intervals * 6 interconnectors = 1728)
        assert len(df) >= 100, f"Expected at least 100 records, got {len(df)}"

        # Should have all required columns
        required_cols = ['settlementdate', 'interconnectorid', 'meteredmwflow',
                        'mwflow', 'exportlimit', 'importlimit']
        for col in required_cols:
            assert col in df.columns, f"Missing column: {col}"

        # Should have valid data
        assert df['mwflow'].notna().sum() > 0, "No valid mwflow values"

        print(f"PASS: Downloaded {len(df)} records for {test_date.date()}")
        print(f"  Columns: {df.columns.tolist()}")
        print(f"  mwflow valid: {df['mwflow'].notna().sum()}")


class TestOutputPath:
    """Test that output goes to correct temp files"""

    def test_temp_file_path(self):
        """Verify output paths end with _backfill_temp.parquet"""
        from backfill_transmission_full import OUTPUT_5MIN, OUTPUT_30MIN

        assert str(OUTPUT_5MIN).endswith('_backfill_temp.parquet'), \
            f"5-min output path should end with _backfill_temp.parquet: {OUTPUT_5MIN}"

        assert str(OUTPUT_30MIN).endswith('_backfill_temp.parquet'), \
            f"30-min output path should end with _backfill_temp.parquet: {OUTPUT_30MIN}"

        print(f"PASS: Output paths are correct")
        print(f"  5-min: {OUTPUT_5MIN}")
        print(f"  30-min: {OUTPUT_30MIN}")


class TestCheckpointing:
    """Test checkpoint save/load functionality"""

    def test_checkpoint_save_load(self):
        """Verify checkpoint saves and loads correctly"""
        from backfill_transmission_full import save_checkpoint, load_checkpoint, CHECKPOINT_FILE
        from datetime import datetime
        import tempfile
        import os

        # Use a temporary checkpoint file for testing
        original_checkpoint = CHECKPOINT_FILE

        with tempfile.TemporaryDirectory() as tmpdir:
            # Override checkpoint file path temporarily
            import backfill_transmission_full
            test_checkpoint = Path(tmpdir) / 'test_checkpoint.json'
            backfill_transmission_full.CHECKPOINT_FILE = test_checkpoint
            backfill_transmission_full.OUTPUT_5MIN = Path(tmpdir) / 'test_5min.parquet'
            backfill_transmission_full.OUTPUT_30MIN = Path(tmpdir) / 'test_30min.parquet'

            # Create test data
            test_date = datetime(2025, 8, 15)
            test_data_5min = [
                {'settlementdate': datetime(2025, 8, 15, 12, 0), 'interconnectorid': 'TEST1', 'mwflow': 100.0},
                {'settlementdate': datetime(2025, 8, 15, 12, 5), 'interconnectorid': 'TEST1', 'mwflow': 105.0},
            ]
            test_data_30min = []

            # Save checkpoint
            save_checkpoint(test_date, test_data_5min, test_data_30min)

            # Verify checkpoint file exists
            assert test_checkpoint.exists(), "Checkpoint file not created"

            # Load checkpoint
            loaded_date, loaded_5min, loaded_30min = load_checkpoint()

            assert loaded_date == test_date, f"Loaded date {loaded_date} != saved date {test_date}"
            assert len(loaded_5min) == len(test_data_5min), "Loaded 5-min data count mismatch"

            # Restore original paths
            backfill_transmission_full.CHECKPOINT_FILE = original_checkpoint

        print("PASS: Checkpoint round-trip successful")


class TestSchemaValidation:
    """Test AEMO schema validation"""

    def test_schema_validation(self):
        """Verify schema validation detects missing columns"""
        from backfill_transmission_full import validate_aemo_schema, REQUIRED_COLUMNS

        # Test with valid schema
        valid_df = pd.DataFrame({col: [1, 2, 3] for col in REQUIRED_COLUMNS})

        try:
            validate_aemo_schema(valid_df)
            print("PASS: Valid schema accepted")
        except ValueError:
            pytest.fail("Valid schema was rejected")

        # Test with missing columns
        invalid_df = pd.DataFrame({
            'INTERCONNECTORID': ['A', 'B'],
            'METEREDMWFLOW': [100, 200]
            # Missing MWFLOW, EXPORTLIMIT, etc.
        })

        with pytest.raises(ValueError) as exc_info:
            validate_aemo_schema(invalid_df)

        assert 'Missing columns' in str(exc_info.value)
        print(f"PASS: Missing columns detected: {exc_info.value}")


class TestAggregation:
    """Test 30-minute aggregation from 5-minute data"""

    def test_30min_aggregation(self):
        """Test that 5-min data aggregates correctly to 30-min"""
        from backfill_transmission_full import aggregate_to_30min
        from datetime import datetime

        # Create test 5-minute data (6 intervals for one 30-min period)
        test_data = []
        base_time = datetime(2025, 8, 15, 12, 0)

        for i in range(6):
            test_data.append({
                'settlementdate': base_time + pd.Timedelta(minutes=5*i + 5),
                'interconnectorid': 'TEST1',
                'meteredmwflow': 100 + i,
                'mwflow': 200 + i,
                'mwlosses': 10 + i,
                'exportlimit': 500,
                'importlimit': 500,
            })

        df_5min = pd.DataFrame(test_data)
        df_30min = aggregate_to_30min(df_5min)

        assert len(df_30min) == 1, f"Expected 1 30-min record, got {len(df_30min)}"

        # Check that values are averaged correctly
        expected_mwflow = sum(200 + i for i in range(6)) / 6
        actual_mwflow = df_30min['mwflow'].iloc[0]

        assert abs(actual_mwflow - expected_mwflow) < 0.01, \
            f"Aggregated mwflow {actual_mwflow} != expected {expected_mwflow}"

        print(f"PASS: 30-min aggregation correct (mwflow avg: {actual_mwflow:.2f})")


def main():
    """Run backfill download tests"""
    print("=" * 60)
    print("Stage 3: Backfill Download Tests")
    print("=" * 60)

    exit_code = pytest.main([__file__, '-v', '--tb=short'])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
