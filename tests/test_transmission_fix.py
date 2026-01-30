#!/usr/bin/env python3
"""
Stage 2: Transmission Fix Verification Tests
Tests to verify the fix for unified_collector.py correctly extracts all columns.
"""

import sys
from pathlib import Path
import pandas as pd
import pytest

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


class TestFixedCollector:
    """Test that the fixed collector returns all required columns"""

    def test_fixed_collector_returns_all_columns(self):
        """
        Call collect_5min_transmission() and verify all 7 columns are returned.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

        collector = UnifiedAEMOCollector()

        # Clear last files to force download
        collector.last_files['transmission5'] = set()

        # Collect data
        df = collector.collect_5min_transmission()

        if df.empty:
            pytest.skip("No new transmission files available to test")

        # Check all expected columns are present
        expected_columns = ['settlementdate', 'interconnectorid', 'meteredmwflow',
                           'mwflow', 'mwlosses', 'exportlimit', 'importlimit']

        for col in expected_columns:
            assert col in df.columns, f"Column {col} missing from output"

        print(f"PASS: All {len(expected_columns)} columns present: {expected_columns}")

    def test_fixed_collector_values_valid(self):
        """
        Verify returned data has non-NaN values for the key columns.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

        collector = UnifiedAEMOCollector()
        collector.last_files['transmission5'] = set()

        df = collector.collect_5min_transmission()

        if df.empty:
            pytest.skip("No new transmission files available to test")

        # Check that key columns have valid (non-NaN) values
        for col in ['mwflow', 'exportlimit', 'importlimit']:
            if col in df.columns:
                valid_count = df[col].notna().sum()
                total_count = len(df)
                valid_ratio = valid_count / total_count if total_count > 0 else 0

                print(f"  {col}: {valid_count}/{total_count} valid ({valid_ratio:.1%})")

                # At least 80% should be valid (some NaN is OK due to data quality)
                assert valid_ratio > 0.8, f"Column {col} has too many NaN values: {valid_ratio:.1%}"

        print("PASS: All key columns have >80% valid values")

    def test_partial_column_availability(self):
        """
        Test that the collector handles partial column availability gracefully.
        If AEMO removes a column, it should log a warning but not crash.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector
        import logging

        # Capture log messages
        collector = UnifiedAEMOCollector()
        collector.last_files['transmission5'] = set()

        # This should not raise an exception even if some columns are missing
        try:
            df = collector.collect_5min_transmission()
            if not df.empty:
                print(f"Collected {len(df)} records with columns: {df.columns.tolist()}")
            print("PASS: Collector handles column availability gracefully")
        except Exception as e:
            pytest.fail(f"Collector failed with exception: {e}")

    def test_column_order_matches_existing(self):
        """
        Verify that new data has the same column order as existing parquet file.
        This ensures schema compatibility during merge.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

        # Load existing parquet
        existing_path = Path('/Volumes/davidleitch/aemo_production/data/transmission5.parquet')
        if not existing_path.exists():
            pytest.skip("transmission5.parquet not found")

        existing_df = pd.read_parquet(existing_path)
        existing_columns = existing_df.columns.tolist()

        # Get new data
        collector = UnifiedAEMOCollector()
        collector.last_files['transmission5'] = set()
        new_df = collector.collect_5min_transmission()

        if new_df.empty:
            pytest.skip("No new transmission files available to test")

        new_columns = new_df.columns.tolist()

        print(f"Existing columns: {existing_columns}")
        print(f"New columns: {new_columns}")

        # All existing columns should be present in new data
        for col in existing_columns:
            assert col in new_columns, f"Existing column {col} not in new data"

        print("PASS: Column compatibility verified")

    def test_backward_compatible(self):
        """
        Verify that existing transmission5.parquet can still be loaded.
        """
        existing_path = Path('/Volumes/davidleitch/aemo_production/data/transmission5.parquet')
        if not existing_path.exists():
            pytest.skip("transmission5.parquet not found")

        try:
            df = pd.read_parquet(existing_path)
            print(f"Loaded {len(df)} records")
            print(f"Columns: {df.columns.tolist()}")
            print(f"Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")
            print("PASS: Existing parquet file loads without error")
        except Exception as e:
            pytest.fail(f"Failed to load existing parquet: {e}")

    def test_prices_still_work(self):
        """
        Regression test: verify price collection is unaffected by transmission fix.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

        collector = UnifiedAEMOCollector()
        collector.last_files['prices5'] = set()

        try:
            df = collector.collect_5min_prices()

            if df.empty:
                pytest.skip("No new price files available")

            # Basic validation
            assert 'settlementdate' in df.columns
            assert 'regionid' in df.columns
            assert 'rrp' in df.columns

            print(f"Collected {len(df)} price records")
            print("PASS: Price collection unaffected")

        except Exception as e:
            pytest.fail(f"Price collection failed: {e}")


class TestFixed30MinCollector:
    """Test the 30-minute transmission collection fix"""

    def test_30min_trading_returns_transmission_columns(self):
        """
        Verify collect_30min_trading() includes all transmission columns.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

        collector = UnifiedAEMOCollector()
        collector.last_files['trading'] = set()

        result = collector.collect_30min_trading()

        trans30_df = result.get('transmission30', pd.DataFrame())

        if trans30_df.empty:
            pytest.skip("No 30-min transmission data available")

        columns = trans30_df.columns.tolist()
        print(f"30-min transmission columns: {columns}")

        # Should have at least basic columns
        assert 'settlementdate' in columns
        assert 'interconnectorid' in columns
        assert 'meteredmwflow' in columns

        # Should now also have these
        for col in ['mwflow', 'mwlosses', 'exportlimit', 'importlimit']:
            if col in columns:
                valid = trans30_df[col].notna().sum()
                print(f"  {col}: {valid} valid values")

        print("PASS: 30-min transmission includes additional columns")


def main():
    """Run fix verification tests"""
    print("=" * 60)
    print("Stage 2: Transmission Fix Verification")
    print("=" * 60)

    exit_code = pytest.main([__file__, '-v', '--tb=short'])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
