#!/usr/bin/env python3
"""
Stage 1: Transmission Data Diagnosis Tests
Tests to confirm the root cause of NaN values in transmission data.

Expected results:
- AEMO CSV files contain all required columns (MWFLOW, EXPORTLIMIT, etc.)
- unified_collector.py is not extracting these columns
- transmission5.parquet has NaN pattern starting July 17, 2025
"""

import sys
from pathlib import Path
import pandas as pd
import pytest
import requests
import zipfile
import io

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


class TestAEMODataSource:
    """Test that AEMO source data contains all required fields"""

    def test_aemo_csv_has_all_fields(self):
        """
        Download current DISPATCHIS file and verify INTERCONNECTORRES table
        has all expected columns including MWFLOW, EXPORTLIMIT, IMPORTLIMIT.
        """
        from bs4 import BeautifulSoup

        # Get latest file from AEMO
        url = 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/'
        headers = {'User-Agent': 'AEMO Dashboard Data Collector'}

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        files = []
        for link in soup.find_all('a'):
            filename = link.text.strip()
            if 'PUBLIC_DISPATCHIS_' in filename and filename.endswith('.zip'):
                files.append(filename)

        assert len(files) > 0, "No DISPATCHIS files found on AEMO server"

        # Download latest file
        latest_file = sorted(files)[-1]
        file_url = f"{url}{latest_file}"

        response = requests.get(file_url, headers=headers, timeout=60)
        response.raise_for_status()

        # Parse the ZIP file
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            csv_files = [f for f in z.namelist() if f.endswith('.csv') or f.endswith('.CSV')]
            assert len(csv_files) > 0, "No CSV file in ZIP"

            csv_content = z.read(csv_files[0]).decode('utf-8', errors='ignore')

        # Parse MMS format to find INTERCONNECTORRES header
        lines = csv_content.strip().split('\n')
        interconnector_header = None

        for line in lines:
            if line.startswith('I,') and 'INTERCONNECTORRES' in line:
                interconnector_header = line.split(',')
                break

        assert interconnector_header is not None, "INTERCONNECTORRES table not found in CSV"

        # Check for required columns
        required_columns = ['INTERCONNECTORID', 'METEREDMWFLOW', 'MWFLOW', 'MWLOSSES',
                          'EXPORTLIMIT', 'IMPORTLIMIT']

        for col in required_columns:
            assert col in interconnector_header, f"Column {col} not found in AEMO data. Found: {interconnector_header}"

        print(f"PASS: AEMO source data contains all required columns: {required_columns}")


class TestUnifiedCollectorBug:
    """Test that confirms the bug in unified_collector.py"""

    def test_unified_collector_missing_columns(self):
        """
        Call collect_5min_transmission() and verify it's missing columns.
        This test confirms the bug exists.
        """
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

        collector = UnifiedAEMOCollector()

        # Clear last files to force download
        collector.last_files['transmission5'] = set()

        # Collect data
        df = collector.collect_5min_transmission()

        if df.empty:
            pytest.skip("No new transmission files available to test")

        # Check what columns were returned
        columns = df.columns.tolist()
        print(f"Columns returned by collect_5min_transmission(): {columns}")

        # The bug: these columns should be present but are likely missing
        expected_columns = ['settlementdate', 'interconnectorid', 'meteredmwflow',
                           'mwflow', 'mwlosses', 'exportlimit', 'importlimit']

        missing = [c for c in expected_columns if c not in columns]

        if missing:
            print(f"BUG CONFIRMED: Missing columns: {missing}")
            # This is expected to fail - it confirms the bug exists
            assert len(missing) == 0, f"Bug confirmed: Missing columns {missing}. Fix unified_collector.py"
        else:
            print("All columns present - bug may have been fixed")


class TestTransmission5NaNPattern:
    """Test the NaN pattern in transmission5.parquet"""

    @pytest.fixture
    def transmission5_df(self):
        """Load transmission5.parquet"""
        path = Path('/Volumes/davidleitch/aemo_production/data/transmission5.parquet')
        if not path.exists():
            pytest.skip("transmission5.parquet not found")
        return pd.read_parquet(path)

    def test_transmission5_nan_pattern(self, transmission5_df):
        """
        Verify NaN pattern matches July 17, 2025 cutoff.
        mwflow should be valid before cutoff, NaN after.
        """
        df = transmission5_df

        # Check if mwflow column exists
        if 'mwflow' not in df.columns:
            pytest.fail("mwflow column does not exist in transmission5.parquet")

        # Find the transition point
        has_mwflow = df[df['mwflow'].notna()]
        no_mwflow = df[df['mwflow'].isna()]

        if len(has_mwflow) == 0:
            pytest.fail("No valid mwflow values found at all")

        if len(no_mwflow) == 0:
            pytest.skip("No NaN mwflow values - data may have been fixed")

        last_valid = has_mwflow['settlementdate'].max()
        first_nan = no_mwflow['settlementdate'].min()

        print(f"Last valid mwflow: {last_valid}")
        print(f"First NaN mwflow: {first_nan}")
        print(f"Valid records: {len(has_mwflow)}")
        print(f"NaN records: {len(no_mwflow)}")

        # The cutoff should be around July 17, 2025
        expected_cutoff = pd.Timestamp('2025-07-17')

        # Allow some tolerance (within a day)
        assert last_valid >= expected_cutoff - pd.Timedelta(days=1), \
            f"Last valid mwflow {last_valid} is earlier than expected cutoff {expected_cutoff}"
        assert last_valid <= expected_cutoff + pd.Timedelta(days=1), \
            f"Last valid mwflow {last_valid} is later than expected cutoff {expected_cutoff}"

        # Verify significant data loss
        nan_ratio = len(no_mwflow) / len(df)
        print(f"NaN ratio: {nan_ratio:.1%}")

        assert nan_ratio > 0.5, f"Expected >50% NaN values, got {nan_ratio:.1%}"

        print(f"PASS: NaN pattern confirmed - cutoff around {last_valid}")


class TestTransmission30AlsoAffected:
    """Test if transmission30.parquet has the same issue"""

    def test_transmission30_also_affected(self):
        """
        Check if transmission30.parquet has the same NaN issue.
        This confirms the scope of the problem.
        """
        path = Path('/Volumes/davidleitch/aemo_production/data/transmission30.parquet')
        if not path.exists():
            pytest.skip("transmission30.parquet not found")

        df = pd.read_parquet(path)

        print(f"Columns in transmission30: {df.columns.tolist()}")
        print(f"Shape: {df.shape}")

        # Check for missing columns
        expected_columns = ['mwflow', 'mwlosses', 'exportlimit', 'importlimit']
        missing = [c for c in expected_columns if c not in df.columns]

        if missing:
            print(f"Missing columns in transmission30: {missing}")

        # Check NaN pattern for mwflow if it exists
        if 'mwflow' in df.columns:
            nan_count = df['mwflow'].isna().sum()
            nan_ratio = nan_count / len(df)
            print(f"mwflow NaN ratio: {nan_ratio:.1%} ({nan_count} of {len(df)})")

            if nan_ratio > 0.5:
                print("CONFIRMED: transmission30 is also affected")
                assert True  # Expected - confirms scope
            else:
                print("transmission30 appears to have valid data")
        else:
            print("mwflow column not present in transmission30")

        # Also check if exportlimit/importlimit are missing
        for col in ['exportlimit', 'importlimit', 'mwlosses']:
            if col not in df.columns:
                print(f"Column {col} missing from transmission30")


def main():
    """Run diagnosis tests"""
    print("=" * 60)
    print("Stage 1: Transmission Data Diagnosis")
    print("=" * 60)

    # Run pytest with verbose output
    exit_code = pytest.main([__file__, '-v', '--tb=short'])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
