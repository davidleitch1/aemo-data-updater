"""
Health Check for AEMO Data Quality

Provides ongoing monitoring of data quality, with specific checks
for transmission data following the July 2025 fix.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import os

from .config import get_config

logger = logging.getLogger('aemo_updater.health_check')


class TransmissionHealthCheck:
    """
    Health check specifically for transmission data quality.

    Monitors for the NaN issue that affected data from July 17, 2025.
    """

    def __init__(self, data_path: Optional[Path] = None):
        if data_path is None:
            data_path = Path(os.getenv('AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data'))

        self.data_path = data_path
        self.transmission5_file = self.data_path / 'transmission5.parquet'
        self.transmission30_file = self.data_path / 'transmission30.parquet'

        # Columns that should have valid (non-NaN) values
        self.critical_columns = ['mwflow', 'exportlimit', 'importlimit']

        # Maximum acceptable NaN ratio
        self.max_nan_ratio = 0.01  # 1%

    def check_transmission_health(self) -> Dict[str, Any]:
        """
        Daily health check for transmission data quality.

        Returns:
            Dict with status and details
        """
        result = {
            'status': 'OK',
            'timestamp': datetime.now().isoformat(),
            'checks': [],
            'warnings': [],
            'errors': []
        }

        # Check 5-minute data
        try:
            result_5min = self._check_transmission_file(
                self.transmission5_file,
                '5-minute'
            )
            result['checks'].append(result_5min)

            if result_5min['status'] == 'ERROR':
                result['errors'].append(result_5min['message'])
                result['status'] = 'ERROR'
            elif result_5min['status'] == 'WARNING':
                result['warnings'].append(result_5min['message'])
                if result['status'] != 'ERROR':
                    result['status'] = 'WARNING'

        except Exception as e:
            result['errors'].append(f"5-min check failed: {e}")
            result['status'] = 'ERROR'

        # Check 30-minute data
        try:
            result_30min = self._check_transmission_file(
                self.transmission30_file,
                '30-minute'
            )
            result['checks'].append(result_30min)

            if result_30min['status'] == 'ERROR':
                result['errors'].append(result_30min['message'])
                result['status'] = 'ERROR'
            elif result_30min['status'] == 'WARNING':
                result['warnings'].append(result_30min['message'])
                if result['status'] != 'ERROR':
                    result['status'] = 'WARNING'

        except Exception as e:
            result['errors'].append(f"30-min check failed: {e}")
            result['status'] = 'ERROR'

        return result

    def _check_transmission_file(self, file_path: Path, name: str) -> Dict[str, Any]:
        """Check a single transmission file for data quality"""
        result = {
            'name': name,
            'file': str(file_path),
            'status': 'OK',
            'message': '',
            'details': {}
        }

        if not file_path.exists():
            result['status'] = 'ERROR'
            result['message'] = f"{name} file not found"
            return result

        df = pd.read_parquet(file_path)

        if df.empty:
            result['status'] = 'ERROR'
            result['message'] = f"{name} file is empty"
            return result

        # Get recent data (last hour)
        recent = df[df['settlementdate'] > pd.Timestamp.now() - pd.Timedelta(hours=1)]
        result['details']['recent_records'] = len(recent)

        if len(recent) == 0:
            result['status'] = 'WARNING'
            result['message'] = f"No {name} data in last hour"
            return result

        # Check NaN ratios for critical columns
        for col in self.critical_columns:
            if col not in df.columns:
                result['status'] = 'WARNING'
                result['message'] = f"Column {col} missing from {name} data"
                continue

            nan_count = recent[col].isna().sum()
            nan_ratio = nan_count / len(recent)
            result['details'][f'{col}_nan_ratio'] = nan_ratio

            if nan_ratio > self.max_nan_ratio:
                result['status'] = 'ERROR'
                result['message'] = f"{name} {col} has {nan_ratio:.1%} NaN values in last hour"
                return result

        result['message'] = f"{name}: {len(recent)} records, all columns valid"
        return result

    def check_recent_quality(self, hours: int = 24) -> Dict[str, Any]:
        """
        Check data quality for recent time period.

        Args:
            hours: Number of hours to check (default 24)

        Returns:
            Dict with quality metrics
        """
        result = {
            'period_hours': hours,
            'timestamp': datetime.now().isoformat(),
            'files': {}
        }

        for file_path, name in [
            (self.transmission5_file, '5-minute'),
            (self.transmission30_file, '30-minute')
        ]:
            try:
                df = pd.read_parquet(file_path)
                cutoff = pd.Timestamp.now() - pd.Timedelta(hours=hours)
                recent = df[df['settlementdate'] > cutoff]

                file_result = {
                    'total_records': len(recent),
                    'date_range': {
                        'start': str(recent['settlementdate'].min()) if len(recent) > 0 else None,
                        'end': str(recent['settlementdate'].max()) if len(recent) > 0 else None
                    },
                    'columns': {}
                }

                for col in self.critical_columns:
                    if col in recent.columns:
                        file_result['columns'][col] = {
                            'valid_count': int(recent[col].notna().sum()),
                            'nan_count': int(recent[col].isna().sum()),
                            'nan_ratio': float(recent[col].isna().sum() / len(recent)) if len(recent) > 0 else 0
                        }

                result['files'][name] = file_result

            except Exception as e:
                result['files'][name] = {'error': str(e)}

        return result

    def get_status_summary(self) -> str:
        """
        Get a simple status summary string.

        Returns:
            Status message like "OK: 288 records, 0.0% NaN" or error message
        """
        try:
            health = self.check_transmission_health()

            if health['status'] == 'OK':
                # Get record count from first check
                if health['checks']:
                    records = health['checks'][0].get('details', {}).get('recent_records', 0)
                    return f"OK: {records} records in last hour"
                return "OK"

            elif health['status'] == 'WARNING':
                return f"WARNING: {health['warnings'][0]}"
            else:
                return f"ERROR: {health['errors'][0]}"

        except Exception as e:
            return f"ERROR: Health check failed - {e}"


def run_health_check() -> Dict[str, Any]:
    """Run complete health check and return results"""
    checker = TransmissionHealthCheck()
    return checker.check_transmission_health()


def main():
    """Command-line health check"""
    import json

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    checker = TransmissionHealthCheck()

    # Run health check
    result = checker.check_transmission_health()

    # Print summary
    print("=" * 60)
    print("TRANSMISSION DATA HEALTH CHECK")
    print("=" * 60)
    print(f"Status: {result['status']}")
    print(f"Time: {result['timestamp']}")
    print()

    for check in result['checks']:
        status_icon = {'OK': '✓', 'WARNING': '⚠', 'ERROR': '✗'}.get(check['status'], '?')
        print(f"[{status_icon}] {check['name']}: {check['message']}")

        if check.get('details'):
            for key, value in check['details'].items():
                if isinstance(value, float):
                    print(f"    {key}: {value:.2%}")
                else:
                    print(f"    {key}: {value}")

    if result['warnings']:
        print("\nWarnings:")
        for w in result['warnings']:
            print(f"  ⚠ {w}")

    if result['errors']:
        print("\nErrors:")
        for e in result['errors']:
            print(f"  ✗ {e}")

    print()
    print("=" * 60)

    # Return exit code based on status
    return 0 if result['status'] == 'OK' else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
