#!/usr/bin/env python3
"""
Data Status Review Script

Analyzes all AEMO parquet data files to identify:
- First and last dates in each file
- Missing data gaps (5-minute or 30-minute intervals)
- File corruption issues
- Record counts

Usage:
    python check_data_status.py [--output OUTPUT_FILE]

Arguments:
    --output: Optional output file path (default: prints to stdout)
"""

import pandas as pd
import os
from datetime import datetime, timedelta
from pathlib import Path
import argparse
import sys


class DataStatusChecker:
    """Check status of AEMO parquet data files."""

    def __init__(self, data_dir: str):
        """
        Initialize the data status checker.

        Args:
            data_dir: Path to directory containing parquet files
        """
        self.data_dir = Path(data_dir)

        # Define expected files and their intervals
        self.files_config = {
            'prices5.parquet': {'interval_min': 5, 'date_col': 'settlementdate'},
            'prices30.parquet': {'interval_min': 30, 'date_col': 'settlementdate'},
            'scada5.parquet': {'interval_min': 5, 'date_col': 'settlementdate'},
            'scada30.parquet': {'interval_min': 30, 'date_col': 'settlementdate'},
            'transmission5.parquet': {'interval_min': 5, 'date_col': 'settlementdate'},
            'transmission30.parquet': {'interval_min': 30, 'date_col': 'settlementdate'},
            'rooftop30.parquet': {'interval_min': 30, 'date_col': 'settlementdate'},
            'curtailment5.parquet': {'interval_min': 5, 'date_col': 'SETTLEMENTDATE'},
        }

    def check_file(self, filename: str) -> dict:
        """
        Check a single parquet file for status.

        Args:
            filename: Name of parquet file to check

        Returns:
            Dictionary with status information
        """
        file_path = self.data_dir / filename
        config = self.files_config.get(filename, {})
        interval_min = config.get('interval_min', 5)
        date_col = config.get('date_col', 'settlementdate')

        result = {
            'filename': filename,
            'exists': False,
            'corrupted': False,
            'first_date': None,
            'last_date': None,
            'record_count': 0,
            'unique_timestamps': 0,
            'gaps': [],
            'error': None
        }

        if not file_path.exists():
            result['error'] = 'File not found'
            return result

        result['exists'] = True

        try:
            # Read parquet file
            df = pd.read_parquet(file_path)

            # Handle different column name cases
            if date_col not in df.columns:
                # Try lowercase version
                date_col_lower = date_col.lower()
                if date_col_lower in df.columns:
                    date_col = date_col_lower
                else:
                    result['error'] = f'Date column not found (tried {date_col})'
                    return result

            # Get basic stats
            result['record_count'] = len(df)

            # Convert to datetime if needed
            if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col])

            # Get unique timestamps
            unique_dates = df[date_col].unique()
            unique_dates = pd.to_datetime(unique_dates)
            unique_dates = sorted(unique_dates)

            result['unique_timestamps'] = len(unique_dates)
            result['first_date'] = unique_dates[0]
            result['last_date'] = unique_dates[-1]

            # Check for gaps (only if we have more than 2 timestamps)
            if len(unique_dates) > 2:
                gaps = self._find_gaps(unique_dates, interval_min)
                result['gaps'] = gaps

        except Exception as e:
            result['corrupted'] = True
            result['error'] = str(e)

        return result

    def _find_gaps(self, timestamps: list, interval_min: int) -> list:
        """
        Find gaps in timestamp sequence.

        Args:
            timestamps: Sorted list of datetime timestamps
            interval_min: Expected interval in minutes

        Returns:
            List of gap dictionaries with start, end, and duration
        """
        gaps = []
        expected_delta = timedelta(minutes=interval_min)

        for i in range(len(timestamps) - 1):
            current = timestamps[i]
            next_ts = timestamps[i + 1]
            actual_delta = next_ts - current

            # Allow small tolerance (e.g., 1 second) for floating point issues
            if actual_delta > expected_delta + timedelta(seconds=1):
                gap = {
                    'start': current,
                    'end': next_ts,
                    'expected': current + expected_delta,
                    'duration_hours': actual_delta.total_seconds() / 3600,
                    'missing_intervals': int(actual_delta.total_seconds() / (interval_min * 60)) - 1
                }
                gaps.append(gap)

        return gaps

    def check_all_files(self) -> dict:
        """
        Check all configured parquet files.

        Returns:
            Dictionary mapping filename to status information
        """
        results = {}
        for filename in self.files_config.keys():
            results[filename] = self.check_file(filename)
        return results

    def format_markdown_report(self, results: dict) -> str:
        """
        Format results as markdown report.

        Args:
            results: Dictionary of file status results

        Returns:
            Markdown formatted report string
        """
        lines = []
        lines.append(f"# AEMO Data Status Report\n")
        lines.append(f"**Report Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        lines.append(f"**Data Directory:** `{self.data_dir}`\n")
        lines.append("---\n")

        # Summary table
        lines.append("## Summary\n")
        lines.append("| File | Status | Records | First Date | Last Date | Gaps |")
        lines.append("|------|--------|---------|------------|-----------|------|")

        for filename, result in results.items():
            if not result['exists']:
                status = "❌ Not Found"
                records = "-"
                first_date = "-"
                last_date = "-"
                gaps = "-"
            elif result['corrupted']:
                status = "⚠️ Corrupted"
                records = "-"
                first_date = "-"
                last_date = "-"
                gaps = "-"
            else:
                status = "✅ OK"
                records = f"{result['record_count']:,}"
                first_date = result['first_date'].strftime('%Y-%m-%d')
                last_date = result['last_date'].strftime('%Y-%m-%d')
                gaps = str(len(result['gaps']))

            lines.append(f"| {filename} | {status} | {records} | {first_date} | {last_date} | {gaps} |")

        lines.append("")

        # Detailed gap information
        lines.append("## Data Gaps Detail\n")

        has_gaps = False
        for filename, result in results.items():
            if result.get('gaps') and len(result['gaps']) > 0:
                has_gaps = True
                lines.append(f"### {filename}\n")
                lines.append(f"**Total Gaps:** {len(result['gaps'])}\n")

                # Show up to 10 largest gaps
                sorted_gaps = sorted(result['gaps'], key=lambda x: x['duration_hours'], reverse=True)
                lines.append("| Gap Start | Gap End | Duration (hours) | Missing Intervals |")
                lines.append("|-----------|---------|------------------|-------------------|")

                for gap in sorted_gaps[:10]:
                    start = gap['start'].strftime('%Y-%m-%d %H:%M')
                    end = gap['end'].strftime('%Y-%m-%d %H:%M')
                    duration = f"{gap['duration_hours']:.1f}"
                    missing = gap['missing_intervals']
                    lines.append(f"| {start} | {end} | {duration} | {missing} |")

                if len(sorted_gaps) > 10:
                    lines.append(f"\n*Showing 10 largest gaps out of {len(sorted_gaps)} total*\n")

                lines.append("")

        if not has_gaps:
            lines.append("✅ **No gaps detected in any files**\n")

        # Error details
        lines.append("## Errors and Issues\n")

        has_errors = False
        for filename, result in results.items():
            if result.get('error'):
                has_errors = True
                lines.append(f"### {filename}\n")
                lines.append(f"**Error:** {result['error']}\n")

        if not has_errors:
            lines.append("✅ **No errors detected**\n")

        return "\n".join(lines)


def main():
    """Main entry point for script."""
    parser = argparse.ArgumentParser(description='Check AEMO parquet data status')
    parser.add_argument(
        '--data-dir',
        default='/Volumes/davidleitch/aemo_production/data',
        help='Path to data directory (default: /Volumes/davidleitch/aemo_production/data)'
    )
    parser.add_argument(
        '--output',
        help='Output file path (default: print to stdout)'
    )

    args = parser.parse_args()

    # Create checker and run
    checker = DataStatusChecker(args.data_dir)
    results = checker.check_all_files()

    # Format report
    report = checker.format_markdown_report(results)

    # Output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)


if __name__ == '__main__':
    main()
