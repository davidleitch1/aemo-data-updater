#!/usr/bin/env python3
"""
Backfill curtailment data from AEMO archive with multi-stage validation

This script downloads historical Next Day Dispatch data from AEMO's archive
to fill gaps in curtailment5.parquet from Sept 30 to present.

Safety features:
1. Test download of ONE file first with validation
2. Download all files to temporary directory
3. Build separate backfill parquet file
4. Validate backfill file 100% before merge
5. Merge with production file
6. Final validation of merged file

Usage:
    python backfill_curtailment.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]

Example:
    python backfill_curtailment.py --start-date 2025-09-30 --end-date 2025-10-15
"""

import sys
import asyncio
import argparse
import pandas as pd
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Tuple

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from aemo_updater.collectors.curtailment_collector import CurtailmentCollector
from aemo_updater.config import PARQUET_FILES, get_logger

logger = get_logger('backfill_curtailment')


class CurtailmentBackfiller:
    """Backfill curtailment data from AEMO archive with validation"""

    def __init__(self, output_file: Path):
        self.output_file = output_file
        # Archive directory for historical data (monthly archives)
        self.archive_url = "https://nemweb.com.au/Reports/Archive/Next_Day_Dispatch/"
        # Current directory for recent data (last ~30 days)
        self.current_url = "https://nemweb.com.au/Reports/Current/Next_Day_Dispatch/"
        self.temp_dir = Path(__file__).parent / 'temp_curtailment_backfill'
        self.backfill_parquet = self.temp_dir / 'backfill_curtailment.parquet'

    def cleanup_temp(self):
        """Remove temporary directory"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
            logger.info(f"Cleaned up temporary directory: {self.temp_dir}")

    async def get_archive_urls_for_date(self, target_date: datetime) -> List[str]:
        """Get Archive or Current directory URLs for a specific date

        For dates > 30 days old: Downloads monthly archive from Archive directory
        For recent dates: Gets files from Current directory
        """
        date_str = target_date.strftime("%Y%m%d")
        days_old = (datetime.now() - target_date).days

        collector = CurtailmentCollector(PARQUET_FILES['curtailment'])

        # Use Archive for dates > 30 days old
        # Exception: Sept 2025 archive not available yet, use Current
        if target_date.year == 2025 and target_date.month == 9:
            return await self._get_from_current(target_date, collector)
        elif days_old > 30:
            return await self._get_from_archive(target_date, collector)
        else:
            return await self._get_from_current(target_date, collector)

    async def _get_from_current(self, target_date: datetime, collector) -> List[str]:
        """Get files from Current directory (last ~30 days)"""
        date_str = target_date.strftime("%Y%m%d")

        try:
            content = await collector.download_file(self.current_url)
            if content is None:
                return []

            from bs4 import BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')

            # Find ZIP files matching our target date
            zip_files = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if href.endswith('.zip') and f'PUBLIC_NEXT_DAY_DISPATCH_{date_str}' in href:
                    if href.startswith('/'):
                        file_url = "https://nemweb.com.au" + href
                    else:
                        file_url = self.current_url + href
                    zip_files.append(file_url)

            return zip_files

        except Exception as e:
            logger.error(f"Error getting Current directory URLs for {date_str}: {e}")
            return []

    async def _get_from_archive(self, target_date: datetime, collector) -> List[str]:
        """Get files from Archive directory (monthly archives)

        Archive structure:
        - Monthly ZIP files: PUBLIC_NEXT_DAY_DISPATCH_YYYYMM01.zip
        - Each monthly ZIP contains daily ZIPs for that month
        - Earliest available: September 2024
        """
        # Get the monthly archive filename (always 1st of month)
        year_month = target_date.strftime("%Y%m")
        monthly_archive = f"PUBLIC_NEXT_DAY_DISPATCH_{year_month}01.zip"
        monthly_url = self.archive_url + monthly_archive

        # Target daily file pattern inside monthly archive
        date_str = target_date.strftime("%Y%m%d")
        daily_pattern = f"PUBLIC_NEXT_DAY_DISPATCH_{date_str}_"

        try:
            # Download monthly archive
            logger.info(f"  Downloading monthly archive: {monthly_archive}")
            monthly_content = await collector.download_file(monthly_url)
            if monthly_content is None:
                logger.warning(f"  Monthly archive not found: {monthly_archive}")
                return []

            # Extract daily ZIP from monthly archive
            import zipfile
            import io

            daily_zips = []
            with zipfile.ZipFile(io.BytesIO(monthly_content)) as monthly_zip:
                # Find daily file for our target date
                for filename in monthly_zip.namelist():
                    if daily_pattern in filename and filename.endswith('.zip'):
                        # Extract the daily ZIP content
                        daily_content = monthly_zip.read(filename)

                        # Save to temp file so collector can process it
                        temp_file = self.temp_dir / filename
                        self.temp_dir.mkdir(exist_ok=True)
                        with open(temp_file, 'wb') as f:
                            f.write(daily_content)

                        # Return local file path as URL (collector will handle it)
                        daily_zips.append(str(temp_file))
                        logger.info(f"  Extracted daily file: {filename} ({len(daily_content):,} bytes)")

            if not daily_zips:
                logger.warning(f"  No daily file found in {monthly_archive} matching {daily_pattern}")

            return daily_zips

        except Exception as e:
            logger.error(f"Error getting Archive URLs for {date_str}: {e}")
            import traceback
            traceback.print_exc()
            return []

    def validate_test_data(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validate test download data
        Returns: (is_valid, error_message)
        """
        if df.empty:
            return False, "DataFrame is empty"

        # Check required columns
        required_cols = ['settlementdate', 'duid', 'availability', 'totalcleared',
                        'semidispatchcap', 'curtailment']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"

        # Check data types
        if not pd.api.types.is_datetime64_any_dtype(df['settlementdate']):
            return False, "settlementdate is not datetime type"

        for col in ['availability', 'totalcleared', 'curtailment']:
            if not pd.api.types.is_numeric_dtype(df[col]):
                return False, f"{col} is not numeric type"

        # Check for negative curtailment
        if (df['curtailment'] < 0).any():
            return False, "Found negative curtailment values"

        # Check DUID count (should be reasonable)
        duid_count = df['duid'].nunique()
        if duid_count < 50:
            return False, f"Too few DUIDs ({duid_count}), expected ~150+"

        # Check record count (5-min intervals for ~157 DUIDs for 1 day = ~45,000 records)
        if len(df) < 1000:
            return False, f"Too few records ({len(df)}), data may be incomplete"

        return True, "OK"

    def validate_backfill_file(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Validate backfill parquet file before merge
        Must pass 100% - stricter validation
        Returns: (is_valid, error_message)
        """
        logger.info("\n" + "="*70)
        logger.info("VALIDATION TEST 2: Backfill Parquet File")
        logger.info("="*70)

        if df.empty:
            return False, "Backfill DataFrame is empty"

        # Check required columns (uppercase for production consistency)
        required_cols = ['SETTLEMENTDATE', 'DUID', 'AVAILABILITY', 'TOTALCLEARED',
                        'SEMIDISPATCHCAP', 'CURTAILMENT']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"

        logger.info(f"✓ All required columns present: {required_cols}")

        # Check data types
        if not pd.api.types.is_datetime64_any_dtype(df['SETTLEMENTDATE']):
            return False, "SETTLEMENTDATE is not datetime type"
        logger.info(f"✓ SETTLEMENTDATE is datetime type")

        for col in ['AVAILABILITY', 'TOTALCLEARED', 'CURTAILMENT']:
            if not pd.api.types.is_numeric_dtype(df[col]):
                return False, f"{col} is not numeric type"
        logger.info(f"✓ Numeric columns have correct types")

        # Check for negative curtailment
        negative_count = (df['CURTAILMENT'] < 0).sum()
        if negative_count > 0:
            return False, f"Found {negative_count} negative curtailment values"
        logger.info(f"✓ No negative curtailment values")

        # Check DUID count
        duid_count = df['DUID'].nunique()
        if duid_count < 100:
            return False, f"Too few DUIDs ({duid_count}), expected ~150+"
        logger.info(f"✓ DUID count: {duid_count} (expected ~150+)")

        # Check date range
        date_range = df['SETTLEMENTDATE'].max() - df['SETTLEMENTDATE'].min()
        logger.info(f"✓ Date range: {df['SETTLEMENTDATE'].min()} to {df['SETTLEMENTDATE'].max()} ({date_range.days} days)")

        # Check for duplicates
        duplicates = df.duplicated(subset=['SETTLEMENTDATE', 'DUID']).sum()
        if duplicates > 0:
            return False, f"Found {duplicates} duplicate records"
        logger.info(f"✓ No duplicate records")

        # Check record count
        logger.info(f"✓ Total records: {len(df):,}")
        logger.info(f"✓ Total curtailment: {df['CURTAILMENT'].sum():,.1f} MW")

        logger.info("\n✅ VALIDATION TEST 2 PASSED - Backfill file is valid")
        return True, "OK"

    def validate_merged_file(self, df: pd.DataFrame, original_count: int, backfill_count: int) -> Tuple[bool, str]:
        """
        Validate final merged file
        Returns: (is_valid, error_message)
        """
        logger.info("\n" + "="*70)
        logger.info("VALIDATION TEST 3: Merged Parquet File")
        logger.info("="*70)

        if df.empty:
            return False, "Merged DataFrame is empty"

        # Check columns
        required_cols = ['SETTLEMENTDATE', 'DUID', 'AVAILABILITY', 'TOTALCLEARED',
                        'SEMIDISPATCHCAP', 'CURTAILMENT']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns after merge: {missing_cols}"
        logger.info(f"✓ All required columns present")

        # Check record count - should be original + backfill (minus duplicates)
        final_count = len(df)
        expected_min = original_count  # At minimum, should have original records
        expected_max = original_count + backfill_count  # Maximum if no duplicates

        if final_count < expected_min:
            return False, f"Record count decreased after merge ({final_count} < {expected_min})"

        if final_count > expected_max:
            return False, f"Record count higher than expected ({final_count} > {expected_max})"

        logger.info(f"✓ Record count: {final_count:,} (original: {original_count:,}, backfill: {backfill_count:,})")
        logger.info(f"  Duplicates removed: {(original_count + backfill_count) - final_count:,}")

        # Check for duplicates in final file
        duplicates = df.duplicated(subset=['SETTLEMENTDATE', 'DUID']).sum()
        if duplicates > 0:
            return False, f"Found {duplicates} duplicate records in merged file"
        logger.info(f"✓ No duplicate records")

        # Check date range
        logger.info(f"✓ Final date range: {df['SETTLEMENTDATE'].min()} to {df['SETTLEMENTDATE'].max()}")

        # Check DUID count
        duid_count = df['DUID'].nunique()
        logger.info(f"✓ Unique DUIDs: {duid_count}")

        # Check for negative curtailment
        negative_count = (df['CURTAILMENT'] < 0).sum()
        if negative_count > 0:
            return False, f"Found {negative_count} negative curtailment values in merged file"
        logger.info(f"✓ No negative curtailment values")

        logger.info(f"✓ Total curtailment: {df['CURTAILMENT'].sum():,.1f} MW")

        logger.info("\n✅ VALIDATION TEST 3 PASSED - Merged file is valid")
        return True, "OK"

    async def test_single_download(self, start_date: datetime) -> bool:
        """
        Test download of ONE file and validate it
        Returns True if test passes
        """
        logger.info("\n" + "="*70)
        logger.info("VALIDATION TEST 1: Single File Download")
        logger.info("="*70)

        # Get URLs or file paths for start date
        urls = await self.get_archive_urls_for_date(start_date)

        if not urls:
            logger.error(f"❌ No archive files found for {start_date.date()}")
            return False

        # Take first URL/path for testing
        test_path = urls[0]
        filename = test_path.split('/')[-1]
        is_local = Path(test_path).exists()

        if is_local:
            logger.info(f"Testing local file: {filename}")
        else:
            logger.info(f"Testing download: {filename}")

        # Create collector
        collector = CurtailmentCollector(PARQUET_FILES['curtailment'])

        try:
            # Download or read local file
            if is_local:
                with open(test_path, 'rb') as f:
                    content = f.read()
                logger.info(f"✓ Read local file {len(content):,} bytes")
            else:
                content = await collector.download_file(test_path)
                if content is None:
                    logger.error(f"❌ Failed to download {filename}")
                    return False
                logger.info(f"✓ Downloaded {len(content):,} bytes")

            # Parse
            df = await collector.parse_data(content, filename)
            if df.empty:
                logger.error(f"❌ No records extracted from {filename}")
                return False

            logger.info(f"✓ Extracted {len(df):,} records")
            logger.info(f"  DUIDs: {df['duid'].nunique()}")
            logger.info(f"  Date range: {df['settlementdate'].min()} to {df['settlementdate'].max()}")

            # Validate
            is_valid, error_msg = self.validate_test_data(df)
            if not is_valid:
                logger.error(f"❌ Validation failed: {error_msg}")
                return False

            logger.info(f"✓ Validation passed")
            logger.info("\n✅ VALIDATION TEST 1 PASSED - Ready to download all files")
            return True

        except Exception as e:
            logger.error(f"❌ Test failed with exception: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def download_all_files(self, start_date: datetime, end_date: datetime) -> List[pd.DataFrame]:
        """
        Download all archive files to temp directory and parse
        Returns list of DataFrames
        """
        logger.info("\n" + "="*70)
        logger.info("DOWNLOADING ALL ARCHIVE FILES")
        logger.info("="*70)

        # Create temp directory
        self.temp_dir.mkdir(exist_ok=True)
        logger.info(f"Temporary directory: {self.temp_dir}")

        collector = CurtailmentCollector(PARQUET_FILES['curtailment'])
        all_dataframes = []

        current_date = start_date
        total_files = 0

        while current_date <= end_date:
            logger.info(f"\nDate: {current_date.date()}")

            # Get URLs for this date
            urls = await self.get_archive_urls_for_date(current_date)

            if not urls:
                logger.warning(f"  No files for {current_date.date()}")
                current_date += timedelta(days=1)
                continue

            # Download each file (or read local if from Archive)
            for file_path in urls:
                filename = file_path.split('/')[-1]
                is_local = Path(file_path).exists()

                if is_local:
                    logger.info(f"  Processing local file {filename}...")
                else:
                    logger.info(f"  Downloading {filename}...")

                try:
                    # Download or read local file
                    if is_local:
                        with open(file_path, 'rb') as f:
                            content = f.read()
                        logger.info(f"  Read {len(content):,} bytes from local file")
                    else:
                        content = await collector.download_file(file_path)
                        if content is None:
                            logger.warning(f"  Failed to download {filename}")
                            continue

                        # Save to temp directory
                        temp_file = self.temp_dir / filename
                        with open(temp_file, 'wb') as f:
                            f.write(content)
                        logger.info(f"  Saved to {temp_file.name} ({len(content):,} bytes)")

                    # Parse
                    df = await collector.parse_data(content, filename)
                    if df.empty:
                        logger.warning(f"  No records from {filename}")
                        continue

                    logger.info(f"  Extracted {len(df):,} records")

                    # Normalize column names to uppercase
                    df.columns = df.columns.str.upper()
                    all_dataframes.append(df)
                    total_files += 1

                except Exception as e:
                    logger.error(f"  Error processing {filename}: {e}")
                    continue

            current_date += timedelta(days=1)

        logger.info(f"\n✓ Downloaded and parsed {total_files} files")
        logger.info(f"✓ Total DataFrames: {len(all_dataframes)}")

        return all_dataframes

    async def build_backfill_parquet(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine all dataframes and build backfill parquet file
        Returns the backfill DataFrame
        """
        logger.info("\n" + "="*70)
        logger.info("BUILDING BACKFILL PARQUET FILE")
        logger.info("="*70)

        if not dataframes:
            logger.error("No dataframes to combine")
            return pd.DataFrame()

        # Combine all dataframes
        logger.info("Combining dataframes...")
        combined_df = pd.concat(dataframes, ignore_index=True)
        logger.info(f"✓ Combined {len(dataframes)} dataframes into {len(combined_df):,} records")

        # Remove duplicates
        logger.info("Removing duplicates...")
        initial_count = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['SETTLEMENTDATE', 'DUID'], keep='last')
        duplicates_removed = initial_count - len(combined_df)
        logger.info(f"✓ Removed {duplicates_removed:,} duplicates")

        # Sort
        logger.info("Sorting by settlement date and DUID...")
        combined_df = combined_df.sort_values(['SETTLEMENTDATE', 'DUID'])
        logger.info(f"✓ Sorted")

        # Save backfill parquet
        logger.info(f"Saving backfill parquet to {self.backfill_parquet}...")
        combined_df.to_parquet(self.backfill_parquet, index=False)
        file_size = self.backfill_parquet.stat().st_size / (1024**2)
        logger.info(f"✓ Saved backfill parquet ({file_size:.1f} MB)")

        return combined_df

    def merge_with_production(self, backfill_df: pd.DataFrame) -> pd.DataFrame:
        """
        Merge backfill data with production file
        Returns merged DataFrame
        """
        logger.info("\n" + "="*70)
        logger.info("MERGING WITH PRODUCTION FILE")
        logger.info("="*70)

        # Load existing production data
        if self.output_file.exists():
            logger.info(f"Loading existing production file: {self.output_file}")
            existing_df = pd.read_parquet(self.output_file)
            existing_df.columns = existing_df.columns.str.upper()
            logger.info(f"✓ Loaded {len(existing_df):,} existing records")
            logger.info(f"  Existing range: {existing_df['SETTLEMENTDATE'].min()} to {existing_df['SETTLEMENTDATE'].max()}")
        else:
            logger.warning("No existing production file found")
            existing_df = pd.DataFrame()

        # Combine
        if not existing_df.empty:
            logger.info("Combining existing and backfill data...")
            merged_df = pd.concat([existing_df, backfill_df], ignore_index=True)

            # Remove duplicates (keep last)
            logger.info("Removing duplicates...")
            initial_count = len(merged_df)
            merged_df = merged_df.drop_duplicates(subset=['SETTLEMENTDATE', 'DUID'], keep='last')
            duplicates_removed = initial_count - len(merged_df)
            logger.info(f"✓ Removed {duplicates_removed:,} duplicates")

            # Sort
            logger.info("Sorting...")
            merged_df = merged_df.sort_values(['SETTLEMENTDATE', 'DUID'])
        else:
            merged_df = backfill_df

        logger.info(f"✓ Final merged data: {len(merged_df):,} records")
        logger.info(f"  Final range: {merged_df['SETTLEMENTDATE'].min()} to {merged_df['SETTLEMENTDATE'].max()}")

        return merged_df, len(existing_df), len(backfill_df)

    async def run_backfill(self, start_date: datetime, end_date: datetime):
        """
        Main backfill process with validation checkpoints
        """
        try:
            # CHECKPOINT 1: Test single file
            logger.info(f"Starting backfill: {start_date.date()} to {end_date.date()}")

            test_passed = await self.test_single_download(start_date)
            if not test_passed:
                logger.error("\n❌ ABORT: Test download failed")
                logger.error("Fix issues before proceeding")
                return False

            # CHECKPOINT 2: Download all files
            logger.info("\nProceeding with full download...")

            dataframes = await self.download_all_files(start_date, end_date)
            if not dataframes:
                logger.error("\n❌ ABORT: No data downloaded")
                return False

            # Build backfill parquet
            backfill_df = await self.build_backfill_parquet(dataframes)
            if backfill_df.empty:
                logger.error("\n❌ ABORT: Backfill file is empty")
                return False

            # CHECKPOINT 3: Validate backfill file
            is_valid, error_msg = self.validate_backfill_file(backfill_df)
            if not is_valid:
                logger.error(f"\n❌ ABORT: Backfill validation failed: {error_msg}")
                logger.error(f"Backfill file saved at: {self.backfill_parquet}")
                logger.error("Fix issues before merging")
                return False

            # CHECKPOINT 4: Merge with production
            logger.info("\nProceeding with merge to production file...")

            merged_df, original_count, backfill_count = self.merge_with_production(backfill_df)

            # CHECKPOINT 5: Validate merged file
            is_valid, error_msg = self.validate_merged_file(merged_df, original_count, backfill_count)
            if not is_valid:
                logger.error(f"\n❌ ABORT: Merged file validation failed: {error_msg}")
                logger.error("Production file NOT updated")
                return False

            # Save merged file
            logger.info(f"\nSaving merged file to {self.output_file}...")
            merged_df.to_parquet(self.output_file, index=False)
            logger.info(f"✓ Saved production file")

            logger.info("\n" + "="*70)
            logger.info("✅ BACKFILL COMPLETE - ALL VALIDATION TESTS PASSED")
            logger.info("="*70)
            logger.info(f"Production file: {self.output_file}")
            logger.info(f"Total records: {len(merged_df):,}")
            logger.info(f"Date range: {merged_df['SETTLEMENTDATE'].min()} to {merged_df['SETTLEMENTDATE'].max()}")
            logger.info(f"Unique DUIDs: {merged_df['DUID'].nunique()}")
            logger.info("="*70)

            return True

        finally:
            # Cleanup
            logger.info("\nCleaning up...")
            self.cleanup_temp()


async def main():
    parser = argparse.ArgumentParser(description='Backfill curtailment data with multi-stage validation')
    parser.add_argument('--start-date', type=str, default='2025-09-30',
                       help='Start date (YYYY-MM-DD), default: 2025-09-30')
    parser.add_argument('--end-date', type=str, default=None,
                       help='End date (YYYY-MM-DD), default: yesterday')

    args = parser.parse_args()

    # Parse dates
    start_date = datetime.strptime(args.start_date, '%Y-%m-%d')

    if args.end_date:
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
    else:
        # Default to yesterday
        end_date = datetime.now() - timedelta(days=1)

    # Validate date range
    if start_date > end_date:
        logger.error("Start date must be before end date")
        return 1

    # Get output file from config
    output_file = PARQUET_FILES['curtailment']['path']

    logger.info("="*70)
    logger.info("CURTAILMENT BACKFILL WITH VALIDATION")
    logger.info("="*70)
    logger.info(f"Date range: {start_date.date()} to {end_date.date()}")
    logger.info(f"Production file: {output_file}")
    logger.info("="*70)
    logger.info("\nProcess:")
    logger.info("  1. Test download of one file")
    logger.info("  2. Download all files to temp directory")
    logger.info("  3. Build backfill parquet file")
    logger.info("  4. Validate backfill file (must pass 100%)")
    logger.info("  5. Merge with production file")
    logger.info("  6. Validate merged file")
    logger.info("="*70)

    # Run backfill
    backfiller = CurtailmentBackfiller(output_file)
    success = await backfiller.run_backfill(start_date, end_date)

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
