#!/usr/bin/env python3
"""
Download and reprocess DISPATCHSCADA archive files for July 17 - Aug 20, 2025
to recover battery charging (negative) values that were filtered out.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import requests
import zipfile
import io
import logging
import time
from typing import List, Dict, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')
START_DATE = datetime(2025, 7, 17)
END_DATE = datetime(2025, 8, 20)
ARCHIVE_BASE_URL = "http://nemweb.com.au/Reports/Archive/Dispatch_SCADA/"
CURRENT_URL = "http://nemweb.com.au/Reports/Current/Dispatch_SCADA/"

def get_archive_urls_for_date_range(start_date: datetime, end_date: datetime) -> List[Dict]:
    """Generate list of archive URLs to download for the date range"""
    urls = []
    current = start_date
    
    while current <= end_date:
        # Archive files are organized by month: YYYYMM/
        month_folder = current.strftime("%Y%m")
        
        # Generate URLs for every 5 minutes of the day
        for hour in range(24):
            for minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
                timestamp = current.replace(hour=hour, minute=minute)
                if timestamp > end_date:
                    break
                    
                # Format: PUBLIC_DISPATCHSCADA_YYYYMMDDHHMI_*.zip
                filename_pattern = f"PUBLIC_DISPATCHSCADA_{timestamp.strftime('%Y%m%d%H%M')}"
                
                urls.append({
                    'timestamp': timestamp,
                    'month_folder': month_folder,
                    'filename_pattern': filename_pattern,
                    'url': None  # Will be determined by checking archive structure
                })
        
        current += timedelta(days=1)
    
    logger.info(f"Generated {len(urls)} potential timestamps to check")
    return urls

def check_archive_structure():
    """Check how AEMO archive is structured and find available files"""
    logger.info("Checking AEMO archive structure...")
    
    # First check if there's a July 2025 folder
    test_url = ARCHIVE_BASE_URL + "202507/"
    response = requests.get(test_url, timeout=30)
    
    if response.status_code == 200:
        logger.info("✅ Found monthly archive folders")
        return "monthly"
    else:
        # Check if files are directly in Archive folder
        test_url = ARCHIVE_BASE_URL
        response = requests.get(test_url, timeout=30)
        if response.status_code == 200 and "PUBLIC_DISPATCHSCADA" in response.text:
            logger.info("✅ Files directly in archive folder")
            return "direct"
        else:
            logger.warning("⚠️ Archive structure unclear, will try current folder")
            return "current"

def find_file_in_archive(timestamp: datetime, archive_type: str) -> Optional[str]:
    """Find the actual file URL for a given timestamp"""
    filename_pattern = f"PUBLIC_DISPATCHSCADA_{timestamp.strftime('%Y%m%d%H%M')}"
    
    if archive_type == "monthly":
        month_folder = timestamp.strftime("%Y%m")
        folder_url = f"{ARCHIVE_BASE_URL}{month_folder}/"
    elif archive_type == "direct":
        folder_url = ARCHIVE_BASE_URL
    else:  # current
        folder_url = CURRENT_URL
    
    try:
        response = requests.get(folder_url, timeout=10)
        if response.status_code == 200:
            # Look for the file in the HTML response
            if filename_pattern in response.text:
                # Extract the exact filename with the ID number
                import re
                pattern = f'({filename_pattern}_\\d+\\.zip)'
                match = re.search(pattern, response.text)
                if match:
                    return folder_url + match.group(1)
    except Exception as e:
        logger.debug(f"Error checking {folder_url}: {e}")
    
    return None

def download_and_parse_scada_file(url: str) -> pd.DataFrame:
    """Download and parse a single DISPATCHSCADA file (handles nested zips)"""
    try:
        response = requests.get(url, timeout=30)
        if response.status_code != 200:
            logger.warning(f"Failed to download {url}: {response.status_code}")
            return pd.DataFrame()
        
        # First level unzip
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Check for nested zip files first
            nested_zips = [f for f in zf.namelist() if f.endswith('.zip')]
            csv_files = [f for f in zf.namelist() if f.endswith('.CSV')]
            
            content_to_parse = None
            
            if nested_zips:
                # Handle nested zip - common in AEMO archives
                logger.debug(f"Found nested zip: {nested_zips[0]}")
                with zf.open(nested_zips[0]) as nested_zip_file:
                    with zipfile.ZipFile(io.BytesIO(nested_zip_file.read())) as nested_zf:
                        nested_csv_files = [f for f in nested_zf.namelist() if f.endswith('.CSV')]
                        if nested_csv_files:
                            with nested_zf.open(nested_csv_files[0]) as csv_file:
                                content_to_parse = csv_file.read().decode('utf-8')
            elif csv_files:
                # Direct CSV in first zip
                with zf.open(csv_files[0]) as csv_file:
                    content_to_parse = csv_file.read().decode('utf-8')
            
            if not content_to_parse:
                logger.debug(f"No CSV content found in {url}")
                return pd.DataFrame()
            
            # Parse the CSV content
            lines = content_to_parse.strip().split('\n')
            
            data_records = []
            for line in lines:
                if line.startswith('D,DISPATCH,UNIT_SCADA'):
                    parts = line.split(',')
                    if len(parts) >= 8:
                        try:
                            settlementdate = parts[4].strip('"')
                            duid = parts[5].strip('"')
                            scadavalue = float(parts[6])
                            
                            # Parse date
                            settlementdate = pd.to_datetime(settlementdate, format='%Y/%m/%d %H:%M:%S')
                            
                            data_records.append({
                                'settlementdate': settlementdate,
                                'duid': duid,
                                'scadavalue': scadavalue
                            })
                        except (ValueError, IndexError):
                            continue
            
            if data_records:
                df = pd.DataFrame(data_records)
                # Important: Keep negative values!
                df = df[df['scadavalue'].notna()]
                
                # Log if we found negative values
                negatives = df[df['scadavalue'] < 0]
                if len(negatives) > 0:
                    logger.debug(f"Found {len(negatives)} negative values in {url}")
                
                return df
                
        return pd.DataFrame()
        
    except Exception as e:
        logger.debug(f"Error processing {url}: {e}")
        return pd.DataFrame()

def process_date_range_batch(start_date: datetime, end_date: datetime, batch_size: int = 100):
    """Process files in batches to avoid memory issues"""
    logger.info(f"Processing DISPATCHSCADA files from {start_date} to {end_date}")
    
    # Check archive structure
    archive_type = check_archive_structure()
    
    # Generate list of timestamps to process
    current = start_date
    all_data = []
    files_processed = 0
    files_with_negatives = 0
    total_negatives = 0
    
    while current <= end_date:
        batch_data = []
        batch_end = min(current + timedelta(hours=12), end_date)  # Process 12 hours at a time
        
        logger.info(f"\nProcessing batch: {current} to {batch_end}")
        
        # Process each 5-minute interval
        batch_current = current
        while batch_current <= batch_end:
            for minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
                timestamp = batch_current.replace(minute=minute, second=0, microsecond=0)
                if timestamp > batch_end:
                    break
                
                # Find and download file
                file_url = find_file_in_archive(timestamp, archive_type)
                
                if file_url:
                    df = download_and_parse_scada_file(file_url)
                    if not df.empty:
                        batch_data.append(df)
                        files_processed += 1
                        
                        # Check for negative values
                        negatives = df[df['scadavalue'] < 0]
                        if len(negatives) > 0:
                            files_with_negatives += 1
                            total_negatives += len(negatives)
                        
                        if files_processed % 50 == 0:
                            logger.info(f"  Processed {files_processed} files, found {total_negatives} negative values")
                
                # Small delay to be respectful to the server
                if files_processed % 10 == 0:
                    time.sleep(0.1)
            
            batch_current += timedelta(hours=1)
        
        # Combine batch data
        if batch_data:
            batch_df = pd.concat(batch_data, ignore_index=True)
            all_data.append(batch_df)
            logger.info(f"  Batch complete: {len(batch_df)} records, {len(batch_df[batch_df['scadavalue'] < 0])} negative values")
        
        current = batch_end + timedelta(minutes=5)
    
    # Combine all data
    if all_data:
        logger.info(f"\nCombining all batches...")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates(subset=['settlementdate', 'duid'])
        
        # Sort by time
        final_df = final_df.sort_values(['settlementdate', 'duid'])
        
        logger.info(f"\n" + "="*60)
        logger.info(f"REPROCESSING COMPLETE")
        logger.info(f"="*60)
        logger.info(f"Files processed: {files_processed}")
        logger.info(f"Files with negative values: {files_with_negatives}")
        logger.info(f"Total records: {len(final_df):,}")
        logger.info(f"Total negative values: {len(final_df[final_df['scadavalue'] < 0]):,}")
        
        # Show battery statistics
        battery_duids = ['HPR1', 'TIB1', 'BLYTHB1', 'DALNTH1', 'LBB1', 'TB2B1']
        logger.info(f"\nBattery DUID statistics:")
        for duid in battery_duids:
            duid_data = final_df[final_df['duid'] == duid]
            if len(duid_data) > 0:
                negatives = duid_data[duid_data['scadavalue'] < 0]
                logger.info(f"  {duid}: {len(duid_data)} records, {len(negatives)} charging ({len(negatives)/len(duid_data)*100:.1f}%)")
        
        return final_df
    else:
        logger.warning("No data collected!")
        return pd.DataFrame()

def merge_with_existing_scada5(new_data: pd.DataFrame):
    """Merge reprocessed data with existing scada5 data"""
    logger.info("\nMerging with existing scada5 data...")
    
    scada5_file = DATA_PATH / 'scada5.parquet'
    if not scada5_file.exists():
        logger.error("scada5.parquet not found!")
        return None
    
    # Load existing data
    existing_df = pd.read_parquet(scada5_file)
    existing_df['settlementdate'] = pd.to_datetime(existing_df['settlementdate'])
    
    # Get data outside our reprocessing range
    keep_mask = (existing_df['settlementdate'] < START_DATE) | (existing_df['settlementdate'] > END_DATE)
    kept_data = existing_df[keep_mask]
    
    logger.info(f"Keeping {len(kept_data):,} records outside reprocessing range")
    logger.info(f"Replacing {len(existing_df) - len(kept_data):,} records with {len(new_data):,} reprocessed records")
    
    # Combine
    combined = pd.concat([kept_data, new_data], ignore_index=True)
    
    # Remove duplicates and sort
    combined = combined.drop_duplicates(subset=['settlementdate', 'duid'], keep='last')
    combined = combined.sort_values(['settlementdate', 'duid'])
    
    # Save to new file
    output_file = DATA_PATH / 'scada5_with_charging.parquet'
    combined.to_parquet(output_file, index=False)
    
    logger.info(f"Saved merged data to {output_file}")
    logger.info(f"Total records: {len(combined):,}")
    logger.info(f"Total negative values: {len(combined[combined['scadavalue'] < 0]):,}")
    
    return output_file

def recalculate_scada30_for_period(scada5_file: Path):
    """Recalculate scada30 for the reprocessed period"""
    logger.info("\nRecalculating scada30 for reprocessed period...")
    
    # Load the corrected scada5 data
    scada5_df = pd.read_parquet(scada5_file)
    scada5_df['settlementdate'] = pd.to_datetime(scada5_df['settlementdate'])
    
    # Filter for reprocessed period
    mask = (scada5_df['settlementdate'] >= START_DATE) & (scada5_df['settlementdate'] <= END_DATE + timedelta(days=1))
    period_data = scada5_df[mask].copy()
    
    logger.info(f"Calculating 30-minute averages for {len(period_data):,} records...")
    
    # Calculate 30-minute endpoints
    period_data['endpoint'] = period_data['settlementdate'].dt.floor('30min') + pd.Timedelta(minutes=30)
    
    # Aggregate
    aggregated = period_data.groupby(['endpoint', 'duid']).agg({
        'scadavalue': 'mean'
    }).reset_index()
    
    aggregated = aggregated.rename(columns={'endpoint': 'settlementdate'})
    
    # Check for negatives
    negatives = aggregated[aggregated['scadavalue'] < 0]
    logger.info(f"Created {len(aggregated):,} 30-minute records")
    logger.info(f"Negative values: {len(negatives):,}")
    
    # Now merge with existing scada30
    scada30_file = DATA_PATH / 'scada30.parquet'
    existing_30 = pd.read_parquet(scada30_file)
    existing_30['settlementdate'] = pd.to_datetime(existing_30['settlementdate'])
    
    # Keep data outside reprocessed range
    keep_mask = (existing_30['settlementdate'] < START_DATE) | (existing_30['settlementdate'] > END_DATE + timedelta(days=1))
    kept_30 = existing_30[keep_mask]
    
    # Combine
    combined_30 = pd.concat([kept_30, aggregated], ignore_index=True)
    combined_30 = combined_30.drop_duplicates(subset=['settlementdate', 'duid'], keep='last')
    combined_30 = combined_30.sort_values(['settlementdate', 'duid'])
    
    # Save
    output_30_file = DATA_PATH / 'scada30_with_charging.parquet'
    combined_30.to_parquet(output_30_file, index=False)
    
    logger.info(f"Saved corrected scada30 to {output_30_file}")
    logger.info(f"Total 30-min records: {len(combined_30):,}")
    logger.info(f"Total negative values: {len(combined_30[combined_30['scadavalue'] < 0]):,}")
    
    return output_30_file

def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("DISPATCHSCADA Archive Reprocessing")
    logger.info(f"Period: {START_DATE.date()} to {END_DATE.date()}")
    logger.info("="*60)
    
    # Step 1: Download and reprocess archive files
    reprocessed_data = process_date_range_batch(START_DATE, END_DATE)
    
    if reprocessed_data.empty:
        logger.error("No data was reprocessed!")
        return
    
    # Step 2: Merge with existing scada5
    scada5_output = merge_with_existing_scada5(reprocessed_data)
    
    if scada5_output:
        # Step 3: Recalculate scada30
        scada30_output = recalculate_scada30_for_period(scada5_output)
        
        logger.info("\n" + "="*60)
        logger.info("REPROCESSING COMPLETE")
        logger.info("="*60)
        logger.info("Created files:")
        logger.info(f"  - {scada5_output} (scada5 with charging data)")
        logger.info(f"  - {scada30_output} (scada30 with charging data)")
        logger.info("\nTo apply these fixes:")
        logger.info(f"  mv {scada5_output} {DATA_PATH / 'scada5.parquet'}")
        logger.info(f"  mv {scada30_output} {DATA_PATH / 'scada30.parquet'}")
        logger.info("\n✅ Success! Battery charging data has been recovered.")

if __name__ == "__main__":
    main()