#!/usr/bin/env python3
"""
Download and reprocess DISPATCHSCADA daily archive files for July 17 - Aug 20, 2025
to recover battery charging (negative) values that were filtered out.

These are daily aggregated files, not individual 5-minute files.
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
import pickle

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

# Load gen_info for region mapping
GEN_INFO_FILE = DATA_PATH / 'gen_info.pkl'

def load_gen_info():
    """Load generator info for DUID to region mapping"""
    if GEN_INFO_FILE.exists():
        with open(GEN_INFO_FILE, 'rb') as f:
            gen_info = pickle.load(f)
        logger.info(f"Loaded gen_info with {len(gen_info)} entries")
        return gen_info
    else:
        logger.warning("gen_info.pkl not found - will not have region data")
        return None

def get_daily_archive_urls(start_date: datetime, end_date: datetime) -> List[Dict]:
    """Generate list of daily archive URLs to download"""
    urls = []
    current = start_date
    
    while current <= end_date:
        # Format: PUBLIC_DISPATCHSCADA_YYYYMMDD.zip
        date_str = current.strftime('%Y%m%d')
        filename = f"PUBLIC_DISPATCHSCADA_{date_str}.zip"
        url = ARCHIVE_BASE_URL + filename
        
        urls.append({
            'date': current,
            'filename': filename,
            'url': url
        })
        
        current += timedelta(days=1)
    
    logger.info(f"Generated {len(urls)} daily archive URLs to process")
    return urls

def download_and_parse_daily_scada(url: str, gen_info_df) -> pd.DataFrame:
    """Download and parse a daily DISPATCHSCADA archive file"""
    try:
        logger.info(f"Downloading {url.split('/')[-1]}...")
        response = requests.get(url, timeout=60)
        if response.status_code != 200:
            logger.warning(f"Failed to download {url}: {response.status_code}")
            return pd.DataFrame()
        
        logger.info(f"Downloaded {len(response.content)/1024/1024:.1f} MB")
        
        all_records = []
        
        # Parse the zip file
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # This daily archive contains multiple 5-minute zip files
            inner_zips = [f for f in zf.namelist() if f.endswith('.zip')]
            csv_files = [f for f in zf.namelist() if f.endswith('.CSV')]
            
            if inner_zips:
                logger.info(f"Found {len(inner_zips)} nested zip files")
                # Process each nested zip
                for inner_zip_name in inner_zips[:None]:  # Process all files
                    with zf.open(inner_zip_name) as inner_zip_file:
                        try:
                            with zipfile.ZipFile(io.BytesIO(inner_zip_file.read())) as inner_zf:
                                # Find CSV files in the inner zip
                                inner_csv_files = [f for f in inner_zf.namelist() if f.endswith('.CSV')]
                                
                                for csv_name in inner_csv_files:
                                    with inner_zf.open(csv_name) as csv_file:
                                        content = csv_file.read().decode('utf-8', errors='ignore')
                                        records = parse_scada_csv_content(content)
                                        if records:
                                            all_records.extend(records)
                        except Exception as e:
                            logger.debug(f"Error processing inner zip {inner_zip_name}: {e}")
                            continue
            
            elif csv_files:
                logger.info(f"Found {len(csv_files)} direct CSV files")
                # Direct CSV files
                for csv_name in csv_files:
                    with zf.open(csv_name) as csv_file:
                        content = csv_file.read().decode('utf-8', errors='ignore')
                        records = parse_scada_csv_content(content)
                        if records:
                            all_records.extend(records)
        
        if all_records:
            df = pd.DataFrame(all_records)
            
            # Add region information if available
            if gen_info_df is not None and not gen_info_df.empty:
                df = df.merge(
                    gen_info_df[['DUID', 'Region']].rename(columns={'DUID': 'duid', 'Region': 'regionid'}),
                    on='duid',
                    how='left'
                )
            
            # Important: Keep ALL values including negatives!
            df = df[df['scadavalue'].notna()]
            
            # Log statistics
            negatives = df[df['scadavalue'] < 0]
            if len(negatives) > 0:
                logger.info(f"✅ Found {len(negatives)} negative values (charging)")
                unique_charging_duids = negatives['duid'].unique()
                logger.info(f"   DUIDs with charging: {', '.join(unique_charging_duids[:5])}...")
            
            logger.info(f"Parsed {len(df)} total records from {url.split('/')[-1]}")
            return df
        else:
            logger.warning(f"No records parsed from {url}")
            return pd.DataFrame()
        
    except Exception as e:
        logger.error(f"Error processing {url}: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

def parse_scada_csv_content(content: str) -> List[Dict]:
    """Parse CSV content and extract DISPATCH_UNIT_SCADA records"""
    records = []
    lines = content.strip().split('\n')
    
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
                    
                    records.append({
                        'settlementdate': settlementdate,
                        'duid': duid,
                        'scadavalue': scadavalue
                    })
                except (ValueError, IndexError) as e:
                    continue
    
    return records

def process_all_daily_archives():
    """Process all daily archives for the specified date range"""
    logger.info(f"Processing daily archives from {START_DATE.date()} to {END_DATE.date()}")
    
    # Load gen_info for region mapping
    gen_info = load_gen_info()
    gen_info_df = pd.DataFrame()
    if gen_info is not None:
        if isinstance(gen_info, pd.DataFrame):
            gen_info_df = gen_info
        else:
            gen_info_df = pd.DataFrame(gen_info)
    
    # Get list of URLs
    urls = get_daily_archive_urls(START_DATE, END_DATE)
    
    all_data = []
    total_negatives = 0
    battery_stats = {}
    
    for url_info in urls:
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing {url_info['date'].date()}")
        logger.info('='*60)
        
        df = download_and_parse_daily_scada(url_info['url'], gen_info_df)
        
        if not df.empty:
            all_data.append(df)
            
            # Track battery statistics
            negatives = df[df['scadavalue'] < 0]
            total_negatives += len(negatives)
            
            for duid in negatives['duid'].unique():
                if duid not in battery_stats:
                    battery_stats[duid] = {'charge_count': 0, 'discharge_count': 0, 'min_charge': 0, 'max_discharge': 0}
                
                duid_data = df[df['duid'] == duid]
                battery_stats[duid]['charge_count'] += len(duid_data[duid_data['scadavalue'] < 0])
                battery_stats[duid]['discharge_count'] += len(duid_data[duid_data['scadavalue'] > 0])
                battery_stats[duid]['min_charge'] = min(battery_stats[duid]['min_charge'], duid_data['scadavalue'].min())
                battery_stats[duid]['max_discharge'] = max(battery_stats[duid]['max_discharge'], duid_data['scadavalue'].max())
        
        # Small delay to be respectful
        time.sleep(0.5)
    
    # Combine all data
    if all_data:
        logger.info(f"\n{'='*60}")
        logger.info("Combining all data...")
        logger.info('='*60)
        
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remove duplicates
        final_df = final_df.drop_duplicates(subset=['settlementdate', 'duid'], keep='last')
        
        # Sort by time
        final_df = final_df.sort_values(['settlementdate', 'duid'])
        
        logger.info(f"\n{'='*60}")
        logger.info("REPROCESSING SUMMARY")
        logger.info('='*60)
        logger.info(f"Total records: {len(final_df):,}")
        logger.info(f"Total negative values (charging): {total_negatives:,}")
        logger.info(f"Date range: {final_df['settlementdate'].min()} to {final_df['settlementdate'].max()}")
        logger.info(f"Unique DUIDs: {final_df['duid'].nunique()}")
        logger.info(f"Unique charging DUIDs: {len(battery_stats)}")
        
        # Show top batteries by charging activity
        logger.info(f"\n{'='*60}")
        logger.info("TOP BATTERIES BY CHARGING ACTIVITY")
        logger.info('='*60)
        
        sorted_batteries = sorted(battery_stats.items(), key=lambda x: x[1]['charge_count'], reverse=True)
        for duid, stats in sorted_batteries[:10]:
            logger.info(f"\n{duid}:")
            logger.info(f"  Charging records: {stats['charge_count']:,}")
            logger.info(f"  Discharge records: {stats['discharge_count']:,}")
            logger.info(f"  Max charge rate: {abs(stats['min_charge']):.1f} MW")
            logger.info(f"  Max discharge rate: {stats['max_discharge']:.1f} MW")
        
        return final_df
    else:
        logger.error("No data collected!")
        return pd.DataFrame()

def merge_with_existing_scada5(new_data: pd.DataFrame):
    """Merge reprocessed data with existing scada5 data"""
    logger.info("\nMerging with existing scada5 data...")
    
    scada5_file = DATA_PATH / 'scada5.parquet'
    if not scada5_file.exists():
        logger.error("scada5.parquet not found!")
        return None
    
    # Load existing data
    logger.info("Loading existing scada5 data...")
    existing_df = pd.read_parquet(scada5_file)
    existing_df['settlementdate'] = pd.to_datetime(existing_df['settlementdate'])
    
    # Get data outside our reprocessing range
    keep_mask = (existing_df['settlementdate'] < START_DATE) | (existing_df['settlementdate'] > END_DATE + timedelta(days=1))
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

def recalculate_scada30_from_scada5(scada5_file: Path):
    """Recalculate scada30 from the corrected scada5 data"""
    logger.info("\nRecalculating scada30...")
    
    # Load corrected scada5
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
    
    # Add region if available in scada5
    if 'regionid' in period_data.columns:
        region_map = period_data.groupby('duid')['regionid'].first().to_dict()
        aggregated['regionid'] = aggregated['duid'].map(region_map)
    
    # Check for negatives
    negatives = aggregated[aggregated['scadavalue'] < 0]
    logger.info(f"Created {len(aggregated):,} 30-minute records")
    logger.info(f"Negative values (charging): {len(negatives):,}")
    
    # Merge with existing scada30
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
    logger.info("DISPATCHSCADA Daily Archive Reprocessing")
    logger.info(f"Period: {START_DATE.date()} to {END_DATE.date()}")
    logger.info("="*60)
    
    # Step 1: Download and reprocess daily archive files
    reprocessed_data = process_all_daily_archives()
    
    if reprocessed_data.empty:
        logger.error("No data was reprocessed!")
        return
    
    # Step 2: Merge with existing scada5
    scada5_output = merge_with_existing_scada5(reprocessed_data)
    
    if scada5_output:
        # Step 3: Recalculate scada30
        scada30_output = recalculate_scada30_from_scada5(scada5_output)
        
        logger.info("\n" + "="*60)
        logger.info("REPROCESSING COMPLETE")
        logger.info("="*60)
        logger.info("Created files:")
        logger.info(f"  - {scada5_output} (scada5 with charging data)")
        logger.info(f"  - {scada30_output} (scada30 with charging data)")
        logger.info("\nTo apply these fixes:")
        logger.info(f"  1. Stop the collector briefly")
        logger.info(f"  2. Backup current files:")
        logger.info(f"     cp {DATA_PATH / 'scada5.parquet'} {DATA_PATH / 'scada5.parquet.backup'}")
        logger.info(f"     cp {DATA_PATH / 'scada30.parquet'} {DATA_PATH / 'scada30.parquet.backup'}")
        logger.info(f"  3. Apply the fixed files:")
        logger.info(f"     mv {scada5_output} {DATA_PATH / 'scada5.parquet'}")
        logger.info(f"     mv {scada30_output} {DATA_PATH / 'scada30.parquet'}")
        logger.info(f"  4. Restart the collector")
        logger.info("\n✅ Success! Battery charging data has been recovered.")

if __name__ == "__main__":
    main()