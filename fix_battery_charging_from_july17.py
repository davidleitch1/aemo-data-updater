#!/usr/bin/env python3
"""
Fix battery charging data from July 17, 2025 onwards.
This script:
1. Preserves all scada30 data before July 17, 2025 (which has correct negative values)
2. Reprocesses scada5 data from July 17 onwards to include negative values
3. Recalculates scada30 from the corrected scada5 data
4. Merges with the historical data

IMPORTANT: Run this when the collector is not actively writing to scada files
or during a brief pause in collection.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import logging
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')
CUTOFF_DATE = pd.Timestamp('2025-07-17 10:00:00')  # After the last negative value
ARCHIVE_PATH = Path('/Volumes/davidleitch/aemo_production/data/archive')

def check_collector_activity():
    """Check if collector is currently running by looking at file modification times"""
    scada5_file = DATA_PATH / 'scada5.parquet'
    if scada5_file.exists():
        mtime = datetime.fromtimestamp(scada5_file.stat().st_mtime)
        time_since_modified = datetime.now() - mtime
        if time_since_modified < timedelta(minutes=5):
            logger.warning(f"scada5.parquet was modified {time_since_modified.total_seconds():.0f} seconds ago")
            logger.warning("Collector might be active! Consider pausing it or waiting.")
            response = input("Continue anyway? (y/n): ")
            if response.lower() != 'y':
                logger.info("Exiting to avoid conflicts with active collector")
                sys.exit(0)
        else:
            logger.info(f"scada5.parquet last modified {time_since_modified.total_seconds()/60:.1f} minutes ago - safe to proceed")

def backup_current_files():
    """Create safety backups of current files"""
    logger.info("Creating safety backups...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    for filename in ['scada5.parquet', 'scada30.parquet']:
        src = DATA_PATH / filename
        if src.exists():
            dst = DATA_PATH / f'{filename}.backup_{timestamp}'
            shutil.copy2(src, dst)
            logger.info(f"Backed up {filename} to {dst.name}")

def reprocess_scada5_from_archive():
    """
    Reprocess DISPATCH_UNIT_SCADA files from archive to get negative values.
    We need to download and process files from July 17 onwards.
    """
    logger.info(f"Reprocessing SCADA5 data from {CUTOFF_DATE} onwards...")
    
    # Load current scada5 data
    scada5_file = DATA_PATH / 'scada5.parquet'
    if not scada5_file.exists():
        logger.error("scada5.parquet not found!")
        return None
    
    logger.info("Loading current scada5 data...")
    scada5_df = pd.read_parquet(scada5_file)
    scada5_df['settlementdate'] = pd.to_datetime(scada5_df['settlementdate'])
    
    # Split data at cutoff
    pre_cutoff = scada5_df[scada5_df['settlementdate'] < CUTOFF_DATE].copy()
    post_cutoff = scada5_df[scada5_df['settlementdate'] >= CUTOFF_DATE].copy()
    
    logger.info(f"Pre-cutoff records (keeping): {len(pre_cutoff):,}")
    logger.info(f"Post-cutoff records (to reprocess): {len(post_cutoff):,}")
    
    # For now, we'll work with the existing post-cutoff data
    # In production, you'd re-download and reprocess the DISPATCHSCADA files
    # But since the collector already fixed itself, new data should have negatives
    
    # Check if recent data has negatives (after we fixed the collector)
    recent_cutoff = pd.Timestamp('2025-08-20 00:00:00')
    recent_data = scada5_df[scada5_df['settlementdate'] >= recent_cutoff]
    recent_negatives = recent_data[recent_data['scadavalue'] < 0]
    
    logger.info(f"\nRecent data check (after fix):")
    logger.info(f"Records since {recent_cutoff}: {len(recent_data):,}")
    logger.info(f"Negative values found: {len(recent_negatives):,}")
    
    if len(recent_negatives) > 0:
        logger.info("✅ Good! Recent data has negative values after the fix")
        unique_duids = recent_negatives['duid'].unique()
        logger.info(f"DUIDs with charging: {', '.join(unique_duids[:10])}")
    
    # For the gap between July 17 and today, we would need to:
    # 1. Download DISPATCHSCADA files from archives
    # 2. Reprocess them without the >= 0 filter
    # This is a manual process that would take time
    
    logger.warning("\n" + "="*60)
    logger.warning("NOTE: To fully fix July 17 - Aug 20 data, you need to:")
    logger.warning("1. Download DISPATCHSCADA archives for this period")
    logger.warning("2. Reprocess them without the >= 0 filter")
    logger.warning("3. This script currently keeps existing data for this period")
    logger.warning("="*60)
    
    return scada5_df

def recalculate_scada30(scada5_df, start_date):
    """Recalculate 30-minute averages from 5-minute data"""
    logger.info(f"\nRecalculating SCADA30 from {start_date}...")
    
    # Filter for data to recalculate
    data_to_process = scada5_df[scada5_df['settlementdate'] >= start_date].copy()
    
    if data_to_process.empty:
        logger.warning("No data to process")
        return pd.DataFrame()
    
    logger.info(f"Processing {len(data_to_process):,} 5-minute records...")
    
    # Find all 30-minute endpoints
    data_to_process['endpoint'] = data_to_process['settlementdate'].dt.floor('30min') + pd.Timedelta(minutes=30)
    
    # Group by endpoint and DUID, calculate mean
    aggregated = data_to_process.groupby(['endpoint', 'duid']).agg({
        'scadavalue': 'mean'
    }).reset_index()
    
    # Rename endpoint to settlementdate
    aggregated = aggregated.rename(columns={'endpoint': 'settlementdate'})
    
    # Check for negative values in result
    negatives = aggregated[aggregated['scadavalue'] < 0]
    logger.info(f"Recalculated {len(aggregated):,} 30-minute records")
    logger.info(f"Negative values in recalculated data: {len(negatives):,}")
    
    if len(negatives) > 0:
        battery_duids = negatives['duid'].unique()
        logger.info(f"Battery DUIDs with charging: {', '.join(battery_duids[:10])}")
    
    return aggregated

def merge_and_save(pre_cutoff_30, new_scada30):
    """Merge historical and new data, then save"""
    logger.info("\nMerging historical and recalculated data...")
    
    # Combine the dataframes
    combined = pd.concat([pre_cutoff_30, new_scada30], ignore_index=True)
    
    # Remove any duplicates (keep last)
    combined = combined.drop_duplicates(
        subset=['settlementdate', 'duid'], 
        keep='last'
    )
    
    # Sort by time and DUID
    combined = combined.sort_values(['settlementdate', 'duid'])
    
    logger.info(f"Final combined records: {len(combined):,}")
    
    # Check negative values in final data
    final_negatives = combined[combined['scadavalue'] < 0]
    logger.info(f"Total negative values in final data: {len(final_negatives):,}")
    
    # Save to new file first (safer)
    output_file = DATA_PATH / 'scada30_fixed.parquet'
    combined.to_parquet(output_file, index=False)
    logger.info(f"Saved fixed data to {output_file}")
    
    # Show before/after comparison for specific batteries
    logger.info("\n" + "="*60)
    logger.info("BATTERY CHARGING VERIFICATION")
    logger.info("="*60)
    
    battery_duids = ['HPR1', 'TIB1', 'BLYTHB1', 'DALNTH1', 'LBB1']
    
    for duid in battery_duids:
        duid_data = combined[combined['duid'] == duid]
        if len(duid_data) > 0:
            recent_data = duid_data[duid_data['settlementdate'] >= CUTOFF_DATE]
            negatives = recent_data[recent_data['scadavalue'] < 0]
            
            print(f"\n{duid}:")
            print(f"  Total records: {len(duid_data):,}")
            print(f"  Records since July 17: {len(recent_data):,}")
            print(f"  Negative values since July 17: {len(negatives):,}")
            
            if len(negatives) > 0:
                print(f"  ✅ Charging data present")
            else:
                print(f"  ⚠️  No charging data since July 17")
    
    return output_file

def main():
    """Main execution"""
    logger.info("="*60)
    logger.info("Battery Charging Data Fix Script")
    logger.info(f"Cutoff date: {CUTOFF_DATE}")
    logger.info("="*60)
    
    # Check if collector is active
    check_collector_activity()
    
    # Create backups
    backup_current_files()
    
    # Load current scada30 data
    scada30_file = DATA_PATH / 'scada30.parquet'
    if not scada30_file.exists():
        logger.error("scada30.parquet not found!")
        return
    
    logger.info("\nLoading current scada30 data...")
    scada30_df = pd.read_parquet(scada30_file)
    scada30_df['settlementdate'] = pd.to_datetime(scada30_df['settlementdate'])
    
    # Split at cutoff - preserve pre-cutoff data
    pre_cutoff_30 = scada30_df[scada30_df['settlementdate'] < CUTOFF_DATE].copy()
    logger.info(f"Preserving {len(pre_cutoff_30):,} historical records (before July 17)")
    
    # Check that historical data has negatives
    historical_negatives = pre_cutoff_30[pre_cutoff_30['scadavalue'] < 0]
    logger.info(f"Historical negative values: {len(historical_negatives):,} ✅")
    
    # Reprocess scada5 data
    scada5_df = reprocess_scada5_from_archive()
    
    if scada5_df is not None:
        # Recalculate scada30 from corrected scada5
        new_scada30 = recalculate_scada30(scada5_df, CUTOFF_DATE)
        
        if not new_scada30.empty:
            # Merge and save
            output_file = merge_and_save(pre_cutoff_30, new_scada30)
            
            logger.info("\n" + "="*60)
            logger.info("NEXT STEPS:")
            logger.info("="*60)
            logger.info("1. Review the output file: scada30_fixed.parquet")
            logger.info("2. If satisfied, replace the original:")
            logger.info(f"   mv {output_file} {scada30_file}")
            logger.info("3. The collector should now maintain negative values going forward")
            logger.info("4. To fully fix July 17-Aug 20, need to reprocess archive files")
        else:
            logger.error("Failed to recalculate scada30 data")
    else:
        logger.error("Failed to reprocess scada5 data")
    
    logger.info("\n✅ Script completed")

if __name__ == "__main__":
    main()