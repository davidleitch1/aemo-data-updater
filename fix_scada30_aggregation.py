#!/usr/bin/env python3
"""
Fix scada30 aggregation to properly include battery charging data.
The scada5 data has charging (negative values) but scada30 doesn't after July 17.
This script recalculates scada30 from scada5 for the affected period.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_PATH = Path('/Volumes/davidleitch/aemo_production/data')

def recalculate_scada30():
    """Recalculate scada30 from scada5 for July 17 onwards"""
    
    logger.info("="*60)
    logger.info("FIXING SCADA30 AGGREGATION")
    logger.info("="*60)
    
    # Load scada5
    logger.info("Loading scada5 data...")
    scada5 = pd.read_parquet(DATA_PATH / 'scada5.parquet')
    scada5['settlementdate'] = pd.to_datetime(scada5['settlementdate'])
    
    # Define the period to recalculate (July 17 10:00 onwards)
    start_fix = pd.Timestamp('2025-07-17 10:00:00')
    
    logger.info(f"Recalculating from {start_fix} onwards")
    
    # Filter scada5 for the period to fix
    scada5_to_fix = scada5[scada5['settlementdate'] >= start_fix].copy()
    logger.info(f"Records to process: {len(scada5_to_fix):,}")
    
    # Check for negative values
    negatives = scada5_to_fix[scada5_to_fix['scadavalue'] < 0]
    logger.info(f"Negative values in scada5: {len(negatives):,}")
    
    # Calculate 30-minute endpoints
    logger.info("Calculating 30-minute averages...")
    scada5_to_fix['endpoint'] = scada5_to_fix['settlementdate'].dt.floor('30min') + pd.Timedelta(minutes=30)
    
    # Group by endpoint and DUID, calculate mean
    aggregated = scada5_to_fix.groupby(['endpoint', 'duid']).agg({
        'scadavalue': 'mean'
    }).reset_index()
    
    # Rename endpoint to settlementdate
    aggregated = aggregated.rename(columns={'endpoint': 'settlementdate'})
    
    # Check for negatives in aggregated data
    agg_negatives = aggregated[aggregated['scadavalue'] < 0]
    logger.info(f"Negative values in aggregated 30-min data: {len(agg_negatives):,}")
    
    if len(agg_negatives) > 0:
        logger.info("✅ Charging data preserved in aggregation!")
        latest_charging = agg_negatives['settlementdate'].max()
        logger.info(f"Latest charging in new aggregation: {latest_charging}")
    
    # Load existing scada30
    logger.info("\nLoading existing scada30...")
    scada30 = pd.read_parquet(DATA_PATH / 'scada30.parquet')
    scada30['settlementdate'] = pd.to_datetime(scada30['settlementdate'])
    
    # Keep data before the fix period
    scada30_keep = scada30[scada30['settlementdate'] < start_fix]
    logger.info(f"Keeping {len(scada30_keep):,} records before {start_fix}")
    
    # Combine
    logger.info("Combining data...")
    combined = pd.concat([scada30_keep, aggregated], ignore_index=True)
    
    # Remove duplicates (keep last)
    combined = combined.drop_duplicates(subset=['settlementdate', 'duid'], keep='last')
    
    # Sort
    combined = combined.sort_values(['settlementdate', 'duid'])
    
    # Final statistics
    final_negatives = combined[combined['scadavalue'] < 0]
    logger.info(f"\nFinal statistics:")
    logger.info(f"Total records: {len(combined):,}")
    logger.info(f"Total negative values: {len(final_negatives):,}")
    
    if len(final_negatives) > 0:
        latest_final_charging = final_negatives['settlementdate'].max()
        logger.info(f"Latest charging: {latest_final_charging}")
    
    # Save
    output_file = DATA_PATH / 'scada30_fixed_charging.parquet'
    combined.to_parquet(output_file, index=False)
    logger.info(f"\nSaved to {output_file}")
    
    return output_file

def verify_fix(fixed_file):
    """Verify the fix worked"""
    import pickle
    
    logger.info("\n" + "="*60)
    logger.info("VERIFICATION")
    logger.info("="*60)
    
    # Load fixed data
    fixed = pd.read_parquet(fixed_file)
    fixed['settlementdate'] = pd.to_datetime(fixed['settlementdate'])
    
    # Load battery DUIDs
    with open(DATA_PATH / 'gen_info.pkl', 'rb') as f:
        gen_info = pickle.load(f)
    
    battery_duids = gen_info[gen_info['Fuel'] == 'Battery Storage']['DUID'].tolist()
    
    # Check battery charging
    battery_data = fixed[fixed['duid'].isin(battery_duids)]
    battery_charging = battery_data[battery_data['scadavalue'] < 0]
    
    logger.info(f"Battery records: {len(battery_data):,}")
    logger.info(f"Battery charging records: {len(battery_charging):,}")
    
    if len(battery_charging) > 0:
        # Check by month
        battery_charging['month'] = battery_charging['settlementdate'].dt.to_period('M')
        monthly = battery_charging.groupby('month').size()
        
        logger.info("\nCharging records by month:")
        for month, count in monthly.tail(6).items():
            logger.info(f"  {month}: {count:,}")
        
        latest = battery_charging['settlementdate'].max()
        logger.info(f"\n✅ SUCCESS! Latest charging: {latest}")
    else:
        logger.info("\n❌ FAILED - No charging data found")

if __name__ == "__main__":
    fixed_file = recalculate_scada30()
    verify_fix(fixed_file)
    
    logger.info("\n" + "="*60)
    logger.info("TO APPLY THE FIX:")
    logger.info("="*60)
    logger.info("1. Backup current scada30:")
    logger.info(f"   cp {DATA_PATH / 'scada30.parquet'} {DATA_PATH / 'scada30.parquet.backup_before_charging_fix2'}")
    logger.info("2. Apply the fix:")
    logger.info(f"   mv {fixed_file} {DATA_PATH / 'scada30.parquet'}")
    logger.info("3. Restart any services using the data")