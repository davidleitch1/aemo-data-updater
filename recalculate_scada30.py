#!/usr/bin/env python3
"""
Efficient recalculation of scada30 from scada5 data
Optimized to handle large volumes of data without timeout
"""

import pandas as pd
from pathlib import Path
import logging
import time
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def recalculate_scada30_efficient():
    """Efficiently recalculate all scada30 data from scada5"""
    
    data_path = Path("data 2")
    scada5_file = data_path / "scada5.parquet"
    scada30_file = data_path / "scada30.parquet"
    
    if not scada5_file.exists():
        logger.error("scada5.parquet not found")
        return
    
    logger.info("Loading scada5 data...")
    scada5_df = pd.read_parquet(scada5_file)
    logger.info(f"Loaded {len(scada5_df)} scada5 records")
    
    # Find the starting point
    if scada30_file.exists():
        scada30_df = pd.read_parquet(scada30_file)
        last_time = scada30_df['settlementdate'].max()
        logger.info(f"Last scada30 timestamp: {last_time}")
        # Go back 3 days to ensure we catch the July 15-17 gap
        start_time = last_time - pd.Timedelta(days=3)
    else:
        # Start from beginning
        start_time = scada5_df['settlementdate'].min()
        logger.info(f"No existing scada30, starting from {start_time}")
    
    # Filter to only data we need to process
    mask = scada5_df['settlementdate'] > start_time
    data_to_process = scada5_df[mask].copy()
    
    if data_to_process.empty:
        logger.info("No new data to process")
        return
    
    logger.info(f"Processing {len(data_to_process)} records from {data_to_process['settlementdate'].min()} to {data_to_process['settlementdate'].max()}")
    
    # Find all 30-minute endpoints
    unique_times = sorted(data_to_process['settlementdate'].unique())
    endpoints = [t for t in unique_times if pd.Timestamp(t).minute in [0, 30]]
    
    if not endpoints:
        logger.info("No 30-minute endpoints found")
        return
    
    logger.info(f"Found {len(endpoints)} endpoints to process")
    
    # Process in batches to avoid memory issues
    batch_size = 10  # Process 10 endpoints at a time
    all_results = []
    
    for batch_start in range(0, len(endpoints), batch_size):
        batch_end = min(batch_start + batch_size, len(endpoints))
        batch_endpoints = endpoints[batch_start:batch_end]
        
        logger.info(f"Processing batch {batch_start//batch_size + 1}/{(len(endpoints) + batch_size - 1)//batch_size}: endpoints {batch_start+1}-{batch_end}")
        
        batch_results = []
        
        for end_time in batch_endpoints:
            end_time = pd.Timestamp(end_time)
            start_time = end_time - pd.Timedelta(minutes=25)
            
            # Get all data for this 30-minute window
            # Use the full scada5 dataset for complete intervals
            window_mask = (
                (scada5_df['settlementdate'] > start_time) & 
                (scada5_df['settlementdate'] <= end_time)
            )
            window_data = scada5_df[window_mask]
            
            if window_data.empty:
                continue
            
            # Group by DUID and calculate sum/2 for whatever intervals are available
            grouped = window_data.groupby('duid')
            
            duids_processed = 0
            
            for duid, group in grouped:
                # Calculate: sum of available intervals / 2
                avg_value = group['scadavalue'].sum() / 2.0
                
                batch_results.append({
                    'settlementdate': end_time,
                    'duid': duid,
                    'scadavalue': avg_value
                })
                duids_processed += 1
            
            if duids_processed > 0:
                logger.debug(f"  {end_time}: processed {duids_processed} DUIDs")
        
        if batch_results:
            batch_df = pd.DataFrame(batch_results)
            all_results.append(batch_df)
            logger.info(f"  Processed {len(batch_results)} DUID aggregations")
    
    if not all_results:
        logger.info("No complete 30-minute periods found")
        return
    
    # Combine all results
    logger.info("Combining results...")
    new_data = pd.concat(all_results, ignore_index=True)
    new_data = new_data.drop_duplicates(subset=['settlementdate', 'duid'])
    new_data = new_data.sort_values(['settlementdate', 'duid'])
    
    logger.info(f"Calculated {len(new_data)} new scada30 records")
    
    # Merge with existing data if any
    if scada30_file.exists():
        logger.info("Merging with existing scada30 data...")
        existing_data = pd.read_parquet(scada30_file)
        
        # Combine and remove duplicates
        combined = pd.concat([existing_data, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=['settlementdate', 'duid'], keep='last')
        combined = combined.sort_values(['settlementdate', 'duid'])
        
        logger.info(f"Total records after merge: {len(combined)} (added {len(combined) - len(existing_data)} new)")
        
        # Save back
        combined.to_parquet(scada30_file, compression='snappy', index=False)
    else:
        # Save new data
        new_data.to_parquet(scada30_file, compression='snappy', index=False)
        logger.info(f"Created new scada30.parquet with {len(new_data)} records")
    
    logger.info("Recalculation complete!")


if __name__ == "__main__":
    start_time = time.time()
    recalculate_scada30_efficient()
    elapsed = time.time() - start_time
    logger.info(f"Total execution time: {elapsed:.1f} seconds")