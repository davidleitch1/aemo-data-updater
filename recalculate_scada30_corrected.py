#!/usr/bin/env python3
"""
Efficient recalculation of all scada30 data using the correct 6-interval method.
This script processes historical data in weekly chunks to manage memory efficiently.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import logging
from datetime import datetime, timedelta
import time
import gc

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Data paths
data_dir = Path('/Volumes/davidleitch/aemo_production/data')

def process_week_chunk(scada5_df, start_date, end_date):
    """
    Process one week of data efficiently.
    
    Args:
        scada5_df: DataFrame with scada5 data for the week (plus buffer)
        start_date: Start of the week
        end_date: End of the week
    
    Returns:
        DataFrame with aggregated 30-minute data
    """
    # Generate all 30-minute endpoints for this week
    endpoints = pd.date_range(
        start=start_date.replace(minute=0 if start_date.minute < 30 else 30, second=0, microsecond=0),
        end=end_date,
        freq='30min'
    )
    
    logger.info(f"  Processing {len(endpoints)} endpoints with {len(scada5_df['duid'].unique())} DUIDs")
    
    # Vectorized approach: group by endpoint and DUID
    results = []
    
    for endpoint in endpoints:
        # CORRECTED: Use 30 minutes for full 6-interval window
        window_start = endpoint - pd.Timedelta(minutes=30)
        
        # Get all data in this window
        window_mask = (
            (scada5_df['settlementdate'] > window_start) & 
            (scada5_df['settlementdate'] <= endpoint)
        )
        window_data = scada5_df[window_mask]
        
        if window_data.empty:
            continue
        
        # Group by DUID and calculate mean for each
        aggregated = window_data.groupby('duid').agg({
            'scadavalue': 'mean'
        }).reset_index()
        
        # Add endpoint timestamp
        aggregated['settlementdate'] = endpoint
        
        # Track number of intervals for validation
        interval_counts = window_data.groupby('duid').size().reset_index(name='num_intervals')
        aggregated = aggregated.merge(interval_counts, on='duid', how='left')
        
        results.append(aggregated)
    
    if results:
        return pd.concat(results, ignore_index=True)
    else:
        return pd.DataFrame()

def main():
    """Main recalculation process with memory-efficient chunking."""
    
    logger.info("=" * 80)
    logger.info("RECALCULATING SCADA30 WITH CORRECT 6-INTERVAL METHOD")
    logger.info("=" * 80)
    
    # Load scada5 data
    logger.info("\n1. Loading scada5 data...")
    scada5_df = pd.read_parquet(data_dir / 'scada5.parquet')
    logger.info(f"   Loaded {len(scada5_df):,} records")
    logger.info(f"   Date range: {scada5_df['settlementdate'].min()} to {scada5_df['settlementdate'].max()}")
    
    # Ensure settlementdate is datetime
    scada5_df['settlementdate'] = pd.to_datetime(scada5_df['settlementdate'])
    
    # Sort by settlementdate for efficiency
    logger.info("\n2. Sorting data by timestamp...")
    scada5_df = scada5_df.sort_values('settlementdate')
    
    # Define processing chunks (7-day blocks)
    start_date = scada5_df['settlementdate'].min()
    end_date = scada5_df['settlementdate'].max()
    
    # Round start to beginning of day
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    logger.info(f"\n3. Processing data from {start_date} to {end_date}")
    logger.info("   Using 7-day chunks for memory efficiency")
    
    # Process in weekly chunks
    all_results = []
    current_date = start_date
    chunk_num = 0
    total_chunks = ((end_date - start_date).days + 6) // 7
    
    start_time = time.time()
    
    while current_date <= end_date:
        chunk_num += 1
        chunk_end = min(current_date + timedelta(days=7), end_date)
        
        logger.info(f"\n4.{chunk_num} Processing chunk {chunk_num}/{total_chunks}: {current_date.date()} to {chunk_end.date()}")
        
        # Get data for this chunk (with 30-minute buffer before)
        chunk_start_buffer = current_date - pd.Timedelta(minutes=30)
        chunk_mask = (
            (scada5_df['settlementdate'] > chunk_start_buffer) & 
            (scada5_df['settlementdate'] <= chunk_end)
        )
        chunk_data = scada5_df[chunk_mask].copy()
        
        if not chunk_data.empty:
            # Process this week
            chunk_results = process_week_chunk(chunk_data, current_date, chunk_end)
            
            if not chunk_results.empty:
                all_results.append(chunk_results)
                logger.info(f"     Generated {len(chunk_results):,} records")
                
                # Show interval distribution for validation
                interval_dist = chunk_results['num_intervals'].value_counts().sort_index()
                logger.info("     Intervals per aggregation:")
                for num_int, count in interval_dist.items():
                    logger.info(f"       {int(num_int)} intervals: {count:,} records ({count/len(chunk_results)*100:.1f}%)")
        
        # Clean up memory
        del chunk_data
        gc.collect()
        
        # Move to next week
        current_date = chunk_end + pd.Timedelta(seconds=1)
        
        # Progress estimate
        elapsed = time.time() - start_time
        if chunk_num > 0:
            rate = chunk_num / elapsed
            remaining_chunks = total_chunks - chunk_num
            eta = remaining_chunks / rate if rate > 0 else 0
            logger.info(f"     Progress: {chunk_num/total_chunks*100:.1f}% complete, ETA: {eta/60:.1f} minutes")
    
    # Combine all results
    logger.info("\n5. Combining all results...")
    if all_results:
        scada30_corrected = pd.concat(all_results, ignore_index=True)
        
        # Remove duplicates (shouldn't be any, but just in case)
        scada30_corrected = scada30_corrected.drop_duplicates(subset=['settlementdate', 'duid'])
        
        # Sort by timestamp and DUID
        scada30_corrected = scada30_corrected.sort_values(['settlementdate', 'duid'])
        
        logger.info(f"   Total records: {len(scada30_corrected):,}")
        
        # Overall interval distribution
        logger.info("\n6. Overall interval distribution:")
        interval_dist = scada30_corrected['num_intervals'].value_counts().sort_index()
        for num_int, count in interval_dist.items():
            logger.info(f"   {int(num_int)} intervals: {count:,} records ({count/len(scada30_corrected)*100:.1f}%)")
        
        # Save the corrected data (without num_intervals column)
        output_file = data_dir / 'scada30_corrected.parquet'
        logger.info(f"\n7. Saving corrected data to {output_file}")
        
        # Drop the validation column before saving
        scada30_corrected_save = scada30_corrected[['settlementdate', 'duid', 'scadavalue']]
        scada30_corrected_save.to_parquet(output_file, compression='snappy', index=False)
        
        logger.info("   ✓ Saved successfully")
        
        # Quick comparison with existing scada30
        logger.info("\n8. Comparing with existing scada30...")
        existing_scada30 = pd.read_parquet(data_dir / 'scada30.parquet')
        
        # Merge for comparison
        comparison = scada30_corrected.merge(
            existing_scada30,
            on=['settlementdate', 'duid'],
            suffixes=('_corrected', '_existing')
        )
        
        if not comparison.empty:
            comparison['difference'] = comparison['scadavalue_corrected'] - comparison['scadavalue_existing']
            comparison['pct_change'] = (comparison['difference'] / comparison['scadavalue_existing'] * 100).replace([np.inf, -np.inf], 0)
            
            # Summary statistics
            logger.info(f"   Compared {len(comparison):,} matching records")
            logger.info(f"   Average difference: {comparison['difference'].mean():.3f} MW")
            logger.info(f"   Average % change: {comparison['pct_change'].mean():.2f}%")
            logger.info(f"   Max absolute difference: {comparison['difference'].abs().max():.2f} MW")
            
            # Show records with significant differences
            significant = comparison[comparison['difference'].abs() > 1.0]
            logger.info(f"   Records with >1 MW difference: {len(significant):,} ({len(significant)/len(comparison)*100:.1f}%)")
            
            # Sample some high-difference cases
            if len(significant) > 0:
                logger.info("\n   Sample of significant differences:")
                sample = significant.nlargest(5, 'difference', keep='first')[['settlementdate', 'duid', 'scadavalue_existing', 'scadavalue_corrected', 'difference', 'pct_change', 'num_intervals']]
                for _, row in sample.iterrows():
                    logger.info(f"     {row['settlementdate']} {row['duid']}: {row['scadavalue_existing']:.1f} → {row['scadavalue_corrected']:.1f} MW "
                              f"(+{row['difference']:.1f} MW, {row['pct_change']:.1f}%, {int(row['num_intervals'])} intervals)")
        
        total_time = time.time() - start_time
        logger.info(f"\n9. Total processing time: {total_time/60:.1f} minutes")
        logger.info(f"   Processing rate: {len(scada30_corrected)/(total_time/60):,.0f} records/minute")
        
    else:
        logger.error("   No results generated!")
    
    logger.info("\n" + "=" * 80)
    logger.info("RECALCULATION COMPLETE")
    logger.info(f"Corrected data saved to: {data_dir}/scada30_corrected.parquet")
    logger.info("Next steps:")
    logger.info("  1. Validate the corrected data")
    logger.info("  2. Backup existing scada30.parquet")
    logger.info("  3. Replace scada30.parquet with scada30_corrected.parquet")
    logger.info("  4. Restart production collectors with the fixed code")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()