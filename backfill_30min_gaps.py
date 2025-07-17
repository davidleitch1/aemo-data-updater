#!/usr/bin/env python3
"""
Backfill gaps in 30-minute data
Handles prices30, transmission30, and rooftop30
Note: scada30 should be recalculated from scada5 data
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
import sys
import argparse
import time

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def backfill_trading_data(collector, start_date, end_date):
    """Backfill 30-minute trading data (prices and transmission) from current reports"""
    logger.info(f"Backfilling trading data from {start_date} to {end_date}")
    
    # We need to collect trading files for the period
    # The collector's method expects to track files, but we need a custom approach
    url = 'http://nemweb.com.au/Reports/Current/TradingIS_Reports/'
    
    try:
        files = collector.get_latest_files(url, 'PUBLIC_TRADINGIS_')
        if not files:
            logger.error("No trading files found")
            return None, None
        
        # Filter files to our date range
        relevant_files = []
        for filename in files:
            # Extract timestamp from filename (e.g., PUBLIC_TRADINGIS_202507171635_0000000472364958.zip)
            try:
                timestamp_str = filename.split('_')[2]
                file_time = pd.to_datetime(timestamp_str, format='%Y%m%d%H%M')
                if start_date <= file_time <= end_date:
                    relevant_files.append(filename)
            except:
                continue
        
        logger.info(f"Found {len(relevant_files)} files in date range")
        
        # Process files to get 30-minute data
        price_data = []
        transmission_data = []
        
        # Sample every 6th file for 30-minute intervals
        sorted_files = sorted(relevant_files)
        files_to_process = []
        for i, filename in enumerate(sorted_files):
            if i % 6 == 0:
                files_to_process.append(filename)
        
        logger.info(f"Processing {len(files_to_process)} files for 30-minute data")
        
        for i, filename in enumerate(files_to_process):
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(files_to_process)} files")
            
            # Add small delay to avoid rate limiting
            time.sleep(0.1)
            
            # Get price data
            price_df = collector.download_and_parse_file(url, filename, 'PRICE')
            if not price_df.empty and 'SETTLEMENTDATE' in price_df.columns:
                clean_price_df = pd.DataFrame()
                clean_price_df['settlementdate'] = pd.to_datetime(
                    price_df['SETTLEMENTDATE'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'REGIONID' in price_df.columns and 'RRP' in price_df.columns:
                    clean_price_df['regionid'] = price_df['REGIONID'].str.strip()
                    clean_price_df['rrp'] = pd.to_numeric(price_df['RRP'], errors='coerce')
                    main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                    clean_price_df = clean_price_df[clean_price_df['regionid'].isin(main_regions)]
                    
                    if not clean_price_df.empty:
                        price_data.append(clean_price_df)
            
            # Get transmission data
            trans_df = collector.download_and_parse_file(url, filename, 'INTERCONNECTORRES')
            if not trans_df.empty and 'SETTLEMENTDATE' in trans_df.columns:
                clean_trans_df = pd.DataFrame()
                clean_trans_df['settlementdate'] = pd.to_datetime(
                    trans_df['SETTLEMENTDATE'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'INTERCONNECTORID' in trans_df.columns and 'METEREDMWFLOW' in trans_df.columns:
                    clean_trans_df['interconnectorid'] = trans_df['INTERCONNECTORID'].str.strip()
                    clean_trans_df['meteredmwflow'] = pd.to_numeric(trans_df['METEREDMWFLOW'], errors='coerce')
                    clean_trans_df = clean_trans_df[clean_trans_df['meteredmwflow'].notna()]
                    
                    if not clean_trans_df.empty:
                        transmission_data.append(clean_trans_df)
        
        # Combine results
        prices_result = None
        transmission_result = None
        
        if price_data:
            prices_result = pd.concat(price_data, ignore_index=True)
            prices_result = prices_result.drop_duplicates(subset=['settlementdate', 'regionid'])
            logger.info(f"Collected {len(prices_result)} price records")
        
        if transmission_data:
            transmission_result = pd.concat(transmission_data, ignore_index=True)
            transmission_result = transmission_result.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
            logger.info(f"Collected {len(transmission_result)} transmission records")
        
        return prices_result, transmission_result
        
    except Exception as e:
        logger.error(f"Error backfilling trading data: {e}")
        return None, None


def backfill_rooftop_data(collector, start_date, end_date):
    """Backfill 30-minute rooftop data from current reports"""
    logger.info(f"Backfilling rooftop data from {start_date} to {end_date}")
    
    url = 'http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/'
    
    try:
        files = collector.get_latest_files(url, 'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_')
        if not files:
            logger.error("No rooftop files found")
            return None
        
        # Filter files to our date range
        relevant_files = []
        for filename in files:
            # Extract timestamp from filename
            try:
                # Format: PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20250717210000_0000000472392274.zip
                # Extract the timestamp part
                parts = filename.split('_')
                # The timestamp is the 6th element (index 5)
                timestamp_str = parts[5]
                file_time = pd.to_datetime(timestamp_str, format='%Y%m%d%H%M%S')
                if start_date <= file_time <= end_date:
                    relevant_files.append(filename)
            except:
                continue
        
        logger.info(f"Found {len(relevant_files)} rooftop files in date range")
        
        # Process files
        all_data = []
        for i, filename in enumerate(sorted(relevant_files)):  # Process all files
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{len(relevant_files)} files")
            
            # Add small delay to avoid rate limiting
            time.sleep(0.1)
            
            df = collector.download_and_parse_file(url, filename, 'ACTUAL')
            
            if not df.empty and 'INTERVAL_DATETIME' in df.columns:
                rooftop_df = pd.DataFrame()
                rooftop_df['settlementdate'] = pd.to_datetime(
                    df['INTERVAL_DATETIME'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'REGIONID' in df.columns and 'POWER' in df.columns:
                    rooftop_df['regionid'] = df['REGIONID'].str.strip()
                    rooftop_df['power'] = pd.to_numeric(df['POWER'], errors='coerce')
                    
                    # Add optional columns
                    if 'QUALITY_INDICATOR' in df.columns:
                        rooftop_df['quality_indicator'] = df['QUALITY_INDICATOR'].str.strip()
                    if 'TYPE' in df.columns:
                        rooftop_df['type'] = df['TYPE'].str.strip()
                    
                    # Filter out invalid values
                    rooftop_df = rooftop_df[rooftop_df['power'].notna()]
                    rooftop_df = rooftop_df[rooftop_df['power'] >= 0]
                    
                    if not rooftop_df.empty:
                        all_data.append(rooftop_df)
        
        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result = result.drop_duplicates(subset=['settlementdate', 'regionid'])
            logger.info(f"Collected {len(result)} rooftop records")
            return result
        else:
            return None
            
    except Exception as e:
        logger.error(f"Error backfilling rooftop data: {e}")
        return None


def recalculate_scada30(collector):
    """Recalculate scada30 from existing scada5 data"""
    logger.info("Recalculating scada30 from scada5 data...")
    
    # The collector already has this method
    scada30_df = collector.collect_30min_scada()
    
    if not scada30_df.empty:
        logger.info(f"Calculated {len(scada30_df)} scada30 records")
        return scada30_df
    else:
        logger.warning("No scada30 data calculated")
        return None


def main():
    parser = argparse.ArgumentParser(description='Backfill gaps in 30-minute AEMO data')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    parser.add_argument('--type', choices=['prices', 'transmission', 'rooftop', 'scada', 'all'], 
                       default='all', help='Type of data to backfill')
    parser.add_argument('--test', action='store_true', help='Test mode - analyze gaps only')
    args = parser.parse_args()
    
    # Initialize collector
    collector = UnifiedAEMOCollector()
    
    # Default date range if not specified
    if args.start_date:
        start_date = pd.to_datetime(args.start_date)
    else:
        start_date = pd.to_datetime('2025-07-17 16:00:00')
    
    if args.end_date:
        end_date = pd.to_datetime(args.end_date)
    else:
        end_date = pd.to_datetime('2025-07-17 20:00:00')
    
    logger.info(f"Backfill period: {start_date} to {end_date}")
    
    if args.test:
        logger.info("=== TEST MODE - Analyzing gaps only ===")
        # Analyze gaps for each file
        for filename in ['prices30.parquet', 'transmission30.parquet', 'rooftop30.parquet', 'scada30.parquet']:
            file_path = collector.data_path / filename
            if file_path.exists():
                df = pd.read_parquet(file_path)
                mask = (df['settlementdate'] >= start_date) & (df['settlementdate'] <= end_date)
                period_data = df[mask]
                
                expected_times = pd.date_range(start=start_date, end=end_date, freq='30min')
                actual_times = period_data['settlementdate'].unique() if not period_data.empty else []
                missing_times = sorted(set(expected_times) - set(actual_times))
                
                logger.info(f"\n{filename}: {len(missing_times)} gaps in period")
                if missing_times and len(missing_times) <= 20:
                    for gap in missing_times:
                        logger.info(f"  - {gap}")
        return
    
    # Backfill based on type
    if args.type in ['prices', 'transmission', 'all']:
        logger.info("\n=== Backfilling trading data (prices and transmission) ===")
        prices_df, trans_df = backfill_trading_data(collector, start_date, end_date)
        
        if prices_df is not None and not prices_df.empty:
            success = collector.merge_and_save(
                prices_df,
                collector.output_files['prices30'],
                ['settlementdate', 'regionid']
            )
            logger.info(f"Prices30 backfill: {'✓ Success' if success else '✗ Failed'}")
        
        if trans_df is not None and not trans_df.empty:
            success = collector.merge_and_save(
                trans_df,
                collector.output_files['transmission30'],
                ['settlementdate', 'interconnectorid']
            )
            logger.info(f"Transmission30 backfill: {'✓ Success' if success else '✗ Failed'}")
    
    if args.type in ['rooftop', 'all']:
        logger.info("\n=== Backfilling rooftop data ===")
        rooftop_df = backfill_rooftop_data(collector, start_date, end_date)
        
        if rooftop_df is not None and not rooftop_df.empty:
            success = collector.merge_and_save(
                rooftop_df,
                collector.output_files['rooftop30'],
                ['settlementdate', 'regionid']
            )
            logger.info(f"Rooftop30 backfill: {'✓ Success' if success else '✗ Failed'}")
    
    if args.type in ['scada', 'all']:
        logger.info("\n=== Recalculating scada30 from scada5 ===")
        scada30_df = recalculate_scada30(collector)
        
        if scada30_df is not None and not scada30_df.empty:
            # For scada30, we should replace all data, not merge
            # because it's calculated from scada5
            try:
                scada30_df.to_parquet(
                    collector.output_files['scada30'],
                    compression='snappy',
                    index=False
                )
                logger.info("Scada30 recalculation: ✓ Success")
            except Exception as e:
                logger.error(f"Scada30 recalculation: ✗ Failed - {e}")
    
    logger.info("\n=== Backfill complete ===")


if __name__ == "__main__":
    main()