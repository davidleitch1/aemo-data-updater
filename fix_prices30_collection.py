#!/usr/bin/env python3
"""
Fixed version of collect_30min_trading that properly aggregates 6 x 5-minute intervals
instead of sampling every 6th file.
"""

import pandas as pd
from typing import Dict
import logging

logger = logging.getLogger(__name__)

def collect_30min_trading_fixed(self) -> Dict[str, pd.DataFrame]:
    """
    Collect 30-minute trading data (prices and transmission) by properly
    aggregating 6 x 5-minute intervals for each 30-minute period.
    
    AEMO convention: timestamps represent the END of the interval.
    - 12:30:00 = average of 12:05, 12:10, 12:15, 12:20, 12:25, 12:30
    """
    # First, collect all 5-minute price and transmission data
    url = self.current_urls['trading']
    files = self.get_latest_files(url, 'PUBLIC_TRADINGIS_')
    
    # Get only new files
    new_files = [f for f in files if f not in self.last_files['trading']]
    
    if not new_files:
        logger.debug("No new trading files found")
        return {'prices30': pd.DataFrame(), 'transmission30': pd.DataFrame()}
    
    logger.info(f"Found {len(new_files)} new trading files")
    
    # Collect ALL 5-minute data (not just every 6th file!)
    price_5min_data = []
    transmission_5min_data = []
    
    # Process last 20 files to get enough data for aggregation
    for filename in new_files[-20:]:
        # Get price data
        price_df = self.download_and_parse_file(url, filename, 'PRICE')
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
                    price_5min_data.append(clean_price_df)
        
        # Get transmission data
        trans_df = self.download_and_parse_file(url, filename, 'INTERCONNECTORRES')
        if not trans_df.empty and 'SETTLEMENTDATE' in trans_df.columns:
            clean_trans_df = pd.DataFrame()
            clean_trans_df['settlementdate'] = pd.to_datetime(
                trans_df['SETTLEMENTDATE'].str.strip('"'), 
                format='%Y/%m/%d %H:%M:%S'
            )
            
            if 'INTERCONNECTORID' in trans_df.columns and 'METEREDMWFLOW' in trans_df.columns:
                clean_trans_df['interconnectorid'] = trans_df['INTERCONNECTORID'].str.strip()
                clean_trans_df['meteredmwflow'] = pd.to_numeric(trans_df['METEREDMWFLOW'], errors='coerce')
                
                # Also get MWFLOW if available
                if 'MWFLOW' in trans_df.columns:
                    clean_trans_df['mwflow'] = pd.to_numeric(trans_df['MWFLOW'], errors='coerce')
                
                clean_trans_df = clean_trans_df[clean_trans_df['meteredmwflow'].notna()]
                
                if not clean_trans_df.empty:
                    transmission_5min_data.append(clean_trans_df)
    
    # Update last files
    self.last_files['trading'].update(new_files)
    
    result = {'prices30': pd.DataFrame(), 'transmission30': pd.DataFrame()}
    
    # Aggregate 5-minute prices to 30-minute
    if price_5min_data:
        logger.info("Aggregating 5-minute prices to 30-minute intervals...")
        combined_5min_prices = pd.concat(price_5min_data, ignore_index=True)
        combined_5min_prices = combined_5min_prices.drop_duplicates(subset=['settlementdate', 'regionid'])
        combined_5min_prices = combined_5min_prices.sort_values(['settlementdate', 'regionid'])
        
        # Generate 30-minute endpoints
        min_time = combined_5min_prices['settlementdate'].min()
        max_time = combined_5min_prices['settlementdate'].max()
        
        # Round to next 30-minute boundary
        first_30min = min_time.ceil('30min')
        if first_30min.minute not in [0, 30]:
            if first_30min.minute < 30:
                first_30min = first_30min.replace(minute=30)
            else:
                first_30min = first_30min.replace(minute=0) + pd.Timedelta(hours=1)
        
        endpoints = pd.date_range(start=first_30min, end=max_time, freq='30min')
        
        # Aggregate for each 30-minute endpoint
        price_30min_records = []
        for endpoint in endpoints:
            # Define the 30-minute window ending at this endpoint
            window_start = endpoint - pd.Timedelta(minutes=30)
            
            # Get all 5-minute prices in this window (exclusive of start, inclusive of end)
            window_data = combined_5min_prices[
                (combined_5min_prices['settlementdate'] > window_start) & 
                (combined_5min_prices['settlementdate'] <= endpoint)
            ]
            
            # Calculate average price for each region
            for region in ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']:
                region_data = window_data[window_data['regionid'] == region]
                
                if len(region_data) > 0:
                    # Calculate average price (should be 6 intervals)
                    avg_price = region_data['rrp'].mean()
                    
                    price_30min_records.append({
                        'settlementdate': endpoint,
                        'regionid': region,
                        'rrp': avg_price
                    })
        
        if price_30min_records:
            result['prices30'] = pd.DataFrame(price_30min_records)
            logger.info(f"Aggregated to {len(result['prices30'])} 30-min price records")
    
    # Aggregate 5-minute transmission to 30-minute
    if transmission_5min_data:
        logger.info("Aggregating 5-minute transmission to 30-minute intervals...")
        combined_5min_trans = pd.concat(transmission_5min_data, ignore_index=True)
        combined_5min_trans = combined_5min_trans.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
        combined_5min_trans = combined_5min_trans.sort_values(['settlementdate', 'interconnectorid'])
        
        # Generate 30-minute endpoints
        min_time = combined_5min_trans['settlementdate'].min()
        max_time = combined_5min_trans['settlementdate'].max()
        
        # Round to next 30-minute boundary
        first_30min = min_time.ceil('30min')
        if first_30min.minute not in [0, 30]:
            if first_30min.minute < 30:
                first_30min = first_30min.replace(minute=30)
            else:
                first_30min = first_30min.replace(minute=0) + pd.Timedelta(hours=1)
        
        endpoints = pd.date_range(start=first_30min, end=max_time, freq='30min')
        
        # Get unique interconnectors
        interconnectors = combined_5min_trans['interconnectorid'].unique()
        
        # Aggregate for each 30-minute endpoint
        trans_30min_records = []
        for endpoint in endpoints:
            # Define the 30-minute window ending at this endpoint
            window_start = endpoint - pd.Timedelta(minutes=30)
            
            # Get all 5-minute transmission in this window
            window_data = combined_5min_trans[
                (combined_5min_trans['settlementdate'] > window_start) & 
                (combined_5min_trans['settlementdate'] <= endpoint)
            ]
            
            # Calculate average flow for each interconnector
            for interconnector in interconnectors:
                ic_data = window_data[window_data['interconnectorid'] == interconnector]
                
                if len(ic_data) > 0:
                    # Calculate average flows
                    record = {
                        'settlementdate': endpoint,
                        'interconnectorid': interconnector,
                        'meteredmwflow': ic_data['meteredmwflow'].mean()
                    }
                    
                    if 'mwflow' in ic_data.columns:
                        record['mwflow'] = ic_data['mwflow'].mean()
                    
                    trans_30min_records.append(record)
        
        if trans_30min_records:
            result['transmission30'] = pd.DataFrame(trans_30min_records)
            logger.info(f"Aggregated to {len(result['transmission30'])} 30-min transmission records")
    
    return result


# This is the REPLACEMENT code for lines 384-467 in unified_collector.py
# The key changes:
# 1. Collects ALL 5-minute files, not just every 6th file
# 2. Properly aggregates 6 x 5-minute intervals for each 30-minute period
# 3. Uses the correct window calculation (exclusive start, inclusive end)
# 4. Calculates mean values for the 6 intervals in each 30-minute window