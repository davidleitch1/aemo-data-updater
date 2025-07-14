#!/usr/bin/env python3
"""
AEMO Spot Price Collector
Downloads AEMO dispatch data and updates historical spot price file.
Adapted from working update_spot.py
"""

import os
import time
import requests
import zipfile
import pandas as pd
from datetime import datetime
import tempfile
from io import StringIO, BytesIO
import csv
import re
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

from .base_collector import BaseCollector
from ..config import get_config, get_logger

logger = get_logger(__name__)


class PriceCollector(BaseCollector):
    """
    AEMO Spot Price Collector
    Downloads and processes dispatch price data from AEMO website
    """
    
    def __init__(self, config):
        """Initialize the collector with configuration"""
        super().__init__(config)
        self.name = "price"
        self.parquet_file = config.spot_hist_file
        
        # Use URL from CLAUDE_UPDATER.md for consistency with transmission collector
        self.base_url = "http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/"
        
        # Ensure the directory exists
        self.parquet_file.parent.mkdir(parents=True, exist_ok=True)
        
    def get_latest_urls(self, start_date: Optional[datetime] = None, 
                       end_date: Optional[datetime] = None) -> List[str]:
        """Get URLs for latest price files"""
        try:
            # Get the main page to find the latest file
            response = self.session.get(self.base_url, timeout=30)
            response.raise_for_status()
            
            # Look for PUBLIC_DISPATCHIS files - pattern from CLAUDE_UPDATER.md
            zip_pattern = r'PUBLIC_DISPATCHIS_\d{12}_\w+\.zip'
            matches = re.findall(zip_pattern, response.text)
            
            if not matches:
                self.logger.warning("No dispatch files found on NEMWEB")
                return []
            
            # Get the latest file
            latest_file = sorted(matches)[-1]
            file_url = self.base_url + latest_file
            
            self.logger.info(f"Found latest price file: {latest_file}")
            return [file_url]
            
        except Exception as e:
            self.logger.error(f"Error getting price URLs: {e}")
            return []

    def parse_data(self, content: bytes) -> pd.DataFrame:
        """
        Parse AEMO dispatch CSV data and extract regional price information.
        Returns DataFrame with SETTLEMENTDATE as index and columns: REGIONID, RRP
        """
        try:
            # First extract CSV from ZIP file
            with zipfile.ZipFile(BytesIO(content)) as zf:
                csv_files = [f for f in zf.namelist() if f.endswith('.CSV')]
                if not csv_files:
                    self.logger.error("No CSV file found in ZIP")
                    return pd.DataFrame()
                
                csv_content = zf.read(csv_files[0]).decode('utf-8')
            
            # AEMO CSV files have variable number of columns per row
            lines = csv_content.strip().split('\n')
            
            price_records = []
            
            for line in lines:
                if not line.strip():
                    continue
                    
                # Split the line by commas, handling quoted fields properly
                reader = csv.reader(StringIO(line))
                try:
                    fields = next(reader)
                except:
                    continue
                
                # Look for DREGION data rows (marked with 'D' in first column)
                if len(fields) >= 9 and fields[0] == 'D' and fields[1] == 'DREGION':
                    try:
                        # Field positions:
                        # 0: D, 1: DREGION, 2: empty, 3: 3, 4: settlement_date, 
                        # 5: runno, 6: regionid, 7: intervention, 8: rrp
                        settlement_date = pd.to_datetime(fields[4])
                        region = fields[6]
                        rrp = float(fields[8])
                        
                        price_records.append({
                            'SETTLEMENTDATE': settlement_date,
                            'REGIONID': region,
                            'RRP': rrp
                        })
                    except (ValueError, TypeError, IndexError) as e:
                        self.logger.debug(f"Error parsing row: {e}")
                        continue
            
            if not price_records:
                self.logger.warning("No valid price records extracted")
                return pd.DataFrame()
                
            # Convert to DataFrame and set SETTLEMENTDATE as index
            temp_df = pd.DataFrame(price_records)
            result_df = temp_df.set_index('SETTLEMENTDATE')
            
            settlement_time = result_df.index[0]
            self.logger.info(f"Extracted {len(result_df)} price records for settlement time: {settlement_time}")
            
            # Show sample of extracted data for verification
            for idx, (settlement_date, row) in enumerate(result_df.iterrows()):
                if idx < 3:  # Just show first 3
                    self.logger.info(f"  Parsed: {row['REGIONID']} = ${row['RRP']:.2f}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error parsing price data: {e}")
            return pd.DataFrame()

    def get_unique_columns(self) -> List[str]:
        """Return columns that make a record unique"""
        # From update_spot.py: unique by SETTLEMENTDATE + REGIONID
        return ['REGIONID']  # SETTLEMENTDATE is the index
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        """Validate price data"""
        if df.empty:
            return False
        
        # Check required columns
        required_cols = ['REGIONID', 'RRP']
        if not all(col in df.columns for col in required_cols):
            self.logger.error(f"Missing required columns. Found: {df.columns.tolist()}")
            return False
        
        # Validate data types and ranges
        try:
            # RRP should be numeric
            if not pd.api.types.is_numeric_dtype(df['RRP']):
                self.logger.error("RRP column is not numeric")
                return False
            
            # Check for reasonable price ranges (can be negative but not too extreme)
            if df['RRP'].min() < -1000 or df['RRP'].max() > 20000:
                self.logger.warning(f"Unusual price range: ${df['RRP'].min():.2f} to ${df['RRP'].max():.2f}")
            
            # Check regions are valid
            valid_regions = {'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}
            invalid_regions = set(df['REGIONID'].unique()) - valid_regions
            if invalid_regions:
                self.logger.warning(f"Unknown regions: {invalid_regions}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {e}")
            return False

