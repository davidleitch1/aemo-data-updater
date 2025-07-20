#!/usr/bin/env python3
"""
Unified AEMO Data Collector
Collects both 5-minute and 30-minute data in a single cycle
Updates new parquet file structure with proper merge logic
"""

import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List, Optional, Set
from bs4 import BeautifulSoup
import time
import os
from dotenv import load_dotenv

# Import alert system
from ..alerts.base_alert import Alert, AlertSeverity
from ..alerts.email_sender import EmailSender

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

class UnifiedAEMOCollector:
    """Unified collector for all AEMO data types"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the unified collector"""
        self.config = config or {}
        
        # Base paths
        self.base_path = Path("/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot")
        self.data_path = self.base_path / "aemo-data-updater" / "data 2"
        
        # Output files
        self.output_files = {
            'prices5': self.data_path / 'prices5.parquet',
            'scada5': self.data_path / 'scada5.parquet',
            'transmission5': self.data_path / 'transmission5.parquet',
            'prices30': self.data_path / 'prices30.parquet',
            'scada30': self.data_path / 'scada30.parquet',
            'transmission30': self.data_path / 'transmission30.parquet',
            'rooftop30': self.data_path / 'rooftop30.parquet',
        }
        
        # URLs for current data
        self.current_urls = {
            'prices5': 'http://nemweb.com.au/Reports/Current/DispatchIS_Reports/',
            'scada5': 'http://nemweb.com.au/Reports/Current/Dispatch_SCADA/',
            'transmission5': 'http://nemweb.com.au/Reports/Current/DispatchIS_Reports/',
            'trading': 'http://nemweb.com.au/Reports/Current/TradingIS_Reports/',
            'rooftop': 'http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/',
        }
        
        # Headers for requests
        self.headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
        
        # Last update timestamps
        self.last_files = {
            'prices5': set(),
            'scada5': set(),
            'transmission5': set(),
            'trading': set(),
            'rooftop': set(),
        }
        
        # Collect all data types every cycle (no frequency limiting)
        self.cycle_count = 0
        
        # Track known DUIDs
        self.known_duids_file = self.data_path / 'known_duids.txt'
        self.known_duids = self._load_known_duids()
        
        # Setup email alerts if enabled
        self.email_alerts_enabled = os.getenv('ENABLE_EMAIL_ALERTS', 'false').lower() == 'true'
        self.email_sender = None
        
        if self.email_alerts_enabled:
            try:
                self.email_sender = EmailSender(
                    smtp_server=os.getenv('SMTP_SERVER', 'smtp.mail.me.com'),
                    smtp_port=int(os.getenv('SMTP_PORT', '587')),
                    sender_email=os.getenv('ALERT_EMAIL', ''),
                    sender_password=os.getenv('ALERT_PASSWORD', ''),
                    recipient_email=os.getenv('RECIPIENT_EMAIL', '')
                )
                logger.info("Email alerts configured successfully")
            except Exception as e:
                logger.error(f"Failed to configure email alerts: {e}")
                self.email_alerts_enabled = False
        
        logger.info("Unified AEMO collector initialized")
    
    def _load_known_duids(self) -> Set[str]:
        """Load known DUIDs from file"""
        if self.known_duids_file.exists():
            with open(self.known_duids_file, 'r') as f:
                return set(line.strip() for line in f if line.strip())
        return set()
    
    def _save_known_duids(self):
        """Save known DUIDs to file"""
        with open(self.known_duids_file, 'w') as f:
            for duid in sorted(self.known_duids):
                f.write(f"{duid}\n")
    
    def _check_new_duids(self, df: pd.DataFrame) -> List[str]:
        """Check for new DUIDs in dataframe"""
        if 'duid' not in df.columns:
            return []
        
        current_duids = set(df['duid'].unique())
        new_duids = current_duids - self.known_duids
        
        if new_duids:
            # Update known DUIDs
            self.known_duids.update(new_duids)
            self._save_known_duids()
            
            # Send email alert
            self._send_new_duid_alert(list(new_duids))
        
        return list(new_duids)
    
    def _send_new_duid_alert(self, new_duids: List[str]):
        """Send email alert for new DUIDs"""
        if not self.email_alerts_enabled or not self.email_sender:
            return
        
        try:
            alert = Alert(
                title=f"New DUIDs Discovered: {len(new_duids)} new units",
                message=f"The following new DUIDs have been discovered in the AEMO data:\n\n" + 
                       "\n".join(f"  • {duid}" for duid in sorted(new_duids)),
                severity=AlertSeverity.INFO,
                source="UnifiedCollector",
                metadata={
                    "new_duids": new_duids,
                    "total_known_duids": len(self.known_duids),
                    "timestamp": datetime.now().isoformat()
                }
            )
            
            success = self.email_sender.send(alert)
            if success:
                logger.info(f"Email alert sent for {len(new_duids)} new DUIDs")
            else:
                logger.error("Failed to send new DUID email alert")
                
        except Exception as e:
            logger.error(f"Error sending new DUID alert: {e}")
    
    def get_latest_files(self, url: str, pattern: str) -> List[str]:
        """Get latest files from a directory matching pattern"""
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            files = []
            for link in soup.find_all('a'):
                filename = link.text.strip()
                if pattern in filename and filename.endswith('.zip'):
                    files.append(filename)
            
            return sorted(files)
        except Exception as e:
            logger.error(f"Error getting files from {url}: {e}")
            return []
    
    def parse_mms_csv(self, content: bytes, table_name: str) -> pd.DataFrame:
        """Parse MMS format CSV content for specific table"""
        try:
            lines = content.decode('utf-8', errors='ignore').strip().split('\n')
            
            header_row = None
            data_rows = []
            
            for line in lines:
                if not line.strip():
                    continue
                    
                parts = line.split(',')
                
                if parts[0] == 'C':  # Comment row
                    continue
                elif parts[0] == 'I' and len(parts) > 2:
                    if parts[2] == table_name:
                        header_row = parts
                elif parts[0] == 'D' and len(parts) > 2:
                    if parts[2] == table_name:
                        data_rows.append(parts[1:])
            
            if header_row and data_rows:
                columns = header_row[4:]
                columns = [col.strip().replace('\r', '') for col in columns]
                
                df = pd.DataFrame(data_rows, columns=['ROW_TYPE', 'DATA_TYPE', 'VERSION'] + columns)
                df = df.drop(['ROW_TYPE', 'DATA_TYPE', 'VERSION'], axis=1)
                
                return df
            else:
                return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error parsing MMS CSV for {table_name}: {e}")
            return pd.DataFrame()
    
    def download_and_parse_file(self, url: str, filename: str, table_name: str) -> pd.DataFrame:
        """Download and parse a single file"""
        try:
            file_url = f"{url}{filename}"
            logger.debug(f"Downloading {file_url}")
            
            response = requests.get(file_url, headers=self.headers, timeout=60)
            response.raise_for_status()
            
            # Process ZIP file
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                files = z.namelist()
                csv_files = [f for f in files if f.endswith('.csv') or f.endswith('.CSV')]
                
                if csv_files:
                    csv_content = z.read(csv_files[0])
                    return self.parse_mms_csv(csv_content, table_name)
            
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error downloading {filename}: {e}")
            return pd.DataFrame()
    
    def collect_5min_prices(self) -> pd.DataFrame:
        """Collect 5-minute price data"""
        url = self.current_urls['prices5']
        files = self.get_latest_files(url, 'PUBLIC_DISPATCHIS_')
        
        # Get only new files
        new_files = [f for f in files if f not in self.last_files['prices5']]
        
        if not new_files:
            logger.debug("No new price files found")
            return pd.DataFrame()
        
        logger.info(f"Found {len(new_files)} new price files")
        
        all_data = []
        for filename in new_files[-5:]:  # Process last 5 files to avoid overload
            df = self.download_and_parse_file(url, filename, 'PRICE')
            
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                # Extract price data
                price_df = pd.DataFrame()
                price_df['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'REGIONID' in df.columns and 'RRP' in df.columns:
                    price_df['regionid'] = df['REGIONID'].str.strip()
                    price_df['rrp'] = pd.to_numeric(df['RRP'], errors='coerce')
                    
                    # Filter to main regions
                    main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                    price_df = price_df[price_df['regionid'].isin(main_regions)]
                    
                    if not price_df.empty:
                        all_data.append(price_df)
        
        # Update last files
        self.last_files['prices5'].update(new_files)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_df = combined_df.sort_values(['settlementdate', 'regionid'])
            logger.info(f"Collected {len(combined_df)} new price records")
            return combined_df
        
        return pd.DataFrame()
    
    def collect_5min_scada(self) -> pd.DataFrame:
        """Collect 5-minute SCADA data"""
        url = self.current_urls['scada5']
        files = self.get_latest_files(url, 'PUBLIC_DISPATCHSCADA_')
        
        # Get only new files
        new_files = [f for f in files if f not in self.last_files['scada5']]
        
        if not new_files:
            logger.debug("No new SCADA files found")
            return pd.DataFrame()
        
        logger.info(f"Found {len(new_files)} new SCADA files")
        
        all_data = []
        for filename in new_files[-5:]:  # Process last 5 files
            df = self.download_and_parse_file(url, filename, 'UNIT_SCADA')
            
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                # Extract SCADA data
                scada_df = pd.DataFrame()
                scada_df['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'DUID' in df.columns and 'SCADAVALUE' in df.columns:
                    scada_df['duid'] = df['DUID'].str.strip()
                    scada_df['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
                    
                    # Filter out invalid values
                    scada_df = scada_df[scada_df['scadavalue'].notna()]
                    scada_df = scada_df[scada_df['scadavalue'] >= 0]
                    
                    if not scada_df.empty:
                        all_data.append(scada_df)
        
        # Update last files
        self.last_files['scada5'].update(new_files)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'duid'])
            combined_df = combined_df.sort_values(['settlementdate', 'duid'])
            logger.info(f"Collected {len(combined_df)} new SCADA records")
            return combined_df
        
        return pd.DataFrame()
    
    def collect_5min_transmission(self) -> pd.DataFrame:
        """Collect 5-minute transmission data"""
        url = self.current_urls['transmission5']
        files = self.get_latest_files(url, 'PUBLIC_DISPATCHIS_')
        
        # Get only new files
        new_files = [f for f in files if f not in self.last_files['transmission5']]
        
        if not new_files:
            logger.debug("No new transmission files found")
            return pd.DataFrame()
        
        logger.info(f"Found {len(new_files)} new transmission files")
        
        all_data = []
        for filename in new_files[-5:]:  # Process last 5 files
            df = self.download_and_parse_file(url, filename, 'INTERCONNECTORRES')
            
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                # Extract transmission data
                trans_df = pd.DataFrame()
                trans_df['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'INTERCONNECTORID' in df.columns and 'METEREDMWFLOW' in df.columns:
                    trans_df['interconnectorid'] = df['INTERCONNECTORID'].str.strip()
                    trans_df['meteredmwflow'] = pd.to_numeric(df['METEREDMWFLOW'], errors='coerce')
                    
                    # Filter out invalid values
                    trans_df = trans_df[trans_df['meteredmwflow'].notna()]
                    
                    if not trans_df.empty:
                        all_data.append(trans_df)
        
        # Update last files
        self.last_files['transmission5'].update(new_files)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
            combined_df = combined_df.sort_values(['settlementdate', 'interconnectorid'])
            logger.info(f"Collected {len(combined_df)} new transmission records")
            return combined_df
        
        return pd.DataFrame()
    
    def collect_30min_trading(self) -> Dict[str, pd.DataFrame]:
        """Collect 30-minute trading data (prices and transmission)"""
        url = self.current_urls['trading']
        files = self.get_latest_files(url, 'PUBLIC_TRADINGIS_')
        
        # Get only new files
        new_files = [f for f in files if f not in self.last_files['trading']]
        
        if not new_files:
            logger.debug("No new trading files found")
            return {'prices30': pd.DataFrame(), 'transmission30': pd.DataFrame()}
        
        logger.info(f"Found {len(new_files)} new trading files")
        
        # Process every 6th file for 30-minute intervals
        trading_files = []
        for i, filename in enumerate(sorted(new_files)):
            if i % 6 == 0:  # Every 6th file = 30-minute intervals
                trading_files.append(filename)
        
        if not trading_files:
            logger.debug("No 30-minute trading files to process")
            return {'prices30': pd.DataFrame(), 'transmission30': pd.DataFrame()}
        
        price_data = []
        transmission_data = []
        
        for filename in trading_files[-10:]:  # Process last 10 files
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
                        price_data.append(clean_price_df)
            
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
                    
                    clean_trans_df = clean_trans_df[clean_trans_df['meteredmwflow'].notna()]
                    
                    if not clean_trans_df.empty:
                        transmission_data.append(clean_trans_df)
        
        # Update last files
        self.last_files['trading'].update(new_files)
        
        result = {'prices30': pd.DataFrame(), 'transmission30': pd.DataFrame()}
        
        if price_data:
            combined_prices = pd.concat(price_data, ignore_index=True)
            combined_prices = combined_prices.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_prices = combined_prices.sort_values(['settlementdate', 'regionid'])
            result['prices30'] = combined_prices
            logger.info(f"Collected {len(combined_prices)} new 30-min price records")
        
        if transmission_data:
            combined_trans = pd.concat(transmission_data, ignore_index=True)
            combined_trans = combined_trans.drop_duplicates(subset=['settlementdate', 'interconnectorid'])
            combined_trans = combined_trans.sort_values(['settlementdate', 'interconnectorid'])
            result['transmission30'] = combined_trans
            logger.info(f"Collected {len(combined_trans)} new 30-min transmission records")
        
        return result
    
    def collect_30min_scada(self) -> pd.DataFrame:
        """Collect 30-minute SCADA data by averaging 6x5min intervals from existing scada5 data"""
        try:
            # First check if we have scada5 and scada30 files
            if not self.output_files['scada5'].exists():
                logger.debug("No scada5.parquet file exists yet")
                return pd.DataFrame()
                
            # Read existing scada5 data
            scada5_df = pd.read_parquet(self.output_files['scada5'])
            
            # Determine the starting point
            if self.output_files['scada30'].exists():
                scada30_df = pd.read_parquet(self.output_files['scada30'])
                last_30min_time = scada30_df['settlementdate'].max()
                logger.info(f"Last scada30 timestamp: {last_30min_time}")
                
                # Only process data after the last 30-minute timestamp
                mask = scada5_df['settlementdate'] > last_30min_time
                data_to_process = scada5_df[mask].copy()
            else:
                # Process all scada5 data
                data_to_process = scada5_df.copy()
                logger.info("No existing scada30 data, processing all scada5 data")
            
            if data_to_process.empty:
                logger.debug("No new 5-minute data to aggregate")
                return pd.DataFrame()
            
            # Find all 30-minute endpoints in the data
            unique_times = data_to_process['settlementdate'].unique()
            endpoints = [t for t in unique_times if t.minute in [0, 30]]
            
            if not endpoints:
                logger.debug("No 30-minute endpoints found in new data")
                return pd.DataFrame()
            
            logger.info(f"Found {len(endpoints)} potential 30-minute endpoints to process")
            
            # Process each endpoint
            aggregated_data = []
            valid_endpoints = 0
            
            for end_time in sorted(endpoints):
                end_time = pd.Timestamp(end_time)
                start_time = end_time - pd.Timedelta(minutes=25)
                
                # Get all DUIDs that have data at this endpoint
                endpoint_duids = data_to_process[
                    data_to_process['settlementdate'] == end_time
                ]['duid'].unique()
                
                duids_with_complete_data = 0
                
                for duid in endpoint_duids:
                    # Get all intervals for this DUID in the 30-minute window
                    # Need to check the full scada5 dataframe, not just new data
                    mask = (
                        (scada5_df['duid'] == duid) & 
                        (scada5_df['settlementdate'] > start_time) & 
                        (scada5_df['settlementdate'] <= end_time)
                    )
                    intervals = scada5_df[mask]
                    
                    # Calculate average: mean of available intervals
                    if len(intervals) > 0:
                        # Mean of intervals (correct mathematical average)
                        avg_value = intervals['scadavalue'].mean()
                        
                        aggregated_data.append({
                            'settlementdate': end_time,
                            'duid': duid,
                            'scadavalue': avg_value
                        })
                        duids_with_complete_data += 1
                
                if duids_with_complete_data > 0:
                    valid_endpoints += 1
                    logger.debug(f"Endpoint {end_time}: aggregated {duids_with_complete_data} DUIDs")
            
            if not aggregated_data:
                logger.info("No complete 30-minute periods found")
                return pd.DataFrame()
            
            # Create final dataframe
            result_df = pd.DataFrame(aggregated_data)
            result_df = result_df.drop_duplicates(subset=['settlementdate', 'duid'])
            result_df = result_df.sort_values(['settlementdate', 'duid'])
            
            logger.info(f"Aggregated {len(result_df)} records for {valid_endpoints} endpoints")
            return result_df
            
        except Exception as e:
            logger.error(f"Error in collect_30min_scada: {e}")
            return pd.DataFrame()
    
    def collect_30min_rooftop(self) -> pd.DataFrame:
        """Collect 30-minute rooftop solar data"""
        url = self.current_urls['rooftop']
        files = self.get_latest_files(url, 'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_')
        
        # Get only new files
        new_files = [f for f in files if f not in self.last_files['rooftop']]
        
        if not new_files:
            logger.debug("No new rooftop files found")
            return pd.DataFrame()
        
        logger.info(f"Found {len(new_files)} new rooftop files")
        
        all_data = []
        # Process only the most recent files but don't mark all as seen
        files_to_process = new_files[-3:] if len(new_files) > 3 else new_files
        for filename in files_to_process:
            df = self.download_and_parse_file(url, filename, 'ACTUAL')
            
            if not df.empty and 'INTERVAL_DATETIME' in df.columns:
                # Extract rooftop data
                rooftop_df = pd.DataFrame()
                rooftop_df['settlementdate'] = pd.to_datetime(
                    df['INTERVAL_DATETIME'].str.strip('"'), 
                    format='%Y/%m/%d %H:%M:%S'
                )
                
                if 'REGIONID' in df.columns and 'POWER' in df.columns:
                    rooftop_df['regionid'] = df['REGIONID'].str.strip()
                    rooftop_df['power'] = pd.to_numeric(df['POWER'], errors='coerce')
                    
                    # Filter out invalid values
                    rooftop_df = rooftop_df[rooftop_df['power'].notna()]
                    rooftop_df = rooftop_df[rooftop_df['power'] >= 0]
                    
                    if not rooftop_df.empty:
                        all_data.append(rooftop_df)
        
        # Update last files - only mark processed files as seen
        self.last_files['rooftop'].update(files_to_process)
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_df = combined_df.sort_values(['settlementdate', 'regionid'])
            logger.info(f"Collected {len(combined_df)} new rooftop records")
            return combined_df
        
        return pd.DataFrame()
    
    def merge_and_save(self, df: pd.DataFrame, output_file: Path, key_columns: List[str]) -> bool:
        """Merge new data with existing parquet file using safe merge logic"""
        try:
            if df.empty:
                return False
            
            # Load existing data if file exists
            if output_file.exists():
                existing_df = pd.read_parquet(output_file)
                
                # Get date range of new data
                date_col = 'settlementdate' if 'settlementdate' in df.columns else df.columns[0]
                new_min_date = df[date_col].min()
                new_max_date = df[date_col].max()
                
                # Filter out existing data in overlapping date range
                # Keep existing data outside the new data's date range
                existing_filtered = existing_df[
                    (existing_df[date_col] < new_min_date) | 
                    (existing_df[date_col] > new_max_date)
                ]
                
                # Also keep any existing data that doesn't conflict with new data
                # This handles the case where new data might have gaps
                if len(existing_filtered) < len(existing_df):
                    overlap_data = existing_df[
                        (existing_df[date_col] >= new_min_date) & 
                        (existing_df[date_col] <= new_max_date)
                    ]
                    
                    # Remove only the records that will be replaced by new data
                    new_keys = set(df[key_columns].apply(tuple, axis=1))
                    overlap_keys = set(overlap_data[key_columns].apply(tuple, axis=1))
                    
                    # Keep overlap data that doesn't conflict with new data
                    non_conflicting = overlap_data[
                        ~overlap_data[key_columns].apply(tuple, axis=1).isin(new_keys)
                    ]
                    
                    existing_filtered = pd.concat([existing_filtered, non_conflicting], ignore_index=True)
                
                # Combine filtered existing with new data
                combined_df = pd.concat([existing_filtered, df], ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=key_columns)
                combined_df = combined_df.sort_values(key_columns)
                
                records_added = len(combined_df) - len(existing_df)
                logger.info(f"Merged {len(df)} new records, net change: {records_added}")
            else:
                # No existing file, just save new data
                combined_df = df.copy()
                logger.info(f"Created new file with {len(combined_df)} records")
            
            # Save to parquet
            combined_df.to_parquet(output_file, compression='snappy', index=False)
            return True
            
        except Exception as e:
            logger.error(f"Error merging data to {output_file}: {e}")
            return False
    
    def run_single_update(self) -> Dict[str, bool]:
        """Run a single update cycle for all data types"""
        logger.info("=== Starting unified update cycle ===")
        start_time = datetime.now()
        
        results = {}
        
        # Always collect 5-minute data
        logger.info("Collecting 5-minute data...")
        
        # Collect 5-minute prices
        try:
            prices5_df = self.collect_5min_prices()
            results['prices5'] = self.merge_and_save(
                prices5_df, 
                self.output_files['prices5'], 
                ['settlementdate', 'regionid']
            )
        except Exception as e:
            logger.error(f"Error collecting 5-min prices: {e}")
            results['prices5'] = False
        
        # Collect 5-minute SCADA
        try:
            scada5_df = self.collect_5min_scada()
            results['scada5'] = self.merge_and_save(
                scada5_df, 
                self.output_files['scada5'], 
                ['settlementdate', 'duid']
            )
            # Check for new DUIDs
            if not scada5_df.empty:
                new_duids = self._check_new_duids(scada5_df)
                if new_duids:
                    logger.info(f"Discovered {len(new_duids)} new DUIDs: {', '.join(new_duids[:5])}{'...' if len(new_duids) > 5 else ''}")
        except Exception as e:
            logger.error(f"Error collecting 5-min SCADA: {e}")
            results['scada5'] = False
        
        # Collect 5-minute transmission
        try:
            transmission5_df = self.collect_5min_transmission()
            results['transmission5'] = self.merge_and_save(
                transmission5_df, 
                self.output_files['transmission5'], 
                ['settlementdate', 'interconnectorid']
            )
        except Exception as e:
            logger.error(f"Error collecting 5-min transmission: {e}")
            results['transmission5'] = False
        
        # Collect 30-minute data (every cycle)
        self.cycle_count += 1
        logger.info("Collecting 30-minute data...")
        
        # Collect 30-minute trading data
        try:
            trading_data = self.collect_30min_trading()
            
            results['prices30'] = self.merge_and_save(
                trading_data['prices30'],
                self.output_files['prices30'],
                ['settlementdate', 'regionid']
            )
            
            results['transmission30'] = self.merge_and_save(
                trading_data['transmission30'],
                self.output_files['transmission30'],
                ['settlementdate', 'interconnectorid']
            )
            
        except Exception as e:
            logger.error(f"Error collecting 30-min trading data: {e}")
            results['prices30'] = False
            results['transmission30'] = False
        
        # Collect 30-minute SCADA
        try:
            scada30_df = self.collect_30min_scada()
            results['scada30'] = self.merge_and_save(
                scada30_df,
                self.output_files['scada30'],
                ['settlementdate', 'duid']
            )
        except Exception as e:
            logger.error(f"Error collecting 30-min SCADA: {e}")
            results['scada30'] = False
        
        # Collect 30-minute rooftop
        try:
            rooftop30_df = self.collect_30min_rooftop()
            results['rooftop30'] = self.merge_and_save(
                rooftop30_df,
                self.output_files['rooftop30'],
                ['settlementdate', 'regionid']
            )
        except Exception as e:
            logger.error(f"Error collecting 30-min rooftop: {e}")
            results['rooftop30'] = False
        
        # Summary
        duration = (datetime.now() - start_time).total_seconds()
        success_count = sum(results.values())
        total_collections = len(results)
        
        logger.info(f"=== Update cycle complete in {duration:.1f}s ===")
        logger.info(f"Results: {success_count}/{total_collections} successful")
        
        # Log individual results
        for data_type, success in results.items():
            status = "✓" if success else "○"
            logger.info(f"  {data_type}: {status}")
        
        return results
    
    def run_continuous(self, update_interval_minutes: float = 4.5):
        """Run continuous monitoring loop"""
        logger.info("Starting unified AEMO data collection...")
        logger.info(f"Update interval: {update_interval_minutes} minutes")
        logger.info("All data types (5-min and 30-min) collected every cycle")
        
        update_interval_seconds = update_interval_minutes * 60
        cycle_count = 0
        
        while True:
            try:
                cycle_count += 1
                logger.info(f"--- Cycle {cycle_count} ---")
                
                # Run update
                results = self.run_single_update()
                
                # Log any successful updates
                if any(results.values()):
                    successful_types = [k for k, v in results.items() if v]
                    logger.info(f"Updated: {', '.join(successful_types)}")
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
            
            # Wait for next cycle
            logger.info(f"Waiting {update_interval_minutes} minutes for next cycle...")
            time.sleep(update_interval_seconds)


def main():
    """Main function for testing"""
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    collector = UnifiedAEMOCollector()
    
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        print("Running single test update...")
        results = collector.run_single_update()
        print(f"Test results: {results}")
    else:
        collector.run_continuous()


if __name__ == "__main__":
    main()