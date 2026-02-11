#!/usr/bin/env python3
"""
Unified AEMO Data Collector
Collects both 5-minute and 30-minute data in a single cycle
Updates new parquet file structure with proper merge logic
"""

import re
import requests
import zipfile
import io
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from bs4 import BeautifulSoup
import time
import os
from dotenv import load_dotenv

# Import alert system
from ..alerts.base_alert import Alert, AlertSeverity
from ..alerts.email_sender import EmailSender

# Import Twilio price alerts
_twilio_import_error = None
try:
    from .twilio_price_alerts import check_price_alerts
    TWILIO_ALERTS_AVAILABLE = True
except Exception as e:
    TWILIO_ALERTS_AVAILABLE = False
    _twilio_import_error = str(e)

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)


def classify_duid_fuel(duid: str) -> Tuple[str, str]:
    """Infer fuel type from DUID naming patterns.

    Returns (fuel_type, confidence) where:
      fuel_type: 'Battery Storage', 'Solar', 'Wind', 'Water',
                 'Rooftop Solar', 'Distributed Gen', or 'Unknown'
      confidence: 'high', 'medium', or 'low'
    """
    d = duid.upper()

    # Aggregated / virtual categories
    if d.startswith('RT_'):
        return ('Rooftop Solar', 'high')
    if d.startswith('DG_'):
        return ('Distributed Gen', 'high')

    # Battery — explicit keywords
    if 'BESS' in d:
        return ('Battery Storage', 'high')
    if 'BATTERY' in d or 'BATRY' in d:
        return ('Battery Storage', 'high')

    # Battery — gen/load split patterns (G/L suffix)
    # e.g. ADPBA1G, BOWWBA1L, HVWWBA1G
    if re.search(r'BA\d+[GL]$', d):
        return ('Battery Storage', 'high')
    # e.g. BULBESG1, CAPBES1G — BES + optional digits + G/L + optional digits
    if re.search(r'BES\d*[GL]\d*$', d):
        return ('Battery Storage', 'high')
    # e.g. HBESSG1, WDBESSG1, RESS1G — ESS + optional digits + G/L + optional digits
    if re.search(r'ESS\d*[GL]\d*$', d):
        return ('Battery Storage', 'high')
    # e.g. VBBG1, LBBL1, TB2BG1, QBYNBG1
    if re.search(r'B[GL]\d+$', d):
        return ('Battery Storage', 'medium')
    # Hornsdale Power Reserve gen/load
    if re.search(r'HPR[GL]\d*$', d):
        return ('Battery Storage', 'high')
    # PBS = Power Battery Storage (e.g. LGAPBS1)
    if 'PBS' in d:
        return ('Battery Storage', 'medium')
    # BBF = Battery Farm (e.g. SWANBBF1)
    if 'BBF' in d:
        return ('Battery Storage', 'medium')
    # SFB = Solar Farm Battery (e.g. QPSFB1)
    if re.search(r'SFB\d', d):
        return ('Battery Storage', 'medium')

    # Solar
    if re.search(r'SF\d', d):
        return ('Solar', 'high')
    if 'SOLAR' in d:
        return ('Solar', 'high')
    if re.search(r'PV\d', d):
        return ('Solar', 'high')
    if re.search(r'SP\d+$', d) and 'PH' not in d:
        return ('Solar', 'medium')

    # Wind
    if 'WF' in d:
        return ('Wind', 'high')
    if 'WIND' in d:
        return ('Wind', 'high')

    # Hydro / Pumped Hydro
    if 'PUMP' in d:
        return ('Water', 'high')
    if re.search(r'PH[GL]\d', d):
        return ('Water', 'high')
    # Well-known hydro DUIDs without pattern indicators
    if d in ('SNOWYP', 'SHOAL1'):
        return ('Water', 'high')

    return ('Unknown', 'low')


class UnifiedAEMOCollector:
    """Unified collector for all AEMO data types"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the unified collector"""
        self.config = config or {}

        # Base paths - use config if provided, otherwise use environment variable
        if 'data_path' in self.config:
            self.data_path = Path(self.config['data_path'])
        else:
            self.data_path = Path(os.getenv('AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data'))
        
        # Output files
        self.output_files = {
            'prices5': self.data_path / 'prices5.parquet',
            'scada5': self.data_path / 'scada5.parquet',
            'transmission5': self.data_path / 'transmission5.parquet',
            'curtailment5': self.data_path / 'curtailment5.parquet',
            'curtailment_regional5': self.data_path / 'curtailment_regional5.parquet',
            'curtailment_duid5': self.data_path / 'curtailment_duid5.parquet',
            'prices30': self.data_path / 'prices30.parquet',
            'scada30': self.data_path / 'scada30.parquet',
            'transmission30': self.data_path / 'transmission30.parquet',
            'rooftop30': self.data_path / 'rooftop30.parquet',
            'demand30': self.data_path / 'demand30.parquet',
            'bdu5': self.data_path / 'bdu5.parquet',
            'predispatch': self.data_path / 'predispatch.parquet',
        }
        
        # URLs for current data
        self.current_urls = {
            'prices5': 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/',
            'scada5': 'http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/',
            'transmission5': 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/',
            'curtailment5': 'https://www.nemweb.com.au/Reports/Current/Next_Day_Dispatch/',
            'trading': 'http://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/',
            'rooftop': 'http://nemweb.com.au/Reports/CURRENT/ROOFTOP_PV/ACTUAL/',
            'demand': 'http://nemweb.com.au/Reports/Current/Operational_Demand/ACTUAL_HH/',
            'demand_less_snsg': 'http://nemweb.com.au/Reports/Current/Operational_Demand_Less_SNSG/ACTUAL_HH/',
            'predispatch': 'http://www.nemweb.com.au/Reports/Current/Predispatch_Reports/',
        }
        
        # Headers for requests
        self.headers = {'User-Agent': 'AEMO Dashboard Data Collector'}

        # Max files to download per data type per cycle (increase for backfill)
        self.max_files_per_cycle = self.config.get('max_files_per_cycle', 5)
        
        # Last update timestamps
        self.last_files = {
            'prices5': set(),
            'scada5': set(),
            'transmission5': set(),
            'curtailment5': set(),
            'curtailment_regional5': set(),
            'curtailment_duid5': set(),
            'trading': set(),
            'rooftop': set(),
            'demand': set(),
            'demand_less_snsg': set(),
            'bdu5': set(),
        }
        
        # Collect all data types every cycle (no frequency limiting)
        self.cycle_count = 0

        # Track last P30 run_time to avoid duplicate collection
        self.last_predispatch_run_time = None
        
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
        
        # Log Twilio alert status
        if TWILIO_ALERTS_AVAILABLE:
            logger.info("Twilio price alerts: ENABLED")
        else:
            logger.warning(f"Twilio price alerts: DISABLED - {_twilio_import_error or 'import failed'}")

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
        """Send email alert for new DUIDs with inferred fuel classification."""
        if not self.email_alerts_enabled or not self.email_sender:
            return

        try:
            # Classify each new DUID
            classified = []
            for duid in sorted(new_duids):
                fuel, confidence = classify_duid_fuel(duid)
                classified.append((duid, fuel, confidence))

            lines = []
            for duid, fuel, conf in classified:
                if fuel != 'Unknown':
                    lines.append(f"  • {duid}  →  {fuel} ({conf} confidence)")
                else:
                    lines.append(f"  • {duid}  →  (unclassified)")

            alert = Alert(
                title=f"New DUIDs Discovered: {len(new_duids)} new units",
                message=(
                    "The following new DUIDs have been discovered in the AEMO data:\n\n"
                    + "\n".join(lines)
                ),
                severity=AlertSeverity.INFO,
                source="UnifiedCollector",
                metadata={
                    "new_duids": new_duids,
                    "classifications": {d: f for d, f, _ in classified},
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
        for filename in new_files[-self.max_files_per_cycle:]:  # Process last 5 files to avoid overload
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
        for filename in new_files[-self.max_files_per_cycle:]:  # Process last 5 files
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
        for filename in new_files[-self.max_files_per_cycle:]:  # Process last 5 files
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

                    # Extract additional columns with explicit handling
                    OPTIONAL_COLUMNS = {
                        'MWFLOW': 'mwflow',
                        'MWLOSSES': 'mwlosses',
                        'EXPORTLIMIT': 'exportlimit',
                        'IMPORTLIMIT': 'importlimit'
                    }

                    for source_col, target_col in OPTIONAL_COLUMNS.items():
                        if source_col in df.columns:
                            trans_df[target_col] = pd.to_numeric(df[source_col], errors='coerce')
                        else:
                            logger.warning(f"Column {source_col} not found in AEMO data")
                            trans_df[target_col] = pd.NA

                    # Log extraction stats for monitoring
                    valid_mwflow = trans_df['mwflow'].notna().sum() if 'mwflow' in trans_df.columns else 0
                    logger.debug(f"Extracted {len(trans_df)} transmission records, {valid_mwflow} with valid mwflow")

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

    def collect_5min_curtailment(self) -> pd.DataFrame:
        """Collect 5-minute curtailment data from Next Day Dispatch files"""
        import re

        url = self.current_urls['curtailment5']
        files = self.get_latest_files(url, 'PUBLIC_NEXT_DAY_DISPATCH_')

        # Get only new files
        new_files = [f for f in files if f not in self.last_files['curtailment5']]

        if not new_files:
            logger.debug("No new curtailment files found")
            return pd.DataFrame()

        logger.info(f"Found {len(new_files)} new curtailment files")

        # Wind/solar DUID pattern
        wind_solar_pattern = re.compile(r'(WF|SF|SOLAR|WIND|PV)', re.IGNORECASE)

        all_data = []
        for filename in new_files[-self.max_files_per_cycle:]:  # Process last 5 files
            try:
                file_url = f"{url}{filename}"
                response = requests.get(file_url, headers=self.headers, timeout=60)
                response.raise_for_status()

                # Process ZIP file - extract UNIT_SOLUTION data
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv') or f.endswith('.CSV')]

                    if csv_files:
                        csv_content = z.read(csv_files[0]).decode('utf-8', errors='ignore')

                        # Parse line by line for UNIT_SOLUTION records
                        for line in csv_content.split('\n'):
                            if not line.startswith('D,DISPATCH,UNIT_SOLUTION'):
                                continue

                            parts = line.split(',')
                            if len(parts) <= 60:
                                continue

                            duid = parts[6].strip('"')

                            # Filter to wind/solar units only
                            if not wind_solar_pattern.search(duid):
                                continue

                            try:
                                settlementdate = parts[4].strip('"')
                                totalcleared = float(parts[14]) if parts[14] else 0.0
                                availability = float(parts[36]) if parts[36] else 0.0
                                semidispatchcap = int(parts[59]) if parts[59] else 0

                                # Calculate curtailment with solar night filter
                                curtailment = 0.0
                                if semidispatchcap == 1:
                                    # For solar, only calculate when availability > 1 MW
                                    if 'SF' in duid or 'SOLAR' in duid.upper():
                                        if availability > 1.0:
                                            curtailment = max(0.0, availability - totalcleared)
                                    else:
                                        # For wind, always calculate when semidispatchcap = 1
                                        curtailment = max(0.0, availability - totalcleared)

                                all_data.append({
                                    'settlementdate': settlementdate,
                                    'duid': duid,
                                    'availability': availability,
                                    'totalcleared': totalcleared,
                                    'semidispatchcap': semidispatchcap,
                                    'curtailment': curtailment
                                })
                            except (ValueError, IndexError):
                                continue

            except Exception as e:
                logger.error(f"Error processing curtailment file {filename}: {e}")
                continue

        # Update last files
        self.last_files['curtailment5'].update(new_files)

        if all_data:
            curtail_df = pd.DataFrame(all_data)
            curtail_df['settlementdate'] = pd.to_datetime(curtail_df['settlementdate'], format='%Y/%m/%d %H:%M:%S')
            curtail_df = curtail_df.drop_duplicates(subset=['settlementdate', 'duid'])
            curtail_df = curtail_df.sort_values(['settlementdate', 'duid'])

            total_curtailment = curtail_df['curtailment'].sum()
            logger.info(f"Collected {len(curtail_df)} curtailment records, total: {total_curtailment:.1f} MW")
            return curtail_df

        return pd.DataFrame()

    def collect_5min_regional_curtailment(self) -> pd.DataFrame:
        """
        Collect 5-minute regional curtailment data from DISPATCHREGIONSUM table.

        Uses AEMO's official UIGF (Unconstrained Intermittent Generation Forecast) data
        to calculate curtailment as: curtailment = UIGF - CLEAREDMW

        Schema:
            settlementdate, regionid, solar_uigf, solar_cleared, solar_curtailment,
            wind_uigf, wind_cleared, wind_curtailment, total_curtailment
        """
        url = self.current_urls['prices5']  # Same source as prices - DispatchIS_Reports
        files = self.get_latest_files(url, 'PUBLIC_DISPATCHIS_')

        # Get only new files
        new_files = [f for f in files if f not in self.last_files['curtailment_regional5']]

        if not new_files:
            logger.debug("No new regional curtailment files found")
            return pd.DataFrame()

        logger.info(f"Found {len(new_files)} new files for regional curtailment")

        all_data = []
        for filename in new_files[-self.max_files_per_cycle:]:  # Process last 5 files
            df = self.download_and_parse_file(url, filename, 'REGIONSUM')

            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                # Extract regional curtailment data
                curtail_df = pd.DataFrame()
                curtail_df['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'),
                    format='%Y/%m/%d %H:%M:%S'
                )

                if 'REGIONID' in df.columns:
                    curtail_df['regionid'] = df['REGIONID'].str.strip()

                    # Extract UIGF and cleared values
                    # Solar
                    if 'SS_SOLAR_UIGF' in df.columns:
                        curtail_df['solar_uigf'] = pd.to_numeric(df['SS_SOLAR_UIGF'], errors='coerce').fillna(0)
                    else:
                        curtail_df['solar_uigf'] = 0.0

                    if 'SS_SOLAR_CLEAREDMW' in df.columns:
                        curtail_df['solar_cleared'] = pd.to_numeric(df['SS_SOLAR_CLEAREDMW'], errors='coerce').fillna(0)
                    else:
                        curtail_df['solar_cleared'] = 0.0

                    # Wind
                    if 'SS_WIND_UIGF' in df.columns:
                        curtail_df['wind_uigf'] = pd.to_numeric(df['SS_WIND_UIGF'], errors='coerce').fillna(0)
                    else:
                        curtail_df['wind_uigf'] = 0.0

                    if 'SS_WIND_CLEAREDMW' in df.columns:
                        curtail_df['wind_cleared'] = pd.to_numeric(df['SS_WIND_CLEAREDMW'], errors='coerce').fillna(0)
                    else:
                        curtail_df['wind_cleared'] = 0.0

                    # Calculate curtailment (UIGF - Cleared, minimum 0)
                    curtail_df['solar_curtailment'] = (curtail_df['solar_uigf'] - curtail_df['solar_cleared']).clip(lower=0)
                    curtail_df['wind_curtailment'] = (curtail_df['wind_uigf'] - curtail_df['wind_cleared']).clip(lower=0)
                    curtail_df['total_curtailment'] = curtail_df['solar_curtailment'] + curtail_df['wind_curtailment']

                    # Filter to main regions
                    main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                    curtail_df = curtail_df[curtail_df['regionid'].isin(main_regions)]

                    if not curtail_df.empty:
                        all_data.append(curtail_df)

        # Update last files
        self.last_files['curtailment_regional5'].update(new_files)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_df = combined_df.sort_values(['settlementdate', 'regionid'])

            total_curtailment = combined_df['total_curtailment'].sum()
            logger.info(f"Collected {len(combined_df)} regional curtailment records, total: {total_curtailment:.1f} MW")
            return combined_df

        return pd.DataFrame()

    def collect_5min_duid_curtailment(self) -> pd.DataFrame:
        """
        Collect 5-minute DUID-level curtailment data using UIGF from Next Day Dispatch.

        Uses AEMO's official UIGF (Unconstrained Intermittent Generation Forecast) data
        from the UNIT_SOLUTION table. UIGF is what a plant COULD generate based on
        resource availability (wind/irradiance), while TOTALCLEARED is what was dispatched.

        Curtailment = UIGF - TOTALCLEARED (clipped to 0)

        Note: UIGF data available in Next_Day_Dispatch from July 1, 2025 onwards.

        Schema:
            settlementdate, duid, uigf, totalcleared, curtailment
        """
        url = self.current_urls['curtailment5']  # Same source as legacy curtailment
        files = self.get_latest_files(url, 'PUBLIC_NEXT_DAY_DISPATCH_')

        # Get only new files
        new_files = [f for f in files if f not in self.last_files['curtailment_duid5']]

        if not new_files:
            logger.debug("No new DUID curtailment files found")
            return pd.DataFrame()

        logger.info(f"Found {len(new_files)} new files for DUID curtailment")

        all_data = []
        for filename in new_files[-self.max_files_per_cycle:]:  # Process last 5 files
            try:
                file_url = f"{url}{filename}"
                response = requests.get(file_url, headers=self.headers, timeout=60)
                response.raise_for_status()

                # Process ZIP file - extract UNIT_SOLUTION data
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_files = [f for f in z.namelist() if f.endswith('.csv') or f.endswith('.CSV')]

                    if csv_files:
                        csv_content = z.read(csv_files[0]).decode('utf-8', errors='ignore')

                        # Parse line by line for UNIT_SOLUTION records
                        for line in csv_content.split('\n'):
                            if not line.startswith('D,DISPATCH,UNIT_SOLUTION'):
                                continue

                            parts = line.split(',')
                            if len(parts) <= 68:  # Need at least 69 columns for UIGF
                                continue

                            try:
                                duid = parts[6].strip('"')
                                settlementdate = parts[4].strip('"')
                                totalcleared = float(parts[14]) if parts[14] else 0.0
                                uigf = float(parts[68]) if parts[68] else 0.0

                                # Only include records with UIGF > 0 (semi-scheduled renewables)
                                if uigf > 0:
                                    curtailment = max(0.0, uigf - totalcleared)

                                    all_data.append({
                                        'settlementdate': settlementdate,
                                        'duid': duid,
                                        'uigf': uigf,
                                        'totalcleared': totalcleared,
                                        'curtailment': curtailment
                                    })
                            except (ValueError, IndexError):
                                continue

            except Exception as e:
                logger.error(f"Error processing DUID curtailment file {filename}: {e}")
                continue

        # Update last files
        self.last_files['curtailment_duid5'].update(new_files)

        if all_data:
            curtail_df = pd.DataFrame(all_data)
            curtail_df['settlementdate'] = pd.to_datetime(curtail_df['settlementdate'], format='%Y/%m/%d %H:%M:%S')
            curtail_df = curtail_df.drop_duplicates(subset=['settlementdate', 'duid'])
            curtail_df = curtail_df.sort_values(['settlementdate', 'duid'])

            total_curtailment = curtail_df['curtailment'].sum()
            unique_duids = curtail_df['duid'].nunique()
            logger.info(f"Collected {len(curtail_df)} DUID curtailment records ({unique_duids} DUIDs), total: {total_curtailment:.1f} MW")
            return curtail_df

        return pd.DataFrame()

    def collect_30min_trading(self) -> Dict[str, pd.DataFrame]:
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

                    # Extract all transmission columns
                    OPTIONAL_COLUMNS = {
                        'MWFLOW': 'mwflow',
                        'MWLOSSES': 'mwlosses',
                        'EXPORTLIMIT': 'exportlimit',
                        'IMPORTLIMIT': 'importlimit'
                    }

                    for source_col, target_col in OPTIONAL_COLUMNS.items():
                        if source_col in trans_df.columns:
                            clean_trans_df[target_col] = pd.to_numeric(trans_df[source_col], errors='coerce')

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

                        # Include all transmission columns if available
                        for col in ['mwflow', 'mwlosses', 'exportlimit', 'importlimit']:
                            if col in ic_data.columns and ic_data[col].notna().any():
                                record[col] = ic_data[col].mean()

                        trans_30min_records.append(record)
            
            if trans_30min_records:
                result['transmission30'] = pd.DataFrame(trans_30min_records)
                logger.info(f"Aggregated to {len(result['transmission30'])} 30-min transmission records")
        
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
                start_time = end_time - pd.Timedelta(minutes=30)
                
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

    def collect_30min_demand(self) -> pd.DataFrame:
        """Collect 30-minute operational demand data"""
        url = self.current_urls['demand']
        files = self.get_latest_files(url, 'PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_')

        # Get only new files
        new_files = [f for f in files if f not in self.last_files['demand']]

        if not new_files:
            logger.debug("No new demand files found")
            return pd.DataFrame()

        logger.info(f"Found {len(new_files)} new demand files")

        all_data = []
        # Process only the most recent files
        files_to_process = new_files[-3:] if len(new_files) > 3 else new_files
        for filename in files_to_process:
            # Demand files are ZIP containing CSV (not MMS table format)
            try:
                file_url = f"{url}{filename}"
                response = requests.get(file_url, headers=self.headers, timeout=60)
                response.raise_for_status()

                # Extract CSV from ZIP
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
                    if not csv_files:
                        logger.warning(f"No CSV file in {filename}")
                        continue

                    with zf.open(csv_files[0]) as f:
                        csv_content = f.read().decode('utf-8', errors='ignore')

                # Parse AEMO CSV format for OPERATIONAL_DEMAND
                demand_df = self._parse_demand_csv(csv_content)

                if not demand_df.empty:
                    all_data.append(demand_df)

            except Exception as e:
                logger.error(f"Error processing demand file {filename}: {e}")
                continue

        # Update last files
        self.last_files['demand'].update(files_to_process)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_df = combined_df.sort_values(['settlementdate', 'regionid'])
            logger.info(f"Collected {len(combined_df)} new demand records")
            return combined_df

        return pd.DataFrame()

    def _parse_demand_csv(self, csv_content: str) -> pd.DataFrame:
        """Parse operational demand CSV format

        Format:
        I,OPERATIONAL_DEMAND,ACTUAL,3,REGIONID,INTERVAL_DATETIME,OPERATIONAL_DEMAND,...
        D,OPERATIONAL_DEMAND,ACTUAL,3,NSW1,"2024/09/29 00:00:00",7416,...
        """
        lines = csv_content.strip().split('\n')

        # Find header and data lines
        header_line = None
        data_lines = []

        for line in lines:
            if line.startswith('I,OPERATIONAL_DEMAND'):
                header_line = line
            elif line.startswith('D,OPERATIONAL_DEMAND'):
                data_lines.append(line)

        if not header_line or not data_lines:
            return pd.DataFrame()

        # Parse header
        headers = header_line.split(',')

        # Parse data rows
        data_rows = []
        for line in data_lines:
            values = line.split(',')
            values = [v.strip('"') for v in values]
            data_rows.append(values)

        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=headers)

        # Extract columns by position: 4=REGIONID, 5=INTERVAL_DATETIME, 6=OPERATIONAL_DEMAND
        try:
            df = df.iloc[:, [4, 5, 6]].copy()
            df.columns = ['regionid', 'settlementdate', 'demand']
        except IndexError:
            logger.error("Cannot extract demand columns from CSV")
            return pd.DataFrame()

        # Convert types
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df['demand'] = pd.to_numeric(df['demand'], errors='coerce')

        # Remove nulls and reorder columns
        df = df.dropna()
        df = df[['settlementdate', 'regionid', 'demand']]

        return df

    def collect_30min_demand_less_snsg(self) -> pd.DataFrame:
        """Collect 30-minute operational demand less SNSG data.

        Source: Operational_Demand_Less_SNSG/ACTUAL_HH/
        Returns DataFrame with columns: [settlementdate, regionid, demand_less_snsg]
        """
        url = self.current_urls['demand_less_snsg']
        files = self.get_latest_files(url, 'PUBLIC_ACTUAL_OPERATIONAL_DEM_LESS_SNSG_HH_')

        new_files = [f for f in files if f not in self.last_files['demand_less_snsg']]

        if not new_files:
            logger.debug("No new demand_less_snsg files found")
            return pd.DataFrame()

        logger.info(f"Found {len(new_files)} new demand_less_snsg files")

        all_data = []
        files_to_process = new_files[-3:] if len(new_files) > 3 else new_files
        for filename in files_to_process:
            try:
                file_url = f"{url}{filename}"
                response = requests.get(file_url, headers=self.headers, timeout=60)
                response.raise_for_status()

                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    csv_files = [f for f in zf.namelist() if f.lower().endswith('.csv')]
                    if not csv_files:
                        continue
                    with zf.open(csv_files[0]) as f:
                        csv_content = f.read().decode('utf-8', errors='ignore')

                snsg_df = self._parse_demand_less_snsg_csv(csv_content)
                if not snsg_df.empty:
                    all_data.append(snsg_df)

            except Exception as e:
                logger.error(f"Error processing demand_less_snsg file {filename}: {e}")
                continue

        self.last_files['demand_less_snsg'].update(files_to_process)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_df = combined_df.sort_values(['settlementdate', 'regionid'])
            logger.info(f"Collected {len(combined_df)} new demand_less_snsg records")
            return combined_df

        return pd.DataFrame()

    def _parse_demand_less_snsg_csv(self, csv_content: str) -> pd.DataFrame:
        """Parse operational demand less SNSG CSV format.

        Format:
        I,OPERATIONAL_DEM_LESS_SNSG,ACTUAL,1,REGIONID,INTERVAL_DATETIME,OPERATIONAL_DEMAND_LESS_SNSG,...
        D,OPERATIONAL_DEM_LESS_SNSG,ACTUAL,1,NSW1,"2025/01/30 04:30:00",6482,...
        """
        lines = csv_content.strip().split('\n')
        data_lines = []
        for line in lines:
            if line.startswith('D,OPERATIONAL_DEM_LESS_SNSG'):
                data_lines.append(line)

        if not data_lines:
            return pd.DataFrame()

        data_rows = []
        for line in data_lines:
            values = line.split(',')
            values = [v.strip('"') for v in values]
            if len(values) >= 7:
                data_rows.append({
                    'regionid': values[4],
                    'settlementdate': values[5],
                    'demand_less_snsg': values[6],
                })

        if not data_rows:
            return pd.DataFrame()

        df = pd.DataFrame(data_rows)
        df['settlementdate'] = pd.to_datetime(df['settlementdate'])
        df['demand_less_snsg'] = pd.to_numeric(df['demand_less_snsg'], errors='coerce')
        df = df.dropna(subset=['demand_less_snsg'])
        df = df[['settlementdate', 'regionid', 'demand_less_snsg']]
        return df

    def collect_5min_bdu(self) -> pd.DataFrame:
        """Collect 5-minute BDU (Bidirectional Unit / battery) data from DISPATCHREGIONSUM.

        Extracts battery generation, charging, and storage level from the same
        DispatchIS REGIONSUM table used for curtailment data.

        Returns DataFrame with columns:
            [settlementdate, regionid, bdu_clearedmw_gen, bdu_clearedmw_load, bdu_energy_storage]
        """
        url = self.current_urls['prices5']  # Same source as prices/curtailment
        files = self.get_latest_files(url, 'PUBLIC_DISPATCHIS_')

        new_files = [f for f in files if f not in self.last_files['bdu5']]

        if not new_files:
            logger.debug("No new BDU files found")
            return pd.DataFrame()

        logger.info(f"Found {len(new_files)} new files for BDU data")

        all_data = []
        for filename in new_files[-self.max_files_per_cycle:]:
            df = self.download_and_parse_file(url, filename, 'REGIONSUM')

            if not df.empty and 'SETTLEMENTDATE' in df.columns and 'REGIONID' in df.columns:
                bdu_df = pd.DataFrame()
                bdu_df['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'),
                    format='%Y/%m/%d %H:%M:%S'
                )
                bdu_df['regionid'] = df['REGIONID'].str.strip()

                bdu_columns = {
                    'BDU_CLEAREDMW_GEN': 'bdu_clearedmw_gen',
                    'BDU_CLEAREDMW_LOAD': 'bdu_clearedmw_load',
                    'BDU_ENERGY_STORAGE': 'bdu_energy_storage',
                }
                for raw_col, out_col in bdu_columns.items():
                    if raw_col in df.columns:
                        bdu_df[out_col] = pd.to_numeric(
                            df[raw_col].str.strip().replace('', pd.NA), errors='coerce'
                        )
                    else:
                        bdu_df[out_col] = pd.NA

                main_regions = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
                bdu_df = bdu_df[bdu_df['regionid'].isin(main_regions)]

                if not bdu_df.empty:
                    all_data.append(bdu_df)

        self.last_files['bdu5'].update(new_files)

        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['settlementdate', 'regionid'])
            combined_df = combined_df.sort_values(['settlementdate', 'regionid'])
            logger.info(f"Collected {len(combined_df)} BDU records")
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

            # Ensure output directory exists
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # Save to parquet
            combined_df.to_parquet(output_file, compression='snappy', index=False)
            return True
            
        except Exception as e:
            logger.error(f"Error merging data to {output_file}: {e}")
            return False
    

    def collect_predispatch(self) -> pd.DataFrame:
        """
        Collect P30 pre-dispatch forecast data from NEMWeb.

        Returns DataFrame with columns:
        - run_time: when P30 was issued
        - settlementdate: forecasted period
        - regionid: region
        - price_forecast: predicted price
        - demand_forecast: predicted demand (MW)
        - solar_forecast: predicted solar (MW)
        - wind_forecast: predicted wind (MW)
        """
        try:
            url = self.current_urls['predispatch']
            response = requests.get(url, headers=self.headers, timeout=30)

            # Find latest P30 file
            import re as regex
            pattern = r'PUBLIC_PREDISPATCH_(\d{12})_(\d{14})_LEGACY\.zip'
            matches = regex.findall(pattern, response.text)

            if not matches:
                logger.warning("No pre-dispatch files found")
                return pd.DataFrame()

            # Get latest file
            latest = sorted(set(matches))[-1]
            run_time = datetime.strptime(latest[0], '%Y%m%d%H%M')

            # Check if we already processed this run
            if self.last_predispatch_run_time == run_time:
                logger.debug(f"P30 run {run_time.strftime('%H:%M')} already processed")
                return pd.DataFrame()

            filename = f"PUBLIC_PREDISPATCH_{latest[0]}_{latest[1]}_LEGACY.zip"
            file_url = url + filename

            logger.info(f"Fetching P30 forecast: {filename}")
            zip_response = requests.get(file_url, headers=self.headers, timeout=60)

            with zipfile.ZipFile(io.BytesIO(zip_response.content)) as zf:
                csv_name = [n for n in zf.namelist() if n.endswith('.CSV')][0]
                csv_content = zf.read(csv_name).decode('utf-8')

            # Parse PDREGION data
            lines = csv_content.split('\n')
            header_line = None
            data_rows = []

            for line in lines:
                if line.startswith('I,PDREGION,'):
                    parts = line.split(',')
                    header_line = parts[4:]
                elif line.startswith('D,PDREGION,'):
                    parts = line.split(',')
                    data_rows.append(parts[4:])

            if header_line is None or not data_rows:
                logger.warning("No PDREGION data found in pre-dispatch file")
                return pd.DataFrame()

            df = pd.DataFrame(data_rows, columns=header_line)

            # Parse and rename columns
            df['regionid'] = df['REGIONID'].astype(str)
            df['settlementdate'] = pd.to_datetime(df['PERIODID'], format='"%Y/%m/%d %H:%M:%S"')
            df['price_forecast'] = pd.to_numeric(df['RRP'], errors='coerce')
            df['demand_forecast'] = pd.to_numeric(df['TOTALDEMAND'], errors='coerce')

            # Solar and wind if available
            if 'SS_SOLAR_AVAILABILITY' in df.columns:
                df['solar_forecast'] = pd.to_numeric(df['SS_SOLAR_AVAILABILITY'], errors='coerce')
            else:
                df['solar_forecast'] = None

            if 'SS_WIND_AVAILABILITY' in df.columns:
                df['wind_forecast'] = pd.to_numeric(df['SS_WIND_AVAILABILITY'], errors='coerce')
            else:
                df['wind_forecast'] = None

            # Add run_time column
            df['run_time'] = run_time

            # Select final columns
            result_df = df[['run_time', 'settlementdate', 'regionid',
                           'price_forecast', 'demand_forecast',
                           'solar_forecast', 'wind_forecast']].copy()

            # Update last run time
            self.last_predispatch_run_time = run_time

            logger.info(f"Collected P30 forecast run {run_time.strftime('%Y-%m-%d %H:%M')} with {len(result_df)} rows")
            return result_df

        except Exception as e:
            logger.error(f"Error collecting predispatch: {e}")
            return pd.DataFrame()

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

            # Check for price alerts (Twilio SMS)
            if TWILIO_ALERTS_AVAILABLE and not prices5_df.empty:
                try:
                    # Convert to format expected by check_price_alerts:
                    # SETTLEMENTDATE as index, uppercase REGIONID and RRP columns
                    alert_df = prices5_df.copy()
                    alert_df = alert_df.rename(columns={
                        'regionid': 'REGIONID',
                        'rrp': 'RRP'
                    })
                    alert_df = alert_df.set_index('settlementdate')
                    check_price_alerts(alert_df)
                except Exception as e:
                    logger.error(f"Error checking price alerts: {e}")
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

        # Collect 5-minute curtailment (legacy DUID-based method)
        try:
            curtailment5_df = self.collect_5min_curtailment()
            results['curtailment5'] = self.merge_and_save(
                curtailment5_df,
                self.output_files['curtailment5'],
                ['settlementdate', 'duid']
            )
        except Exception as e:
            logger.error(f"Error collecting 5-min curtailment: {e}")
            results['curtailment5'] = False

        # Collect 5-minute regional curtailment (new UIGF-based method)
        try:
            curtailment_regional5_df = self.collect_5min_regional_curtailment()
            results['curtailment_regional5'] = self.merge_and_save(
                curtailment_regional5_df,
                self.output_files['curtailment_regional5'],
                ['settlementdate', 'regionid']
            )
        except Exception as e:
            logger.error(f"Error collecting 5-min regional curtailment: {e}")
            results['curtailment_regional5'] = False

        # Collect 5-minute DUID curtailment (new UIGF-based method)
        try:
            curtailment_duid5_df = self.collect_5min_duid_curtailment()
            results['curtailment_duid5'] = self.merge_and_save(
                curtailment_duid5_df,
                self.output_files['curtailment_duid5'],
                ['settlementdate', 'duid']
            )
        except Exception as e:
            logger.error(f"Error collecting 5-min DUID curtailment: {e}")
            results['curtailment_duid5'] = False

        # Collect 5-minute BDU (battery) data
        try:
            bdu5_df = self.collect_5min_bdu()
            results['bdu5'] = self.merge_and_save(
                bdu5_df,
                self.output_files['bdu5'],
                ['settlementdate', 'regionid']
            )
        except Exception as e:
            logger.error(f"Error collecting 5-min BDU: {e}")
            results['bdu5'] = False

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

        # Collect 30-minute demand
        try:
            demand30_df = self.collect_30min_demand()
            results['demand30'] = self.merge_and_save(
                demand30_df,
                self.output_files['demand30'],
                ['settlementdate', 'regionid']
            )
        except Exception as e:
            logger.error(f"Error collecting 30-min demand: {e}")
            results['demand30'] = False

        # Collect 30-minute demand less SNSG and merge into demand30
        try:
            demand_less_snsg_df = self.collect_30min_demand_less_snsg()
            if not demand_less_snsg_df.empty:
                demand30_path = self.output_files['demand30']
                if demand30_path.exists():
                    existing = pd.read_parquet(demand30_path)
                    # Ensure column exists
                    if 'demand_less_snsg' not in existing.columns:
                        existing['demand_less_snsg'] = pd.NA
                    # Drop old values for the timestamps we have new data for,
                    # then merge new values via left join
                    merged = existing.merge(
                        demand_less_snsg_df[['settlementdate', 'regionid', 'demand_less_snsg']],
                        on=['settlementdate', 'regionid'],
                        how='left',
                        suffixes=('', '_new')
                    )
                    # Use new value where available, keep existing otherwise
                    merged['demand_less_snsg'] = merged['demand_less_snsg_new'].combine_first(
                        merged['demand_less_snsg']
                    )
                    merged = merged.drop(columns=['demand_less_snsg_new'])
                    merged.to_parquet(demand30_path, compression='snappy', index=False)
                    logger.info(f"Updated {len(demand_less_snsg_df)} demand_less_snsg records in demand30")
                    results['demand_less_snsg'] = True
                else:
                    results['demand_less_snsg'] = False
            else:
                results['demand_less_snsg'] = True  # No new data is OK
        except Exception as e:
            logger.error(f"Error collecting demand_less_snsg: {e}")
            results['demand_less_snsg'] = False

        # Collect P30 pre-dispatch forecasts
        try:
            predispatch_df = self.collect_predispatch()
            if not predispatch_df.empty:
                results['predispatch'] = self.merge_and_save(
                    predispatch_df,
                    self.output_files['predispatch'],
                    ['run_time', 'settlementdate', 'regionid']
                )
            else:
                results['predispatch'] = True  # No new data is OK
        except Exception as e:
            logger.error(f"Error collecting predispatch: {e}")
            results['predispatch'] = False

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