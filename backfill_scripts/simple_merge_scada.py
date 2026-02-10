#!/usr/bin/env python3
"""
Simple merge script to add backfill data to existing scada5 parquet

Usage:
    python simple_merge_scada.py
"""

import pandas as pd
import sys
sys.path.insert(0, '../src')

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector
import requests
import zipfile
import io
from datetime import timedelta

# Initialize collector
collector = UnifiedAEMOCollector()

# Download archive
print("Downloading Oct 9 SCADA archive...")
archive_url = 'http://nemweb.com.au/Reports/ARCHIVE/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_20251009.zip'
response = requests.get(archive_url, headers={'User-Agent': 'AEMO Dashboard'}, timeout=60)

all_scada_data = []

with zipfile.ZipFile(io.BytesIO(response.content)) as outer_zip:
    files = outer_zip.namelist()

    # Get files for 10:05-13:05 (to get data for 10:00-13:00)
    start = pd.to_datetime('2025-10-09 10:05')
    end = pd.to_datetime('2025-10-09 13:05')

    for fname in files:
        if not fname.endswith('.zip'):
            continue

        try:
            parts = fname.split('_')
            timestamp_str = parts[2][:12]
            file_time = pd.to_datetime(timestamp_str, format='%Y%m%d%H%M')

            if start <= file_time <= end:
                # Extract nested ZIP
                nested_content = outer_zip.read(fname)

                with zipfile.ZipFile(io.BytesIO(nested_content)) as inner_zip:
                    csv_content = inner_zip.read(inner_zip.namelist()[0])

                    # Parse
                    df = collector.parse_mms_csv(csv_content, 'UNIT_SCADA')

                    if not df.empty and 'SETTLEMENTDATE' in df.columns:
                        scada_df = pd.DataFrame()
                        scada_df['settlementdate'] = pd.to_datetime(df['SETTLEMENTDATE'].str.strip('"'), format='%Y/%m/%d %H:%M:%S')
                        scada_df['duid'] = df['DUID'].str.strip()
                        scada_df['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
                        scada_df = scada_df[scada_df['scadavalue'].notna()]

                        if not scada_df.empty:
                            all_scada_data.append(scada_df)
                            print(f"  {file_time}: {len(scada_df)} records")
        except:
            pass

print(f"\nCollected {len(all_scada_data)} files")

# Combine
backfill_df = pd.concat(all_scada_data, ignore_index=True)
backfill_df = backfill_df.drop_duplicates(subset=['settlementdate', 'duid'])
backfill_df = backfill_df.sort_values(['settlementdate', 'duid'])

print(f"Total backfill records: {len(backfill_df)}")
print(f"Date range: {backfill_df['settlementdate'].min()} to {backfill_df['settlementdate'].max()}")

# Load existing
existing_df = pd.read_parquet('/Volumes/davidleitch/aemo_production/data/scada5.parquet')
print(f"\nExisting records: {len(existing_df)}")

# Simple merge: combine and remove duplicates
combined = pd.concat([existing_df, backfill_df], ignore_index=True)
combined = combined.drop_duplicates(subset=['settlementdate', 'duid'], keep='last')
combined = combined.sort_values(['settlementdate', 'duid'])

print(f"Combined records: {len(combined)}")
print(f"Net change: {len(combined) - len(existing_df)}")

# Save
print("\nSaving...")
combined.to_parquet('/Volumes/davidleitch/aemo_production/data/scada5.parquet', compression='snappy', index=False)

print("✓ Done!")
