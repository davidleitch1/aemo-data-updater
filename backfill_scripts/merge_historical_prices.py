#!/usr/bin/env python3
"""
Merge historical backfill with existing prices5.parquet

Uses simple concat + drop_duplicates approach (proven to work)
"""

import pandas as pd
from pathlib import Path

# File paths
backfill_file = Path("/tmp/prices5_historical_backfill.parquet")
existing_file = Path("/Volumes/davidleitch/aemo_production/data/prices5.parquet")

print("="*70)
print("Merging Historical Backfill with prices5.parquet")
print("="*70)

# Load backfill data
print(f"\nLoading backfill data from {backfill_file}...")
backfill_df = pd.read_parquet(backfill_file)
print(f"  Records: {len(backfill_df):,}")
print(f"  Date range: {backfill_df['settlementdate'].min()} to {backfill_df['settlementdate'].max()}")

# Load existing data
print(f"\nLoading existing data from {existing_file}...")
existing_df = pd.read_parquet(existing_file)
print(f"  Records: {len(existing_df):,}")
print(f"  Date range: {existing_df['settlementdate'].min()} to {existing_df['settlementdate'].max()}")

# Simple merge: combine and remove duplicates
print("\nMerging (concat + drop_duplicates)...")
combined = pd.concat([existing_df, backfill_df], ignore_index=True)
combined = combined.drop_duplicates(subset=['settlementdate', 'regionid'], keep='last')
combined = combined.sort_values(['settlementdate', 'regionid'])

print(f"  Combined records: {len(combined):,}")
print(f"  Net change: {len(combined) - len(existing_df):,}")
print(f"  Final date range: {combined['settlementdate'].min()} to {combined['settlementdate'].max()}")

# Save
print(f"\nSaving to {existing_file}...")
combined.to_parquet(existing_file, compression='snappy', index=False)

file_size_mb = existing_file.stat().st_size / (1024 * 1024)
print(f"✓ Saved: {file_size_mb:.1f} MB")

print("\n" + "="*70)
print("MERGE COMPLETE")
print("="*70)
print(f"Final prices5.parquet:")
print(f"  Records: {len(combined):,}")
print(f"  Date range: {combined['settlementdate'].min()} to {combined['settlementdate'].max()}")
print(f"  File size: {file_size_mb:.1f} MB")
print("="*70)
