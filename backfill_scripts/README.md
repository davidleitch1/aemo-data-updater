# AEMO Data Backfill Scripts

**Location:** `/Volumes/davidleitch/aemo_production/aemo-data-updater/backfill_scripts/`

**Purpose:** Scripts for backfilling missing or incomplete AEMO data

---

## Overview

This directory contains scripts for backfilling gaps in AEMO parquet data files. Gaps can occur due to:
- Collector downtime
- Network interruptions
- AEMO data publication delays
- Script errors or crashes

---

## General Backfill Procedure

All backfill scripts follow this general pattern:

1. **Identify Gap**: Use `check_data_status.py` to find gaps
2. **Locate Source Files**: Determine if data is in Current or Archive directory on NEMWeb
3. **Download Test File**: Verify structure and content with one file
4. **Download All Files**: Download all files covering the gap period
5. **Build Temp Parquet**: Create temporary parquet file with gap data
6. **Merge**: Merge temp parquet with existing file (preserving all existing data)

---

## Available Scripts

### 1. `backfill_30min_gaps.py`
**Purpose:** Backfill gaps in 30-minute data (prices30, transmission30, rooftop30)

**Data Sources:**
- Prices: TradingIS_Reports (Current)
- Transmission: TradingIS_Reports (Current)
- Rooftop: ROOFTOP_PV/ACTUAL (Current)
- SCADA30: Recalculated from scada5 data

**Usage:**
```bash
# Backfill all 30-min data types
python backfill_30min_gaps.py --start-date "2025-10-09" --end-date "2025-10-10" --type all

# Backfill only prices
python backfill_30min_gaps.py --start-date "2025-10-09" --end-date "2025-10-10" --type prices

# Test mode - analyze gaps only
python backfill_30min_gaps.py --start-date "2025-10-09" --end-date "2025-10-10" --test
```

**Supported Types:**
- `prices` - Regional prices (30-min)
- `transmission` - Interconnector flows (30-min)
- `rooftop` - Rooftop solar generation (30-min)
- `scada` - Generator output (recalculated from 5-min)
- `all` - All of the above

---

### 2. `backfill_from_archive.py` ✅ **WORKING**
**Purpose:** Backfill 5-minute data from AEMO Archive (nested ZIP handler)

**Data Sources:**
- Prices: DispatchIS_Reports/Archive (nested ZIPs)
- SCADA: Dispatch_SCADA/Archive (nested ZIPs)
- Transmission: DispatchIS_Reports/Archive (nested ZIPs)

**Key Features:**
- Handles AEMO Archive nested ZIP structure (daily ZIP → 288 nested ZIPs → CSVs)
- Automatic timestamp filtering (AEMO files named with exact timestamp they contain)
- Test mode available to verify before full backfill
- Supports all three 5-minute data types

**Usage:**
```bash
# Backfill prices only
python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices

# Test mode - download and parse but don't merge
python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type prices --test

# Backfill all data types
python backfill_from_archive.py --start "2025-10-09 10:00" --end "2025-10-09 13:00" --type all
```

**Supported Types:**
- `prices` - 5-minute dispatch prices
- `scada` - 5-minute generator output
- `transmission` - 5-minute interconnector flows
- `all` - All of the above

**Known Issue:** The merge_and_save() method reports "net change: 0" even when extracting new records. Use simple merge scripts (see below) as workaround.

---

### 3. `simple_merge.py`, `simple_merge_scada.py`, `simple_merge_transmission.py`
**Purpose:** Simple merge workaround for backfill_from_archive.py merge bug

These scripts provide a straightforward approach to merging backfill data with existing parquet files:

- `simple_merge.py` - Merges prices5 backfill data
- `simple_merge_scada.py` - Merges scada5 backfill data
- `simple_merge_transmission.py` - Merges transmission5 backfill data

**Usage:**
```bash
# Each script is self-contained and downloads + merges for Oct 9 period
python simple_merge.py              # prices5
python simple_merge_scada.py        # scada5
python simple_merge_transmission.py # transmission5
```

**Note:** These are example scripts for the Oct 9-10 backfill. Modify the date range in the script for different periods.

---

### 4. `backfill_5min_gaps.py`
**Purpose:** Backfill gaps in 5-minute data from Current directory

**Current Status:** ⚠️ **IN DEVELOPMENT - USE backfill_from_archive.py INSTEAD**

**Known Limitations:**
- Only handles Current directory (last 1-2 days)
- Does not support Archive nested ZIPs
- Gaps older than 2 days require backfill_from_archive.py

---

### 5. `backfill_curtailment.py`
**Purpose:** Backfill curtailment5 data (legacy availability-based method)

**Data Source:** Next_Day_Dispatch (Current)

**Usage:**
```bash
python backfill_curtailment.py --start-date "2025-10-09" --end-date "2025-10-10"
```

---

### 6. `backfill_duid_curtailment.py` ✅ **NEW - UIGF-Based Method**
**Purpose:** Backfill DUID-level curtailment data using UIGF from Next_Day_Dispatch UNIT_SOLUTION table

**Date Added:** January 5, 2026

**Data Source:**
- Archive: `http://nemweb.com.au/Reports/ARCHIVE/Next_Day_Dispatch/` (monthly ZIPs)
- Current: `http://nemweb.com.au/Reports/Current/Next_Day_Dispatch/` (daily files)

**Key Features:**
- Uses UIGF (Unconstrained Intermittent Generation Forecast) - what plant COULD generate
- Curtailment = UIGF - TOTALCLEARED (clipped to 0)
- Only includes semi-scheduled renewables (UIGF > 0)
- UIGF data available from July 1, 2025 onwards

**Output File:** `curtailment_duid5.parquet`

**Schema:**
| Column | Type | Description |
|--------|------|-------------|
| settlementdate | datetime | 5-minute dispatch interval |
| duid | string | Dispatchable Unit Identifier |
| uigf | float | Unconstrained Intermittent Generation Forecast (MW) |
| totalcleared | float | Actual dispatched generation (MW) |
| curtailment | float | UIGF - TOTALCLEARED, minimum 0 (MW) |

**Usage:**
```bash
# Full backfill from July 2025 (takes ~2 minutes)
cd /Volumes/davidleitch/aemo_production/aemo-data-updater
/Users/davidleitch/miniforge3/bin/python3 scripts/backfill_duid_curtailment.py
```

**Backfill Results (January 5, 2026):**
- Total records: 6,931,202
- Unique DUIDs: 196
- Date range: 2025-07-01 to 2026-01-05
- Total curtailment: 5.4 million MWh

**Top Curtailed DUIDs:**
1. MUWAWF1 (Murra Warra 1): 2.1M MW-intervals
2. GPWFEST2 (Golden Plains 2): 1.8M MW-intervals
3. GPWFEST1 (Golden Plains 1): 1.7M MW-intervals
4. MACARTH1 (MacArthur): 1.7M MW-intervals
5. CUSF1 (Coleambally Solar): 1.6M MW-intervals

---

### 7. `recalculate_scada30.py`
**Purpose:** Recalculate scada30.parquet from existing scada5 data

**When to Use:**
- After backfilling scada5 data
- When scada30 values are incorrect
- To fix aggregation errors

**Usage:**
```bash
python recalculate_scada30.py
```

**Note:** This replaces entire scada30 file by recalculating from scada5

---

### 8. `recalculate_scada30_corrected.py`
**Purpose:** Corrected version of scada30 recalculation

**Differences from `recalculate_scada30.py`:**
- Improved aggregation logic
- Better handling of missing intervals
- More efficient processing

**Usage:**
```bash
python recalculate_scada30_corrected.py
```

---

## AEMO Data Sources

### Current Data (Last 1-2 Days)

| Data Type | URL | File Pattern |
|-----------|-----|--------------|
| 5-min Prices | http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/ | PUBLIC_DISPATCHIS_YYYYMMDDHHMM_*.zip |
| 5-min SCADA | http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/ | PUBLIC_DISPATCHSCADA_YYYYMMDDHHMM_*.zip |
| 5-min Transmission | http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/ | PUBLIC_DISPATCHIS_YYYYMMDDHHMM_*.zip |
| 30-min Trading | http://nemweb.com.au/Reports/CURRENT/TradingIS_Reports/ | PUBLIC_TRADINGIS_YYYYMMDDHHMM_*.zip |
| 30-min Rooftop | http://nemweb.com.au/Reports/CURRENT/ROOFTOP_PV/ACTUAL/ | PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_*.zip |
| 5-min Curtailment (legacy) | http://nemweb.com.au/Reports/CURRENT/Next_Day_Dispatch/ | PUBLIC_NEXT_DAY_DISPATCH_*.zip |
| 5-min DUID Curtailment (UIGF) | http://nemweb.com.au/Reports/CURRENT/Next_Day_Dispatch/ | PUBLIC_NEXT_DAY_DISPATCH_*.zip |

### Archive Data (Older than 2 Days)

| Data Type | URL | File Pattern |
|-----------|-----|--------------|
| 5-min Prices | http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/ | PUBLIC_DISPATCHIS_YYYYMMDD.zip (daily) |
| 5-min SCADA | http://nemweb.com.au/Reports/ARCHIVE/Dispatch_SCADA/ | PUBLIC_DISPATCHSCADA_YYYYMMDD.zip (daily) |
| 30-min Trading | http://nemweb.com.au/Reports/ARCHIVE/TradingIS_Reports/ | PUBLIC_TRADINGIS_YYYYMMDD.zip (daily) |

**Important:** Archive files are daily consolidated ZIPs containing all individual 5-minute files for that day. They require nested ZIP extraction.

---

## Checking for Data Gaps

**Step 1:** Run data status check
```bash
cd /Volumes/davidleitch/aemo_production/aemo-data-updater
/Users/davidleitch/miniforge3/bin/python3 check_data_status.py
```

**Step 2:** Review gaps in output
```
### scada5.parquet

**Total Gaps:** 40

| Gap Start | Gap End | Duration (hours) | Missing Intervals |
|-----------|---------|------------------|-------------------|
| 2025-10-09 10:00 | 2025-10-09 11:00 | 1.0 | 11 |
| 2025-10-09 11:40 | 2025-10-09 12:35 | 0.9 | 10 |
```

**Step 3:** Determine backfill approach

- **Gaps < 1 hour**: Often acceptable, may not need backfill
- **Gaps 1-4 hours**: Use backfill scripts if data is critical
- **Gaps > 4 hours**: Investigate cause, may indicate collector failure
- **Gaps > 1 day**: Check if AEMO had publication issues

---

## Example: Backfilling October 9-10 Gaps (COMPLETED)

**Date Completed:** October 17, 2025

Based on data status check run on 2025-10-17, we identified and successfully filled gaps on Oct 9-10:

**Identified Gaps:**
- prices5: Oct 9 10:05-11:15 (1.2h), Oct 9 12:05-13:00 (0.9h)
- scada5: Oct 9 10:00-11:00 (1.0h), Oct 9 11:40-12:35 (0.9h)
- transmission5: Oct 9 10:05-11:15 (1.2h), Oct 9 12:05-13:00 (0.9h)

**Decision:** 8 days old (as of Oct 17) → Data in Archive (nested ZIPs)

**Solution Used:** Archive backfill with simple merge workaround

### Backfill Process

**Step 1: Use backfill_from_archive.py to extract data**
```bash
cd /Volumes/davidleitch/aemo_production/aemo-data-updater/backfill_scripts

# This script downloads daily archive and extracts nested ZIPs for time period
/Users/davidleitch/miniforge3/bin/python3 backfill_from_archive.py \
  --start "2025-10-09 10:00" \
  --end "2025-10-09 13:00" \
  --type prices
```

**Step 2: Use simple merge scripts to add data**

Due to a bug in `UnifiedAEMOCollector.merge_and_save()` (reports "net change: 0" even with new records), we created simple merge scripts as a workaround:

```bash
# Prices
/Users/davidleitch/miniforge3/bin/python3 simple_merge.py

# SCADA
/Users/davidleitch/miniforge3/bin/python3 simple_merge_scada.py

# Transmission
/Users/davidleitch/miniforge3/bin/python3 simple_merge_transmission.py
```

**Results:**
- ✅ prices5: Added 120 records
- ✅ scada5: Added 10,553 records
- ✅ transmission5: Added 144 records

All October 9-10 gaps successfully filled.

---

## Example: Historical MMSDM Backfill Jan 2022 - Jun 2024 (COMPLETED)

**Date Completed:** October 17, 2025

Successfully backfilled prices5.parquet from MMSDM Historical Archive, extending data back to January 1, 2022.

**Data Source:** AEMO MMSDM Historical Archive
- URL: `http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/`
- Coverage: January 2022 - June 2024 (30 months)
- Archive Structure: Monthly ZIP files containing DISPATCHPRICE tables

**Scripts Created:**
1. `backfill_prices5_historical.py` - Download and parse MMSDM monthly files
2. `merge_historical_prices.py` - Simple merge script for backfill data

### Archive Structure Discovery

**2022-2023 Structure:**
```
MMSDM/2022/MMSDM_2022_01/MMSDM_Historical_Data_SQLLoader/DATA/
    PUBLIC_DVD_DISPATCHPRICE_202201010000.zip
```

**2024 Structure (Different):**
```
MMSDM/2024/MMSDM_2024_01.zip (10 GB outer ZIP containing nested structure)
  └── MMSDM_2024_01/MMSDM_Historical_Data_SQLLoader/DATA/
      └── PUBLIC_DVD_DISPATCHPRICE_202401010000.zip
```

### Backfill Process

**Step 1: Download and test one month (Jan 2022)**
```bash
cd /Volumes/davidleitch/aemo_production/aemo-data-updater/backfill_scripts
/Users/davidleitch/miniforge3/bin/python3 backfill_prices5_historical.py \
  --start-year 2022 --start-month 1 --end-year 2022 --end-month 1 --test
```

**Step 2: Download all months (Jan 2022 - Jun 2024)**
```bash
/Users/davidleitch/miniforge3/bin/python3 backfill_prices5_historical.py \
  --start-year 2022 --start-month 1 --end-year 2024 --end-month 6
```
- Successfully downloaded 28 months (Jan 2022 - Dec 2023, Mar 2024 - Jun 2024)
- Jan-Feb 2024 required special handling due to different archive structure

**Step 3: Handle Jan-Feb 2024 (Large Archive ZIPs)**

Jan-Feb 2024 data stored in large outer ZIP files:
```python
# Extract nested DISPATCHPRICE from outer ZIP
with zipfile.ZipFile('MMSDM_2024_01.zip', 'r') as outer_zip:
    inner_zip_path = 'MMSDM_2024_01/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_DISPATCHPRICE_202401010000.zip'
    inner_zip_content = outer_zip.read(inner_zip_path)

    with zipfile.ZipFile(io.BytesIO(inner_zip_content)) as inner_zip:
        csv_content = inner_zip.read('PUBLIC_DVD_DISPATCHPRICE_202401010000.CSV')
        # Parse using UnifiedAEMOCollector
```

**Step 4: Merge with existing prices5.parquet**
```bash
/Users/davidleitch/miniforge3/bin/python3 merge_historical_prices.py
```

### Results

**Final prices5.parquet:**
- Records: 1,994,780 (from 1,908,375)
- Date range: **2022-01-01** to 2025-10-17
- Time span: 1,385 days (~3.8 years)
- Net addition: 1,313,280 records
- File size: 16.7 MB

**Data Quality:**
- ✓ Continuous 5-minute coverage from Jan 2022 to present
- ✓ All 5 NEM regions (NSW1, QLD1, SA1, TAS1, VIC1)
- ✓ Only 70 minor gaps found (largest 4 hours at 2024-07-01 junction)
- ✓ All 46 months from Jan 2022 to Oct 2025 present

**Key Learnings:**
1. MMSDM 2022-2023 uses subdirectory structure with individual table ZIPs
2. MMSDM 2024+ uses large outer ZIPs containing all monthly data
3. Python's zipfile module handles nested ZIPs better than command-line unzip
4. MMSDM files use same MMS CSV format as DispatchIS_Reports
5. UnifiedAEMOCollector.parse_mms_csv() works without modification

### Simple Merge Workaround

The simple merge scripts use basic pandas operations instead of the complex merge_and_save() logic:

```python
# Simple merge approach
combined = pd.concat([existing_df, backfill_df], ignore_index=True)
combined = combined.drop_duplicates(subset=['settlementdate', 'regionid'], keep='last')
combined = combined.sort_values(['settlementdate', 'regionid'])
```

This approach:
- Combines existing and new data
- Removes duplicates (keeping last occurrence)
- Sorts by key columns
- Successfully adds new records without merge_and_save() bug

---

## Best Practices

1. **Always test first**: Use `--test` flag to verify before full backfill
2. **Check data status before and after**: Confirm gaps are filled
3. **Backup parquet files**: Copy existing files before merge
4. **Verify merge logic**: Ensure existing data is preserved
5. **Monitor file sizes**: Large increases may indicate duplicates
6. **Log everything**: Keep backfill_log.txt for reference

---

## Merge Logic

All backfill scripts use the `UnifiedAEMOCollector.merge_and_save()` method:

```python
def merge_and_save(df: pd.DataFrame, output_file: Path, key_columns: List[str]):
    """
    Merge new data with existing parquet file.

    Process:
    1. Load existing parquet
    2. Filter out existing data in new data's date range
    3. Combine filtered existing + new data
    4. Remove duplicates based on key columns
    5. Sort and save

    Result: All existing data preserved, gaps filled with new data
    """
```

**Key Columns by Data Type:**
- prices5/prices30: `['settlementdate', 'regionid']`
- scada5/scada30: `['settlementdate', 'duid']`
- transmission5/transmission30: `['settlementdate', 'interconnectorid']`
- rooftop30: `['settlementdate', 'regionid']`
- curtailment5: `['settlementdate', 'duid']` (legacy availability-based)
- curtailment_regional5: `['settlementdate', 'regionid']` (UIGF-based by region)
- curtailment_duid5: `['settlementdate', 'duid']` (UIGF-based by DUID)

---

## Troubleshooting

### "No files found for period"
- **Cause**: Data not in Current directory (> 2 days old)
- **Solution**: Use Archive backfill or manual download

### "Test download returned empty DataFrame"
- **Cause**: Wrong table name or file format changed
- **Solution**: Check AEMO file structure, update parser

### "Merge failed - file locked"
- **Cause**: Dashboard or collector accessing file
- **Solution**: Stop collector/dashboard, retry merge

### "Duplicate records after merge"
- **Cause**: Backfill data overlaps with existing
- **Solution**: Merge logic should handle this automatically

---

## Future Enhancements

1. **Automated archive handling**: Modify `backfill_5min_gaps.py` to handle nested ZIPs
2. **Gap detection integration**: Auto-run backfill for detected gaps
3. **Scheduled backfill**: Daily cron job to fill any gaps
4. **Validation checks**: Verify data quality after backfill
5. **Rollback capability**: Restore previous parquet if backfill fails

---

## Related Documentation

- [Data Status Report](../documentation/data_status.md)
- [MAIN.md](../documentation/MAIN.md#backfill-scripts-most-recent)
- [Unified Collector](../src/aemo_updater/collectors/unified_collector.py)

---

**Last Updated:** January 5, 2026
**Maintained By:** Data Operations Team
