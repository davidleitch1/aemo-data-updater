# Battery Charging Data Fix - Summary
*Date: August 20, 2025*

## Problem Identified
Battery storage units were showing only discharge (positive) values but no charging (negative) values from July 17, 2025 onwards.

## Root Cause
Line 320 in `src/aemo_updater/collectors/unified_collector.py` was filtering out all negative SCADA values:
```python
scada_df = scada_df[scada_df['scadavalue'] >= 0]  # This line was removing charging data
```

## Solution Applied

### 1. Fixed the Collector (Completed)
- Removed the problematic line 320 from unified_collector.py
- Collector now preserves negative values for battery charging

### 2. Reprocessed Historical Data (Completed)
- Downloaded and reprocessed DISPATCHSCADA archive files for July 17 - August 20, 2025
- Successfully recovered 233,670 negative values (charging records) in scada5
- Recalculated scada30 from the corrected scada5 data

### 3. Applied Fixes to Production (Completed)
- Created backups of existing files
- Replaced scada5.parquet and scada30.parquet with corrected versions
- Battery charging data is now available for analysis

## Results

### SCADA5 (5-minute data)
- **Total records processed**: 4,350,983
- **Negative values recovered**: 233,670 (5.37%)
- **Top charging batteries**: MANNUMB1, TARBESS1, LVES1, RANGEB1, TB2B1

### SCADA30 (30-minute data)
- **Total records**: 40,843,666
- **Total negative values**: 934,760 (2.29%)
- **Key batteries verified**: HPR1, VICBG1, TIB1, BLYTHB1, DALNTH1 all showing charging data

## Files Created
- `scada5.parquet.backup_20250820_194003_before_charging_fix`
- `scada30.parquet.backup_20250820_194003_before_charging_fix`

## Next Steps
1. Monitor the dashboard to verify battery charging displays correctly
2. The collector will now maintain negative values going forward
3. No further action required - the system is self-healing

## Important Notes
- The daily archive files from AEMO contained the negative values
- The problem was entirely in our processing pipeline, not the source data
- Future data collection will automatically include charging values