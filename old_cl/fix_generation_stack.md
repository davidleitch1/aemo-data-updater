# Generation Stack Fix - Status Summary
*Date: August 20, 2025*

## Current Status
The generation stack chart is now displaying data but with two critical issues:
1. **Missing Rooftop Solar data** - The yellow band that should appear during daylight hours is completely absent
2. **Date range issue** - Dashboard is querying 2024 data instead of 2025 (seen in logs)

## What We've Accomplished

### Successfully Fixed
1. **Battery Charging Data Recovery** ✅
   - Removed line 320 from `unified_collector.py` that was filtering negative values
   - Reprocessed DISPATCHSCADA archives for July 17 - August 20, 2025
   - Successfully recovered 233,670 charging records in scada5
   - Battery storage now shows bidirectional flow (charging/discharging)

2. **Data Files Updated** ✅
   - `scada5.parquet` - Contains 5-minute data with battery charging
   - `scada30.parquet` - Contains 30-minute aggregated data with battery charging
   - Both files have data through August 20, 2025 at 20:30

## Current Problems

### 1. Missing Rooftop Solar
**Symptom**: No yellow rooftop solar band visible in generation stack chart during daylight hours

**Likely Cause**: When we recalculated scada30 from scada5 to fix battery charging, we didn't include rooftop solar data (which is stored separately in `rooftop30.parquet`)

**Evidence from logs**:
```
Error loading rooftop data via DuckDB: Binder Error: Referenced column "rooftop_solar_mw" not found
```

### 2. Date Query Issue  
**Symptom**: Dashboard requesting data from 2024 instead of 2025

**Evidence from logs**:
```
load_price_data called - start: 2024-08-19 20:18:45.720610
```

**Impact**: When viewing "Last 24 hours", it's actually showing data from a year ago

## Root Cause Analysis

When fixing the battery charging data, the process was:
1. Reprocessed scada5 from DISPATCHSCADA archives → ✅ Successful
2. Recalculated scada30 from scada5 → ✅ Successful for generation data
3. **MISSED**: Didn't merge rooftop solar data from `rooftop30.parquet` → ❌ Causing missing solar

The rooftop solar data is:
- Stored separately in `rooftop30.parquet`
- Not included in DISPATCHSCADA files
- Needs to be merged at the visualization layer or included in aggregated views

## Files Modified

### Data Files
- `/Volumes/davidleitch/aemo_production/data/scada5.parquet` - Updated with charging data
- `/Volumes/davidleitch/aemo_production/data/scada30.parquet` - Updated but missing rooftop integration

### Code Files
- `/Volumes/davidleitch/aemo_production/aemo-data-updater/src/aemo_updater/collectors/unified_collector.py` - Removed line 320

### Backup Files Created
- `scada5.parquet.backup_20250820_194003_before_charging_fix`
- `scada30.parquet.backup_20250820_194003_before_charging_fix`
- `scada30.parquet.backup_before_charging_fix2`

## Next Steps to Fix

### Immediate Actions Needed

1. **Fix Rooftop Solar Integration**
   - Option A: Update DuckDB views to properly join rooftop data
   - Option B: Create a combined view that includes scada30 + rooftop30
   - Option C: Revert scada30 and redo the fix properly with rooftop included

2. **Fix Date Query Issue**
   - Investigate why dashboard is defaulting to 2024
   - Check if it's a timezone issue or date calculation bug
   - May be related to the data restructuring

3. **Verify Data Integrity**
   - Confirm rooftop30.parquet still has current data
   - Check if the dashboard's DuckDB views need recreating
   - Ensure no other data streams were affected

## Testing Required

Once fixed, need to verify:
1. Rooftop solar appears during daylight hours (6am - 6pm)
2. Battery storage shows both charging (negative) and discharging (positive)
3. Date ranges are correct (showing 2025 data, not 2024)
4. All regions display properly
5. Daily summary table shows correct totals

## Lessons Learned

1. When modifying core data files, must consider ALL data sources, not just the primary one
2. Rooftop solar is a separate data stream that needs special handling
3. Always test the full dashboard after data structure changes
4. Keep comprehensive backups before major data modifications

## Recovery Options

If we need to rollback:
1. We have full backups of original scada files
2. The collector has been fixed and won't filter negatives going forward
3. Can restore backups and redo the process including rooftop data

## Current State Summary

- **Battery Charging**: ✅ Fixed and working
- **Rooftop Solar**: ❌ Missing from display
- **Date Queries**: ❌ Querying wrong year (2024 instead of 2025)
- **Other Generation**: ✅ Displaying correctly
- **Data Completeness**: Partial - have generation data but missing rooftop