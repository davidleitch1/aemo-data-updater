# Next Session Prompt - AEMO Dashboard Fix

## Current Situation
We need to fix the AEMO Energy Dashboard which has two critical issues after fixing battery charging data:

1. **Missing Rooftop Solar**: The dashboard is not displaying rooftop solar data (yellow band during daylight hours)
2. **Wrong Year Bug**: Dashboard is querying 2024 data instead of 2025

## What Was Done Previously
- Fixed battery charging data by removing filter in `unified_collector.py` (line 320)
- Reprocessed DISPATCHSCADA archives for July 17-Aug 20, 2025
- Recalculated scada30 from scada5, BUT forgot to include rooftop solar data
- Created backups of all original files

## Key Files and Locations
- **Data Directory**: `/Volumes/davidleitch/aemo_production/data/`
- **Dashboard Code**: `/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2/`
- **Rooftop Data**: `rooftop30.parquet` (separate file, not included in scada30)
- **Backups Available**: 
  - `scada30.parquet.backup_20250820_194003_before_charging_fix`
  - `scada5.parquet.backup_20250820_194003_before_charging_fix`

## Error Evidence from Logs
```
Error loading rooftop data via DuckDB: Binder Error: Referenced column "rooftop_solar_mw" not found
load_price_data called - start: 2024-08-19 20:18:45.720610  # Wrong year!
```

## Priority Tasks
1. **URGENT**: Fix rooftop solar integration
   - Check if `rooftop30.parquet` still has current data
   - Either fix DuckDB views OR merge rooftop data into scada30
   
2. **URGENT**: Fix date query bug (2024 vs 2025)
   - Find where the date calculation is going wrong
   - Check if related to data modifications

3. **Verify**: Ensure battery charging still works after fixes

## Testing Checklist
- [ ] Rooftop solar appears during daylight hours (6am-6pm)
- [ ] Dashboard shows 2025 data, not 2024
- [ ] Battery storage shows charging (negative) and discharging (positive)
- [ ] All regions display data correctly
- [ ] Daily summary table has correct totals

## Important Context
- The production dashboard is at: `/Volumes/davidleitch/aemo_production/aemo-energy-dashboard2/`
- The data collector is running and working correctly
- We have the fix_generation_stack.md file with full details
- Battery charging fix is permanent in the collector

## Suggested Approach
1. First check if rooftop30.parquet has current data
2. Investigate the date bug - why is it querying 2024?
3. Decide whether to:
   - Fix the existing scada30 file by merging rooftop data
   - Update DuckDB views to join rooftop data
   - Or revert and redo the entire fix properly

## Command to Start Dashboard
```bash
cd /Volumes/davidleitch/aemo_production/aemo-energy-dashboard2
./start_dashboard.sh
```

Dashboard runs on http://localhost:5006