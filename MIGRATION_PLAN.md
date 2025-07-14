# AEMO Data Updater Migration Plan

## Overview

This document outlines the plan to migrate working updater code from the existing AEMO_spot directory into the new unified updater structure in aemo-data-updater.

## Analysis of Existing Code

### 1. Price Updates (update_spot.py)
**Current Location**: `aemo-spot-dashboard/update_spot.py` and `aemo-energy-dashboard/src/aemo_dashboard/spot_prices/update_spot.py`

**Key Features**:
- Downloads from `http://nemweb.com.au/Reports/Current/Dispatch_Reports/`
- Looks for `PUBLIC_DISPATCH_YYYYMMDDHHMM_*_LEGACY.zip` files
- Parses DREGION data rows from CSV inside ZIP
- Maintains historical data in parquet format with SETTLEMENTDATE as index
- Includes Twilio price alerts integration
- 4-minute update interval
- Robust error handling and temporary file cleanup

**Data Structure**:
- DataFrame with SETTLEMENTDATE as index
- Columns: REGIONID, RRP
- Parquet format with snappy compression

### 2. Generation Updates (update_generation.py)
**Current Location**: `aemo-energy-dashboard/src/aemo_dashboard/generation/update_generation.py`

**Key Features**:
- Downloads from `http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/`
- Looks for DISPATCHSCADA ZIP files
- Parses `D,DISPATCH,UNIT_SCADA` data rows
- Stores in parquet format
- 4.5-minute update interval
- Handles migration from pickle to parquet format
- Tracks last processed file to avoid duplicates

**Data Structure**:
- Columns: settlementdate, duid, scadavalue
- No index, sorted by settlementdate
- Parquet format with snappy compression

**Special Handling**:
- gen_info.pkl provides DUID to fuel/region mapping
- Email alerts for unknown DUIDs
- Exception list management for DUIDs to ignore

### 3. Transmission Updates (update_transmission.py)
**Current Location**: `aemo-energy-dashboard/src/aemo_dashboard/transmission/update_transmission.py`

**Key Features**:
- Downloads from `http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/`
- Looks for DISPATCHIS ZIP files
- Parses `D,DISPATCH,INTERCONNECTORRES` data rows
- Stores transmission flow data including limits and losses
- 4.5-minute update interval
- Interconnector mapping for region pairs

**Data Structure**:
- Columns: settlementdate, interconnectorid, meteredmwflow, mwflow, exportlimit, importlimit, mwlosses
- No index, sorted by settlementdate
- Parquet format with snappy compression

### 4. Rooftop Solar Updates (update_rooftop.py)
**Current Location**: `aemo-energy-dashboard/src/aemo_dashboard/rooftop/update_rooftop.py`

**Key Features**:
- Downloads from `http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/`
- Looks for ROOFTOP_PV_ACTUAL_MEASUREMENT ZIP files
- Parses `D,ROOFTOP,ACTUAL` data rows
- **Special**: Converts 30-minute data to 5-minute pseudo-data using weighted averaging
- 15-minute update interval (different from others)
- Includes historical backfill capability from archive files

**Data Structure**:
- Columns: settlementdate, NSW1, QLD1, SA1, TAS1, VIC1 (pivoted format)
- No index
- Parquet format

**Conversion Algorithm**:
- Each 30-min period creates 6 x 5-min periods
- Uses weighted average between consecutive 30-min values
- Formula: `((6 - j) * current_value + j * next_value) / 6`

## Current State Analysis

### Running Service
- **Process**: `aemo-combined-update` running since Friday 3PM
- **Location**: `/Users/davidleitch/miniforge3/bin/aemo-combined-update`
- **Must Not Interrupt**: This service is actively collecting 5-minute data

### Data Interfaces

#### Parquet File Formats (Must Remain Identical)

**Generation (gen_output.parquet)**
```python
columns = ['settlementdate', 'duid', 'scadavalue']
dtypes = {'settlementdate': datetime64, 'duid': str, 'scadavalue': float64}
```

**Price (spot_hist.parquet)**
```python
columns = ['SETTLEMENTDATE', 'REGIONID', 'RRP']
dtypes = {'SETTLEMENTDATE': datetime64, 'REGIONID': str, 'RRP': float64}
# Note: SETTLEMENTDATE is the index in the parquet file
```

**Transmission (transmission_flows.parquet)**
```python
columns = ['settlementdate', 'interconnectorid', 'meteredmwflow', 
           'mwflow', 'exportlimit', 'importlimit', 'mwlosses']
```

**Rooftop Solar (rooftop_solar.parquet)**
```python
columns = ['settlementdate', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
# 30-minute to 5-minute conversion algorithm must remain identical
```

## Migration Strategy

### Phase 1: Core Infrastructure ✓
Already completed:
- Basic collector structure established
- Service framework in place
- Shared configuration and logging

### Phase 2: Migrate Working Logic
For each collector, we need to:

1. **Price Collector**:
   - Copy parsing logic from `parse_dispatch_data()`
   - Maintain SETTLEMENTDATE as index structure
   - Integrate Twilio alerts (make optional)
   - Preserve temporary file cleanup patterns

2. **Generation Collector**:
   - Copy SCADA parsing logic
   - Integrate gen_info.pkl loading for DUID mapping
   - Implement unknown DUID alert system
   - Preserve file tracking to avoid reprocessing

3. **Transmission Collector**:
   - Copy DISPATCHINTERCONNECTORRES parsing
   - Maintain interconnector mapping dictionary
   - Preserve all flow metrics (metered, target, limits, losses)

4. **Rooftop Collector**:
   - Copy 30-to-5-minute conversion algorithm exactly
   - Maintain pivoted data format
   - Different update interval (15 min vs 4.5 min for others)
   - Include backfill capability

### Phase 3: Shared Features

1. **Email Alerts** (from gen_dash.py):
   - Unknown DUID detection and alerting
   - Exception list management
   - Rate limiting (24-hour cooldown)
   - HTML email formatting
   - Environment variable configuration

2. **Data Integrity**:
   - Duplicate detection and removal
   - Timestamp validation
   - Data type enforcement
   - Missing data handling

3. **Error Recovery**:
   - Network timeout handling
   - Partial download recovery
   - Corrupted file detection
   - Graceful degradation

### Phase 4: Testing Strategy

1. **Unit Tests**:
   - Test each parser with sample data
   - Test 30-to-5-minute conversion
   - Test duplicate detection
   - Test error scenarios

2. **Integration Tests**:
   - Compare output with existing collectors
   - Verify data formats match exactly
   - Test transition from old to new system

3. **Parallel Running**:
   - Run new updater alongside old for validation
   - Compare outputs for discrepancies
   - Monitor for missing data

## Implementation Order

1. **Price Collector** (simplest, well-defined format)
2. **Generation Collector** (requires gen_info.pkl integration)
3. **Transmission Collector** (similar to generation)
4. **Rooftop Collector** (most complex with conversion logic)
5. **Shared alert system** (after individual collectors work)
6. **Dashboard integration** (update paths and imports)

## Key Patterns to Preserve

1. **URL Construction**:
   ```python
   # Handle both absolute and relative paths
   if filename.startswith('/'):
       file_url = "http://nemweb.com.au" + filename
   else:
       file_url = self.base_url + filename
   ```

2. **ZIP File Handling**:
   ```python
   # Consistent pattern across all collectors
   with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
       csv_files = [name for name in zip_file.namelist() if name.endswith('.CSV')]
   ```

3. **Duplicate Prevention**:
   ```python
   # Filter by timestamp
   latest_existing = self.data['settlementdate'].max()
   truly_new = new_df[new_df['settlementdate'] > latest_existing]
   ```

4. **Parquet Storage**:
   ```python
   # Consistent compression and format
   df.to_parquet(filepath, compression='snappy', index=False)
   ```

## Configuration Requirements

1. **File Paths**:
   - spot_hist.parquet
   - gen_output.parquet
   - transmission_flows.parquet
   - rooftop_solar.parquet
   - gen_info.pkl (reference data)

2. **Update Intervals**:
   - Price: 4 minutes
   - Generation: 4.5 minutes
   - Transmission: 4.5 minutes
   - Rooftop: 15 minutes

3. **Environment Variables**:
   - ALERT_EMAIL
   - ALERT_PASSWORD
   - RECIPIENT_EMAIL
   - SMTP_SERVER
   - SMTP_PORT
   - ENABLE_EMAIL_ALERTS
   - AUTO_ADD_TO_EXCEPTIONS

## Success Criteria

1. All data formats remain identical to existing system
2. No data loss during migration
3. Update intervals maintained
4. Alert systems continue functioning
5. Dashboard continues working without modification
6. Historical data preserved
7. Performance equal or better than current system

## Risk Mitigation

1. **Backup all existing data** before migration
2. **Run in parallel** for at least 24 hours
3. **Compare outputs** programmatically
4. **Keep old code** accessible for rollback
5. **Test each component** individually first
6. **Document any deviations** from original behavior