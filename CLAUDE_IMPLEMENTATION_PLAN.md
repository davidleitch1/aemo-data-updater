# AEMO Data Updater Implementation Plan

## Overview
This plan outlines the steps to complete the AEMO Data Updater by migrating the existing working code into the new collector-based architecture while preserving all current functionality.

## Current Status
- ✅ Project structure created with UV environment
- ✅ Base collector abstract class defined
- ✅ Configuration system implemented
- ✅ Status dashboard created and working
- ❌ Concrete collector implementations missing
- ❌ Service initialization failing due to abstract classes

## Implementation Order
We will implement collectors in order of complexity and criticality:

### Phase 1: Price Collector (Simplest)
**Source**: `aemo-spot-dashboard/update_spot.py`
**Key Features to Migrate**:
- Download from `http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/`
- Parse DREGION table from CSV files
- Store with SETTLEMENTDATE as index
- Include Twilio price alerts (later phase)

**Implementation Steps**:
1. Copy URL handling logic from update_spot.py
2. Implement parse_data() to extract DREGION rows
3. Preserve exact column names: SETTLEMENTDATE, REGIONID, RRP
4. Ensure SETTLEMENTDATE becomes the index in parquet

### Phase 2: Generation Collector
**Source**: `aemo-energy-dashboard/aemo_dashboard/generation/update_generation.py`
**Key Features to Migrate**:
- Download from `http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/`
- Parse D,DISPATCH,UNIT_SCADA rows
- Use gen_info.pkl for DUID mappings
- Email alerts for unknown DUIDs

**Implementation Steps**:
1. Copy CSV parsing logic for UNIT_SCADA data
2. Integrate gen_info.pkl loading
3. Implement unknown DUID detection
4. Add email alert system with 24-hour cooldown

### Phase 3: Transmission Collector
**Source**: `aemo-energy-dashboard/aemo_dashboard/transmission/update_transmission.py`
**Key Features to Migrate**:
- Download from CURRENT and ARCHIVE URLs
- Parse DISPATCHINTERCONNECTORRES data
- Handle nested ZIP structure for archives
- Apply interconnector mapping

**Implementation Steps**:
1. Implement dual URL support (CURRENT/ARCHIVE)
2. Add nested ZIP extraction for archive files
3. Copy interconnector mapping logic
4. Preserve all flow columns (meteredmwflow, limits, losses)

### Phase 4: Rooftop Solar Collector (Most Complex)
**Source**: `aemo-energy-dashboard/aemo_dashboard/rooftop/update_rooftop.py`
**Key Features to Migrate**:
- Download from `http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/`
- **CRITICAL**: 30-minute to 5-minute conversion algorithm
- Weighted interpolation: `((6-j)*current + j*next)/6`
- Special handling for end-of-data scenarios

**Implementation Steps**:
1. Copy entire conversion algorithm EXACTLY
2. Preserve the 6-step interpolation logic
3. Handle edge cases at data boundaries
4. Maintain regional column structure

## Critical Requirements from CLAUDE_UPDATER.md

### 1. User-Agent Headers (MANDATORY)
```python
headers = {'User-Agent': 'AEMO Dashboard Data Collector'}
```
**Without this, all requests will fail with 403 errors!**

### 2. Update Intervals
- Price/Generation/Transmission: 4.5 minutes (270 seconds)
- Rooftop Solar: Check every 15 minutes (data published every 30 min)

### 3. Data Storage Paths
Must align with existing dashboard expectations:
- Generation: `gen_output.parquet`
- Prices: `spot_hist.parquet` (now pointing to aemo-spot-dashboard location)
- Transmission: `transmission_flows.parquet`
- Rooftop: `rooftop_solar.parquet`

### 4. Archive Handling (Transmission)
- Daily archives contain 288 nested 5-minute ZIPs
- Must extract: Daily ZIP → 5-min ZIP → CSV

## Testing Strategy

### Unit Tests (Already Created)
- ✅ URL construction
- ✅ Data parsing simulation
- ✅ Parquet operations
- ✅ Rooftop conversion algorithm

### Integration Tests (To Create)
1. Test each collector against live NEMWEB
2. Verify data compatibility with existing files
3. Test backfill functionality
4. Validate data integrity checks

### Validation Steps
1. Compare output with existing updater data
2. Ensure column names match exactly
3. Verify data types are preserved
4. Test dashboard can read new data

## Alert System Integration

### Phase 1: Data Freshness Alerts
- Threshold: 30 minutes (already updated in dashboard)
- Future: Integrate Twilio for SMS alerts

### Phase 2: Data Quality Alerts
- Unknown DUID alerts (from generation)
- Price spike alerts (from existing update_spot.py)
- Missing data alerts

## Migration Checklist

### For Each Collector:
- [ ] Copy working code from existing updater
- [ ] Adapt to BaseCollector interface
- [ ] Preserve exact data formats
- [ ] Include all error handling
- [ ] Add logging throughout
- [ ] Test against live data
- [ ] Verify dashboard compatibility

### Service Level:
- [ ] Fix collector initialization in service.py
- [ ] Add proper config passing to collectors
- [ ] Implement retry logic
- [ ] Add health check endpoints
- [ ] Create systemd service file

## Risk Mitigation

1. **Data Format Changes**: Keep exact column names and types
2. **Missing Data**: Implement backfill immediately after basic updates work
3. **Performance**: Monitor file sizes, implement retention if needed
4. **Reliability**: Each collector fails independently

## Success Criteria

1. All 4 data types updating successfully
2. Dashboard shows all green status indicators
3. Data format 100% compatible with existing files
4. No data gaps after 24 hours of operation
5. Integrity checks pass consistently

## Next Steps

1. Start with PriceCollector implementation (simplest)
2. Test thoroughly before moving to next collector
3. Deploy incrementally - can run alongside existing updaters
4. Gradually transition from old to new updaters
5. Implement alerting once core functionality proven

## Notes on Existing Code Quality

The existing updaters have been running reliably for months with:
- Proven error handling
- Correct data formats
- Efficient update strategies

Our goal is to preserve this reliability while adding:
- Better monitoring (status dashboard)
- Unified service architecture
- Improved maintainability
- Enhanced alerting capabilities