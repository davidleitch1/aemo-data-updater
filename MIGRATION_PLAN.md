# AEMO Data Updater Migration Plan

## Overview

This document outlines the plan to separate the AEMO data updater from the dashboard into its own repository while maintaining continuous data collection.

## Current State Analysis

### Running Service
- **Process**: `aemo-combined-update` running since Friday 3PM
- **Location**: `/Users/davidleitch/miniforge3/bin/aemo-combined-update`
- **Must Not Interrupt**: This service is actively collecting 5-minute data

### Files to Migrate

#### Core Update Modules
1. `/src/aemo_dashboard/combined/update_all.py` → `/src/aemo_updater/service.py`
2. `/src/aemo_dashboard/generation/update_generation.py` → `/src/aemo_updater/collectors/generation_collector.py`
3. `/src/aemo_dashboard/spot_prices/update_spot.py` → `/src/aemo_updater/collectors/price_collector.py`
4. `/src/aemo_dashboard/transmission/update_transmission.py` → `/src/aemo_updater/collectors/transmission_collector.py`
5. `/src/aemo_dashboard/transmission/backfill_transmission.py` → (integrate into transmission_collector.py)
6. `/src/aemo_dashboard/rooftop/update_rooftop.py` → `/src/aemo_updater/collectors/rooftop_collector.py`

#### Shared Components
1. `/src/aemo_dashboard/shared/config.py` → Adapt for updater-specific needs
2. `/src/aemo_dashboard/shared/logging_config.py` → Copy and simplify
3. `/src/aemo_dashboard/shared/email_alerts.py` → Copy if needed

#### Data Files (Reference Only - Not Moved)
- `gen_info.pkl` - DUID mappings (read-only reference)
- Parquet files remain in current locations

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
```

**Transmission (transmission_flows.parquet)**
```python
columns = ['settlementdate', 'interconnectorid', 'meteredmwflow', 
           'mwflow', 'exportlimit', 'importlimit', 'mwlosses']
```

**Rooftop Solar (rooftop_solar.parquet)**
```python
columns = ['settlementdate', 'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1', ...]
# 30-minute to 5-minute conversion algorithm must remain identical
```

### Dependencies Analysis

#### Python Packages (Already in pyproject.toml)
- pandas, numpy - Data processing
- requests, aiohttp - HTTP downloads
- beautifulsoup4 - HTML parsing
- pyarrow - Parquet file support
- python-dotenv - Environment configuration
- panel - Status UI

#### External Dependencies
- NEMWEB access (HTTP with User-Agent headers)
- File system access to parquet locations

## Migration Steps

### Phase 1: Preparation (Before Any Changes)

1. **Document Current State**
   - [x] Record running process details
   - [x] List all files to migrate
   - [x] Document data interfaces
   - [ ] Test current updater is working

2. **Create New Repository Structure**
   - [x] Create `aemo-data-updater` directory
   - [x] Set up pyproject.toml
   - [x] Create base architecture
   - [ ] Initialize git repository

### Phase 2: Code Migration

1. **Copy Update Modules**
   ```bash
   # Copy each updater module and adapt imports
   cp src/aemo_dashboard/generation/update_generation.py → new repo
   # Repeat for all updaters
   ```

2. **Key Code Adaptations**
   - Change imports from `aemo_dashboard.shared` to `aemo_updater`
   - Ensure file paths use config
   - Preserve exact data processing logic

3. **Critical Algorithms to Preserve**
   - Rooftop solar 30→5 minute conversion:
     ```python
     value = ((6-j)*current + j*next) / 6  # for j in 0..5
     ```
   - Transmission flow direction logic
   - DUID to region mapping

### Phase 3: Testing (Parallel Operation)

1. **Set Up Test Environment**
   ```bash
   cd aemo-data-updater
   uv venv
   source .venv/bin/activate
   uv pip install -e .
   ```

2. **Configure for Test**
   - Use different log file
   - Write to test parquet files initially
   - Run in dry-run mode

3. **Validation Tests**
   - Compare output with current updater
   - Verify data formats match exactly
   - Check all collectors work

### Phase 4: Deployment

1. **Create New Git Repository**
   ```bash
   cd aemo-data-updater
   git init
   git remote add origin https://github.com/davidleitch1/aemo-data-updater.git
   ```

2. **Switchover Plan**
   - Schedule during low activity (early morning)
   - Stop old updater
   - Start new updater
   - Verify data collection continues
   - Monitor for 24 hours

3. **Cleanup**
   - Remove update code from dashboard repo
   - Update dashboard dependencies
   - Archive old update scripts

## Risk Mitigation

### Data Loss Prevention
- Keep old updater running until new one proven
- Test with parallel operation first
- Have rollback plan ready

### Interface Compatibility
- No changes to parquet file formats
- Maintain exact column names
- Preserve data types

### Monitoring
- Watch log files during transition
- Check parquet file updates
- Verify no gaps in data

## Timeline

1. **Today**: Complete code migration (2-3 hours)
2. **Tomorrow**: Test parallel operation (1 day)
3. **Day 3**: Deploy and monitor (1 day)
4. **Day 4**: Clean up old code

## Checklist Before Switchover

- [ ] All collectors migrated and tested
- [ ] Status UI working
- [ ] Backfill functionality tested
- [ ] Data integrity checks passing
- [ ] Log files being created
- [ ] Parquet files updating correctly
- [ ] No import errors
- [ ] Documentation complete

## Post-Migration

### Dashboard Changes Needed
1. Remove all update-related code
2. Update imports to remove update modules
3. Verify dashboard still works with read-only access
4. Update CLAUDE_DASHBOARD.md

### Updater Repository
1. Set up GitHub Actions for CI/CD
2. Create release process
3. Document deployment procedures
4. Set up monitoring/alerts