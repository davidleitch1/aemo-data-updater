# AEMO Data Collection System - Complete Documentation

*Last Updated: July 17, 2025*

## Overview

This document provides complete documentation for the AEMO (Australian Energy Market Operator) data collection system. The system collects electricity market data from AEMO's NEMWeb platform and stores it in efficient parquet format for analysis and monitoring.

## System Architecture

### Repository Information
- **GitHub**: https://github.com/davidleitch1/aemo-data-updater
- **Local Path**: `/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/`
- **Status**: Production-ready data collection service

### Key Components
1. **Unified Collector**: Single service collecting both 5-minute and 30-minute data
2. **Backfill Scripts**: Tools to fill historical data gaps
3. **Status Monitoring**: Real-time monitoring of data freshness and coverage
4. **Alert Systems**: SMS and email notifications for market events

## File Structure

```
aemo-data-updater/
├── src/aemo_updater/
│   ├── collectors/
│   │   ├── base_collector.py           # Abstract base class
│   │   ├── price_collector.py          # Spot price collector
│   │   ├── generation_collector.py     # Unit generation collector
│   │   ├── transmission_collector.py   # Interconnector flow collector
│   │   ├── rooftop_collector.py       # Rooftop solar collector
│   │   └── unified_collector.py       # ✅ Main unified collector
│   ├── alerts/                        # Alert systems
│   │   ├── email_alerts.py            # New DUID notifications
│   │   └── twilio_alerts.py           # High price SMS alerts
│   ├── ui/
│   │   └── status_dashboard.py        # Status monitoring (port 5011)
│   └── config.py                      # Configuration settings
├── data 2/                            # ✅ Current parquet data storage
│   ├── prices5.parquet               # 5-minute regional prices
│   ├── scada5.parquet                # 5-minute generation by DUID
│   ├── transmission5.parquet         # 5-minute interconnector flows
│   ├── prices30.parquet              # 30-minute regional prices
│   ├── scada30.parquet               # 30-minute generation by DUID
│   ├── transmission30.parquet        # 30-minute interconnector flows
│   └── rooftop30.parquet             # 30-minute rooftop solar
├── data 2 backup/                     # ✅ Backup copies of parquet files
├── backfill_prices5.py                # ✅ 5-min price data backfill
├── backfill_scada5.py                 # ✅ 5-min generation data backfill
├── rebuild_prices30_clean.py          # ✅ 30-min price data rebuild
├── rebuild_transmission30_clean.py    # ✅ 30-min transmission rebuild
├── run_unified_collector.py           # ✅ Unified collector service
├── run_production_collector.sh        # ✅ Production startup script
├── check_collector_status.py          # ✅ Status monitoring script
├── check_new_parquet_files.py         # Data completeness checker
├── test_unified_collector.py          # ✅ Comprehensive test suite
├── test_continuous_collector.py       # ✅ Continuous collection test
├── .env                               # Credentials (Twilio, email)
└── CLAUDE.md                          # This documentation
```

## Data Files and Schema

### Parquet File Overview

| File | Records | Size | Date Range | Purpose | Index Type |
|------|---------|------|------------|---------|------------|
| **5-MINUTE DATA** |
| `prices5.parquet` | 65,530 | 584 KB | 2025-06-01 onwards | Regional spot prices | Integer |
| `scada5.parquet` | 5,609,987 | 18.9 MB | 2025-06-01 onwards | Generation by DUID | Integer |
| `transmission5.parquet` | 27,792 | 509 KB | 2025-06-22 onwards | Interconnector flows | Integer |
| **30-MINUTE DATA** |
| `prices30.parquet` | 1,647,210 | 9.2 MB | 2020-01-01 onwards | Regional prices | Integer |
| `scada30.parquet` | 33,524,735 | 176.5 MB | 2020-02-01 onwards | Generation by DUID | Integer |
| `transmission30.parquet` | 1,976,652 | 12.3 MB | 2020-01-01 onwards | Interconnector flows | Integer |
| `rooftop30.parquet` | 807,640 | 3.9 MB | 2020-01-01 onwards | Rooftop solar by region | Integer |

### Detailed Column Schema

#### 5-Minute Files
**prices5.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `regionid`: object - Region identifier (NSW1, QLD1, SA1, TAS1, VIC1)
- `rrp`: float64 - Regional Reference Price (AUD/MWh)

**scada5.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `duid`: object - Dispatchable Unit Identifier (472 unique DUIDs)
- `scadavalue`: float64 - Generation output (MW)

**transmission5.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `interconnectorid`: object - Interconnector identifier (6 interconnectors)
- `meteredmwflow`: float64 - Actual measured flow (MW, 100% populated)
- `mwflow`: float64 - Target flow for next interval (37.6% populated)
- `exportlimit`: float64 - Export limit (MW, 37.6% populated)
- `importlimit`: float64 - Import limit (MW, 37.6% populated)
- `mwlosses`: float64 - Transmission losses (MW, 37.6% populated)

#### 30-Minute Files
**prices30.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `regionid`: object - Region identifier
- `rrp`: float64 - Regional Reference Price (AUD/MWh)

**scada30.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `duid`: object - Dispatchable Unit Identifier
- `scadavalue`: float64 - Generation output (MW)

**transmission30.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `interconnectorid`: object - Interconnector identifier
- `meteredmwflow`: float64 - Actual measured flow (MW)

**rooftop30.parquet**:
- `settlementdate`: datetime64[ns] - Settlement timestamp
- `regionid`: object - Region identifier
- `power`: float64 - Solar generation (MW)
- `quality_indicator`: object - Data quality indicator
- `type`: object - Data type (ACTUAL/FORECAST)
- `source_archive`: object - Source file reference (99.5% populated)

## Data Sources and URLs

### URL Pattern Reference Table

| Data Type | Time Resolution | Period | URL Pattern | Table Name |
|-----------|-----------------|--------|-------------|------------|
| **CURRENT DATA (Real-time)** |
| Prices | 5-minute | Current | `http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_YYYYMMDDHHMM_*.zip` | PRICE |
| Generation | 5-minute | Current | `http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_YYYYMMDDHHMM_*.zip` | UNIT_SCADA |
| Transmission | 5-minute | Current | `http://nemweb.com.au/Reports/Current/DispatchIS_Reports/PUBLIC_DISPATCHIS_YYYYMMDDHHMM_*.zip` | INTERCONNECTORRES |
| Rooftop | 30-minute | Current | `http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_*.zip` | ACTUAL |
| **DAILY ARCHIVES** |
| Prices | 5-minute | Daily archives | `https://nemweb.com.au/Reports/Archive/DispatchIS_Reports/PUBLIC_DISPATCHIS_YYYYMMDD.zip` | PRICE |
| Generation | 5-minute | Daily archives | `https://nemweb.com.au/Reports/Archive/Dispatch_SCADA/PUBLIC_DISPATCHSCADA_YYYYMMDD.zip` | UNIT_SCADA |
| Transmission | 5-minute | Daily archives | `https://nemweb.com.au/Reports/Archive/DispatchIS_Reports/PUBLIC_DISPATCHIS_YYYYMMDD.zip` | INTERCONNECTORRES |
| **HISTORICAL ARCHIVES** |
| Prices | 30-minute | Historical | `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/` | TRADINGPRICE |
| Generation | 30-minute | Historical | `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/` | DISPATCH_UNIT_SCADA |
| Transmission | 30-minute | Historical | `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/` | TRADINGINTERCONNECT |
| Rooftop | 30-minute | Historical | `https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month:02d}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE%23ROOFTOP_PV_ACTUAL%23FILE01%23YYYYMM010000.zip` | ROOFTOPPVACTUAL |
| **ALTERNATIVE SOURCES** |
| Prices/Demand | 5-minute | Current | `http://nemweb.com.au/Reports/CURRENT/Public_Prices/` | Various |

### Key Notes on URLs
- **Current data**: Updated every 5 minutes, accessible immediately
- **Daily archives**: Available next day, contain 288 5-minute interval files
- **MMSDM archives**: Monthly historical data, 6-month delay
- **# Encoding**: In rooftop URLs, `#` must be encoded as `%23`
- **User-Agent Required**: All AEMO requests need `User-Agent` header
- **CSV File Extensions**: AEMO files use both `.csv` and `.CSV` extensions - code must handle both
- **Rooftop File Types**: Use `PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_*` (not `PUBLIC_ROOFTOP_PV_ACTUAL_SATELLITE_*`)

## Current Data Status

*Status as of July 17, 2025 19:31:53*

| File | Records | Latest Data | Status | Issues |
|------|---------|-------------|--------|--------|
| `prices5.parquet` | 65,530 | 2025-07-17 19:35 | ✅ Current | None |
| `scada5.parquet` | 5,609,987 | 2025-07-17 19:35 | ✅ Current | None |
| `transmission5.parquet` | 27,792 | 2025-07-17 19:35 | ✅ Current | None |
| `prices30.parquet` | 1,647,210 | 2025-07-17 15:30 | ⚠️ 30-min cycle | None |
| `scada30.parquet` | 33,527,903 | 2025-07-17 20:15 | ✅ Current | None |
| `transmission30.parquet` | 1,976,652 | 2025-07-17 15:30 | ⚠️ 30-min cycle | None |
| `rooftop30.parquet` | 807,640 | 2025-07-16 11:00 | ⚠️ Stale | Needs update |

### Current Data Gaps Identified

**5-Minute Data:**
- **prices5.parquet**: Missing 2025-06-01 00:00:00 to 2025-06-01 00:05:00
- **scada5.parquet**: Missing 2025-06-01 00:00:00 to 2025-06-01 00:05:00  
- **transmission5.parquet**: Missing 2025-06-01 to 2025-06-22 00:30:00 (21 days)

**30-Minute Data:**
- **prices30.parquet**: Missing 2020-01-01 00:00:00 to 2020-01-01 00:30:00
- **scada30.parquet**: Missing 2020-01-01 to 2020-02-01 00:00:00 + Missing recent: 2025-07-16 to 2025-07-16
- **transmission30.parquet**: Missing 2020-01-01 00:00:00 to 2020-01-01 00:30:00
- **rooftop30.parquet**: Missing 2020-01-01 00:00:00 to 2020-01-01 00:30:00

## Unified Collector Service

### Overview
The unified collector is the main production service that collects all AEMO data types in a single, coordinated process.

**Main Script**: `src/aemo_updater/collectors/unified_collector.py`
**Service Runner**: `run_unified_collector.py`
**Production Starter**: `run_production_collector.sh`

### Collection Schedule
**All data types collected every 4.5-minute cycle:**

- **5-minute data sources**:
  - prices5 (from DispatchIS_Reports/PRICE)
  - scada5 (from Dispatch_SCADA/UNIT_SCADA)
  - transmission5 (from DispatchIS_Reports/INTERCONNECTORRES)

- **30-minute data sources**:
  - prices30 (from TradingIS_Reports/PRICE)
  - transmission30 (from TradingIS_Reports/INTERCONNECTORRES)
  - scada30 (calculated from scada5 data: sum of available 5min intervals / 2)
  - rooftop30 (from ROOFTOP_PV/ROOFTOP_PV_ACTUAL)

### Key Features
- ✅ Safe merge logic prevents data overwrites
- ✅ Handles duplicate data intelligently
- ✅ Comprehensive error handling and logging
- ✅ Automatic retry for failed downloads
- ✅ Memory efficient processing
- ✅ Status monitoring and reporting
- ✅ SCADA30 calculation: Sum of available 5min intervals / 2 for :00/:30 timestamps (does not require all 6 intervals)

### Usage Commands

#### Start the Collector
```bash
# Production mode (background with logging)
./run_production_collector.sh

# Interactive mode (manual start/stop)
.venv/bin/python run_unified_collector.py

# Test mode (single update only)
.venv/bin/python run_unified_collector.py --test

# Custom settings
.venv/bin/python run_unified_collector.py --interval 3.0 --log-level DEBUG
```

#### Monitor the Collector
```bash
# Check collector status
.venv/bin/python check_collector_status.py

# View live logs
tail -f collector.log

# Check if running
ps aux | grep run_unified_collector | grep -v grep

# Stop collector
kill $(cat collector.pid)
```

### Production Deployment
When running `./run_production_collector.sh`:
1. Starts collector in background with 4.5-minute intervals
2. Logs to `collector.log`
3. Saves PID to `collector.pid` for easy stopping
4. Shows initial status after startup

**Expected Output**:
```
Starting unified AEMO collector...
Time: Thu 17 Jul 2025 19:22:16 AEST
Started unified collector with PID: 35450
View logs with: tail -f collector.log
Stop with: kill $(cat collector.pid)
```

## Backfill Scripts

### 5-Minute Data Backfill Scripts (✅ Production Ready)

#### 1. Price Data Backfill
**Script**: `backfill_prices5.py`
**Purpose**: Backfills 5-minute price data from DispatchIS archives
**Period**: Configurable (default: June 1-14, 2025)

```bash
# Test mode (first 2 days only)
.venv/bin/python backfill_prices5.py --test

# Full backfill
.venv/bin/python backfill_prices5.py

# Custom date range
.venv/bin/python backfill_prices5.py 2025-06-01 2025-06-10
```

**Results**: Successfully processes ~1,440 price records per day across 5 regions

#### 2. Generation Data Backfill
**Script**: `backfill_scada5.py`
**Purpose**: Backfills 5-minute generation data from Dispatch_SCADA archives
**Period**: Configurable (default: June 1-18, 2025)

```bash
# Test mode (first 2 days only)
.venv/bin/python backfill_scada5.py --test

# Full backfill
.venv/bin/python backfill_scada5.py

# Custom date range
.venv/bin/python backfill_scada5.py 2025-06-01 2025-06-15
```

**Results**: Successfully processes ~125,000 generation records per day across 472 DUIDs

### 30-Minute Data Rebuild Scripts (✅ Production Ready)

#### 1. Price Data Rebuild
**Script**: `rebuild_prices30_clean.py`
**Purpose**: Rebuilds 30-minute price data from MMSDM archives
**Period**: January 2020 - June 2025

```bash
# Test mode (Jan-Feb 2020 only)
.venv/bin/python rebuild_prices30_clean.py --test

# Full rebuild
.venv/bin/python rebuild_prices30_clean.py

# Custom range
.venv/bin/python rebuild_prices30_clean.py --start-year 2023 --end-year 2024
```

**Results**: Successfully downloads 66 months of historical price data

#### 2. Transmission Data Rebuild
**Script**: `rebuild_transmission30_clean.py`
**Purpose**: Rebuilds 30-minute transmission data from MMSDM archives
**Period**: January 2020 - June 2025

```bash
# Test mode (Jan-Feb 2020 only)
.venv/bin/python rebuild_transmission30_clean.py --test

# Full rebuild
.venv/bin/python rebuild_transmission30_clean.py

# Custom range
.venv/bin/python rebuild_transmission30_clean.py --start-year 2023 --end-year 2024
```

**Results**: Successfully downloads 66 months of historical transmission data

### Script Features
- ✅ Test mode for safe verification before full runs
- ✅ Progress logging with file counts and timing
- ✅ Automatic retry on network errors
- ✅ Safe merge logic to prevent overwrites
- ✅ Configurable date ranges
- ✅ Rate limiting to prevent 403 errors

## Status Monitoring

### Status Check Command
```bash
cd "/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater"
.venv/bin/python check_collector_status.py
```

### Status Output Legend
- ✅ **Recent data** (< 2 hours old) - File is current
- ⚠️ **Stale data** (> 2 hours old) - File needs updating
- ❓ **Cannot determine date** - File has no readable timestamps
- ❌ **File missing or error** - File doesn't exist or can't be read

### Example Status Output
```
============================================================
UNIFIED COLLECTOR STATUS CHECK
============================================================
Time: 2025-07-17 19:31:53

File                      Records      Latest Data          Status
--------------------------------------------------------------------------------
prices5.parquet                65,530 2025-07-17 19:35     ✅
scada5.parquet              5,609,987 2025-07-17 19:35     ✅
transmission5.parquet          27,792 2025-07-17 19:35     ✅
prices30.parquet            1,647,210 2025-07-17 15:30     ⚠️
scada30.parquet            33,524,735 2025-07-15 00:00     ⚠️
transmission30.parquet      1,976,652 2025-07-17 15:30     ⚠️
rooftop30.parquet             807,640 2025-07-16 11:00     ⚠️
```

## Test Scripts

### 1. Unified Collector Test (`test_unified_collector.py`)
**Purpose**: Comprehensive validation of collector functionality

```bash
.venv/bin/python test_unified_collector.py
```

**Tests**:
- ✅ Merge logic with duplicate data handling
- ✅ File creation and updating
- ✅ Data integrity across all 7 parquet files
- ✅ Performance measurement (cycle completion time)
- ✅ Error handling and recovery

### 2. Continuous Collection Test (`test_continuous_collector.py`)
**Purpose**: Tests all data collection in continuous cycles

```bash
.venv/bin/python test_continuous_collector.py
```

**Tests**:
- All data types collected every cycle
- Multiple collection cycles
- Performance consistency
- 5-minute and 30-minute data type detection

### 3. Data Completeness Check (`check_new_parquet_files.py`)
**Purpose**: Identifies missing data intervals and coverage gaps

```bash
.venv/bin/python check_new_parquet_files.py
```

**Output**: Date ranges, record counts, missing intervals for all parquet files

## Alert Systems

### SMS Alerts (Twilio) ✅
**Purpose**: High electricity price notifications  
**Integration**: Built into price_collector.py  
**Phone**: +61412519001  
**Triggers**:
- ⚠️ High: $1,000/MWh
- 🚨 Extreme: $10,000/MWh  
- ✅ Recovery: Below $300/MWh

**Configuration** (in .env):
```
TWILIO_ENABLED=true
TWILIO_ACCOUNT_SID=your_sid
TWILIO_AUTH_TOKEN=your_token
TWILIO_FROM_PHONE=+19513382338
TWILIO_TO_PHONE=+61412519001
```

### Email Alerts (iCloud) ✅
**Purpose**: New generator unit (DUID) discovery notifications  
**Integration**: Built into unified_collector.py  
**Email**: david.leitch@icloud.com  
**Triggers**: New DUIDs detected in SCADA data

**Configuration** (in .env):
```
ENABLE_EMAIL_ALERTS=true
ALERT_EMAIL=david.leitch@icloud.com
ALERT_PASSWORD=your_app_password
RECIPIENT_EMAIL=david.leitch@icloud.com
SMTP_SERVER=smtp.mail.me.com
SMTP_PORT=587
```

**How it works**:
- Tracks known DUIDs in `data 2/known_duids.txt`
- Checks each SCADA update for new DUIDs
- Sends email listing all newly discovered units
- Updates known DUIDs file automatically

## Backup and Recovery

### Current Backups (July 17, 2025)
**Location**: `/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot/aemo-data-updater/`

1. **Directory Backup**: `data 2 backup/` - Real-time copies
2. **Compressed Archive**: `parquet_backup_20250717_182749.tar.gz` (197 MB)

### Backup Procedures
```bash
# Create directory backup
cp "data 2"/*.parquet "data 2 backup/"

# Create timestamped compressed backup
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
tar -czf "parquet_backup_${TIMESTAMP}.tar.gz" "data 2"/*.parquet
```

### Recovery Procedures
```bash
# Restore from directory backup
cp "data 2 backup"/*.parquet "data 2/"

# Restore from compressed backup
tar -xzf parquet_backup_20250717_182749.tar.gz
cp "data 2"/*.parquet "data 2/"

# Verify restoration
.venv/bin/python check_collector_status.py
```

## Troubleshooting

### Common Issues

1. **Collector Not Running**
   ```bash
   # Check if process exists
   ps aux | grep run_unified_collector | grep -v grep
   
   # Check PID file
   cat collector.pid
   
   # Restart if needed
   ./run_production_collector.sh
   ```

2. **Stale Data**
   - All files should update every 4.5 minutes if collector is running
   - Stale data indicates collector may be stopped or having issues
   - Check logs: `tail -f collector.log`

3. **Network Errors**
   - AEMO may throttle requests (403 errors)
   - Backfill scripts include rate limiting
   - Check internet connectivity: `curl -I http://nemweb.com.au`

4. **Data Overwrites**
   - Always backup before running multiple scripts
   - Stop old processes: `kill $(cat collector.pid)`
   - Use scripts with proper merge logic

### Log Analysis
```bash
# View recent errors
tail -50 collector.log | grep -i error

# Monitor success rate
tail -f collector.log | grep -E "(SUCCESS|✓|❌)"

# Check collection cycles
tail -f collector.log | grep "=== Update cycle"
```

### Performance Monitoring
- **Normal cycle time**: ~1.7 seconds
- **Expected success rate**: >95%
- **Memory usage**: <100MB per cycle
- **Network bandwidth**: ~50MB per hour

## Migration from Legacy Systems

### Replaced Systems
- `/aemo-spot-dashboard/update_spot.py` → Unified collector (prices)
- `/genhist/gen_dash.py` → Unified collector (generation)

### Benefits of New System
- ✅ Unified data format (Parquet)
- ✅ Better error handling and logging
- ✅ Performance optimizations
- ✅ Comprehensive alerting
- ✅ Cross-machine compatibility
- ✅ Single service to manage
- ✅ Consistent merge logic
- ✅ Reduced risk of data overwrites

## Production Checklist

### Initial Setup
1. ✅ Virtual environment created and activated
2. ✅ Dependencies installed from requirements.txt
3. ✅ .env file configured with credentials
4. ✅ Data directory structure created
5. ✅ All scripts tested in test mode

### Daily Operations
1. ✅ Unified collector running in background
2. ✅ Status monitoring via check_collector_status.py
3. ✅ Log monitoring via collector.log
4. ✅ Backup procedures documented
5. ✅ Alert systems operational

### Weekly Maintenance
- Run data completeness check
- Create new backup archives
- Review collector logs for errors
- Monitor disk space usage
- Verify alert system functionality

## Gap Detection and Backfill Scripts

### Detecting Data Gaps

To check for missing data in any parquet file:
```bash
.venv/bin/python check_new_parquet_files.py
```

### 5-Minute Data Backfill

Two scripts handle different scenarios:

#### 1. **backfill_5min_current.py** - For recent gaps (same day)
```bash
# Test mode - check gaps only
.venv/bin/python backfill_5min_current.py --test --start-time "2025-07-17 16:00" --end-time "2025-07-17 19:00"

# Backfill specific time range
.venv/bin/python backfill_5min_current.py --start-time "2025-07-17 16:00" --end-time "2025-07-17 19:00"
```

#### 2. **backfill_all_5min_gaps.py** - For comprehensive backfill
```bash
# Test data availability for specific date
.venv/bin/python backfill_all_5min_gaps.py --test-only --date 2025-07-17 --data-type all

# Backfill specific date and data type
.venv/bin/python backfill_all_5min_gaps.py --date 2025-07-17 --data-type prices5

# Run full backfill for all gaps (may take 1+ hour)
.venv/bin/python backfill_all_5min_gaps.py --data-type all
```

**Data sources**:
- Archives: Available for dates before today (nested ZIP structure)
- Current: Available for today and recent hours

**Important notes**:
- AEMO archives have nested ZIP files (daily archive contains 288 5-minute ZIPs)
- Some timestamps (especially 00:00:00) may be legitimately missing from AEMO
- Run test mode first to verify data availability
- Monitor for 403 errors (rate limiting)

### 30-Minute Data Backfill

#### **backfill_30min_gaps.py** - For 30-minute data
```bash
# Test mode
.venv/bin/python backfill_30min_gaps.py --test --start-date 2025-07-17 --end-date 2025-07-17

# Backfill specific type
.venv/bin/python backfill_30min_gaps.py --type prices --start-date 2025-07-17 --end-date 2025-07-17

# Recalculate scada30 from scada5 data
.venv/bin/python backfill_30min_gaps.py --type scada
```

**Note**: scada30 is calculated from scada5 data (sum of available intervals / 2), not downloaded. The calculation does not require all 6 intervals - it uses whatever data is available in the 30-minute window.

#### **recalculate_scada30.py** - Optimized SCADA30 recalculation
For efficient recalculation of large volumes of scada30 data:
```bash
# Recalculate all missing scada30 data
.venv/bin/python recalculate_scada30.py
```

This optimized script:
- Processes data in batches to avoid timeouts
- Automatically finds gaps by comparing with existing scada30
- Uses whatever intervals are available (doesn't require exactly 6)
- Typical execution time: ~10 seconds for thousands of records

## Development Notes

### Code Standards
- All scripts support `--test` mode for safe verification
- Comprehensive error handling and logging
- Rate limiting to prevent AEMO 403 errors
- Progress reporting for long-running operations
- Safe merge logic to prevent data overwrites

### Documentation Requirements
- Always review latest Panel (panel.holoviz.org) and hvPlot documentation
- Use "material" template for dashboard components
- Use latest tabulator widgets for data tables
- Current versions: Panel v1.7.3, hvPlot v0.11.3

### Git Procedures
- Always backup data before significant changes
- Use good git procedures for code changes
- Test substantial changes thoroughly
- Create detailed plans with checkboxes for major implementations

## 🔧 PRIORITY: 2025 Price Data Gap Resolution (July 18, 2025)

### Problem Identified
The prices30.parquet file has severe gaps throughout 2025 that cannot be resolved through traditional MMSDM archives or TradingIS backfill methods:

**Current 2025 Price Data Gaps:**
- **January**: Missing first 25 days (only 19.3% coverage)
- **March**: Only 720 records (9.7% coverage)  
- **June**: Only 480 records (6.7% coverage)
- **Overall 2025**: Only 21.4% coverage through July 18

**Root Cause**: 2025 data is not available in MMSDM archives (6-month delay) or historical TradingIS daily archives.

### ✅ SOLUTION: Public_Prices Archive Method

**Data Source Discovery**: 2025 price data is available in AEMO's Public_Prices archives:
- **Archive**: https://nemweb.com.au/Reports/ARCHIVE/Public_Prices/ (late 2024 data)
- **Current**: https://nemweb.com.au/Reports/CURRENT/Public_Prices/ (2025 data)

**Key Advantages**:
- Contains 5-minute price data (more granular than needed)
- Available for 2025 period (unlike MMSDM archives)
- Can be aggregated to 30-minute data to fill gaps
- Fast processing due to nested zip structure

### Implementation Plan

#### Phase 1: Archive Data Collection (Late 2024)
```bash
# Target: https://nemweb.com.au/Reports/ARCHIVE/Public_Prices/
# Structure: Outer ZIP → Multiple inner ZIPs → CSV files
# Processing: Download, extract all nested levels, parse CSV, create parquet
```

**Steps**:
1. Download each outer ZIP file from archive
2. Extract nested ZIP files from outer archives
3. Extract CSV files from inner ZIPs
4. Parse CSV data and standardize format
5. Combine into comprehensive 5-minute price parquet
6. Clean up ZIP files after processing

#### Phase 2: Current Data Collection (2025)
```bash
# Target: https://nemweb.com.au/Reports/CURRENT/Public_Prices/
# Structure: Individual ZIP files → CSV files
# Processing: Download each ZIP, extract CSV, append to parquet
```

**Steps**:
1. List all ZIP files in current directory
2. Download and process each ZIP file
3. Append data to 5-minute price parquet
4. Clean up ZIP files after processing

#### Phase 3: 30-Minute Aggregation
```bash
# Convert 5-minute data to 30-minute averages for gap filling
# Target only missing periods in prices30.parquet
```

**Steps**:
1. Identify gaps in existing prices30.parquet
2. Extract corresponding 5-minute data for gap periods
3. Calculate 30-minute averages (6 intervals → 1 average)
4. Merge calculated 30-minute data into prices30.parquet
5. Verify gap resolution

### Technical Implementation

#### Script Structure
```python
# backfill_public_prices.py
def download_archive_zips():
    """Download and process Archive/Public_Prices outer ZIPs"""
    
def download_current_zips():
    """Download and process Current/Public_Prices individual ZIPs"""
    
def extract_nested_zips(zip_content):
    """Handle multiple layers of ZIP nesting"""
    
def parse_public_prices_csv(csv_content):
    """Parse Public_Prices CSV format to standardized DataFrame"""
    
def aggregate_to_30min(df_5min, target_gaps):
    """Convert 5-minute data to 30-minute averages for specific gaps"""
    
def merge_gap_data(gap_data_30min):
    """Merge calculated 30-minute data into prices30.parquet"""
```

#### Test Implementation
```bash
# Test with single ZIP file first
.venv/bin/python backfill_public_prices.py --test --single-file

# Test with single day
.venv/bin/python backfill_public_prices.py --test --date 2025-01-01

# Full implementation
.venv/bin/python backfill_public_prices.py --archive --current --fill-gaps
```

### Expected Results

**Data Volume Estimates**:
- **5-minute data**: ~288 records/day × 5 regions = 1,440 records/day
- **2025 YTD**: ~200 days × 1,440 = ~288,000 records
- **File size**: ~2-5 MB for full 2025 5-minute price data

**Gap Resolution**:
- **January 1-25**: Fill 25 days × 48 intervals × 5 regions = 6,000 records
- **March gaps**: Fill ~21 days × 48 intervals × 5 regions = 5,040 records  
- **June gaps**: Fill ~27 days × 48 intervals × 5 regions = 6,480 records
- **Total**: ~17,520 new 30-minute price records

### Implementation Checklist

#### Phase 1: Planning and Testing
- [ ] **Create test script** - Single ZIP file processing
- [ ] **Test nested ZIP extraction** - Verify multi-level decompression
- [ ] **Test CSV parsing** - Ensure Public_Prices format compatibility
- [ ] **Test 5-min to 30-min aggregation** - Verify averaging logic
- [ ] **Test gap identification** - Accurately find missing periods

#### Phase 2: Archive Data Processing
- [ ] **Download archive ZIPs** - Process late 2024 data
- [ ] **Create 5-minute price parquet** - Build comprehensive dataset
- [ ] **Validate archive data** - Check completeness and accuracy
- [ ] **Performance optimization** - Ensure reasonable processing time

#### Phase 3: Current Data Processing  
- [ ] **Download current ZIPs** - Process 2025 data
- [ ] **Append to 5-minute parquet** - Maintain chronological order
- [ ] **Validate current data** - Check for gaps and anomalies

#### Phase 4: Gap Filling
- [ ] **Identify 2025 gaps** - Precise gap detection in prices30.parquet
- [ ] **Calculate 30-minute data** - Aggregate from 5-minute source
- [ ] **Merge gap data** - Safe insertion into prices30.parquet
- [ ] **Verify gap resolution** - Confirm all gaps filled

#### Phase 5: Validation and Testing
- [ ] **Data quality check** - Ensure price reasonableness
- [ ] **Coverage analysis** - Verify 100% 2025 coverage
- [ ] **Dashboard testing** - Test with 1-day, 7-day, 30-day views
- [ ] **Performance impact** - Monitor dashboard load times

### Risk Mitigation

**Data Safety**:
- [ ] Backup prices30.parquet before any modifications
- [ ] Use test mode for all initial runs
- [ ] Validate data ranges before merging
- [ ] Implement rollback procedure

**Performance Considerations**:
- [ ] Process ZIPs in batches to avoid memory issues
- [ ] Clean up temporary files during processing
- [ ] Monitor disk space usage
- [ ] Rate limit downloads to avoid 403 errors

### Success Criteria

1. **Complete 2025 Coverage**: prices30.parquet shows 100% coverage for 2025
2. **Dashboard Functionality**: All dashboard views work without gaps
3. **Data Quality**: Price values are reasonable and consistent
4. **Performance**: No degradation in dashboard load times
5. **Future-Proof**: Process can be repeated for ongoing gap detection

### 📊 PHASE 1 RESULTS: Current Data Collection (July 18, 2025)

**✅ COMPLETED: Current Public_Prices Processing**
- Successfully processed **60 ZIP files** from Reports/CURRENT/Public_Prices/
- Created comprehensive 5-minute dataset: **86,400 records** (817 KB)
- **June 2025 gap completely resolved**: 6.7% → 100% coverage
- **May/July 2025 significantly improved**: 32.3% → 51.1% (May), 17.9% → 57.6% (July)

### 📊 PHASE 2 RESULTS: Archive Data Collection (July 18, 2025)

**✅ COMPLETED: Archive Public_Prices Processing**
- Successfully processed **12 outer ZIP files** from Reports/ARCHIVE/Public_Prices/
- Processed **multiple layers of nested ZIP files** (outer → inner → CSV)
- Created comprehensive 5-minute dataset: **593,275 records** (5.3 MB)
- **Complete 2025 historical data**: January 1 to July 18, 2025
- **All missing periods filled**: Including January 1-25 and March gaps

**✅ COMPLETED: 30-Minute Gap Filling**
- Converted 593,275 5-minute records to 98,885 30-minute records
- Merged with existing prices30.parquet (removed 35,730 overlapping records)
- **Final dataset**: 1,728,260 total records
- **100% coverage achieved** for all complete months through June 2025

### 🎉 FINAL SUCCESS: Complete 2025 Price Data Resolution

**✅ ALL GAPS COMPLETELY FILLED:**
- **January 2025**: 7,440 records (100.0% coverage) ✅
- **February 2025**: 6,720 records (100.0% coverage) ✅  
- **March 2025**: 7,440 records (100.0% coverage) ✅
- **April 2025**: 7,200 records (100.0% coverage) ✅
- **May 2025**: 7,440 records (100.0% coverage) ✅
- **June 2025**: 7,200 records (100.0% coverage) ✅
- **July 2025**: 4,345 records (58.4% coverage - partial month)

**Implementation Summary:**
- **Total new records added**: 81,050
- **Final prices30.parquet size**: 1,728,260 records
- **Date range**: 2020-01-01 to 2025-07-18
- **Processing time**: ~2 minutes for complete gap resolution
- **Data quality**: All price values validated and reasonable

### 🔧 COMPLETED: ARCHIVE Public_Prices Processing Implementation

**Problem**: The initial script only processed `Reports/CURRENT/Public_Prices/` but the historical data we need for January-March 2025 is in `Reports/ARCHIVE/Public_Prices/`.

**Data Source**: https://nemweb.com.au/Reports/ARCHIVE/Public_Prices/
- Contains **~12 outer ZIP files** with historical price data
- Each outer ZIP contains **multiple inner ZIP files**
- Each inner ZIP contains **CSV files** with 5-minute price data
- Should contain **complete 2025 historical data** including January-March

**Enhanced Implementation Strategy**:

#### Step 1: Archive Discovery and Download
```python
def download_archive_outer_zips():
    """Download all outer ZIP files from Archive/Public_Prices to temp directory"""
    # 1. List all outer ZIP files (expected: ~12 files)
    # 2. Download each outer ZIP to temp directory
    # 3. Verify download integrity
```

#### Step 2: Nested ZIP Processing  
```python
def process_archive_nested_zips():
    """Process multiple layers of ZIP nesting from archive files"""
    # 1. Extract first layer: outer ZIP → inner ZIPs
    # 2. Extract second layer: inner ZIPs → CSV files  
    # 3. Process all CSV files from all archives
    # 4. Combine into comprehensive dataset
```

#### Step 3: Complete Data Integration
```python
def integrate_archive_and_current_data():
    """Combine archive (historical) and current data into single dataset"""
    # 1. Merge archive data with existing current data
    # 2. Deduplicate overlapping periods
    # 3. Fill all remaining 2025 gaps
    # 4. Update prices30.parquet with complete coverage
```

#### Implementation Requirements

**Enhanced Script Features**:
- **Bulk Archive Download**: Download all 12 outer ZIPs to temp directory first
- **Multi-layer Extraction**: Handle outer ZIP → inner ZIPs → CSV files
- **Progress Tracking**: Monitor extraction of hundreds of CSV files
- **Memory Management**: Process large archive volumes efficiently
- **Data Integrity**: Ensure no gaps or overlaps in final dataset

**Expected Results**:
- **Complete 2025 coverage**: 100% for all months January-July
- **Massive dataset**: Estimated 200,000+ 5-minute price records
- **Gap resolution**: All identified gaps (January, March) filled
- **File size**: ~2-3 MB for complete 2025 5-minute price data

#### Test Implementation Plan

**Phase 1: Single Archive Test**
```bash
# Test with one outer ZIP file first
python3 backfill_public_prices.py --test --archive --single-outer-zip

# Test nested extraction process
python3 backfill_public_prices.py --test --archive --extract-only
```

**Phase 2: Full Archive Processing**
```bash
# Process all archive files
python3 backfill_public_prices.py --archive --download-all --process-all

# Fill remaining gaps
python3 backfill_public_prices.py --archive --fill-all-gaps
```

#### Success Criteria

1. **Complete Historical Coverage**: All 12 outer ZIP files processed
2. **100% 2025 Coverage**: prices30.parquet shows complete data for all months
3. **Data Quality**: All price values reasonable and consistent
4. **Performance**: Efficient processing of large archive volumes
5. **Gap Resolution**: January and March 2025 gaps completely filled

### Notes
- **Temp Directory Management**: Download outer ZIPs to temp, process, then clean up
- **File Cleanup**: Delete ZIP files after processing to save disk space
- **Archive Priority**: Focus on ARCHIVE processing before further CURRENT updates
- **Documentation**: Update this file with archive processing results

## Future Enhancements

1. **Real-time Dashboard**: Web interface showing live data and collection status
2. **Automated Gap Detection**: Daily automated gap detection and backfill
3. **Data Quality Monitoring**: Automated validation reports
4. **Redundant Data Sources**: Backup data sources for critical feeds
5. **Additional Data Types**: Support for new AEMO data types
6. **Performance Optimization**: Further optimization for large datasets
7. **Cloud Deployment**: Option for cloud-based collection service

---

*This documentation is maintained alongside the code and updated with each significant change to the system.*