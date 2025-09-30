# AEMO Data Updater

Standalone service for collecting Australian Energy Market Operator (AEMO) data from NEMWEB.

## Features

- **Automated Data Collection**: Downloads generation, price, transmission, and rooftop solar data every 5 minutes
- **Status Dashboard**: Web-based UI for monitoring service health and data integrity
- **Data Integrity Checks**: Identifies gaps and data quality issues
- **Backfill Capability**: Fill historical data gaps from AEMO archives
- **Robust Error Handling**: Continues operation even if individual collectors fail

## Installation

```bash
# Clone the repository
cd aemo-data-updater

# Create virtual environment with UV
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .
```

## Configuration

Create a `.env` file in the project root:

```bash
# Data storage path
AEMO_DATA_PATH=/path/to/data/storage

# Update interval (minutes)
UPDATE_INTERVAL_MINUTES=4.5

# Status UI settings
STATUS_UI_PORT=5011
STATUS_UI_HOST=localhost

# Logging
LOG_LEVEL=INFO

# Optional email alerts
ENABLE_EMAIL_ALERTS=false
ALERT_EMAIL=your-email@example.com
ALERT_PASSWORD=your-app-password
RECIPIENT_EMAIL=alerts@example.com
```

## Usage

### Run the Data Collection Service

```bash
# Start the service
python -m aemo_updater service

# Or use the installed script
aemo-updater service
```

### Run the Status Dashboard

```bash
# Start the monitoring UI
python -m aemo_updater ui

# Or use the installed script
aemo-updater-ui

# Access at http://localhost:5011
```

## Status Dashboard Features

### Real-time Status Indicators
- Green: Data is up-to-date and healthy
- Orange: Data is stale or has minor issues  
- Red: Critical issues or missing data

### Control Panel
- **Start/Stop Service**: Control the data collection service
- **Force Update**: Trigger immediate data collection
- **Clear Logs**: Clear the activity log

### Backfill Controls
- Select data source (All, Generation, Price, Transmission, Rooftop)
- Choose date range for backfill
- Run backfill with single click

### Data Integrity Check
- Analyzes all parquet files for:
  - Missing time periods
  - Data quality issues
  - File consistency
- Provides recommendations for fixing issues

## Data Files

The updater maintains these parquet files:

| File | Description | Update Frequency |
|------|-------------|------------------|
| `gen_output.parquet` | Generation by DUID | 5 minutes |
| `spot_hist.parquet` | Regional spot prices | 5 minutes |
| `transmission_flows.parquet` | Interconnector flows | 5 minutes |
| `rooftop_solar.parquet` | Distributed PV generation | 30 minutes |

## Architecture

```
┌─────────────────┐
│   NEMWEB Data   │
│    Sources      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AEMO Updater   │
│    Service      │
├─────────────────┤
│ • Generation    │
│ • Price         │
│ • Transmission  │
│ • Rooftop Solar │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Parquet Files  │
│  (Shared with   │
│   Dashboard)    │
└─────────────────┘
```

## Troubleshooting

### 403 Forbidden Errors
- The service includes required User-Agent headers
- Check if NEMWEB URLs have changed

### Missing Data
1. Check status dashboard for issues
2. Run integrity check to identify gaps
3. Use backfill to retrieve historical data

### Service Won't Start
- Check logs in `logs/aemo_updater.log`
- Verify all required directories exist
- Ensure no other instance is running

## Development

### Running Tests
```bash
pytest tests/
```

### Code Style
```bash
# Format code
black src/

# Lint
ruff src/
```

## License

[Add your license here]