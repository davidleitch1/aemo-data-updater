"""
Configuration for AEMO Data Updater
Central configuration for all data collection and storage settings
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base paths
BASE_PATH = Path(os.getenv('AEMO_DATA_PATH', '/Users/davidleitch/Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot'))
PROJECT_ROOT = Path(__file__).parent.parent.parent
LOG_PATH = PROJECT_ROOT / 'logs'
DATA_PATH = BASE_PATH  # Parquet files stored in main AEMO_spot directory

# Ensure directories exist
LOG_PATH.mkdir(exist_ok=True)

# Data file locations (shared with dashboard)
PARQUET_FILES = {
    'generation': {
        'path': DATA_PATH / 'aemo-energy-dashboard' / 'data' / 'gen_output.parquet',
        'description': 'Generation SCADA data',
        'update_interval': 300,  # 5 minutes
        'retention_days': 30,
    },
    'price': {
        'path': DATA_PATH / 'aemo-energy-dashboard' / 'data' / 'spot_hist.parquet',
        'description': 'Spot price data',
        'update_interval': 300,
        'retention_days': 365,
    },
    'transmission': {
        'path': DATA_PATH / 'transmission_flows.parquet',
        'description': 'Interconnector flow data',
        'update_interval': 300,
        'retention_days': 30,
    },
    'rooftop': {
        'path': DATA_PATH / 'rooftop_solar.parquet',
        'description': 'Rooftop solar generation',
        'update_interval': 1800,  # 30 minutes
        'retention_days': 30,
    },
}

# NEMWEB URLs
NEMWEB_URLS = {
    'generation': {
        'current': 'http://nemweb.com.au/Reports/CURRENT/Dispatch_SCADA/',
        'archive': 'http://nemweb.com.au/Reports/ARCHIVE/Dispatch_SCADA/',
        'file_pattern': r'PUBLIC_DISPATCHSCADA_\d{12}_\w+\.zip',
    },
    'price': {
        'current': 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/',
        'archive': 'http://nemweb.com.au/Reports/ARCHIVE/DispatchIS_Reports/',
        'file_pattern': r'PUBLIC_DISPATCHIS_\d{12}_\w+\.zip',
    },
    'transmission': {
        'current': 'http://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/',
        'archive': 'https://www.nemweb.com.au/REPORTS/ARCHIVE/DispatchIS_Reports/',
        'file_pattern': r'PUBLIC_DISPATCHIS_\d{12}_\w+\.zip',
        'archive_pattern': r'PUBLIC_DISPATCHIS_\d{8}\.zip',  # Daily archives
    },
    'rooftop': {
        'current': 'http://nemweb.com.au/Reports/Current/ROOFTOP_PV/ACTUAL/',
        'archive': 'http://nemweb.com.au/Reports/Archive/ROOFTOP_PV/ACTUAL/',
        'file_pattern': r'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_\d+_\w+\.zip',
    },
}

# Update settings
UPDATE_INTERVAL_SECONDS = int(os.getenv('UPDATE_INTERVAL_MINUTES', '4.5')) * 60
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds
REQUEST_TIMEOUT = 60  # seconds

# HTTP headers (required for NEMWEB)
HTTP_HEADERS = {
    'User-Agent': 'AEMO Dashboard Data Collector',
    'Accept': 'application/zip, text/html',
}

# Logging configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = LOG_PATH / 'aemo_updater.log'

# Status UI configuration
STATUS_UI_PORT = int(os.getenv('STATUS_UI_PORT', '5011'))
STATUS_UI_HOST = os.getenv('STATUS_UI_HOST', 'localhost')

# Email alerts (optional)
ENABLE_EMAIL_ALERTS = os.getenv('ENABLE_EMAIL_ALERTS', 'false').lower() == 'true'
ALERT_EMAIL = os.getenv('ALERT_EMAIL', '')
ALERT_PASSWORD = os.getenv('ALERT_PASSWORD', '')
RECIPIENT_EMAIL = os.getenv('RECIPIENT_EMAIL', '')
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))

# Data quality thresholds
QUALITY_THRESHOLDS = {
    'max_age_minutes': 15,  # Data older than this is considered stale
    'min_records_per_update': {
        'generation': 100,  # Expect ~500+ DUIDs
        'price': 5,         # 5 regions
        'transmission': 6,  # 6 main interconnectors
        'rooftop': 5,       # 5 regions
    },
}

# Backfill settings
MAX_BACKFILL_DAYS = 30  # Maximum days to backfill at once
BACKFILL_CHUNK_SIZE = 7  # Process in weekly chunks