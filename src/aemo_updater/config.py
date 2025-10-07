"""
Configuration for AEMO Data Updater
Central configuration for all data collection and storage settings
"""

import os
from pathlib import Path

# Load environment variables (with fallback if dotenv not available)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Warning: python-dotenv not available, using system environment variables only")

# Base paths
# Use Path.home() for cross-machine compatibility with iCloud
DEFAULT_AEMO_PATH = Path.home() / 'Library/Mobile Documents/com~apple~CloudDocs/snakeplay/AEMO_spot'
BASE_PATH = Path(os.getenv('AEMO_DATA_PATH', str(DEFAULT_AEMO_PATH)))
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
    'curtailment': {
        'path': DATA_PATH / 'aemo-energy-dashboard' / 'data' / 'curtailment5.parquet',
        'description': 'Wind/solar curtailment data',
        'update_interval': 300,  # 5 minutes
        'retention_days': 3650,  # Keep 10 years (effectively all data for long-term analysis)
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
UPDATE_INTERVAL_SECONDS = int(float(os.getenv('UPDATE_INTERVAL_MINUTES', '4.5')) * 60)
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds
REQUEST_TIMEOUT = 60  # seconds

# HTTP headers (required for NEMWEB)
# Use browser user-agent to avoid 406 errors
HTTP_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,application/zip,*/*;q=0.8',
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

# SMS alerts via Twilio (optional) - Uses same env vars as existing price alerts
ENABLE_SMS_ALERTS = os.getenv('ENABLE_SMS_ALERTS', 'false').lower() == 'true'
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID', '')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN', '')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER', '')  # Your Twilio phone number
MY_PHONE_NUMBER = os.getenv('MY_PHONE_NUMBER', '')          # Recipient phone number

# Data quality thresholds
QUALITY_THRESHOLDS = {
    'max_age_minutes': 30,  # Default: Data older than this is considered stale
    'max_age_minutes_by_type': {
        'generation': 30,     # 5-minute data, alert after 30 min
        'price': 30,          # 5-minute data, alert after 30 min
        'transmission': 30,   # 5-minute data, alert after 30 min
        'rooftop': 90,        # 30-minute data, alert after 90 min
    },
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


import logging
from types import SimpleNamespace


def get_logger(name: str) -> logging.Logger:
    """Get a configured logger instance"""
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        # Set up logging format
        formatter = logging.Formatter(LOG_FORMAT)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # File handler
        if not LOG_FILE.parent.exists():
            LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Set level
        logger.setLevel(getattr(logging, LOG_LEVEL.upper()))
    
    return logger


def get_config():
    """Get configuration as a namespace object for compatibility"""
    config = SimpleNamespace()
    
    # Data file paths
    config.gen_output_file = Path(os.getenv('GEN_OUTPUT_FILE', str(PARQUET_FILES['generation']['path'])))
    config.spot_hist_file = Path(os.getenv('SPOT_HIST_FILE', str(PARQUET_FILES['price']['path']))) 
    config.transmission_output_file = PARQUET_FILES['transmission']['path']
    config.rooftop_solar_file = PARQUET_FILES['rooftop']['path']
    
    # Update settings
    config.update_interval_minutes = float(os.getenv('UPDATE_INTERVAL_MINUTES', '4.5'))
    
    # Paths
    config.data_dir = DATA_PATH
    config.logs_dir = LOG_PATH
    
    # Email settings
    config.email_enabled = ENABLE_EMAIL_ALERTS
    config.alert_email = ALERT_EMAIL
    config.alert_password = ALERT_PASSWORD
    config.recipient_email = RECIPIENT_EMAIL
    config.smtp_server = SMTP_SERVER
    config.smtp_port = SMTP_PORT
    
    # SMS settings
    config.sms_enabled = ENABLE_SMS_ALERTS
    config.twilio_account_sid = TWILIO_ACCOUNT_SID
    config.twilio_auth_token = TWILIO_AUTH_TOKEN
    config.twilio_phone_number = TWILIO_PHONE_NUMBER
    config.my_phone_number = MY_PHONE_NUMBER
    
    return config