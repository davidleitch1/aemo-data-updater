"""
Central alert management system
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
import asyncio
from pathlib import Path
import json

from .base_alert import Alert, AlertSeverity, AlertChannel
from .email_sender import EmailSender
from .sms_sender import SMSSender

logger = logging.getLogger(__name__)


class AlertManager:
    """Manages alert sending and throttling"""
    
    def __init__(self, config: dict):
        """Initialize alert manager
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.email_sender = None
        self.sms_sender = None
        
        # Alert throttling - prevent spam
        self.sent_alerts: Dict[str, datetime] = {}
        self.throttle_minutes = 60  # Don't repeat same alert for 60 minutes
        
        # Alert history file
        self.history_file = Path(config.get('logs_dir', '.')) / 'alert_history.json'
        self._load_history()
        
        # Initialize channels
        self._init_email()
        self._init_sms()
        
    def _init_email(self):
        """Initialize email channel if configured"""
        if self.config.get('email_enabled'):
            try:
                self.email_sender = EmailSender(
                    smtp_server=self.config.get('smtp_server', 'smtp.gmail.com'),
                    smtp_port=self.config.get('smtp_port', 587),
                    sender_email=self.config.get('alert_email'),
                    sender_password=self.config.get('alert_password'),
                    recipient_email=self.config.get('recipient_email')
                )
                logger.info("Email alerts configured")
            except Exception as e:
                logger.error(f"Failed to configure email alerts: {e}")
                
    def _init_sms(self):
        """Initialize SMS channel if configured"""
        if (self.config.get('sms_enabled') and 
            self.config.get('twilio_account_sid') and
            self.config.get('twilio_auth_token')):
            try:
                self.sms_sender = SMSSender(
                    account_sid=self.config.get('twilio_account_sid'),
                    auth_token=self.config.get('twilio_auth_token'),
                    twilio_phone_number=self.config.get('twilio_phone_number'),
                    my_phone_number=self.config.get('my_phone_number')
                )
                logger.info("SMS alerts configured")
            except Exception as e:
                logger.error(f"Failed to configure SMS alerts: {e}")
                
    def _load_history(self):
        """Load alert history from file"""
        if self.history_file.exists():
            try:
                with open(self.history_file, 'r') as f:
                    history = json.load(f)
                    # Convert timestamps back to datetime
                    self.sent_alerts = {
                        k: datetime.fromisoformat(v) 
                        for k, v in history.items()
                    }
            except Exception as e:
                logger.error(f"Failed to load alert history: {e}")
                
    def _save_history(self):
        """Save alert history to file"""
        try:
            # Convert datetimes to ISO format for JSON
            history = {
                k: v.isoformat() 
                for k, v in self.sent_alerts.items()
            }
            with open(self.history_file, 'w') as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save alert history: {e}")
            
    def _should_send_alert(self, alert_key: str) -> bool:
        """Check if alert should be sent based on throttling
        
        Args:
            alert_key: Unique key for this alert type
            
        Returns:
            True if alert should be sent
        """
        if alert_key not in self.sent_alerts:
            return True
            
        last_sent = self.sent_alerts[alert_key]
        time_since = datetime.now() - last_sent
        
        return time_since > timedelta(minutes=self.throttle_minutes)
        
    async def send_alert(self, alert: Alert, channel: AlertChannel = AlertChannel.BOTH) -> bool:
        """Send an alert via specified channel(s)
        
        Args:
            alert: Alert to send
            channel: Which channel(s) to use
            
        Returns:
            True if alert sent successfully via at least one channel
        """
        # Create unique key for throttling
        alert_key = f"{alert.source}:{alert.title}"
        
        # Check throttling
        if not self._should_send_alert(alert_key):
            logger.debug(f"Alert throttled: {alert_key}")
            return False
            
        success = False
        
        # Send via email
        if channel in (AlertChannel.EMAIL, AlertChannel.BOTH) and self.email_sender:
            if self.email_sender.send(alert):
                success = True
                
        # Send via SMS (only for high severity)
        if (channel in (AlertChannel.SMS, AlertChannel.BOTH) and 
            self.sms_sender and
            alert.severity in (AlertSeverity.ERROR, AlertSeverity.CRITICAL)):
            if self.sms_sender.send(alert):
                success = True
                
        # Update throttling
        if success:
            self.sent_alerts[alert_key] = datetime.now()
            self._save_history()
            
        return success
        
    async def check_data_freshness(self, statuses: Dict[str, dict], 
                                   thresholds: Dict[str, int]) -> List[Alert]:
        """Check data freshness and generate alerts
        
        Args:
            statuses: Status dictionary from service
            thresholds: Age thresholds by data type
            
        Returns:
            List of alerts generated
        """
        alerts = []
        
        for data_type, status in statuses.items():
            file_info = status.get('file_info', {})
            
            if not file_info.get('exists'):
                # Missing file alert
                alert = Alert(
                    title=f"{data_type.title()} data file missing",
                    message=f"The {data_type} parquet file does not exist. Data collection may have failed.",
                    severity=AlertSeverity.ERROR,
                    source=data_type,
                    metadata={'file_path': str(status.get('file_path', 'unknown'))}
                )
                alerts.append(alert)
                
            elif file_info.get('modified'):
                # Check staleness
                age_minutes = (datetime.now() - file_info['modified']).total_seconds() / 60
                threshold = thresholds.get(data_type, 30)
                
                if age_minutes > threshold:
                    alert = Alert(
                        title=f"{data_type.title()} data is stale",
                        message=f"The {data_type} data is {age_minutes:.0f} minutes old (threshold: {threshold} minutes). Data updates may have stopped.",
                        severity=AlertSeverity.WARNING if age_minutes < threshold * 2 else AlertSeverity.ERROR,
                        source=data_type,
                        metadata={
                            'age_minutes': int(age_minutes),
                            'threshold_minutes': threshold,
                            'last_modified': file_info['modified'].isoformat()
                        }
                    )
                    alerts.append(alert)
                    
        return alerts
        
    def test_channels(self) -> Dict[str, bool]:
        """Test all configured alert channels
        
        Returns:
            Dictionary of channel test results
        """
        results = {}
        
        if self.email_sender:
            results['email'] = self.email_sender.test_connection()
        else:
            results['email'] = False
            
        if self.sms_sender:
            results['sms'] = self.sms_sender.test_connection()
        else:
            results['sms'] = False
            
        return results