"""
SMS alert sender implementation using Twilio
"""

import logging
from typing import Optional

try:
    from twilio.rest import Client
    TWILIO_AVAILABLE = True
except ImportError:
    TWILIO_AVAILABLE = False
    logger = logging.getLogger(__name__)
    logger.warning("Twilio not installed. SMS alerts will not be available.")

from .base_alert import Alert

logger = logging.getLogger(__name__)


class SMSSender:
    """Send alerts via SMS using Twilio"""
    
    def __init__(self, account_sid: str, auth_token: str,
                 twilio_phone_number: str, my_phone_number: str):
        """Initialize SMS sender
        
        Args:
            account_sid: Twilio account SID
            auth_token: Twilio auth token
            twilio_phone_number: Twilio phone number (from)
            my_phone_number: Recipient phone number
        """
        if not TWILIO_AVAILABLE:
            raise ImportError("Twilio package not installed. Run: pip install twilio")
            
        self.account_sid = account_sid
        self.auth_token = auth_token
        self.twilio_phone_number = twilio_phone_number
        self.my_phone_number = my_phone_number
        self.client = None
        
    def _get_client(self) -> 'Client':
        """Get or create Twilio client"""
        if self.client is None:
            self.client = Client(self.account_sid, self.auth_token)
        return self.client
        
    def send(self, alert: Alert) -> bool:
        """Send an alert via SMS
        
        Args:
            alert: Alert to send
            
        Returns:
            True if sent successfully, False otherwise
        """
        try:
            client = self._get_client()
            
            # Format message for SMS
            message_body = alert.format_for_sms()
            
            # Send SMS
            message = client.messages.create(
                body=message_body,
                from_=self.twilio_phone_number,
                to=self.my_phone_number
            )
            
            logger.info(f"SMS alert sent successfully: {alert.title} (SID: {message.sid})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send SMS alert: {e}")
            return False
            
    def test_connection(self) -> bool:
        """Test SMS connection by sending a test message
        
        Returns:
            True if test message sent successfully, False otherwise
        """
        try:
            client = self._get_client()
            
            # Send test message
            message = client.messages.create(
                body="AEMO Updater: SMS alerts configured successfully",
                from_=self.twilio_phone_number,
                to=self.my_phone_number
            )
            
            logger.info(f"SMS test message sent successfully (SID: {message.sid})")
            return True
            
        except Exception as e:
            logger.error(f"SMS connection test failed: {e}")
            return False