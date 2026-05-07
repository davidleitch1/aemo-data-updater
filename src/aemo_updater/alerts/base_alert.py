"""
Base alert definitions and interfaces
"""

from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertChannel(Enum):
    """Available alert channels"""
    EMAIL = "email"
    SMS = "sms"
    BOTH = "both"


@dataclass
class Alert:
    """Alert data structure.

    The `id` and `dedup_key` fields are used by the alerts plugin
    dispatcher (see docs/alerts_plugin_architecture.md):
      * `id`        — kebab-case alert identifier matching the catalogue
                      in docs/alerts.md (e.g. 'spot-price-high-breach').
                      Used by the routing table to look up sinks.
      * `dedup_key` — plugin-internal key for rate-limit / armed-state
                      tracking (e.g. 'price-NSW1'). Opaque to sinks.
    Both default to None for backwards compatibility with pre-plugin
    call sites.
    """
    title: str
    message: str
    severity: AlertSeverity
    source: str  # Which collector/component triggered this
    timestamp: datetime = None
    metadata: Optional[Dict[str, Any]] = None
    id: Optional[str] = None
    dedup_key: Optional[str] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()
            
    def format_for_sms(self) -> str:
        """Format alert for SMS (160 char limit)"""
        # SMS format: SEVERITY: Source - Title
        sms_text = f"{self.severity.value.upper()}: {self.source} - {self.title}"
        if len(sms_text) > 160:
            sms_text = sms_text[:157] + "..."
        return sms_text
        
    def format_for_email(self) -> tuple[str, str]:
        """Format alert for email (subject, body)"""
        subject = f"[{self.severity.value.upper()}] AEMO Updater: {self.title}"
        
        body = f"""
AEMO Data Updater Alert
=======================

Severity: {self.severity.value.upper()}
Source: {self.source}
Time: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}

{self.message}
"""
        
        if self.metadata:
            body += "\n\nAdditional Details:\n"
            for key, value in self.metadata.items():
                body += f"  {key}: {value}\n"
                
        return subject, body