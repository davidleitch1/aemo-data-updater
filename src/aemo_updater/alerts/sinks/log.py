"""LogSink — always-on emitter that writes alerts to the Python logger.

Useful as the default fallback for any alert ID not yet wired into the
routing table, and as a paper trail alongside SMS/email/APNs sinks for
operators reviewing logs.
"""
from __future__ import annotations

import logging

from ..base_alert import Alert, AlertSeverity


logger = logging.getLogger(__name__)


_SEVERITY_TO_LEVEL = {
    AlertSeverity.CRITICAL: logging.CRITICAL,
    AlertSeverity.ERROR:    logging.ERROR,
    AlertSeverity.WARNING:  logging.WARNING,
    AlertSeverity.INFO:     logging.INFO,
}


class LogSink:
    """Logs each alert at a level matching its severity."""

    name = 'log'

    def __init__(self) -> None:
        self.enabled = True

    def emit(self, alert: Alert) -> None:
        level = _SEVERITY_TO_LEVEL.get(alert.severity, logging.INFO)
        logger.log(
            level,
            'ALERT [%s/%s] %s — %s',
            alert.severity.value,
            alert.id or alert.source,
            alert.title,
            alert.message,
        )
