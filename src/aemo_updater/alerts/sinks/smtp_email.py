"""SmtpEmailSink — emit alerts as email via SMTP.

Reads creds from env on construction by default. Supports the existing
`EMAIL_*` (preferred) + `ALERT_*` (fallback) env-var schemes used by
the collector and the dashboard so the migration doesn't require an
.env edit.

Distinct from the existing `EmailSender` class in the same package:
that class is procedural (callers wrap their own retry/logging);
`SmtpEmailSink` adapts it to the standard sink interface
(`name`, `enabled`, `emit(alert) -> None`).
"""
from __future__ import annotations

import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Callable, Optional

from ..base_alert import Alert


logger = logging.getLogger(__name__)


def _default_smtp_factory(host: str, port: int):  # pragma: no cover
    return smtplib.SMTP(host, port)


def _env_int(name: str, default: Optional[int] = None) -> Optional[int]:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


class SmtpEmailSink:
    """Send alerts via SMTP."""

    name = 'email'

    def __init__(
        self,
        smtp_server: Optional[str] = None,
        smtp_port: Optional[int] = None,
        sender_email: Optional[str] = None,
        sender_password: Optional[str] = None,
        recipient_email: Optional[str] = None,
        login_email: Optional[str] = None,
        smtp_factory: Callable = _default_smtp_factory,
    ) -> None:
        self.smtp_server = (
            smtp_server
            or os.getenv('EMAIL_SMTP_SERVER')
            or os.getenv('SMTP_SERVER')
        )
        self.smtp_port = (
            smtp_port
            if smtp_port is not None
            else _env_int('EMAIL_SMTP_PORT') or _env_int('SMTP_PORT', 587)
        )
        self.sender_email = (
            sender_email
            or os.getenv('EMAIL_ADDRESS')
            or os.getenv('ALERT_EMAIL')
        )
        self.sender_password = (
            sender_password
            or os.getenv('EMAIL_PASSWORD')
            or os.getenv('ALERT_PASSWORD')
        )
        self.recipient_email = (
            recipient_email
            or os.getenv('RECIPIENT_EMAIL')
            or self.sender_email
        )
        self.login_email = (
            login_email
            or os.getenv('EMAIL_LOGIN')
            or os.getenv('ALERT_LOGIN')
            or self.sender_email
        )
        self._smtp_factory = smtp_factory
        self.enabled = bool(
            self.smtp_server
            and self.smtp_port
            and self.sender_email
            and self.sender_password
            and self.recipient_email
        )

    def emit(self, alert: Alert) -> None:
        if not self.enabled:
            return
        try:
            subject, body = alert.format_for_email()
            msg = MIMEMultipart()
            msg['From'] = self.sender_email
            msg['To'] = self.recipient_email
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'plain'))
            with self._smtp_factory(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.login_email, self.sender_password)
                server.send_message(msg)
        except Exception:
            logger.exception('SmtpEmailSink: send failed for alert %s', alert.id)
