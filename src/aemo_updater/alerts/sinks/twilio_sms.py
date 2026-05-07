"""TwilioSmsSink — emit alerts as Twilio SMS to a single admin phone.

Stateless. Reads creds from env on construction by default; constructor
args override for testing. `enabled` is true only when the four
required values (account_sid, auth_token, from_number, to_number) are
all present, so the dispatcher can skip the sink cleanly when running
in dev / CI without Twilio configured.
"""
from __future__ import annotations

import logging
import os
from typing import Callable, Optional

from ..base_alert import Alert


logger = logging.getLogger(__name__)


def _default_client_factory(account_sid: str, auth_token: str):  # pragma: no cover
    """Lazy-import twilio so test environments without it can still
    import this module."""
    from twilio.rest import Client
    return Client(account_sid, auth_token)


class TwilioSmsSink:
    """Send alerts via Twilio SMS."""

    name = 'sms'

    def __init__(
        self,
        account_sid: Optional[str] = None,
        auth_token: Optional[str] = None,
        from_number: Optional[str] = None,
        to_number: Optional[str] = None,
        client_factory: Callable = _default_client_factory,
    ) -> None:
        self.account_sid = account_sid or os.getenv('TWILIO_ACCOUNT_SID')
        self.auth_token = auth_token or os.getenv('TWILIO_AUTH_TOKEN')
        self.from_number = (
            from_number
            or os.getenv('TWILIO_FROM_NUMBER')
            or os.getenv('TWILIO_PHONE_NUMBER')
        )
        self.to_number = (
            to_number
            or os.getenv('MY_PHONE_NUMBER')
            or os.getenv('ALERT_PHONE_NUMBER')
        )
        self.enabled = bool(
            self.account_sid and self.auth_token and self.from_number and self.to_number
        )
        self._client = None
        if self.enabled:
            try:
                self._client = client_factory(self.account_sid, self.auth_token)
            except Exception:
                logger.exception('TwilioSmsSink: client init failed; disabling')
                self.enabled = False

    def emit(self, alert: Alert) -> None:
        if not self.enabled or self._client is None:
            return
        try:
            self._client.messages.create(
                body=alert.format_for_sms(),
                from_=self.from_number,
                to=self.to_number,
            )
        except Exception:
            logger.exception('TwilioSmsSink: send failed for alert %s', alert.id)
