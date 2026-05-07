"""Alert management for AEMO Data Updater.

Plugin-based architecture (see
aemo-energy-dashboard2/docs/alerts_plugin_architecture.md):

  Plugin (detector)  →  Alert  →  Routing table  →  Sink (channel)

Production wiring is via `build_default_dispatcher()` — call once at
collector startup, then `dispatcher.run_cycle(db_path, data_dir)` after
each merge cycle.

Backward-compatible: legacy `AlertManager`, `AlertChannel`,
`AlertSeverity`, `Alert` exports retained for code that hasn't moved
to plugins yet.
"""
from __future__ import annotations

from typing import Callable, Optional

from .alert_manager import AlertManager
from .base_alert import Alert, AlertChannel, AlertSeverity
from .context import AlertContext
from .dispatcher import AlertDispatcher
from .plugins.price_breach import PriceBreachPlugin
from .routing import ALERT_ROUTING, DEFAULT_SINKS
from .sinks.log import LogSink
from .sinks.smtp_email import SmtpEmailSink
from .sinks.twilio_sms import TwilioSmsSink


__all__ = [
    'AlertManager', 'AlertChannel', 'AlertSeverity', 'Alert',
    'AlertContext', 'AlertDispatcher',
    'build_default_dispatcher',
]


def build_default_dispatcher(
    *,
    sinks: Optional[dict] = None,
    plugins: Optional[list] = None,
    routing: Optional[dict] = None,
    twilio_client_factory: Optional[Callable] = None,
    smtp_factory: Optional[Callable] = None,
    price_query_fn: Optional[Callable] = None,
) -> AlertDispatcher:
    """Construct the production alert dispatcher.

    All parameters are keyword-only and primarily for tests:
      * `sinks` / `plugins` / `routing` — full overrides
      * `twilio_client_factory` / `smtp_factory` — inject fake clients
      * `price_query_fn` — inject synthetic price rows for the
        PriceBreachPlugin so tests don't need a DuckDB

    With no arguments, the dispatcher is built from env (see
    individual sink classes for the env-var names) and the canonical
    ALERT_ROUTING table.
    """
    if sinks is None:
        sinks = {
            'log':   LogSink(),
            'sms':   TwilioSmsSink(client_factory=twilio_client_factory) if twilio_client_factory
                     else TwilioSmsSink(),
            'email': SmtpEmailSink(smtp_factory=smtp_factory) if smtp_factory
                     else SmtpEmailSink(),
        }
    if plugins is None:
        plugins = [PriceBreachPlugin(query_fn=price_query_fn)]
    return AlertDispatcher(
        plugins=plugins,
        sinks=sinks,
        routing=routing if routing is not None else ALERT_ROUTING,
    )
