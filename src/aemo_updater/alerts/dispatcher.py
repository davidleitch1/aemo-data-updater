"""AlertDispatcher — the glue between plugins and sinks.

Invoked once per collector cycle:

    dispatcher.run_cycle(db_path=..., data_dir=...)

Iterates every registered plugin, calls `evaluate(ctx)`, looks up the
sinks for each emitted alert via the routing table, fans out, and
catches exceptions on both sides so one plugin or one sink failing
does not block the rest.

See aemo-energy-dashboard2/docs/alerts_plugin_architecture.md for the
overall design.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from .base_alert import Alert
from .context import AlertContext
from .routing import ALERT_ROUTING, DEFAULT_SINKS


logger = logging.getLogger(__name__)


# NEM time = UTC+10 (no DST applied to data — AEMO timestamps are naive
# AEST). Importing the project-wide helper would create a dashboard ↔
# collector dependency, so we duplicate the small `nem_now` helper
# locally; both paths agree on AEST = UTC+10.
from datetime import timedelta, timezone as _tz

_NEM_TZ = _tz(timedelta(hours=10))


class AlertDispatcher:
    """Glues plugins → routing table → sinks. Stateless apart from
    `_last_run_at` which is held in memory across cycles within a
    single process lifetime."""

    def __init__(
        self,
        plugins: Iterable[Any],
        sinks: dict[str, Any],
        routing: dict[str, list[str]] = ALERT_ROUTING,
        default_sinks: list[str] = DEFAULT_SINKS,
    ) -> None:
        self.plugins = list(plugins)
        self.sinks = dict(sinks)
        self.routing = dict(routing)
        self.default_sinks = list(default_sinks)
        self._last_run_at: Optional[datetime] = None

    def run_cycle(self, db_path: str, data_dir: Path) -> None:
        """Evaluate every plugin once and dispatch every emitted alert."""
        now_utc = datetime.now(timezone.utc)
        ctx = AlertContext(
            db_path=db_path,
            data_dir=data_dir,
            now=now_utc,
            last_run_at=self._last_run_at or now_utc,
            nem_now=datetime.now(_NEM_TZ).replace(tzinfo=None),
        )

        for plugin in self.plugins:
            try:
                alerts = plugin.evaluate(ctx)
            except Exception:
                logger.exception('plugin %s failed', getattr(plugin, 'name', repr(plugin)))
                continue

            for alert in alerts or []:
                self._dispatch_one(alert)

        self._last_run_at = now_utc

    # ── internals ─────────────────────────────────────────────────────

    def _dispatch_one(self, alert: Alert) -> None:
        """Fan a single alert out to the sinks listed in the routing
        table (or DEFAULT_SINKS if the alert id isn't routed)."""
        sink_names = self.routing.get(alert.id or '', self.default_sinks)
        for sink_name in sink_names:
            sink = self.sinks.get(sink_name)
            if sink is None:
                logger.warning('sink %r referenced in routing but not registered', sink_name)
                continue
            if not getattr(sink, 'enabled', True):
                continue
            try:
                sink.emit(alert)
            except Exception:
                logger.exception(
                    'sink %s failed for alert %s',
                    getattr(sink, 'name', sink_name),
                    alert.id,
                )
