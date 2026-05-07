"""DataFreshnessPlugin — alert when AEMO source data stops flowing.

Replaces `alert_manager.py:check_data_freshness` (which checked
parquet file mtimes; those files no longer exist as the collector
writes DuckDB directly).

Modern check: `MAX(settlementdate)` per critical DuckDB table vs
`ctx.nem_now`. Lag exceeding the per-table threshold → emit
`data-file-stale`. Missing DB / missing table → `data-file-missing`.

Per-table state file `data_dir/data_freshness_plugin.json` carries
the last-alerted timestamp per table so the same outage doesn't
produce a fresh alert on every cycle.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from ..base_alert import Alert, AlertSeverity
from ..context import AlertContext


logger = logging.getLogger(__name__)


STATE_FILENAME = 'data_freshness_plugin.json'

# Per-table staleness thresholds in MINUTES. AEMO publishes the 5-min
# tables every 5 min and the 30-min tables every 30 min, so these
# allow ~3-6 missed publishes before alerting — quiet enough to not
# fire on transient delays, loud enough to surface real outages.
DEFAULT_THRESHOLDS: dict[str, int] = {
    'prices5':       30,
    'scada5':        30,
    'transmission5': 30,
    'rooftop30':    120,  # 30-min cadence; 4 missed ≈ 2h
    'demand30':     120,
}

# After firing, suppress re-firing for the same table for this many
# minutes — long enough that an extended outage produces ~1 alert/h.
THROTTLE_MINUTES = 60


class DataFreshnessPlugin:
    """Detect stale / missing AEMO source data."""

    name = 'data_freshness'

    def __init__(self, thresholds: Optional[dict[str, int]] = None) -> None:
        self.thresholds = dict(thresholds) if thresholds is not None else dict(DEFAULT_THRESHOLDS)

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        state = self._load_state(ctx.data_dir)
        now_nem = ctx.nem_now
        alerts: list[Alert] = []

        # Missing-DB shortcut — fire one missing alert per table the
        # plugin would have checked.
        if not os.path.exists(ctx.db_path):
            for table in self.thresholds:
                a = self._missing_alert(table, ctx, reason='database file not found')
                if a and self._should_fire(a, state, now_nem):
                    alerts.append(a)
                    self._record_fire(a, state, now_nem)
            if alerts:
                self._save_state(ctx.data_dir, state)
            return alerts

        try:
            import duckdb
            conn = duckdb.connect(ctx.db_path, read_only=True)
        except Exception:
            logger.exception('DataFreshnessPlugin: cannot open DuckDB')
            return []

        try:
            existing = {
                t for (t,) in conn.execute(
                    "SELECT table_name FROM information_schema.tables"
                ).fetchall()
            }
            for table, threshold_min in self.thresholds.items():
                if table not in existing:
                    a = self._missing_alert(table, ctx, reason='table not in database')
                    if self._should_fire(a, state, now_nem):
                        alerts.append(a)
                        self._record_fire(a, state, now_nem)
                    continue

                row = conn.execute(
                    f'SELECT MAX(settlementdate) FROM "{table}"'
                ).fetchone()
                max_dt = row[0] if row else None
                if max_dt is None:
                    a = self._missing_alert(table, ctx, reason='table is empty')
                    if self._should_fire(a, state, now_nem):
                        alerts.append(a)
                        self._record_fire(a, state, now_nem)
                    continue

                age_min = (now_nem - max_dt).total_seconds() / 60
                if age_min > threshold_min:
                    a = self._stale_alert(table, age_min, threshold_min, ctx)
                    if self._should_fire(a, state, now_nem):
                        alerts.append(a)
                        self._record_fire(a, state, now_nem)
        finally:
            conn.close()

        if alerts:
            self._save_state(ctx.data_dir, state)
        return alerts

    # ── Alert factories ──────────────────────────────────────────────

    def _stale_alert(self, table: str, age_min: float, threshold_min: int, ctx: AlertContext) -> Alert:
        # Escalate to ERROR if more than 2× the threshold (mirrors
        # legacy alert_manager severity bump).
        sev = AlertSeverity.ERROR if age_min > threshold_min * 2 else AlertSeverity.WARNING
        return Alert(
            title=f'{table} stale ({int(age_min)}m old, threshold {threshold_min}m)',
            message=(f'No new {table} rows for {int(age_min)} minutes. '
                     f'Threshold {threshold_min}m. AEMO may have stopped publishing '
                     f'or the collector pipeline may be stuck.'),
            severity=sev,
            source='data_freshness',
            timestamp=ctx.now,
            metadata={'table': table, 'age_minutes': int(age_min),
                      'threshold_minutes': threshold_min},
            id='data-file-stale',
            dedup_key=f'stale-{table}',
        )

    def _missing_alert(self, table: str, ctx: AlertContext, reason: str) -> Alert:
        return Alert(
            title=f'{table} data missing',
            message=f'{table}: {reason}. Collector pipeline may have failed.',
            severity=AlertSeverity.ERROR,
            source='data_freshness',
            timestamp=ctx.now,
            metadata={'table': table, 'reason': reason},
            id='data-file-missing',
            dedup_key=f'missing-{table}',
        )

    # ── Throttle ─────────────────────────────────────────────────────

    def _should_fire(self, alert: Alert, state: dict, now_nem: datetime) -> bool:
        last = state.get(alert.dedup_key)
        if last is None:
            return True
        try:
            last_dt = datetime.fromisoformat(last) if isinstance(last, str) else last
        except Exception:
            return True
        return (now_nem - last_dt).total_seconds() / 60 >= THROTTLE_MINUTES

    def _record_fire(self, alert: Alert, state: dict, now_nem: datetime) -> None:
        state[alert.dedup_key] = now_nem.isoformat()

    # ── State I/O ────────────────────────────────────────────────────

    def _load_state(self, data_dir: Path) -> dict:
        path = data_dir / STATE_FILENAME
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}

    def _save_state(self, data_dir: Path, state: dict) -> None:
        path = data_dir / STATE_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, default=str))
