"""RenewableRecordsPlugin — emit when an NEM-wide renewable record
breaks.

Five single-value metrics, all NEM-wide:
  * renewable_pct → renewable-record-percentage  (% of demand)
  * wind_mw       → wind-record-mw                (MW)
  * solar_mw      → solar-record-mw               (MW)
  * water_mw      → hydro-record-mw               (MW)
  * rooftop_mw    → rooftop-solar-record-mw       (MW)

Functionally equivalent to the record-tracking logic in
`/Users/davidleitch/aemo_production/aemo-energy-dashboard2/standalone_renewable_gauge_with_alerts_updated.py`.

Shadow mode (step 9): routes to `['log']` only. Standalone gauge
daemon (tmux services:2) keeps firing SMS as source of truth.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Optional

from ..base_alert import Alert, AlertSeverity
from ..context import AlertContext


logger = logging.getLogger(__name__)


STATE_FILENAME = 'renewable_records_plugin.json'

METRICS = ('renewable_pct', 'wind_mw', 'solar_mw', 'water_mw', 'rooftop_mw')

METRIC_TO_ALERT_ID = {
    'renewable_pct': 'renewable-record-percentage',
    'wind_mw':       'wind-record-mw',
    'solar_mw':      'solar-record-mw',
    'water_mw':      'hydro-record-mw',
    'rooftop_mw':    'rooftop-solar-record-mw',
}

METRIC_LABEL = {
    'renewable_pct': 'renewable %',
    'wind_mw':       'wind',
    'solar_mw':      'solar',
    'water_mw':      'hydro',
    'rooftop_mw':    'rooftop solar',
}

METRIC_UNIT = {
    'renewable_pct': '%',
    'wind_mw':       'MW',
    'solar_mw':      'MW',
    'water_mw':      'MW',
    'rooftop_mw':    'MW',
}

METRIC_EMOJI = {
    'renewable_pct': '🌱',
    'wind_mw':       '🌬️',
    'solar_mw':      '☀️',
    'water_mw':      '💧',
    'rooftop_mw':    '🏠',
}


def _default_seed_fn(ctx: AlertContext) -> dict:  # pragma: no cover
    """Read the standalone gauge daemon's per-fuel state file
    (`renewable_records_calculated.json`) as the seed. The file's
    shape is `{"all_time": {"renewable_pct": {value, timestamp},
    "wind_mw": {...}, ...}, "hourly": {...}}`.

    Falls back to zeros if the file is missing or malformed.
    """
    legacy_path = ctx.data_dir / 'renewable_records_calculated.json'
    if legacy_path.exists():
        try:
            data = json.loads(legacy_path.read_text())
            all_time = data.get('all_time', {})
            return {
                m: {'value': float(all_time.get(m, {}).get('value', 0)),
                    'timestamp': all_time.get(m, {}).get('timestamp')}
                for m in METRICS
            }
        except Exception:
            pass
    return {m: {'value': 0.0, 'timestamp': None} for m in METRICS}


def _default_latest_fn(ctx: AlertContext) -> Optional[dict]:  # pragma: no cover
    """Compute latest renewable metrics from DuckDB.

    Wind/solar/water sourced from `generation_by_fuel_5min` at
    `MAX(settlementdate)`; rooftop from `rooftop30` (separate cadence,
    use latest 30-min value); renewable_pct = sum of all renewable MW
    / demand5 total.
    """
    import duckdb
    conn = duckdb.connect(ctx.db_path, read_only=True)
    try:
        row = conn.execute(
            """WITH latest AS (SELECT MAX(settlementdate) AS t FROM generation_by_fuel_5min)
                SELECT g.fuel_type,
                       SUM(g.total_generation_mw) AS mw
                  FROM generation_by_fuel_5min g, latest
                 WHERE g.settlementdate = latest.t
                 GROUP BY g.fuel_type"""
        ).fetchall()
        gen_by_fuel = {fuel: float(mw or 0) for fuel, mw in row}

        rooftop_row = conn.execute(
            """SELECT SUM(power) FROM rooftop30
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM rooftop30)"""
        ).fetchone()
        rooftop_mw = float(rooftop_row[0] or 0) if rooftop_row else 0.0

        # demand30 is the only demand table the collector maintains;
        # column is `demand` (NEM-wide load by region — sum to get total).
        demand_row = conn.execute(
            """SELECT SUM(demand) FROM demand30
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM demand30)"""
        ).fetchone()
        demand_mw = float(demand_row[0] or 0) if demand_row else 0.0

        ts_row = conn.execute(
            'SELECT MAX(settlementdate) FROM generation_by_fuel_5min'
        ).fetchone()
        ts = str(ts_row[0]) if ts_row and ts_row[0] else None
    finally:
        conn.close()

    wind = gen_by_fuel.get('Wind', 0.0)
    solar = gen_by_fuel.get('Solar', 0.0)
    water = gen_by_fuel.get('Water', 0.0)
    total_renewable = wind + solar + water + rooftop_mw
    pct = (total_renewable / demand_mw * 100) if demand_mw > 0 else 0.0

    return {
        'timestamp':     ts,
        'renewable_pct': pct,
        'wind_mw':       wind,
        'solar_mw':      solar,
        'water_mw':      water,
        'rooftop_mw':    rooftop_mw,
    }


class RenewableRecordsPlugin:
    """Detect new NEM-wide renewable-energy records."""

    name = 'renewable_records'
    severity = AlertSeverity.INFO

    def __init__(
        self,
        seed_fn: Optional[Callable[[AlertContext], dict]] = None,
        latest_fn: Optional[Callable[[AlertContext], Optional[dict]]] = None,
    ) -> None:
        self.seed_fn = seed_fn or _default_seed_fn
        self.latest_fn = latest_fn or _default_latest_fn

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        state = self._load_state(ctx.data_dir)
        if not state:
            try:
                state = self.seed_fn(ctx)
                self._save_state(ctx.data_dir, state)
                logger.info('RenewableRecordsPlugin: seeded state on first run')
            except Exception:
                logger.exception('RenewableRecordsPlugin: seed failed')
            return []

        try:
            current = self.latest_fn(ctx)
        except Exception:
            logger.exception('RenewableRecordsPlugin: latest_fn failed')
            return []
        if current is None:
            return []

        alerts: list[Alert] = []
        ts = current.get('timestamp')
        for metric in METRICS:
            cur_val = float(current.get(metric, 0))
            rec = state.setdefault(metric, {'value': 0.0, 'timestamp': None})
            if cur_val > rec['value']:
                alerts.append(self._build_alert(
                    metric=metric,
                    new_val=cur_val, old_val=rec['value'],
                    ts=ts, ctx=ctx,
                ))
                rec['value'] = cur_val
                rec['timestamp'] = ts
        if alerts:
            self._save_state(ctx.data_dir, state)
        return alerts

    def _build_alert(self, *, metric: str, new_val: float, old_val: float,
                     ts: Optional[str], ctx: AlertContext) -> Alert:
        return Alert(
            title=(f'{METRIC_EMOJI[metric]} New {METRIC_LABEL[metric]} record: '
                   f'{new_val:.1f}{METRIC_UNIT[metric]} '
                   f'(prev {old_val:.1f}{METRIC_UNIT[metric]})'),
            message=f'{METRIC_LABEL[metric]} reached {new_val:.1f}{METRIC_UNIT[metric]} at {ts}',
            severity=self.severity,
            source='renewable_records',
            timestamp=ctx.now,
            metadata={'metric': metric, 'new_value': new_val,
                      'old_value': old_val, 'timestamp': ts},
            id=METRIC_TO_ALERT_ID[metric],
            dedup_key=f'renewable-{metric}',
        )

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
