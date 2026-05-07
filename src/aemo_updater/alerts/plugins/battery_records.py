"""BatteryRecordsPlugin — emit when a NEM-wide or per-region battery
SOC / discharge / charge record is broken.

Functionally equivalent to `battery_monitor.py:check_records`.

**Shadow mode (step 8)**: routing is `['log']` only. The standalone
`battery_monitor.py` daemon keeps running as the SMS source; this
plugin observes in parallel for ~24h. Cutover commit (later) flips
routing to `['sms', 'log']` and stops the daemon.

State: `<data_dir>/battery_records_plugin.json` (SEPARATE from
daemon's `battery_records.json`). On first run, the plugin calls
`seed_fn(ctx)` to populate state from `MAX(...)` over the full
`bdu5` history — same shape the daemon's `--seed` mode produces.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Optional

from ..base_alert import Alert, AlertSeverity
from ..context import AlertContext


logger = logging.getLogger(__name__)


STATE_FILENAME = 'battery_records_plugin.json'

REGIONS = ('NSW1', 'QLD1', 'VIC1', 'SA1')
METRICS = ('soc_mwh', 'discharge_mw', 'charge_mw')

# Map metric → kebab-case ID prefix.
_METRIC_ID_PREFIX = {
    'soc_mwh': 'battery-soc-record',
    'discharge_mw': 'battery-discharge-record',
    'charge_mw': 'battery-charge-record',
}

_METRIC_LABEL = {
    'soc_mwh': 'SOC',
    'discharge_mw': 'discharge',
    'charge_mw': 'charge',
}


def _default_seed_fn(ctx: AlertContext) -> dict:  # pragma: no cover
    """Compute all-time maxima from full bdu5 history."""
    import duckdb
    conn = duckdb.connect(ctx.db_path, read_only=True)
    try:
        rdf = conn.execute(
            """SELECT regionid,
                      MAX(bdu_energy_storage)  AS soc_mwh,
                      MAX(bdu_clearedmw_gen)   AS discharge_mw,
                      MAX(bdu_clearedmw_load)  AS charge_mw
                 FROM bdu5
                WHERE regionid IN ('NSW1','QLD1','VIC1','SA1')
                GROUP BY regionid"""
        ).fetchdf()
        ndf = conn.execute(
            """WITH per_t AS (
                  SELECT settlementdate,
                         SUM(bdu_energy_storage) AS soc_mwh,
                         SUM(bdu_clearedmw_gen)  AS discharge_mw,
                         SUM(bdu_clearedmw_load) AS charge_mw
                    FROM bdu5
                   WHERE regionid IN ('NSW1','QLD1','VIC1','SA1')
                   GROUP BY 1
                )
                SELECT MAX(soc_mwh) AS soc_mwh,
                       MAX(discharge_mw) AS discharge_mw,
                       MAX(charge_mw) AS charge_mw
                  FROM per_t"""
        ).fetchone()
    finally:
        conn.close()

    seed: dict = {'nem': {m: {'value': float(ndf[i] or 0), 'timestamp': None}
                          for i, m in enumerate(METRICS)}}
    for _, row in rdf.iterrows():
        seed[row['regionid']] = {
            m: {'value': float(row[m] or 0), 'timestamp': None}
            for m in METRICS
        }
    return seed


def _default_latest_fn(ctx: AlertContext) -> Optional[dict]:  # pragma: no cover
    """Return latest 5-min reading or None if empty."""
    import duckdb
    conn = duckdb.connect(ctx.db_path, read_only=True)
    try:
        df = conn.execute(
            """SELECT settlementdate, regionid,
                      bdu_energy_storage  AS soc_mwh,
                      bdu_clearedmw_gen   AS discharge_mw,
                      bdu_clearedmw_load  AS charge_mw
                 FROM bdu5
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM bdu5)
                  AND regionid IN ('NSW1','QLD1','VIC1','SA1')"""
        ).fetchdf()
    finally:
        conn.close()
    if df.empty:
        return None

    out = {'timestamp': str(df['settlementdate'].iloc[0])}
    nem = {m: 0.0 for m in METRICS}
    for _, row in df.iterrows():
        out[row['regionid']] = {m: float(row[m] or 0) for m in METRICS}
        for m in METRICS:
            nem[m] += float(row[m] or 0)
    out['nem'] = nem
    return out


class BatteryRecordsPlugin:
    """Detect new NEM-wide / per-region battery records."""

    name = 'battery_records'
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
                logger.info('BatteryRecordsPlugin: seeded state on first run')
            except Exception:
                logger.exception('BatteryRecordsPlugin: seed failed')
            return []

        try:
            current = self.latest_fn(ctx)
        except Exception:
            logger.exception('BatteryRecordsPlugin: latest_fn failed')
            return []
        if current is None:
            return []

        alerts: list[Alert] = []
        for scope in ('nem', *REGIONS):
            cur = current.get(scope)
            if cur is None:
                continue
            for metric in METRICS:
                cur_val = float(cur[metric])
                rec = state.setdefault(scope, {}).setdefault(
                    metric, {'value': 0.0, 'timestamp': None})
                if cur_val > rec['value']:
                    alerts.append(self._build_alert(
                        scope=scope, metric=metric,
                        new_val=cur_val, old_val=rec['value'],
                        ts=current.get('timestamp'), ctx=ctx,
                    ))
                    rec['value'] = cur_val
                    rec['timestamp'] = current.get('timestamp')
        if alerts:
            self._save_state(ctx.data_dir, state)
        return alerts

    def _build_alert(self, *, scope: str, metric: str,
                     new_val: float, old_val: float,
                     ts: Optional[str], ctx: AlertContext) -> Alert:
        scope_id = scope.lower() if scope == 'nem' else scope.lower()
        alert_id = f'{_METRIC_ID_PREFIX[metric]}-{scope_id}'
        scope_label = 'NEM' if scope == 'nem' else scope
        unit = 'MWh' if metric == 'soc_mwh' else 'MW'
        return Alert(
            title=f'🔋 {scope_label} new battery {_METRIC_LABEL[metric]} record: '
                  f'{new_val:.1f} {unit} (prev {old_val:.1f})',
            message=f'New {scope_label} {metric} record at {ts}',
            severity=self.severity,
            source='battery_records',
            timestamp=ctx.now,
            metadata={'scope': scope, 'metric': metric,
                      'new_value': new_val, 'old_value': old_val,
                      'timestamp': ts},
            id=alert_id,
            dedup_key=f'{metric}-{scope}',
        )

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
