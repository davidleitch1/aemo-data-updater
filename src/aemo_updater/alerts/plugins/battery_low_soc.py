"""BatteryLowSocPlugin — alert when a region's battery fleet SOC
drops below 5% (with 15% recovery hysteresis).

Functionally equivalent to `battery_monitor.py:check_low_soc`.

Shadow mode (step 8): routes to `['log']` only. Standalone
`battery_monitor.py` daemon keeps firing SMS as the source of truth.

State `<data_dir>/battery_low_soc_plugin.json` per-region:
  { "NSW1": {"active": bool, "triggered_at": iso-ts | null}, ... }
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Optional

from ..base_alert import Alert, AlertSeverity
from ..context import AlertContext


logger = logging.getLogger(__name__)


STATE_FILENAME = 'battery_low_soc_plugin.json'

REGIONS = ('NSW1', 'QLD1', 'VIC1', 'SA1')
TRIGGER_PCT = 5.0
RECOVER_PCT = 15.0


# Capacity per region (MWh) — used to convert SOC MWh → percentage.
# Matches battery_monitor.py FLEET_CAPACITY_MWH.
FLEET_CAPACITY_MWH = {
    'NSW1': 5758.0,
    'QLD1': 5322.0,
    'SA1':  2301.3,
    'VIC1': 4708.0,
}


def _default_soc_pct_fn(ctx: AlertContext) -> dict[str, float]:  # pragma: no cover
    """Latest SOC% per region from bdu5."""
    import duckdb
    conn = duckdb.connect(ctx.db_path, read_only=True)
    try:
        df = conn.execute(
            """SELECT regionid, bdu_energy_storage AS soc_mwh
                 FROM bdu5
                WHERE settlementdate = (SELECT MAX(settlementdate) FROM bdu5)
                  AND regionid IN ('NSW1','QLD1','VIC1','SA1')"""
        ).fetchdf()
    finally:
        conn.close()
    out: dict[str, float] = {}
    for _, row in df.iterrows():
        cap = FLEET_CAPACITY_MWH.get(row['regionid'])
        if cap:
            out[row['regionid']] = float(row['soc_mwh'] or 0) / cap * 100
    return out


class BatteryLowSocPlugin:
    """Per-region low-SOC trigger + recovery alerts with hysteresis."""

    name = 'battery_low_soc'
    severity = AlertSeverity.WARNING

    def __init__(self, soc_pct_fn: Optional[Callable[[AlertContext], dict[str, float]]] = None) -> None:
        self.soc_pct_fn = soc_pct_fn or _default_soc_pct_fn

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        state = self._load_state(ctx.data_dir)
        # Initialise any missing regions as inactive.
        for r in REGIONS:
            state.setdefault(r, {'active': False, 'triggered_at': None})

        try:
            soc_pct = self.soc_pct_fn(ctx)
        except Exception:
            logger.exception('BatteryLowSocPlugin: soc_pct_fn failed')
            return []

        alerts: list[Alert] = []
        for region, pct in soc_pct.items():
            if region not in state:
                continue
            rec = state[region]
            if not rec['active'] and pct < TRIGGER_PCT:
                alerts.append(Alert(
                    title=f'⚠️ {region} battery fleet SOC {pct:.1f}% — below {TRIGGER_PCT:.0f}%',
                    message=f'{region} fleet at {pct:.1f}%; trigger threshold {TRIGGER_PCT:.0f}%',
                    severity=self.severity,
                    source='battery_low_soc',
                    timestamp=ctx.now,
                    metadata={'region': region, 'soc_pct': pct, 'state': 'trigger'},
                    id=f'battery-low-soc-{region.lower()}',
                    dedup_key=f'low-soc-{region}',
                ))
                rec['active'] = True
                rec['triggered_at'] = ctx.now.isoformat()
            elif rec['active'] and pct > RECOVER_PCT:
                alerts.append(Alert(
                    title=f'✅ {region} battery fleet SOC {pct:.1f}% — recovered above {RECOVER_PCT:.0f}%',
                    message=f'{region} fleet recovered to {pct:.1f}%',
                    severity=AlertSeverity.INFO,
                    source='battery_low_soc',
                    timestamp=ctx.now,
                    metadata={'region': region, 'soc_pct': pct, 'state': 'recovery'},
                    id=f'battery-low-soc-recovery-{region.lower()}',
                    dedup_key=f'low-soc-{region}',
                ))
                rec['active'] = False
                rec['triggered_at'] = None

        if alerts:
            self._save_state(ctx.data_dir, state)
        else:
            # Still persist the initial state file (if missing) so the
            # next cycle finds initialised entries.
            if not (ctx.data_dir / STATE_FILENAME).exists():
                self._save_state(ctx.data_dir, state)
        return alerts

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
