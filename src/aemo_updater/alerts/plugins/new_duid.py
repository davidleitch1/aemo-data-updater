"""NewDuidPlugin — emit one Alert per cycle when previously-unseen
auto-classified DUIDs appear in `duid_mapping`.

The collector still does the auto-classification (writes new
DUID rows into `duid_mapping` with site name `[auto-classified <date>]`).
This plugin's job is alerting only:
  1. Query `duid_mapping` for rows whose site name matches
     `[auto-classified%`.
  2. Compare against this plugin's own state file
     (`<data_dir>/new_duid_plugin.json`).
  3. Emit one combined Alert listing the truly-new DUIDs.

State file is distinct from the legacy `unknown_duids_alerts.json` so
the two paths don't collide. **Seed-on-first-run** prevents the
plugin from alerting about every historical entry on rollout — the
first invocation populates the state without firing alerts; only
subsequent additions trigger.

Routing: `new-duid-detected` → `['email', 'log']` (admin-only,
roughly-weekly cadence; SMS would be excessive).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Optional

from ..base_alert import Alert, AlertSeverity
from ..context import AlertContext


logger = logging.getLogger(__name__)


STATE_FILENAME = 'new_duid_plugin.json'


DuidRow = tuple[str, str, str]
"""(duid, fuel, region)"""


def _default_query_fn(ctx: AlertContext) -> list[DuidRow]:  # pragma: no cover
    """Read auto-classified rows from the read-only DuckDB replica."""
    import duckdb
    conn = duckdb.connect(ctx.db_path, read_only=True)
    try:
        rows = conn.execute(
            """SELECT duid, fuel, region
                 FROM duid_mapping
                WHERE "site name" LIKE '[auto-classified%'
                ORDER BY duid""",
        ).fetchall()
    finally:
        conn.close()
    return [(d, f or 'Unknown', r or '') for d, f, r in rows]


class NewDuidPlugin:
    """Detect and alert on newly auto-classified DUIDs."""

    name = 'new_duid'
    severity = AlertSeverity.WARNING

    def __init__(self, query_fn: Optional[Callable[[AlertContext], Iterable[DuidRow]]] = None) -> None:
        self.query_fn = query_fn or _default_query_fn

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        rows = list(self.query_fn(ctx))
        state = self._load_state(ctx.data_dir)

        # Seed-on-first-run: state file missing or marker absent →
        # populate without firing.
        is_seed_run = '_seeded' not in state
        if is_seed_run:
            for duid, fuel, region in rows:
                state[duid] = {
                    'first_seen': ctx.now.isoformat(),
                    'last_alerted': None,
                    'fuel': fuel,
                    'region': region,
                }
            state['_seeded'] = ctx.now.isoformat()
            self._save_state(ctx.data_dir, state)
            logger.info(
                'NewDuidPlugin: seeded state with %d existing auto-classified DUIDs '
                '(no alerts fired on first run)',
                len(rows),
            )
            return []

        # Subsequent runs: alert on DUIDs not yet in state.
        new_rows: list[DuidRow] = [
            (d, f, r) for d, f, r in rows if d not in state
        ]
        if not new_rows:
            return []

        # One combined Alert per cycle (matches legacy 'New DUIDs
        # Discovered: N new units' shape).
        for duid, fuel, region in new_rows:
            state[duid] = {
                'first_seen': ctx.now.isoformat(),
                'last_alerted': ctx.now.isoformat(),
                'fuel': fuel,
                'region': region,
            }
        self._save_state(ctx.data_dir, state)

        return [self._build_alert(new_rows, ctx)]

    # ── Alert construction ───────────────────────────────────────────

    def _build_alert(self, new_rows: list[DuidRow], ctx: AlertContext) -> Alert:
        n = len(new_rows)
        lines = []
        for duid, fuel, region in sorted(new_rows):
            fuel_part = f'{fuel} ({region})' if region else fuel
            lines.append(f'  • {duid}  →  {fuel_part}')

        title = f'New DUIDs Discovered: {n} new unit{"s" if n != 1 else ""}'
        message = (
            'The following new DUIDs have been discovered in the AEMO data '
            'and auto-classified into duid_mapping:\n\n'
            + '\n'.join(lines)
            + '\n\nReview at your convenience and update Site Name / Owner / '
            'Capacity if desired (the auto-classification leaves these blank).'
        )
        return Alert(
            title=title,
            message=message,
            severity=self.severity,
            source='new_duid',
            timestamp=ctx.now,
            metadata={
                'new_duids': sorted(d for d, _, _ in new_rows),
                'classifications': {d: f for d, f, _ in new_rows},
                'count': n,
            },
            id='new-duid-detected',
            dedup_key=f'new-duid-{ctx.now.strftime("%Y%m%dT%H%M")}',
        )

    # ── State I/O ────────────────────────────────────────────────────

    def _load_state(self, data_dir: Path) -> dict:
        path = data_dir / STATE_FILENAME
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception:
            logger.exception('NewDuidPlugin: state file unreadable; starting fresh')
            return {}

    def _save_state(self, data_dir: Path, state: dict) -> None:
        path = data_dir / STATE_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, default=str))
