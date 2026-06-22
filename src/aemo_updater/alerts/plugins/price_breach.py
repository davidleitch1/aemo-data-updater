"""PriceBreachPlugin — per-region high/extreme/recovery price alerts.

Drop-in replacement for the legacy
`collectors/twilio_price_alerts.py:check_price_alerts` state machine.
Same thresholds, same per-region armed/disarmed semantics, same
recovery-with-duration message.

State file: `<data_dir>/price_breach.json` — JSON, distinct from the
legacy `price_alert_state.pkl` so the old + new paths can run in
parallel during migration. Each region: `{armed, armed_at, last_price}`.

Alert IDs emitted (catalogued in
aemo-energy-dashboard2/docs/alerts.md):
  * spot-price-high-breach   — armed at >= $1k/MWh (and not extreme)
  * spot-price-extreme-spike — armed at >= $10k/MWh
  * spot-price-recovery      — disarmed (price <= $300/MWh)
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional

from ..base_alert import Alert, AlertSeverity
from ..context import AlertContext


logger = logging.getLogger(__name__)


HIGH_THRESHOLD = 1000.0
LOW_THRESHOLD = 300.0
EXTREME_THRESHOLD = 10000.0

STATE_FILENAME = 'price_breach.json'

VALID_REGIONS = ('NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1')

# AEMO stores settlementdate as naive AEST (UTC+10, no DST). The
# incremental watermark `ctx.last_run_at` is UTC-aware, so it must be
# converted to NEM-naive before comparing against settlementdate.
_NEM_TZ = timezone(timedelta(hours=10))


PriceRow = tuple[datetime, str, float]
"""(settlementdate_nem_naive, regionid, rrp)"""


def _default_query_fn(ctx: AlertContext) -> list[PriceRow]:  # pragma: no cover
    """Read price rows newer than `ctx.last_run_at` from the live DuckDB.

    The connection is opened per-call (~1ms) so concurrent reads / the
    collector's rename-swap of aemo_readonly.duckdb don't hold a lock.
    """
    import duckdb

    # Convert the UTC-aware watermark to NEM-naive (UTC+10) so it is
    # comparable to settlementdate. Using ctx.nem_now.tzinfo here was a
    # bug: nem_now is already naive (tzinfo=None), so the conversion fell
    # back to UTC and every cycle re-read ~10h of prices, re-firing the
    # same high/recovery alerts.
    last_run_nem = ctx.last_run_at.astimezone(_NEM_TZ).replace(tzinfo=None)
    conn = duckdb.connect(ctx.db_path, read_only=True)
    try:
        rows = conn.execute(
            """SELECT settlementdate, regionid, rrp
                 FROM prices5
                WHERE settlementdate > ?
                ORDER BY settlementdate ASC""",
            [last_run_nem],
        ).fetchall()
    finally:
        conn.close()
    return [(ts, regid, float(rrp)) for ts, regid, rrp in rows]


class PriceBreachPlugin:
    """Per-region spot price state machine."""

    name = 'price_breach'
    severity = AlertSeverity.CRITICAL

    def __init__(self, query_fn: Optional[Callable[[AlertContext], Iterable[PriceRow]]] = None) -> None:
        self.query_fn = query_fn or _default_query_fn

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        prices = list(self.query_fn(ctx))
        if not prices:
            return []
        state = self._load_state(ctx.data_dir)
        alerts, new_state = _process_prices(prices, state)
        # Always persist updated state (last_price tracking even when
        # no alert fired keeps state warm for next cycle).
        self._save_state(ctx.data_dir, new_state)
        return alerts

    # ── State I/O ─────────────────────────────────────────────────────

    def _load_state(self, data_dir: Path) -> dict:
        path = data_dir / STATE_FILENAME
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception:
            logger.exception('PriceBreachPlugin: state file unreadable, starting fresh')
            return {}

    def _save_state(self, data_dir: Path, state: dict) -> None:
        path = data_dir / STATE_FILENAME
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2, default=str))


# ── Pure logic (no I/O, easy to test) ────────────────────────────────


def _process_prices(prices: Iterable[PriceRow], state: dict) -> tuple[list[Alert], dict]:
    """Run the state machine over the price rows in order.
    Returns (alerts_to_emit, updated_state). Caller persists state."""
    alerts: list[Alert] = []
    state = dict(state)  # shallow copy; per-region mutation is OK

    for ts, region, price in prices:
        # Initialise region state lazily.
        rec = state.setdefault(region, {
            'armed': False,
            'armed_at': None,
            'last_price': 0.0,
        })

        # ── Arm ───
        if price >= HIGH_THRESHOLD and not rec['armed']:
            extreme = price >= EXTREME_THRESHOLD
            alert_id = 'spot-price-extreme-spike' if extreme else 'spot-price-high-breach'
            emoji = '🚨🚨🚨' if extreme else '⚠️'
            urgency = 'EXTREME' if extreme else 'HIGH'

            # Title doubles as the SMS body in the legacy module —
            # preserve the format so step 5 SMS output is byte-identical.
            title = (
                f'ITK price alert {emoji} {region} {urgency} PRICE: '
                f'${price:.2f}/MWh at {ts.strftime("%H:%M on %d/%m/%Y")}. '
                f'Threshold: ${HIGH_THRESHOLD:.0f}'
            )
            alerts.append(Alert(
                title=title,
                message=f'{region} price ${price:.2f} at {ts.isoformat()}',
                severity=AlertSeverity.CRITICAL,
                source='price_breach',
                timestamp=ts,
                metadata={'region': region, 'price': price,
                          'threshold': HIGH_THRESHOLD,
                          'extreme': extreme},
                id=alert_id,
                dedup_key=f'price-{region}',
            ))
            rec['armed'] = True
            rec['armed_at'] = ts.isoformat() if isinstance(ts, datetime) else ts

        # ── Disarm + recovery ───
        elif price <= LOW_THRESHOLD and rec['armed']:
            armed_at = rec['armed_at']
            duration_str = ''
            if armed_at:
                try:
                    armed_dt = (
                        datetime.fromisoformat(armed_at)
                        if isinstance(armed_at, str)
                        else armed_at
                    )
                    delta = ts - armed_dt
                    total_min = int(delta.total_seconds() / 60)
                    h, m = divmod(total_min, 60)
                    duration_str = f'Duration: {h}h {m}m' if h else f'Duration: {m}m'
                except Exception:  # malformed timestamp → just omit
                    pass

            title = (
                f'ITK price alert ✅ {region} PRICE RECOVERED: '
                f'${price:.2f}/MWh at {ts.strftime("%H:%M on %d/%m/%Y")}. '
                f'Below ${LOW_THRESHOLD:.0f}. {duration_str}'.strip()
            )
            alerts.append(Alert(
                title=title,
                message=f'{region} recovered to ${price:.2f}',
                severity=AlertSeverity.INFO,
                source='price_breach',
                timestamp=ts,
                metadata={'region': region, 'price': price,
                          'duration_str': duration_str},
                id='spot-price-recovery',
                dedup_key=f'price-{region}',
            ))
            rec['armed'] = False
            rec['armed_at'] = None

        # last_price always updates regardless
        rec['last_price'] = price

    return alerts, state
