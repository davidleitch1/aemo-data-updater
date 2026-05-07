"""Step 4 of the alerts plugin migration.

`PriceBreachPlugin` extracts the per-region armed/disarmed state
machine from `collectors/twilio_price_alerts.py:check_price_alerts`
into the standard plugin shape.

Semantics preserved exactly:
  * HIGH_THRESHOLD = 1000.0  — arm + emit `spot-price-high-breach`
  * EXTREME_THRESHOLD = 10000 — arm + emit `spot-price-extreme-spike`
  * LOW_THRESHOLD = 300.0    — disarm + emit `spot-price-recovery`
  * State per-region: { armed: bool, armed_at: datetime, last_price: float }
  * Each row processed in settlement-date order; state mutates row by row.
  * `last_price` updates regardless of whether an alert fired.

State file is `data_dir/price_breach.json` (new path — does NOT collide
with the legacy `price_alert_state.pkl` so the old + new paths can run
in parallel during the migration).

Routing for spot-price-* IDs is intentionally `['log']` only at this
step. Step 5 flips to `['sms', 'log']` and deletes the old path
simultaneously to avoid duplicate SMS.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.plugins.price_breach import (
    PriceBreachPlugin,
    HIGH_THRESHOLD,
    LOW_THRESHOLD,
    EXTREME_THRESHOLD,
)
from aemo_updater.alerts.routing import ALERT_ROUTING


def _ctx(tmp_path: Path) -> AlertContext:
    now = datetime.now(timezone.utc)
    return AlertContext(
        db_path='/nonexistent/test.duckdb',
        data_dir=tmp_path,
        now=now,
        last_run_at=now,
        nem_now=datetime(2026, 5, 7, 18, 25),  # NEM-naive
    )


def _plugin_with(prices: list[tuple[datetime, str, float]]) -> PriceBreachPlugin:
    """Build a plugin whose DB query returns the given price tuples."""
    return PriceBreachPlugin(query_fn=lambda ctx: list(prices))


# ── Constants are right ──────────────────────────────────────────────


def test_plugin_thresholds_match_legacy():
    """If these drift, behaviour drifts. Regression-guard."""
    assert HIGH_THRESHOLD == 1000.0
    assert LOW_THRESHOLD == 300.0
    assert EXTREME_THRESHOLD == 10000.0


# ── State machine: arming ────────────────────────────────────────────


def test_first_high_price_emits_high_breach(tmp_path):
    plugin = _plugin_with([(datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0)])
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert len(alerts) == 1
    a = alerts[0]
    assert a.id == 'spot-price-high-breach'
    assert a.severity == AlertSeverity.CRITICAL
    assert 'NSW1' in a.title
    # state file was written
    state_file = tmp_path / 'price_breach.json'
    assert state_file.exists()
    data = json.loads(state_file.read_text())
    assert data['NSW1']['armed'] is True


def test_extreme_price_emits_extreme_spike(tmp_path):
    plugin = _plugin_with([(datetime(2026, 5, 7, 18, 25), 'SA1', 14200.0)])
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert len(alerts) == 1
    assert alerts[0].id == 'spot-price-extreme-spike'
    assert alerts[0].severity == AlertSeverity.CRITICAL


def test_already_armed_region_emits_nothing_on_subsequent_high(tmp_path):
    """Second 5-min interval at >$1k for the same already-armed region
    must NOT re-fire."""
    ctx = _ctx(tmp_path)
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),
        (datetime(2026, 5, 7, 18, 30), 'NSW1', 1500.0),
    ])
    alerts = plugin.evaluate(ctx)
    assert len(alerts) == 1
    assert alerts[0].id == 'spot-price-high-breach'


# ── State machine: disarming / recovery ──────────────────────────────


def test_armed_region_disarms_when_price_drops_below_low(tmp_path):
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),  # arm
        (datetime(2026, 5, 7, 19, 30), 'NSW1', 250.0),   # disarm
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert [a.id for a in alerts] == ['spot-price-high-breach', 'spot-price-recovery']
    assert alerts[1].severity == AlertSeverity.INFO
    state = json.loads((tmp_path / 'price_breach.json').read_text())
    assert state['NSW1']['armed'] is False


def test_recovery_message_includes_duration(tmp_path):
    """The legacy module includes 'Duration: Xh Ym' in the recovery
    message. Preserve that — the user reads it operationally."""
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),
        (datetime(2026, 5, 7, 19, 30), 'NSW1', 250.0),
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    recovery = next(a for a in alerts if a.id == 'spot-price-recovery')
    assert 'Duration' in recovery.title or 'Duration' in recovery.message
    # 65 min armed → "1h 5m"
    assert '1h 5m' in (recovery.title + recovery.message) or \
           '65m' in (recovery.title + recovery.message)


def test_armed_then_low_then_low_only_fires_recovery_once(tmp_path):
    """Once a region disarms, subsequent low prices must NOT re-fire
    recovery (it's the disarm transition, not the low-price level,
    that triggers)."""
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),  # arm
        (datetime(2026, 5, 7, 19, 30), 'NSW1', 250.0),   # disarm + recovery
        (datetime(2026, 5, 7, 19, 35), 'NSW1', 200.0),   # nothing — already disarmed
        (datetime(2026, 5, 7, 19, 40), 'NSW1', 180.0),   # nothing
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    ids = [a.id for a in alerts]
    assert ids == ['spot-price-high-breach', 'spot-price-recovery']


# ── Region isolation ─────────────────────────────────────────────────


def test_high_breach_in_one_region_does_not_disturb_others(tmp_path):
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),
        (datetime(2026, 5, 7, 18, 30), 'VIC1', 90.0),    # normal price, irrelevant
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    state = json.loads((tmp_path / 'price_breach.json').read_text())
    assert state['NSW1']['armed'] is True
    assert state['VIC1']['armed'] is False


def test_multiple_regions_arm_independently(tmp_path):
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),
        (datetime(2026, 5, 7, 18, 25), 'SA1', 14200.0),
        (datetime(2026, 5, 7, 18, 30), 'VIC1', 1100.0),
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    ids = sorted(a.id for a in alerts)
    assert ids == ['spot-price-extreme-spike',
                   'spot-price-high-breach', 'spot-price-high-breach']


# ── State persistence across plugin instances ────────────────────────


def test_state_persists_across_plugin_restarts(tmp_path):
    """Two plugin instances sharing the same data_dir → second run
    sees the first run's armed state and skips re-firing."""
    ctx = _ctx(tmp_path)
    p1 = _plugin_with([(datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0)])
    alerts1 = p1.evaluate(ctx)
    assert len(alerts1) == 1

    p2 = _plugin_with([(datetime(2026, 5, 7, 18, 30), 'NSW1', 1500.0)])
    alerts2 = p2.evaluate(ctx)  # shouldn't re-fire — already armed
    assert alerts2 == []


# ── dedup_key set ────────────────────────────────────────────────────


def test_breach_alerts_have_dedup_key_per_region(tmp_path):
    """The dispatcher's eventual rate-limit/aggregation will key off
    `dedup_key`. Each region must have a unique dedup_key."""
    plugin = _plugin_with([
        (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),
        (datetime(2026, 5, 7, 18, 25), 'SA1', 1420.0),
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    keys = {a.dedup_key for a in alerts}
    assert len(keys) == 2  # two distinct regions, two distinct dedup_keys


# ── Routing wiring ───────────────────────────────────────────────────


def test_routing_table_includes_price_alerts():
    """All three price-alert IDs must be present in ALERT_ROUTING.
    Step-specific channel assertions live in
    test_step5_cutover.test_routing_for_price_alerts_now_includes_sms."""
    assert 'spot-price-high-breach' in ALERT_ROUTING
    assert 'spot-price-extreme-spike' in ALERT_ROUTING
    assert 'spot-price-recovery' in ALERT_ROUTING


# ── No prices → no alerts ────────────────────────────────────────────


def test_empty_query_returns_no_alerts(tmp_path):
    plugin = _plugin_with([])
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert alerts == []
    assert not (tmp_path / 'price_breach.json').exists()  # no state to write
