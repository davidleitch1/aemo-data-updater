"""Step 8 of the alerts plugin migration — battery plugins (shadow mode).

Two plugins extracted from `/Users/davidleitch/aemo_production/battery_monitor.py`:
  * `BatteryRecordsPlugin`  — 15 alerts (3 metrics × {NEM, 4 regions})
  * `BatteryLowSocPlugin`   — 8 alerts (4 regions × trigger + recovery)

**Shadow mode**: all 23 alert IDs route to `['log']` only at this step.
The standalone `battery_monitor.py` daemon keeps running as the source
of truth for SMS delivery. Plugins observe in parallel for ~24h,
operator compares plugin log entries to daemon SMS, then the cutover
commit (later) flips routing to `['sms', 'log']` and stops the daemon.

Plugin state files are separate from the daemon's
`battery_records.json` so the two paths don't write to the same file.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.plugins.battery_records import BatteryRecordsPlugin
from aemo_updater.alerts.plugins.battery_low_soc import BatteryLowSocPlugin
from aemo_updater.alerts.routing import ALERT_ROUTING


REGIONS = ('NSW1', 'QLD1', 'VIC1', 'SA1')
METRICS = ('soc_mwh', 'discharge_mw', 'charge_mw')


def _ctx(tmp_path: Path) -> AlertContext:
    now = datetime.now(timezone.utc)
    return AlertContext(
        db_path='/nonexistent.duckdb',
        data_dir=tmp_path,
        now=now,
        last_run_at=now,
        nem_now=datetime(2026, 5, 7, 12, 30),
    )


# ── BatteryRecordsPlugin ─────────────────────────────────────────────


def test_battery_records_first_run_seeds_state_without_emitting(tmp_path):
    """First run uses seed_fn to populate the state file with current
    all-time maxima; no alerts fire."""
    seed = {
        'nem':  {m: {'value': 13000.0, 'timestamp': '2026-04-12T15:55'} for m in METRICS},
        'NSW1': {m: {'value':  4900.0, 'timestamp': '2026-04-07T16:50'} for m in METRICS},
        'QLD1': {m: {'value':  4100.0, 'timestamp': '2026-03-28T15:55'} for m in METRICS},
        'VIC1': {m: {'value':  3300.0, 'timestamp': '2025-12-26T17:15'} for m in METRICS},
        'SA1':  {m: {'value':  1700.0, 'timestamp': '2026-05-04T15:10'} for m in METRICS},
    }
    plugin = BatteryRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: None,  # never reached on first run
    )
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert alerts == []
    state = json.loads((tmp_path / 'battery_records_plugin.json').read_text())
    assert state['NSW1']['soc_mwh']['value'] == 4900.0


def test_battery_records_new_record_emits_alert(tmp_path):
    """After seed: a higher reading should produce one Alert with the
    correct id and update the state's value."""
    ctx = _ctx(tmp_path)
    seed = {
        'nem':  {m: {'value': 1.0, 'timestamp': None} for m in METRICS},
        'NSW1': {m: {'value': 1.0, 'timestamp': None} for m in METRICS},
        'QLD1': {m: {'value': 1.0, 'timestamp': None} for m in METRICS},
        'VIC1': {m: {'value': 1.0, 'timestamp': None} for m in METRICS},
        'SA1':  {m: {'value': 1.0, 'timestamp': None} for m in METRICS},
    }
    p1 = BatteryRecordsPlugin(seed_fn=lambda ctx: seed, latest_fn=lambda ctx: None)
    p1.evaluate(ctx)  # seed

    # Now NSW1 SOC reading beats the seeded 1.0
    p2 = BatteryRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: {
            'timestamp': '2026-05-07 12:30',
            'NSW1': {'soc_mwh': 5000.0, 'discharge_mw': 0.5, 'charge_mw': 0.5},
            'QLD1': {'soc_mwh': 0.5, 'discharge_mw': 0.5, 'charge_mw': 0.5},
            'VIC1': {'soc_mwh': 0.5, 'discharge_mw': 0.5, 'charge_mw': 0.5},
            'SA1':  {'soc_mwh': 0.5, 'discharge_mw': 0.5, 'charge_mw': 0.5},
            'nem':  {'soc_mwh': 5002.0, 'discharge_mw': 2.0, 'charge_mw': 2.0},
        },
    )
    alerts = p2.evaluate(ctx)
    ids = {a.id for a in alerts}
    assert 'battery-soc-record-nsw1' in ids
    assert 'battery-soc-record-nem' in ids  # also a NEM-wide record
    state = json.loads((tmp_path / 'battery_records_plugin.json').read_text())
    assert state['NSW1']['soc_mwh']['value'] == 5000.0


def test_battery_records_no_record_emits_nothing(tmp_path):
    ctx = _ctx(tmp_path)
    seed = {
        scope: {m: {'value': 9999.0, 'timestamp': None} for m in METRICS}
        for scope in ('nem', *REGIONS)
    }
    p1 = BatteryRecordsPlugin(seed_fn=lambda ctx: seed, latest_fn=lambda ctx: None)
    p1.evaluate(ctx)

    p2 = BatteryRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: {
            'timestamp': '2026-05-07 12:30',
            'NSW1': {'soc_mwh': 100, 'discharge_mw': 100, 'charge_mw': 100},
            'QLD1': {'soc_mwh': 100, 'discharge_mw': 100, 'charge_mw': 100},
            'VIC1': {'soc_mwh': 100, 'discharge_mw': 100, 'charge_mw': 100},
            'SA1':  {'soc_mwh': 100, 'discharge_mw': 100, 'charge_mw': 100},
            'nem':  {'soc_mwh': 400, 'discharge_mw': 400, 'charge_mw': 400},
        },
    )
    assert p2.evaluate(ctx) == []


def test_battery_records_alert_metadata_carries_scope_metric_and_values(tmp_path):
    ctx = _ctx(tmp_path)
    seed = {
        scope: {m: {'value': 100.0, 'timestamp': None} for m in METRICS}
        for scope in ('nem', *REGIONS)
    }
    p1 = BatteryRecordsPlugin(seed_fn=lambda ctx: seed, latest_fn=lambda ctx: None)
    p1.evaluate(ctx)

    p2 = BatteryRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: {
            'timestamp': '2026-05-07 12:30',
            **{r: {m: 50.0 for m in METRICS} for r in REGIONS},
            'NSW1': {'soc_mwh': 200.0, 'discharge_mw': 50.0, 'charge_mw': 50.0},
            'nem':  {'soc_mwh': 350, 'discharge_mw': 200, 'charge_mw': 200},
        },
    )
    alerts = p2.evaluate(ctx)
    a = next(a for a in alerts if a.id == 'battery-soc-record-nsw1')
    assert a.metadata['scope'] == 'NSW1'
    assert a.metadata['metric'] == 'soc_mwh'
    assert a.metadata['new_value'] == 200.0
    assert a.metadata['old_value'] == 100.0


# ── BatteryLowSocPlugin ──────────────────────────────────────────────


def test_low_soc_drop_below_5pct_fires_trigger(tmp_path):
    plugin = BatteryLowSocPlugin(soc_pct_fn=lambda ctx: {
        'NSW1': 80.0, 'QLD1': 4.0, 'VIC1': 60.0, 'SA1': 50.0,
    })
    alerts = plugin.evaluate(_ctx(tmp_path))
    ids = {a.id for a in alerts}
    assert 'battery-low-soc-qld1' in ids
    state = json.loads((tmp_path / 'battery_low_soc_plugin.json').read_text())
    assert state['QLD1']['active'] is True


def test_low_soc_active_region_does_not_re_fire(tmp_path):
    ctx = _ctx(tmp_path)
    p1 = BatteryLowSocPlugin(soc_pct_fn=lambda ctx: {
        'NSW1': 80, 'QLD1': 4, 'VIC1': 60, 'SA1': 50,
    })
    p1.evaluate(ctx)  # arms QLD1

    p2 = BatteryLowSocPlugin(soc_pct_fn=lambda ctx: {
        'NSW1': 80, 'QLD1': 3, 'VIC1': 60, 'SA1': 50,  # still low
    })
    assert p2.evaluate(ctx) == []  # already armed; no re-fire


def test_low_soc_above_15pct_fires_recovery(tmp_path):
    ctx = _ctx(tmp_path)
    p1 = BatteryLowSocPlugin(soc_pct_fn=lambda ctx: {
        'NSW1': 80, 'QLD1': 4, 'VIC1': 60, 'SA1': 50,
    })
    p1.evaluate(ctx)  # arms QLD1

    p2 = BatteryLowSocPlugin(soc_pct_fn=lambda ctx: {
        'NSW1': 80, 'QLD1': 20, 'VIC1': 60, 'SA1': 50,  # recovered
    })
    alerts = p2.evaluate(ctx)
    ids = {a.id for a in alerts}
    assert 'battery-low-soc-recovery-qld1' in ids
    state = json.loads((tmp_path / 'battery_low_soc_plugin.json').read_text())
    assert state['QLD1']['active'] is False


def test_low_soc_hysteresis_dead_zone(tmp_path):
    """Inactive region between 5% and 15% must not trigger; active
    region between 5% and 15% must not recover."""
    ctx = _ctx(tmp_path)
    plugin = BatteryLowSocPlugin(soc_pct_fn=lambda ctx: {
        'NSW1': 10.0,  # in dead-zone, inactive → no alert
        'QLD1': 80.0, 'VIC1': 80.0, 'SA1': 80.0,
    })
    assert plugin.evaluate(ctx) == []


# ── Routing entries (shadow mode = log only) ─────────────────────────


def test_battery_routing_record_alerts_log_only_in_shadow_mode():
    for scope in ('nem', 'nsw1', 'qld1', 'vic1', 'sa1'):
        for metric in ('soc', 'discharge', 'charge'):
            alert_id = f'battery-{metric}-record-{scope}'
            assert alert_id in ALERT_ROUTING, f'{alert_id} missing from routing'
            assert ALERT_ROUTING[alert_id] == ['log'], (
                f'{alert_id} should be log-only in shadow mode (step 8); '
                f'cutover step will flip to sms+log'
            )


def test_battery_routing_low_soc_alerts_log_only_in_shadow_mode():
    for region in ('nsw1', 'qld1', 'vic1', 'sa1'):
        for kind in ('battery-low-soc', 'battery-low-soc-recovery'):
            alert_id = f'{kind}-{region}'
            assert alert_id in ALERT_ROUTING
            assert ALERT_ROUTING[alert_id] == ['log']
