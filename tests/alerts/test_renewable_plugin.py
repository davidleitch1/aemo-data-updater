"""Step 9 of the alerts plugin migration — RenewableRecordsPlugin
(shadow mode).

Extracted from
`/Users/davidleitch/aemo_production/aemo-energy-dashboard2/standalone_renewable_gauge_with_alerts_updated.py`.

Five single-value metrics (NEM-wide all-time max):
  * renewable-record-percentage  — renewable share of demand (%)
  * wind-record-mw               — utility wind output peak (MW)
  * solar-record-mw              — utility solar peak (MW)
  * hydro-record-mw              — hydro peak (MW)
  * rooftop-solar-record-mw      — rooftop solar peak (MW)

Shadow mode: routes to ['log'] only at this step. Standalone gauge
daemon (tmux services:2) keeps firing SMS as the source of truth.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.plugins.renewable_records import (
    RenewableRecordsPlugin,
    METRICS,
    METRIC_TO_ALERT_ID,
    STATE_FILENAME,
)
from aemo_updater.alerts.routing import ALERT_ROUTING


def _ctx(tmp_path: Path) -> AlertContext:
    now = datetime.now(timezone.utc)
    return AlertContext(
        db_path='/nonexistent.duckdb',
        data_dir=tmp_path,
        now=now,
        last_run_at=now,
        nem_now=datetime(2026, 5, 7, 12, 30),
    )


# ── Seeding ──────────────────────────────────────────────────────────


def test_first_run_seeds_state_without_emitting(tmp_path):
    seed = {
        'renewable_pct': {'value': 78.0, 'timestamp': '2026-04-01T12:00'},
        'wind_mw':       {'value': 12000.0, 'timestamp': '2026-03-15T19:00'},
        'solar_mw':      {'value':  9000.0, 'timestamp': '2026-02-10T13:00'},
        'water_mw':      {'value':  6500.0, 'timestamp': '2025-09-22T18:00'},
        'rooftop_mw':    {'value': 14500.0, 'timestamp': '2026-01-05T13:00'},
    }
    plugin = RenewableRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: None,
    )
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert alerts == []
    state = json.loads((tmp_path / STATE_FILENAME).read_text())
    assert state['wind_mw']['value'] == 12000.0


# ── Subsequent runs ──────────────────────────────────────────────────


def test_new_record_emits_one_alert_per_metric(tmp_path):
    ctx = _ctx(tmp_path)
    seed = {m: {'value': 1.0, 'timestamp': None} for m in METRICS}
    p1 = RenewableRecordsPlugin(seed_fn=lambda ctx: seed, latest_fn=lambda ctx: None)
    p1.evaluate(ctx)

    p2 = RenewableRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: {
            'timestamp': '2026-05-07T12:30',
            'renewable_pct': 0.5,  # below seeded
            'wind_mw':       13000.0,  # NEW RECORD
            'solar_mw':      0.5,
            'water_mw':      0.5,
            'rooftop_mw':    0.5,
        },
    )
    alerts = p2.evaluate(ctx)
    assert len(alerts) == 1
    assert alerts[0].id == 'wind-record-mw'
    assert alerts[0].metadata['new_value'] == 13000.0


def test_no_new_record_no_alerts(tmp_path):
    ctx = _ctx(tmp_path)
    seed = {m: {'value': 9999.0, 'timestamp': None} for m in METRICS}
    p1 = RenewableRecordsPlugin(seed_fn=lambda ctx: seed, latest_fn=lambda ctx: None)
    p1.evaluate(ctx)

    p2 = RenewableRecordsPlugin(
        seed_fn=lambda ctx: seed,
        latest_fn=lambda ctx: {m: 100.0 for m in METRICS},
    )
    assert p2.evaluate(ctx) == []


def test_metric_to_alert_id_mapping():
    """The five canonical alert IDs from docs/alerts.md."""
    assert METRIC_TO_ALERT_ID['renewable_pct'] == 'renewable-record-percentage'
    assert METRIC_TO_ALERT_ID['wind_mw'] == 'wind-record-mw'
    assert METRIC_TO_ALERT_ID['solar_mw'] == 'solar-record-mw'
    assert METRIC_TO_ALERT_ID['water_mw'] == 'hydro-record-mw'
    assert METRIC_TO_ALERT_ID['rooftop_mw'] == 'rooftop-solar-record-mw'


# ── Routing entries (shadow mode = log only) ─────────────────────────


def test_routing_renewable_records_log_only_in_shadow_mode():
    for alert_id in ('renewable-record-percentage', 'wind-record-mw',
                     'solar-record-mw', 'hydro-record-mw',
                     'rooftop-solar-record-mw'):
        assert alert_id in ALERT_ROUTING, f'{alert_id} missing from routing'
        assert ALERT_ROUTING[alert_id] == ['log'], (
            f'{alert_id} should be log-only in shadow mode (step 9)'
        )
