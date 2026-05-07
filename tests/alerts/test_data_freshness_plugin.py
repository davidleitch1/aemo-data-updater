"""Step 7 of the alerts plugin migration.

`DataFreshnessPlugin` replaces the legacy
`alert_manager.py:check_data_freshness` (which checked parquet file
mtimes — those files no longer exist; the collector writes DuckDB
directly).

Modern semantics: query `MAX(settlementdate)` per critical DuckDB
table, compute lag against `ctx.nem_now`, fire `data-file-stale` when
lag exceeds the per-table threshold. If the table itself is missing or
the DuckDB file doesn't exist, fire `data-file-missing`.

Per-table state file `data_dir/data_freshness_plugin.json` carries the
last-alerted timestamp per table for 60-min throttling — prevents the
same outage producing N alerts over N cycles.

Routing per catalogue:
  * `data-file-stale`   → email + log (admin operational)
  * `data-file-missing` → email + sms + log (escalated)
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.plugins.data_freshness import (
    DataFreshnessPlugin,
    DEFAULT_THRESHOLDS,
    STATE_FILENAME,
    THROTTLE_MINUTES,
)
from aemo_updater.alerts.routing import ALERT_ROUTING


def _make_test_db(tmp_path: Path, max_settlements: dict[str, datetime]) -> Path:
    """Build a tiny DuckDB with the given (table → max(settlementdate))."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    path = tmp_path / 'test_freshness.duckdb'
    conn = duckdb.connect(str(path))
    try:
        for table, max_dt in max_settlements.items():
            conn.execute(f'CREATE TABLE {table} (settlementdate TIMESTAMP, val DOUBLE)')
            conn.execute(f'INSERT INTO {table} VALUES (?, 1.0)', [max_dt])
    finally:
        conn.close()
    return path


def _ctx(tmp_path: Path, db_path: Path, nem_now: datetime) -> AlertContext:
    return AlertContext(
        db_path=str(db_path),
        data_dir=tmp_path,
        now=datetime.now(timezone.utc),
        last_run_at=datetime.now(timezone.utc),
        nem_now=nem_now,
    )


# ── Fresh data → no alerts ───────────────────────────────────────────


def test_fresh_data_emits_no_alerts(tmp_path):
    nem_now = datetime(2026, 5, 7, 12, 30)
    db = _make_test_db(tmp_path, {
        'prices5': nem_now - timedelta(minutes=2),
        'scada5':  nem_now - timedelta(minutes=2),
    })
    plugin = DataFreshnessPlugin(thresholds={'prices5': 30, 'scada5': 30})
    alerts = plugin.evaluate(_ctx(tmp_path, db, nem_now))
    assert alerts == []


# ── Stale data → data-file-stale ─────────────────────────────────────


def test_stale_table_emits_data_file_stale(tmp_path):
    nem_now = datetime(2026, 5, 7, 12, 30)
    db = _make_test_db(tmp_path, {
        'prices5': nem_now - timedelta(minutes=90),  # way over 30min
    })
    plugin = DataFreshnessPlugin(thresholds={'prices5': 30})
    alerts = plugin.evaluate(_ctx(tmp_path, db, nem_now))
    assert len(alerts) == 1
    a = alerts[0]
    assert a.id == 'data-file-stale'
    assert a.severity in (AlertSeverity.WARNING, AlertSeverity.ERROR)
    assert 'prices5' in a.title
    assert (a.metadata or {}).get('table') == 'prices5'
    assert (a.metadata or {}).get('age_minutes', 0) >= 90


def test_only_stale_tables_emit_alerts(tmp_path):
    """Mix of fresh + stale: only stale fires."""
    nem_now = datetime(2026, 5, 7, 12, 30)
    db = _make_test_db(tmp_path, {
        'prices5': nem_now - timedelta(minutes=2),    # fresh
        'scada5':  nem_now - timedelta(minutes=90),   # stale
    })
    plugin = DataFreshnessPlugin(thresholds={'prices5': 30, 'scada5': 30})
    alerts = plugin.evaluate(_ctx(tmp_path, db, nem_now))
    ids_tables = [(a.id, (a.metadata or {}).get('table')) for a in alerts]
    assert ('data-file-stale', 'scada5') in ids_tables
    assert ('data-file-stale', 'prices5') not in ids_tables


# ── Missing DB / table → data-file-missing ───────────────────────────


def test_missing_db_emits_data_file_missing(tmp_path):
    bogus_db = tmp_path / 'does-not-exist.duckdb'
    nem_now = datetime(2026, 5, 7, 12, 30)
    plugin = DataFreshnessPlugin(thresholds={'prices5': 30})
    alerts = plugin.evaluate(_ctx(tmp_path, bogus_db, nem_now))
    # Expect at least one data-file-missing alert
    missing = [a for a in alerts if a.id == 'data-file-missing']
    assert missing, f'expected data-file-missing alert; got {[a.id for a in alerts]}'


def test_missing_table_emits_data_file_missing(tmp_path):
    nem_now = datetime(2026, 5, 7, 12, 30)
    # DB exists but doesn't have prices5
    db = _make_test_db(tmp_path, {'scada5': nem_now - timedelta(minutes=2)})
    plugin = DataFreshnessPlugin(thresholds={'prices5': 30, 'scada5': 30})
    alerts = plugin.evaluate(_ctx(tmp_path, db, nem_now))
    ids_tables = [(a.id, (a.metadata or {}).get('table')) for a in alerts]
    assert ('data-file-missing', 'prices5') in ids_tables


# ── Throttling ───────────────────────────────────────────────────────


def test_throttle_suppresses_repeat_within_window(tmp_path):
    """Same stale state on two consecutive cycles — second cycle must
    not re-fire. After the throttle window expires, we'd re-fire (not
    tested here; relies on time math)."""
    nem_now = datetime(2026, 5, 7, 12, 30)
    db = _make_test_db(tmp_path, {'prices5': nem_now - timedelta(minutes=90)})

    p1 = DataFreshnessPlugin(thresholds={'prices5': 30})
    alerts1 = p1.evaluate(_ctx(tmp_path, db, nem_now))
    assert len(alerts1) == 1

    p2 = DataFreshnessPlugin(thresholds={'prices5': 30})
    alerts2 = p2.evaluate(_ctx(tmp_path, db, nem_now + timedelta(minutes=5)))
    assert alerts2 == []


def test_throttle_clears_after_window(tmp_path):
    nem_now = datetime(2026, 5, 7, 12, 30)
    db = _make_test_db(tmp_path, {'prices5': nem_now - timedelta(minutes=90)})

    p1 = DataFreshnessPlugin(thresholds={'prices5': 30})
    p1.evaluate(_ctx(tmp_path, db, nem_now))

    # Bump nem_now beyond throttle window — alert fires again.
    p2 = DataFreshnessPlugin(thresholds={'prices5': 30})
    later = nem_now + timedelta(minutes=THROTTLE_MINUTES + 1)
    db2 = _make_test_db(tmp_path / 'd2', {'prices5': later - timedelta(minutes=90)})
    alerts2 = p2.evaluate(_ctx(tmp_path, db2, later))
    assert len(alerts2) == 1


# ── Defaults ─────────────────────────────────────────────────────────


def test_default_thresholds_cover_critical_tables():
    """Plugin used with no overrides must monitor at least the
    critical 5-min tables: prices5, scada5, transmission5."""
    for t in ('prices5', 'scada5', 'transmission5'):
        assert t in DEFAULT_THRESHOLDS, f'{t} missing from DEFAULT_THRESHOLDS'


def test_dedup_key_per_table(tmp_path):
    nem_now = datetime(2026, 5, 7, 12, 30)
    db = _make_test_db(tmp_path, {
        'prices5': nem_now - timedelta(minutes=90),
        'scada5':  nem_now - timedelta(minutes=90),
    })
    plugin = DataFreshnessPlugin(thresholds={'prices5': 30, 'scada5': 30})
    alerts = plugin.evaluate(_ctx(tmp_path, db, nem_now))
    keys = {a.dedup_key for a in alerts}
    assert len(keys) == 2  # one per table


# ── Routing entries ──────────────────────────────────────────────────


def test_routing_data_file_stale_is_email_log_only():
    sinks = ALERT_ROUTING['data-file-stale']
    assert 'email' in sinks
    assert 'log' in sinks
    assert 'sms' not in sinks  # admin email only; SMS escalation reserved for missing


def test_routing_data_file_missing_includes_sms():
    sinks = ALERT_ROUTING['data-file-missing']
    assert 'email' in sinks
    assert 'sms' in sinks
    assert 'log' in sinks
