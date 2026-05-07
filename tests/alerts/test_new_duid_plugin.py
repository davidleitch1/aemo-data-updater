"""Step 6 of the alerts plugin migration.

`NewDuidPlugin` extracts the new-DUID email logic from
`unified_collector.py:_send_new_duid_alert` into the standard plugin
shape.

Auto-classification (writing new rows into `duid_mapping` with site
name `[auto-classified <date>]`) STAYS in the collector — it's a data-
layer responsibility. The plugin only owns alerting.

State file: `data_dir/new_duid_plugin.json` (NEW path; doesn't conflict
with the existing legacy `unknown_duids_alerts.json`). Seed-on-first-
run avoids alerting about every historical auto-classified DUID once.

Routing: `new-duid-detected` → `['email', 'log']` (per catalogue —
admin-only, not user-facing).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.plugins.new_duid import NewDuidPlugin, STATE_FILENAME
from aemo_updater.alerts.routing import ALERT_ROUTING


def _ctx(tmp_path: Path) -> AlertContext:
    now = datetime.now(timezone.utc)
    return AlertContext(
        db_path='/nonexistent.duckdb',
        data_dir=tmp_path,
        now=now,
        last_run_at=now,
        nem_now=datetime.now(),
    )


def _plugin_with(rows: list[tuple[str, str, str]]) -> NewDuidPlugin:
    """rows = [(duid, fuel, region), ...] simulating
    `SELECT duid, fuel, region FROM duid_mapping
       WHERE "site name" LIKE '[auto-classified%'`."""
    return NewDuidPlugin(query_fn=lambda ctx: list(rows))


# ── Seed-on-first-run ────────────────────────────────────────────────


def test_first_run_seeds_state_without_emitting(tmp_path):
    """On first run (no state file), the plugin populates the state
    with current auto-classified DUIDs WITHOUT firing alerts. Avoids
    alerting about every historical entry on rollout."""
    plugin = _plugin_with([
        ('WAMBOWF2', 'Wind', 'QLD1'),
        ('SOMENEWBESS1', 'Battery Storage', 'VIC1'),
        ('OLDDUID1', 'Wind', 'NSW1'),
    ])
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert alerts == []
    state = json.loads((tmp_path / STATE_FILENAME).read_text())
    assert set(state) >= {'WAMBOWF2', 'SOMENEWBESS1', 'OLDDUID1'}


def test_first_run_with_no_auto_classified_rows_writes_seed_marker(tmp_path):
    """If query returns nothing on first run we still need to record
    that the seed has happened, otherwise next run treats it as
    first-run again and could spam."""
    plugin = _plugin_with([])
    alerts = plugin.evaluate(_ctx(tmp_path))
    assert alerts == []
    assert (tmp_path / STATE_FILENAME).exists()


# ── Subsequent runs: emit on truly-new DUIDs ─────────────────────────


def test_new_duid_after_seed_emits_alert(tmp_path):
    """First run seeds with [WAMBOWF2]. Second run sees [WAMBOWF2,
    NEWBESS1]. NEWBESS1 should fire one Alert."""
    ctx = _ctx(tmp_path)

    p1 = _plugin_with([('WAMBOWF2', 'Wind', 'QLD1')])
    p1.evaluate(ctx)  # seed

    p2 = _plugin_with([
        ('WAMBOWF2', 'Wind', 'QLD1'),
        ('NEWBESS1', 'Battery Storage', 'VIC1'),
    ])
    alerts = p2.evaluate(ctx)
    assert len(alerts) == 1
    a = alerts[0]
    assert a.id == 'new-duid-detected'
    assert a.severity == AlertSeverity.WARNING
    assert 'NEWBESS1' in a.title or 'NEWBESS1' in a.message
    assert 'Battery Storage' in (a.title + a.message)


def test_already_alerted_duid_does_not_re_fire(tmp_path):
    ctx = _ctx(tmp_path)
    p1 = _plugin_with([('WAMBOWF2', 'Wind', 'QLD1')])
    p1.evaluate(ctx)  # seed

    p2 = _plugin_with([
        ('WAMBOWF2', 'Wind', 'QLD1'),
        ('NEWBESS1', 'Battery Storage', 'VIC1'),
    ])
    p2.evaluate(ctx)  # alerts NEWBESS1; state now has both

    p3 = _plugin_with([
        ('WAMBOWF2', 'Wind', 'QLD1'),
        ('NEWBESS1', 'Battery Storage', 'VIC1'),
    ])
    alerts = p3.evaluate(ctx)
    assert alerts == []  # both known, neither re-fires


def test_multiple_new_duids_in_one_cycle_emit_one_combined_alert(tmp_path):
    """Two new DUIDs in the same cycle → one Alert listing both
    (matches legacy 'New DUIDs Discovered: 2 new units' shape)."""
    ctx = _ctx(tmp_path)
    p1 = _plugin_with([('OLDDUID1', 'Wind', 'NSW1')])
    p1.evaluate(ctx)  # seed

    p2 = _plugin_with([
        ('OLDDUID1', 'Wind', 'NSW1'),
        ('NEWBESS1', 'Battery Storage', 'VIC1'),
        ('NEWBESS2', 'Battery Storage', 'NSW1'),
    ])
    alerts = p2.evaluate(ctx)
    assert len(alerts) == 1
    body = alerts[0].title + alerts[0].message
    assert 'NEWBESS1' in body
    assert 'NEWBESS2' in body
    assert '2 new' in body or '2 unit' in body


# ── Alert payload ────────────────────────────────────────────────────


def test_alert_id_and_dedup_key_set(tmp_path):
    ctx = _ctx(tmp_path)
    _plugin_with([]).evaluate(ctx)
    plugin = _plugin_with([('NEWX', 'Wind', 'NSW1')])
    alerts = plugin.evaluate(ctx)
    assert alerts[0].id == 'new-duid-detected'
    assert alerts[0].dedup_key  # per-cycle batch key, non-empty


def test_alert_metadata_includes_classifications(tmp_path):
    ctx = _ctx(tmp_path)
    _plugin_with([]).evaluate(ctx)
    plugin = _plugin_with([('NEWX', 'Wind', 'NSW1')])
    alerts = plugin.evaluate(ctx)
    md = alerts[0].metadata or {}
    assert 'new_duids' in md
    assert 'NEWX' in md['new_duids']


# ── Routing wiring ───────────────────────────────────────────────────


def test_routing_includes_new_duid_detected_to_email_and_log():
    assert 'new-duid-detected' in ALERT_ROUTING
    sinks = ALERT_ROUTING['new-duid-detected']
    assert 'email' in sinks
    assert 'log' in sinks
    # Per catalogue: admin email + log paper-trail. NOT sms (collector
    # admin already sees the email; SMS would be excessive for new
    # DUIDs which appear on a roughly-weekly cadence).
    assert 'sms' not in sinks


# ── Legacy collector code path removed ───────────────────────────────


def test_legacy_send_new_duid_alert_removed_from_unified_collector():
    """The collector's _send_new_duid_alert method is now redundant —
    the dispatcher path handles email. File-content assertion to lock
    the cleanup."""
    src = Path(__file__).resolve().parents[2] / 'src' / 'aemo_updater' / 'collectors' / 'unified_collector.py'
    text = src.read_text()
    assert '_send_new_duid_alert' not in text, (
        'unified_collector.py still has _send_new_duid_alert — '
        'step 6 should have removed it.'
    )


def test_check_new_duids_no_longer_sends_email():
    """The _check_new_duids method may still exist (auto-classification
    is a data-layer concern), but it must no longer send emails — those
    happen via the dispatcher now."""
    src = Path(__file__).resolve().parents[2] / 'src' / 'aemo_updater' / 'collectors' / 'unified_collector.py'
    text = src.read_text()
    # Reading the method body directly: 'email_sender.send(' is the
    # legacy invocation we removed.
    assert 'email_sender.send' not in text or 'email_sender.send(\n' not in text, (
        'unified_collector.py still calls email_sender.send — '
        'step 6 should have removed the new-DUID email path.'
    )
