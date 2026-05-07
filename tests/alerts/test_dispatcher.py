"""Step 2 of the alerts plugin migration.

Adds the dispatcher skeleton:
  * `AlertContext` dataclass — carries db_path / data_dir / now / etc
    and is passed to every plugin's evaluate().
  * `AlertDispatcher` class — iterates plugins, looks up sinks per
    alert via the routing table, fans out, catches exceptions on
    both sides so one failure doesn't block others.
  * `routing.py` — `ALERT_ROUTING: dict[str, list[str]]` (empty for
    now) + `DEFAULT_SINKS = ['log']` for unrouted alert IDs.
  * `sinks/log.py` — `LogSink` always-on emitter that writes to the
    Python logger.

No real plugins or channels yet — pure scaffolding.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.dispatcher import AlertDispatcher
from aemo_updater.alerts.routing import ALERT_ROUTING, DEFAULT_SINKS
from aemo_updater.alerts.sinks.log import LogSink


# ── Fakes ────────────────────────────────────────────────────────────


class FakePlugin:
    name = 'fake'

    def __init__(self, emits: list[Alert] | None = None, raises: Exception | None = None):
        self.emits = emits or []
        self.raises = raises
        self.evaluate_calls = 0

    def evaluate(self, ctx: AlertContext) -> list[Alert]:
        self.evaluate_calls += 1
        if self.raises:
            raise self.raises
        return list(self.emits)


class FakeSink:
    def __init__(self, name: str = 'fake', enabled: bool = True, raises: Exception | None = None):
        self.name = name
        self.enabled = enabled
        self.raises = raises
        self.emitted: list[Alert] = []

    def emit(self, alert: Alert) -> None:
        if self.raises:
            raise self.raises
        self.emitted.append(alert)


def _alert(alert_id: str = 'test-alert', severity: AlertSeverity = AlertSeverity.INFO) -> Alert:
    return Alert(
        title=alert_id, message='body',
        severity=severity, source='test',
        id=alert_id,
    )


def _ctx(tmp_path: Path) -> AlertContext:
    now = datetime.now(timezone.utc)
    return AlertContext(
        db_path='/nonexistent/test.duckdb',
        data_dir=tmp_path,
        now=now,
        last_run_at=now,
        nem_now=datetime.now(),
    )


# ── AlertContext ─────────────────────────────────────────────────────


def test_alert_context_has_required_fields(tmp_path):
    ctx = _ctx(tmp_path)
    assert ctx.db_path == '/nonexistent/test.duckdb'
    assert ctx.data_dir == tmp_path
    assert ctx.now is not None
    assert ctx.last_run_at is not None
    assert ctx.nem_now is not None


# ── Routing ──────────────────────────────────────────────────────────


def test_routing_table_starts_empty():
    """At step 2 the routing table is intentionally empty — plugins are
    wired in later steps. DEFAULT_SINKS catches everything."""
    assert ALERT_ROUTING == {}
    assert DEFAULT_SINKS == ['log']


# ── Dispatcher.run_cycle: golden path ────────────────────────────────


def test_dispatcher_calls_plugin_evaluate(tmp_path):
    plugin = FakePlugin(emits=[_alert()])
    sink = FakeSink(name='log')
    d = AlertDispatcher(plugins=[plugin], sinks={'log': sink}, routing={'test-alert': ['log']})
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert plugin.evaluate_calls == 1


def test_dispatcher_fans_out_via_routing_table(tmp_path):
    plugin = FakePlugin(emits=[_alert('multi-channel-alert')])
    sms = FakeSink(name='sms')
    email = FakeSink(name='email')
    d = AlertDispatcher(
        plugins=[plugin],
        sinks={'sms': sms, 'email': email},
        routing={'multi-channel-alert': ['sms', 'email']},
    )
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert len(sms.emitted) == 1
    assert len(email.emitted) == 1


def test_dispatcher_uses_default_sinks_for_unrouted_alert(tmp_path):
    plugin = FakePlugin(emits=[_alert('unrouted-alert')])
    log = FakeSink(name='log')
    d = AlertDispatcher(plugins=[plugin], sinks={'log': log}, routing={})
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert len(log.emitted) == 1


def test_dispatcher_skips_disabled_sinks(tmp_path):
    plugin = FakePlugin(emits=[_alert()])
    disabled = FakeSink(name='log', enabled=False)
    d = AlertDispatcher(plugins=[plugin], sinks={'log': disabled}, routing={'test-alert': ['log']})
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert disabled.emitted == []


def test_dispatcher_skips_unknown_sink_names_silently(tmp_path):
    """Routing references a sink name that wasn't registered — should
    skip, not raise."""
    plugin = FakePlugin(emits=[_alert()])
    log = FakeSink(name='log')
    d = AlertDispatcher(
        plugins=[plugin],
        sinks={'log': log},
        routing={'test-alert': ['nonexistent', 'log']},
    )
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert len(log.emitted) == 1  # log still got the alert despite nonexistent sink


# ── Dispatcher.run_cycle: error isolation ────────────────────────────


def test_dispatcher_isolates_plugin_exceptions(tmp_path):
    """A plugin raising must not block subsequent plugins."""
    bad = FakePlugin(raises=RuntimeError('plugin boom'))
    good = FakePlugin(emits=[_alert('good-alert')])
    log = FakeSink(name='log')
    d = AlertDispatcher(plugins=[bad, good], sinks={'log': log}, routing={'good-alert': ['log']})
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert good.evaluate_calls == 1
    assert len(log.emitted) == 1


def test_dispatcher_isolates_sink_exceptions(tmp_path):
    """One sink raising must not block other sinks for the same alert."""
    plugin = FakePlugin(emits=[_alert('multi-sink-alert')])
    bad = FakeSink(name='sms', raises=RuntimeError('sink boom'))
    good = FakeSink(name='email')
    d = AlertDispatcher(
        plugins=[plugin],
        sinks={'sms': bad, 'email': good},
        routing={'multi-sink-alert': ['sms', 'email']},
    )
    d.run_cycle(db_path='/x', data_dir=tmp_path)
    assert len(good.emitted) == 1


# ── LogSink ──────────────────────────────────────────────────────────


def test_log_sink_emits_to_python_logger(caplog):
    sink = LogSink()
    assert sink.enabled is True
    a = _alert('log-test', AlertSeverity.WARNING)
    with caplog.at_level(logging.INFO, logger='aemo_updater.alerts.sinks.log'):
        sink.emit(a)
    assert any('log-test' in r.message for r in caplog.records), caplog.records


def test_log_sink_severity_maps_to_log_level(caplog):
    """CRITICAL/ERROR/WARNING/INFO alerts should log at matching levels
    so existing log filters work."""
    sink = LogSink()
    levels = [
        (AlertSeverity.CRITICAL, logging.CRITICAL),
        (AlertSeverity.ERROR, logging.ERROR),
        (AlertSeverity.WARNING, logging.WARNING),
        (AlertSeverity.INFO, logging.INFO),
    ]
    for sev, expected_level in levels:
        with caplog.at_level(logging.DEBUG, logger='aemo_updater.alerts.sinks.log'):
            caplog.clear()
            sink.emit(_alert(f'sev-{sev.value}', severity=sev))
            recs = [r for r in caplog.records if r.name == 'aemo_updater.alerts.sinks.log']
            assert recs, f'no records captured for {sev}'
            assert recs[0].levelno == expected_level, (
                f'{sev} mapped to level {recs[0].levelname}, expected {logging.getLevelName(expected_level)}'
            )
