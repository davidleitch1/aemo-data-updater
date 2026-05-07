"""Step 5 of the alerts plugin migration — cutover.

Three things land in the same commit so there's no SMS-duplication
window:
  1. Flip routing for spot-price-* from ['log'] to ['sms', 'log'].
  2. Add `build_default_dispatcher()` factory in `alerts/__init__.py`
     that constructs the standard plugin + sink set from env.
  3. Delete the legacy `collectors/twilio_price_alerts.py` module +
     remove every call site (duckdb collector, unified collector,
     price collector).

Tests assert: routing flipped, factory wires correctly, end-to-end
SMS fires through the new path with the legacy-format body, and the
legacy module is gone (importing it raises ImportError).
"""
from __future__ import annotations

import importlib
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.context import AlertContext
from aemo_updater.alerts.dispatcher import AlertDispatcher
from aemo_updater.alerts.routing import ALERT_ROUTING
from aemo_updater.alerts.sinks.log import LogSink
from aemo_updater.alerts.sinks.smtp_email import SmtpEmailSink
from aemo_updater.alerts.sinks.twilio_sms import TwilioSmsSink
from aemo_updater.alerts.plugins.price_breach import PriceBreachPlugin


# ── Routing flipped ──────────────────────────────────────────────────


def test_routing_for_price_alerts_now_includes_sms():
    """All three price-alert IDs must include 'sms' (and keep 'log')
    after step 5. This is the legacy path's replacement."""
    for alert_id in ('spot-price-high-breach',
                     'spot-price-extreme-spike',
                     'spot-price-recovery'):
        assert 'sms' in ALERT_ROUTING[alert_id], (
            f"{alert_id} routing should include 'sms' at step 5; "
            f"got {ALERT_ROUTING[alert_id]}"
        )
        assert 'log' in ALERT_ROUTING[alert_id]


# ── Default dispatcher factory ───────────────────────────────────────


def test_build_default_dispatcher_returns_dispatcher_with_standard_sinks(monkeypatch):
    """`build_default_dispatcher()` constructs a production-ready
    dispatcher with LogSink + SmtpEmailSink + TwilioSmsSink and at
    least the PriceBreachPlugin registered."""
    monkeypatch.setenv('TWILIO_ACCOUNT_SID', 'SID')
    monkeypatch.setenv('TWILIO_AUTH_TOKEN', 'TOK')
    monkeypatch.setenv('TWILIO_FROM_NUMBER', '+10000000000')
    monkeypatch.setenv('MY_PHONE_NUMBER', '+61400000000')
    monkeypatch.setenv('EMAIL_ADDRESS', 'from@example.com')
    monkeypatch.setenv('EMAIL_PASSWORD', 'secret')
    monkeypatch.setenv('EMAIL_SMTP_SERVER', 'smtp.fastmail.com')
    monkeypatch.setenv('RECIPIENT_EMAIL', 'to@example.com')

    from aemo_updater.alerts import build_default_dispatcher

    d = build_default_dispatcher(
        twilio_client_factory=lambda sid, tok: MagicMock(),
        smtp_factory=lambda host, port: MagicMock(),
    )
    assert isinstance(d, AlertDispatcher)
    assert 'log' in d.sinks
    assert 'sms' in d.sinks
    assert 'email' in d.sinks
    assert any(isinstance(p, PriceBreachPlugin) for p in d.plugins)


def test_build_default_dispatcher_uses_alert_routing_by_default(monkeypatch):
    monkeypatch.setenv('TWILIO_ACCOUNT_SID', 'SID')
    monkeypatch.setenv('TWILIO_AUTH_TOKEN', 'TOK')
    monkeypatch.setenv('TWILIO_FROM_NUMBER', '+1')
    monkeypatch.setenv('MY_PHONE_NUMBER', '+61')
    from aemo_updater.alerts import build_default_dispatcher
    d = build_default_dispatcher(
        twilio_client_factory=lambda sid, tok: MagicMock(),
        smtp_factory=lambda host, port: MagicMock(),
    )
    assert d.routing is ALERT_ROUTING or d.routing == ALERT_ROUTING


# ── End-to-end: high-price → SMS through dispatcher ──────────────────


def test_default_dispatcher_fires_sms_on_high_price(tmp_path, monkeypatch):
    """Synthetic NSW1 price of $1,420 should drive PriceBreachPlugin
    to emit `spot-price-high-breach`, which the routing table fans
    out to TwilioSmsSink, which calls client.messages.create with
    the legacy SMS body format."""
    monkeypatch.setenv('TWILIO_ACCOUNT_SID', 'SID')
    monkeypatch.setenv('TWILIO_AUTH_TOKEN', 'TOK')
    monkeypatch.setenv('TWILIO_FROM_NUMBER', '+10000000000')
    monkeypatch.setenv('MY_PHONE_NUMBER', '+61400000000')

    fake_twilio = MagicMock()
    from aemo_updater.alerts import build_default_dispatcher
    d = build_default_dispatcher(
        twilio_client_factory=lambda sid, tok: fake_twilio,
        smtp_factory=lambda host, port: MagicMock(),
        plugins=[PriceBreachPlugin(query_fn=lambda ctx: [
            (datetime(2026, 5, 7, 18, 25), 'NSW1', 1420.0),
        ])],
    )
    d.run_cycle(db_path='/nonexistent', data_dir=tmp_path)

    assert fake_twilio.messages.create.call_count == 1
    body = fake_twilio.messages.create.call_args.kwargs['body']
    # Legacy format markers preserved (visible to operators who've
    # been receiving these SMS for months).
    assert 'NSW1' in body
    assert 'HIGH PRICE' in body
    assert '$1420.00' in body or '1420' in body
    # Step-5 path doesn't produce other SMS in this isolated test
    # because we passed a single-plugin override above.


def test_default_dispatcher_extreme_spike_includes_emoji_in_body(tmp_path, monkeypatch):
    monkeypatch.setenv('TWILIO_ACCOUNT_SID', 'SID')
    monkeypatch.setenv('TWILIO_AUTH_TOKEN', 'TOK')
    monkeypatch.setenv('TWILIO_FROM_NUMBER', '+10000000000')
    monkeypatch.setenv('MY_PHONE_NUMBER', '+61400000000')

    fake_twilio = MagicMock()
    from aemo_updater.alerts import build_default_dispatcher
    d = build_default_dispatcher(
        twilio_client_factory=lambda sid, tok: fake_twilio,
        smtp_factory=lambda host, port: MagicMock(),
        plugins=[PriceBreachPlugin(query_fn=lambda ctx: [
            (datetime(2026, 5, 7, 18, 25), 'SA1', 14200.0),
        ])],
    )
    d.run_cycle(db_path='/nonexistent', data_dir=tmp_path)
    body = fake_twilio.messages.create.call_args.kwargs['body']
    assert 'EXTREME' in body
    assert 'SA1' in body


def test_default_dispatcher_does_nothing_when_no_alerts(tmp_path, monkeypatch):
    monkeypatch.setenv('TWILIO_ACCOUNT_SID', 'SID')
    monkeypatch.setenv('TWILIO_AUTH_TOKEN', 'TOK')
    monkeypatch.setenv('TWILIO_FROM_NUMBER', '+1')
    monkeypatch.setenv('MY_PHONE_NUMBER', '+61')

    fake_twilio = MagicMock()
    from aemo_updater.alerts import build_default_dispatcher
    d = build_default_dispatcher(
        twilio_client_factory=lambda sid, tok: fake_twilio,
        smtp_factory=lambda host, port: MagicMock(),
        plugins=[PriceBreachPlugin(query_fn=lambda ctx: [
            (datetime(2026, 5, 7, 18, 25), 'NSW1', 80.0),  # normal price
        ])],
    )
    d.run_cycle(db_path='/nonexistent', data_dir=tmp_path)
    fake_twilio.messages.create.assert_not_called()


# ── Legacy module gone ───────────────────────────────────────────────


def test_legacy_twilio_price_alerts_module_is_deleted():
    """The legacy module must no longer be importable. (Step 5
    deletes the file; future code should use PriceBreachPlugin via
    the dispatcher.)"""
    with pytest.raises(ImportError):
        importlib.import_module('aemo_updater.collectors.twilio_price_alerts')


def test_unified_collector_duckdb_no_longer_imports_legacy():
    """File-content assertion: the production collector must not
    contain the legacy import / call line."""
    src = Path(__file__).resolve().parents[2] / 'src' / 'aemo_updater' / 'collectors' / 'unified_collector_duckdb.py'
    text = src.read_text()
    assert 'twilio_price_alerts' not in text, (
        'unified_collector_duckdb.py still references the deleted '
        'legacy module — step 5 should have removed all references.'
    )
    assert 'check_price_alerts' not in text


def test_unified_collector_no_longer_imports_legacy():
    src = Path(__file__).resolve().parents[2] / 'src' / 'aemo_updater' / 'collectors' / 'unified_collector.py'
    text = src.read_text()
    assert 'twilio_price_alerts' not in text


def test_price_collector_no_longer_imports_legacy():
    src = Path(__file__).resolve().parents[2] / 'src' / 'aemo_updater' / 'collectors' / 'price_collector.py'
    text = src.read_text()
    assert 'twilio_price_alerts' not in text


# ── Collector wiring (presence check) ────────────────────────────────


def test_unified_collector_duckdb_calls_dispatcher_run_cycle():
    """Production collector must invoke `dispatcher.run_cycle(...)`
    once per merge cycle. File-content assertion is enough — the
    actual integration test is end-to-end above."""
    src = Path(__file__).resolve().parents[2] / 'src' / 'aemo_updater' / 'collectors' / 'unified_collector_duckdb.py'
    text = src.read_text()
    assert 'dispatcher' in text.lower()
    assert 'run_cycle' in text or 'AlertDispatcher' in text
