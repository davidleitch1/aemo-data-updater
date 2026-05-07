"""Step 3 of the alerts plugin migration.

Add `TwilioSmsSink` and `SmtpEmailSink` — concrete channel emitters
that take an Alert and dispatch via SMS / email respectively.

The existing modules (`collectors/twilio_price_alerts.py`,
`alerts/email_sender.py`) keep working unchanged; the new sinks are
parallel, with no plugins using them yet.

Both sinks must:
  * read credentials from env on construction
  * set `enabled = True` only when all required creds are present
  * no-op (log + return) when `enabled is False`
  * catch + log exceptions from the underlying client (never raise to
    the dispatcher; the dispatcher already has its own catch-all but
    sinks should be defensive in their own right)
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.sinks.twilio_sms import TwilioSmsSink
from aemo_updater.alerts.sinks.smtp_email import SmtpEmailSink


def _alert(title: str = 'NSW1 spot $1,420 — breached $1k/MWh',
           severity: AlertSeverity = AlertSeverity.CRITICAL) -> Alert:
    return Alert(
        title=title, message='body',
        severity=severity, source='price_breach',
        id='spot-price-high-breach',
    )


# ── TwilioSmsSink ────────────────────────────────────────────────────


def test_twilio_sink_enabled_when_creds_present():
    sink = TwilioSmsSink(
        account_sid='SID', auth_token='TOK',
        from_number='+10000000000', to_number='+61400000000',
        client_factory=lambda sid, tok: MagicMock(),
    )
    assert sink.name == 'sms'
    assert sink.enabled is True


def test_twilio_sink_disabled_when_any_cred_missing():
    sink = TwilioSmsSink(
        account_sid='SID', auth_token=None,
        from_number='+10000000000', to_number='+61400000000',
        client_factory=lambda sid, tok: MagicMock(),
    )
    assert sink.enabled is False


def test_twilio_sink_emit_calls_messages_create_with_sms_body():
    fake_client = MagicMock()
    sink = TwilioSmsSink(
        account_sid='SID', auth_token='TOK',
        from_number='+10000000000', to_number='+61400000000',
        client_factory=lambda sid, tok: fake_client,
    )
    a = _alert()
    sink.emit(a)
    assert fake_client.messages.create.call_count == 1
    kwargs = fake_client.messages.create.call_args.kwargs
    assert kwargs['from_'] == '+10000000000'
    assert kwargs['to'] == '+61400000000'
    # body matches Alert.format_for_sms() exactly
    assert kwargs['body'] == a.format_for_sms()


def test_twilio_sink_emit_noop_when_disabled():
    """When the sink is disabled, emit must not touch the client at
    all (we may not even have one)."""
    sink = TwilioSmsSink(
        account_sid=None, auth_token=None,
        from_number=None, to_number=None,
        client_factory=lambda sid, tok: MagicMock(),
    )
    assert sink.enabled is False
    # Should not raise even though there's no live client
    sink.emit(_alert())


def test_twilio_sink_swallows_client_exceptions(caplog):
    """If Twilio's API call raises, the sink must log + swallow so the
    dispatcher can keep running other sinks. (The dispatcher also has
    its own catch but sinks should be defensive.)"""
    fake_client = MagicMock()
    fake_client.messages.create.side_effect = RuntimeError('twilio outage')
    sink = TwilioSmsSink(
        account_sid='SID', auth_token='TOK',
        from_number='+10000000000', to_number='+61400000000',
        client_factory=lambda sid, tok: fake_client,
    )
    with caplog.at_level(logging.ERROR, logger='aemo_updater.alerts.sinks.twilio_sms'):
        sink.emit(_alert())  # must not raise
    assert any('twilio' in r.message.lower() for r in caplog.records)


def test_twilio_sink_reads_creds_from_env(monkeypatch):
    """If constructor args are omitted, sink reads creds from env vars
    matching the existing collector's pattern."""
    monkeypatch.setenv('TWILIO_ACCOUNT_SID', 'SID_ENV')
    monkeypatch.setenv('TWILIO_AUTH_TOKEN', 'TOK_ENV')
    monkeypatch.setenv('TWILIO_FROM_NUMBER', '+10000000001')
    monkeypatch.setenv('MY_PHONE_NUMBER', '+61400000001')
    captured = {}

    def factory(sid, tok):
        captured['sid'] = sid
        captured['tok'] = tok
        return MagicMock()

    sink = TwilioSmsSink(client_factory=factory)
    assert sink.enabled is True
    assert captured == {'sid': 'SID_ENV', 'tok': 'TOK_ENV'}


# ── SmtpEmailSink ────────────────────────────────────────────────────


def test_smtp_sink_enabled_when_creds_present():
    sink = SmtpEmailSink(
        smtp_server='smtp.fastmail.com', smtp_port=587,
        sender_email='from@example.com',
        sender_password='secret',
        recipient_email='to@example.com',
        smtp_factory=lambda *args, **kw: MagicMock(),
    )
    assert sink.name == 'email'
    assert sink.enabled is True


def test_smtp_sink_disabled_when_any_cred_missing():
    sink = SmtpEmailSink(
        smtp_server='smtp.fastmail.com', smtp_port=587,
        sender_email='from@example.com',
        sender_password=None,            # missing
        recipient_email='to@example.com',
        smtp_factory=lambda *args, **kw: MagicMock(),
    )
    assert sink.enabled is False


def test_smtp_sink_emit_invokes_smtp_session():
    """Verify login + send_message both fire with the right args."""
    smtp_session = MagicMock()
    smtp_session.__enter__ = MagicMock(return_value=smtp_session)
    smtp_session.__exit__ = MagicMock(return_value=False)

    sink = SmtpEmailSink(
        smtp_server='smtp.fastmail.com', smtp_port=587,
        sender_email='from@example.com',
        sender_password='secret',
        recipient_email='to@example.com',
        smtp_factory=lambda host, port: smtp_session,
    )
    a = _alert()
    sink.emit(a)
    smtp_session.starttls.assert_called_once()
    smtp_session.login.assert_called_once()
    smtp_session.send_message.assert_called_once()
    sent_msg = smtp_session.send_message.call_args.args[0]
    subject, body = a.format_for_email()
    assert sent_msg['Subject'] == subject
    assert sent_msg['From'] == 'from@example.com'
    assert sent_msg['To'] == 'to@example.com'


def test_smtp_sink_uses_separate_login_email_when_provided():
    """Fastmail-style aliases: send FROM 'dash_services@', LOGIN as
    'davidleitch@'. The collector .env already sets ALERT_LOGIN
    distinct from ALERT_EMAIL — sink must respect that split."""
    smtp_session = MagicMock()
    smtp_session.__enter__ = MagicMock(return_value=smtp_session)
    smtp_session.__exit__ = MagicMock(return_value=False)

    sink = SmtpEmailSink(
        smtp_server='smtp.fastmail.com', smtp_port=587,
        sender_email='dash_services@fastmail.com',
        login_email='davidleitch@fastmail.com',
        sender_password='secret',
        recipient_email='davidleitch@fastmail.com',
        smtp_factory=lambda host, port: smtp_session,
    )
    sink.emit(_alert())
    login_args = smtp_session.login.call_args.args
    assert login_args[0] == 'davidleitch@fastmail.com'  # LOGIN, not From


def test_smtp_sink_emit_noop_when_disabled():
    sink = SmtpEmailSink(
        smtp_server=None, smtp_port=None,
        sender_email=None, sender_password=None, recipient_email=None,
        smtp_factory=lambda host, port: MagicMock(),
    )
    assert sink.enabled is False
    sink.emit(_alert())  # must not raise


def test_smtp_sink_swallows_smtp_exceptions(caplog):
    smtp_session = MagicMock()
    smtp_session.__enter__ = MagicMock(return_value=smtp_session)
    smtp_session.__exit__ = MagicMock(return_value=False)
    smtp_session.login.side_effect = RuntimeError('smtp 535 auth failed')

    sink = SmtpEmailSink(
        smtp_server='smtp.fastmail.com', smtp_port=587,
        sender_email='from@example.com',
        sender_password='secret',
        recipient_email='to@example.com',
        smtp_factory=lambda host, port: smtp_session,
    )
    with caplog.at_level(logging.ERROR, logger='aemo_updater.alerts.sinks.smtp_email'):
        sink.emit(_alert())  # must not raise
    assert any('smtp' in r.message.lower() for r in caplog.records)


def test_smtp_sink_reads_creds_from_env(monkeypatch):
    """Sink should read from the existing collector .env names:
       EMAIL_LOGIN, EMAIL_ADDRESS, EMAIL_PASSWORD,
       EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT, RECIPIENT_EMAIL.
    Falls back to ALERT_LOGIN/ALERT_EMAIL/ALERT_PASSWORD/SMTP_SERVER/
    SMTP_PORT for older code paths."""
    monkeypatch.setenv('EMAIL_LOGIN', 'login@fastmail.com')
    monkeypatch.setenv('EMAIL_ADDRESS', 'send_as@fastmail.com')
    monkeypatch.setenv('EMAIL_PASSWORD', 'secret')
    monkeypatch.setenv('EMAIL_SMTP_SERVER', 'smtp.fastmail.com')
    monkeypatch.setenv('EMAIL_SMTP_PORT', '587')
    monkeypatch.setenv('RECIPIENT_EMAIL', 'to@fastmail.com')
    sink = SmtpEmailSink(smtp_factory=lambda host, port: MagicMock())
    assert sink.enabled is True
    assert sink.sender_email == 'send_as@fastmail.com'
    assert sink.login_email == 'login@fastmail.com'
    assert sink.recipient_email == 'to@fastmail.com'
