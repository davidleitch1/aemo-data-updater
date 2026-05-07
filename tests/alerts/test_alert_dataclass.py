"""Step 1 of the alerts plugin migration.

Extend `Alert` with two optional fields:
  * `id`         — kebab-case alert identifier matching docs/alerts.md catalogue
  * `dedup_key`  — plugin-internal key for rate-limit / armed-state tracking

Existing call sites must keep working unchanged (backwards compatibility).
"""
from __future__ import annotations

from datetime import datetime

from aemo_updater.alerts.base_alert import Alert, AlertSeverity


def test_alert_can_be_constructed_with_id_and_dedup_key():
    """🔴 fails before step 1 — base_alert.py has no id or dedup_key fields."""
    a = Alert(
        title='High price breach',
        message='NSW1 spot $1,420',
        severity=AlertSeverity.CRITICAL,
        source='price_breach',
        id='spot-price-high-breach',
        dedup_key='price-NSW1',
    )
    assert a.id == 'spot-price-high-breach'
    assert a.dedup_key == 'price-NSW1'


def test_alert_id_and_dedup_key_default_to_none():
    """Backwards compat: callers that don't pass id/dedup_key still work,
    and the missing fields default to None (not raise)."""
    a = Alert(
        title='Anything',
        message='body',
        severity=AlertSeverity.INFO,
        source='legacy_path',
    )
    assert a.id is None
    assert a.dedup_key is None


def test_existing_call_site_compat():
    """Mimics how the existing `unified_collector.py` line ~270 builds an
    Alert today — title/message/severity/source positionally, optional
    metadata. Must still work unchanged after step 1."""
    a = Alert(
        title='New DUIDs Discovered: 1 new units',
        message='WAMBOWF2 detected',
        severity=AlertSeverity.WARNING,
        source='new_duid',
        metadata={'duid': 'WAMBOWF2'},
    )
    assert a.timestamp is not None
    assert isinstance(a.timestamp, datetime)
    assert a.metadata == {'duid': 'WAMBOWF2'}
    # SMS + email formatters unchanged
    sms = a.format_for_sms()
    assert 'WARNING' in sms
    subject, body = a.format_for_email()
    assert subject.startswith('[WARNING]')
    assert 'WAMBOWF2' in body


def test_id_and_dedup_key_carry_through_format_for_email():
    """If id/dedup_key are set, the email body should still render
    correctly. We don't require them to appear in the email — they're
    plumbing for the dispatcher, not user-facing — just that their
    presence doesn't break formatting."""
    a = Alert(
        title='Wind record set',
        message='NEM wind 12,341 MW (prev 12,200 MW)',
        severity=AlertSeverity.INFO,
        source='renewable_records',
        id='wind-record-mw',
        dedup_key='wind-record',
    )
    subject, body = a.format_for_email()
    assert subject.startswith('[INFO]')
    assert 'NEM wind 12,341 MW' in body
