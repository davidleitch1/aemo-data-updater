"""ApnsPushSink — Phase B of the alerts plugin migration.

Sends iOS push notifications via APNs HTTP/2 to every device that has
registered through `POST /v1/devices/register` on the API server.

Two payload kinds depending on alert id:
  * `new-duid-detected`            → silent push (content-available: 1)
                                     just bumps the app icon badge
  * `spot-price-*` and everything   → visible alert (banner + sound +
    else routed to apns                badge)

Token registry is a JSON file `<data_dir>/apns_tokens.json`
(API-server-writable, sink reads only). Tokens marked inactive after
APNs returns 410 Unregistered or BadDeviceToken.

Tests use injected `jwt_fn` and `http_post_fn` so no real APNs traffic
or real .p8 key required.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from aemo_updater.alerts.base_alert import Alert, AlertSeverity
from aemo_updater.alerts.sinks.apns_push import (
    ApnsPushSink,
    _payload_for_alert,
)


def _alert(alert_id: str = 'spot-price-high-breach',
           severity: AlertSeverity = AlertSeverity.CRITICAL,
           title: str = 'NSW1 high price') -> Alert:
    return Alert(
        title=title, message='body',
        severity=severity, source='price_breach',
        id=alert_id,
    )


def _write_tokens(tmp_path: Path, tokens: dict) -> Path:
    path = tmp_path / 'apns_tokens.json'
    path.write_text(json.dumps(tokens))
    return path


# ── Enabled / disabled state ─────────────────────────────────────────


def test_sink_enabled_when_all_creds_present(tmp_path):
    sink = ApnsPushSink(
        team_id='SBC584KMW4',
        key_id='3H247DG76U',
        bundle_id='com.itkservices.aemo-mobile',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tmp_path / 'apns_tokens.json',
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=lambda url, headers, body: (200, ''),
    )
    assert sink.name == 'apns'
    assert sink.enabled is True


def test_sink_disabled_when_any_cred_missing(tmp_path):
    sink = ApnsPushSink(
        team_id='SBC584KMW4',
        key_id='3H247DG76U',
        bundle_id='com.itkservices.aemo-mobile',
        key_path=None,                  # missing
        tokens_path=tmp_path / 'apns_tokens.json',
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=lambda url, headers, body: (200, ''),
    )
    assert sink.enabled is False


def test_sink_emit_noop_when_disabled(tmp_path):
    """Disabled sink must not even attempt to load tokens or call HTTP."""
    http = MagicMock()
    sink = ApnsPushSink(
        team_id=None, key_id=None, bundle_id=None,
        key_path=None, tokens_path=None,
        jwt_fn=lambda: 'fake-jwt', http_post_fn=http,
    )
    sink.emit(_alert())  # must not raise
    http.assert_not_called()


# ── Token loading + fan-out ──────────────────────────────────────────


def test_emit_posts_to_each_active_token(tmp_path):
    tokens_path = _write_tokens(tmp_path, {
        'TOKEN_AAA': {'active': True,  'user_label': 'David'},
        'TOKEN_BBB': {'active': True,  'user_label': 'Friend'},
        'TOKEN_OLD': {'active': False, 'user_label': 'Lapsed'},
    })
    posts = []
    sink = ApnsPushSink(
        team_id='T', key_id='K', bundle_id='B',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tokens_path,
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=lambda url, headers, body: (posts.append((url, headers, body)) or (200, '')),
    )
    sink.emit(_alert())
    posted_urls = [p[0] for p in posts]
    assert any('TOKEN_AAA' in u for u in posted_urls)
    assert any('TOKEN_BBB' in u for u in posted_urls)
    assert not any('TOKEN_OLD' in u for u in posted_urls)
    assert len(posts) == 2


def test_emit_no_tokens_no_http(tmp_path):
    _write_tokens(tmp_path, {})
    http = MagicMock()
    sink = ApnsPushSink(
        team_id='T', key_id='K', bundle_id='B',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tmp_path / 'apns_tokens.json',
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=http,
    )
    sink.emit(_alert())
    http.assert_not_called()


def test_emit_missing_tokens_file_no_http(tmp_path):
    """No tokens file at all (no devices have registered yet) must not
    crash the dispatcher."""
    http = MagicMock()
    sink = ApnsPushSink(
        team_id='T', key_id='K', bundle_id='B',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tmp_path / 'apns_tokens.json',  # doesn't exist
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=http,
    )
    sink.emit(_alert())
    http.assert_not_called()


# ── Payload shape ────────────────────────────────────────────────────


def test_price_alert_payload_visible():
    payload = _payload_for_alert(_alert('spot-price-high-breach',
                                        severity=AlertSeverity.CRITICAL))
    assert 'aps' in payload
    aps = payload['aps']
    # Visible alert: title + body in `alert` dict, sound default,
    # badge present.
    assert 'alert' in aps
    assert aps['alert'].get('title')  # non-empty
    assert aps.get('sound') == 'default'
    assert aps.get('badge') == 1
    # Not silent-only
    assert 'content-available' not in aps


def test_price_alert_payload_includes_deep_link_to_today():
    """Tap on a price-breach push should deep-link to Today tab,
    1H view, breaching region. Region comes from metadata['region']."""
    a = Alert(
        title='NSW1 spot $1,420 — breached $1k',
        message='body',
        severity=AlertSeverity.CRITICAL,
        source='price_breach',
        metadata={'region': 'NSW1'},
        id='spot-price-high-breach',
    )
    payload = _payload_for_alert(a)
    dl = payload.get('deep_link')
    assert dl is not None, f'price alert must include deep_link; got {payload}'
    assert dl.get('tab') == 'today'
    assert dl.get('region') == 'NSW1'
    assert dl.get('period') == '1h'


def test_extreme_spike_payload_also_has_deep_link():
    a = Alert(
        title='SA1 extreme', message='body',
        severity=AlertSeverity.CRITICAL, source='price_breach',
        metadata={'region': 'SA1'},
        id='spot-price-extreme-spike',
    )
    payload = _payload_for_alert(a)
    assert payload['deep_link']['region'] == 'SA1'


def test_recovery_payload_also_has_deep_link():
    a = Alert(
        title='QLD1 recovered', message='body',
        severity=AlertSeverity.INFO, source='price_breach',
        metadata={'region': 'QLD1'},
        id='spot-price-recovery',
    )
    payload = _payload_for_alert(a)
    assert payload['deep_link']['region'] == 'QLD1'


def test_new_duid_payload_no_deep_link():
    """New DUID is silent + no deep-link target yet (would land on a
    Browse → Stations 'recently added' view that doesn't exist; defer
    to a later step)."""
    payload = _payload_for_alert(_alert('new-duid-detected',
                                        severity=AlertSeverity.WARNING))
    assert 'deep_link' not in payload


def test_new_duid_payload_is_silent_with_badge():
    payload = _payload_for_alert(_alert('new-duid-detected',
                                        severity=AlertSeverity.WARNING))
    aps = payload['aps']
    # Silent push: content-available 1, badge bump, no banner/sound
    assert aps.get('content-available') == 1
    assert aps.get('badge') == 1
    assert 'alert' not in aps
    assert 'sound' not in aps


def test_extreme_spike_payload_uses_distinctive_sound():
    payload = _payload_for_alert(_alert('spot-price-extreme-spike',
                                        severity=AlertSeverity.CRITICAL))
    # Extreme spikes deserve attention even when phone is on Do Not
    # Disturb — use the critical sound (only meaningful with the
    # 'critical' UNNotificationInterruptionLevel on iOS, but the
    # payload should at least flag it).
    aps = payload['aps']
    # We do at least mark interruption-level critical:
    assert aps.get('interruption-level') == 'critical'


# ── HTTP request shape ───────────────────────────────────────────────


def test_post_url_and_headers(tmp_path):
    _write_tokens(tmp_path, {'TOKENXYZ': {'active': True}})
    posts = []
    sink = ApnsPushSink(
        team_id='SBC584KMW4', key_id='3H247DG76U',
        bundle_id='com.itkservices.aemo-mobile',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tmp_path / 'apns_tokens.json',
        jwt_fn=lambda: 'fake-jwt-XYZ',
        http_post_fn=lambda url, headers, body: (posts.append((url, headers, body)) or (200, '')),
    )
    sink.emit(_alert())
    url, headers, body = posts[0]
    # APNs production endpoint
    assert url.endswith('/3/device/TOKENXYZ')
    assert 'api.push.apple.com' in url
    # Bearer JWT in Authorization
    assert headers.get('authorization') == 'bearer fake-jwt-XYZ'
    # Topic header is the bundle id
    assert headers.get('apns-topic') == 'com.itkservices.aemo-mobile'
    # Body is JSON
    assert isinstance(body, bytes)
    payload = json.loads(body.decode())
    assert 'aps' in payload


# ── Error handling: 410 Unregistered → mark token inactive ───────────


def test_410_unregistered_marks_token_inactive(tmp_path):
    tokens_path = _write_tokens(tmp_path, {
        'GOOD_TOKEN': {'active': True},
        'GONE_TOKEN': {'active': True},
    })

    def fake_post(url, headers, body):
        if 'GONE_TOKEN' in url:
            return (410, '{"reason":"Unregistered"}')
        return (200, '')

    sink = ApnsPushSink(
        team_id='T', key_id='K', bundle_id='B',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tokens_path,
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=fake_post,
    )
    sink.emit(_alert())
    state = json.loads(tokens_path.read_text())
    assert state['GONE_TOKEN']['active'] is False
    assert state['GOOD_TOKEN']['active'] is True


def test_400_bad_device_token_also_deactivates(tmp_path):
    tokens_path = _write_tokens(tmp_path, {'BAD_TOKEN': {'active': True}})

    def fake_post(url, headers, body):
        return (400, '{"reason":"BadDeviceToken"}')

    sink = ApnsPushSink(
        team_id='T', key_id='K', bundle_id='B',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tokens_path,
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=fake_post,
    )
    sink.emit(_alert())
    state = json.loads(tokens_path.read_text())
    assert state['BAD_TOKEN']['active'] is False


def test_emit_swallows_http_exceptions(tmp_path, caplog):
    _write_tokens(tmp_path, {'TOKEN': {'active': True}})

    def boom(url, headers, body):
        raise RuntimeError('network down')

    sink = ApnsPushSink(
        team_id='T', key_id='K', bundle_id='B',
        key_path=tmp_path / 'fake.p8',
        tokens_path=tmp_path / 'apns_tokens.json',
        jwt_fn=lambda: 'fake-jwt',
        http_post_fn=boom,
    )
    sink.emit(_alert())  # must not raise
