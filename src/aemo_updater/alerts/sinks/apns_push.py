"""ApnsPushSink — deliver alerts as iOS push notifications via APNs HTTP/2.

Phase B of the alerts plugin migration. Reads device tokens from the
JSON registry written by the API server's
`POST /v1/devices/register` endpoint, signs an ES256 JWT with the
APNs `.p8` auth key, and posts one HTTP/2 request per active token.

Two payload shapes:
  * `new-duid-detected`              → silent push (content-available
                                        + badge bump only). The user
                                        sees the app-icon badge tick
                                        up; no banner / sound.
  * everything else routed to apns   → visible alert (banner + sound +
                                        badge). For
                                        spot-price-extreme-spike the
                                        interruption-level is
                                        'critical' so the alert
                                        surfaces through Do Not Disturb.

Tokens marked inactive (in-place in the JSON file) when APNs returns
410 Unregistered or 400 BadDeviceToken. The API server can re-activate
them on the next /v1/devices/register call from the same device.
"""
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Callable, Optional

from ..base_alert import Alert, AlertSeverity


logger = logging.getLogger(__name__)


APNS_PRODUCTION_HOST = 'https://api.push.apple.com'
APNS_SANDBOX_HOST    = 'https://api.sandbox.push.apple.com'
# Whichever environment a token belongs to is set lazily — see
# `emit()`. We try production first (the common case for TestFlight
# + App Store users) and fall back to sandbox if APNs returns
# BadDeviceToken. After the first successful delivery, the
# environment is cached on the token record so subsequent pushes
# go straight to the right host.

# When APNs responds with one of these statuses, mark the token
# inactive — the iOS app uninstalled, reset, or otherwise lost
# permission, and pushing to it will keep failing.
_TERMINAL_STATUSES = (400, 410)


_PRICE_ALERT_IDS = {
    'spot-price-high-breach',
    'spot-price-extreme-spike',
    'spot-price-recovery',
}


def _payload_for_alert(alert: Alert) -> dict:
    """Build the APNs JSON payload for a given Alert. Pure — no side
    effects. Visible vs silent push depends on alert.id.

    Price alerts also carry a `deep_link` block: tapping the
    notification should land the iOS app on Today tab, 1H view,
    breaching region. The region comes from `alert.metadata['region']`
    (set by `PriceBreachPlugin`)."""
    if alert.id == 'new-duid-detected':
        # Silent push: no banner, no sound, just bumps the badge.
        return {'aps': {'content-available': 1, 'badge': 1}}

    # Visible alert (price breach / extreme spike / recovery / other)
    aps: dict = {
        'alert': {
            'title': alert.title or 'Nem Analyst',
            'body':  alert.message or '',
        },
        'sound': 'default',
        'badge': 1,
    }
    # Extreme spikes get critical-priority interruption so they fire
    # even through Focus / Do Not Disturb. Requires the iOS app to
    # have requested the 'critical' permission.
    if alert.id == 'spot-price-extreme-spike':
        aps['interruption-level'] = 'critical'
    payload: dict = {'aps': aps}

    # Deep-link target — only meaningful for the price alert family.
    if alert.id in _PRICE_ALERT_IDS:
        region = (alert.metadata or {}).get('region')
        if region:
            payload['deep_link'] = {
                'tab':    'today',
                'region': region,
                'period': '1h',
            }
    return payload


def _default_jwt_fn(team_id: str, key_id: str, key_path: Path) -> Callable[[], str]:  # pragma: no cover
    """Build a JWT factory that signs APNs auth tokens with ES256.
    Tokens are valid for ~1h; APNs requires re-signing periodically."""
    import jwt  # PyJWT
    private_key = key_path.read_text()

    _last: dict = {'token': None, 'issued_at': 0.0}

    def _make() -> str:
        now = time.time()
        # Re-sign if we don't have one or the existing token is older
        # than ~50 minutes.
        if not _last['token'] or now - _last['issued_at'] > 50 * 60:
            _last['token'] = jwt.encode(
                {'iss': team_id, 'iat': int(now)},
                private_key,
                algorithm='ES256',
                headers={'alg': 'ES256', 'kid': key_id},
            )
            _last['issued_at'] = now
        return _last['token']
    return _make


def _default_http_post_fn(url: str, headers: dict, body: bytes) -> tuple[int, str]:  # pragma: no cover
    """Default HTTP/2 POST via httpx. Tests inject a stub."""
    import httpx
    with httpx.Client(http2=True, timeout=10.0) as client:
        resp = client.post(url, headers=headers, content=body)
        return resp.status_code, resp.text


class ApnsPushSink:
    """Send alerts via Apple Push Notification service (HTTP/2)."""

    name = 'apns'

    def __init__(
        self,
        team_id: Optional[str] = None,
        key_id: Optional[str] = None,
        bundle_id: Optional[str] = None,
        key_path: Optional[Path] = None,
        tokens_path: Optional[Path] = None,
        jwt_fn: Optional[Callable[[], str]] = None,
        http_post_fn: Optional[Callable[[str, dict, bytes], tuple[int, str]]] = None,
    ) -> None:
        self.team_id = team_id or os.getenv('APNS_TEAM_ID')
        self.key_id = key_id or os.getenv('APNS_KEY_ID')
        self.bundle_id = bundle_id or os.getenv('APNS_BUNDLE_ID')
        self.key_path = (
            Path(key_path) if key_path
            else (Path(os.environ['APNS_KEY_PATH']) if os.getenv('APNS_KEY_PATH') else None)
        )
        self.tokens_path = (
            Path(tokens_path) if tokens_path
            else (Path(os.environ['APNS_TOKENS_PATH']) if os.getenv('APNS_TOKENS_PATH') else None)
        )

        self.enabled = bool(
            self.team_id and self.key_id and self.bundle_id
            and self.key_path and self.tokens_path
        )

        self.http_post_fn = http_post_fn or _default_http_post_fn

        if jwt_fn is not None:
            self.jwt_fn = jwt_fn
        elif self.enabled:
            try:
                self.jwt_fn = _default_jwt_fn(self.team_id, self.key_id, self.key_path)
            except Exception:
                logger.exception('ApnsPushSink: jwt setup failed; disabling')
                self.enabled = False
                self.jwt_fn = None
        else:
            self.jwt_fn = None

    def emit(self, alert: Alert) -> None:
        if not self.enabled:
            return
        tokens = self._load_tokens()
        if not tokens:
            return

        payload = _payload_for_alert(alert)
        body = json.dumps(payload).encode('utf-8')

        try:
            jwt_token = self.jwt_fn()
        except Exception:
            logger.exception('ApnsPushSink: jwt signing failed')
            return

        headers_template = {
            'authorization': f'bearer {jwt_token}',
            'apns-topic': self.bundle_id,
            'apns-push-type': 'alert' if 'alert' in payload['aps'] else 'background',
            'apns-priority': '10' if 'alert' in payload['aps'] else '5',
            'content-type': 'application/json',
        }

        deactivated: list[str] = []
        env_updated = False
        for token, info in tokens.items():
            if not info.get('active', True):
                continue
            # Try the environment we already know works; default to
            # production for new tokens.
            preferred = info.get('environment', 'production')
            host = APNS_PRODUCTION_HOST if preferred == 'production' else APNS_SANDBOX_HOST
            try:
                status, body_text = self.http_post_fn(
                    f'{host}/3/device/{token}', headers_template, body,
                )
            except Exception:
                logger.exception('ApnsPushSink: post to %s failed', token[:8])
                continue

            if 200 <= status < 300:
                continue  # delivered

            # 400 BadDeviceToken on production usually means the token
            # was issued by the sandbox APNs (e.g. Xcode-sideloaded
            # debug build). Retry the other environment.
            other_env = 'sandbox' if preferred == 'production' else 'production'
            other_host = APNS_SANDBOX_HOST if preferred == 'production' else APNS_PRODUCTION_HOST
            if status == 400 and 'BadDeviceToken' in (body_text or ''):
                try:
                    status2, body_text2 = self.http_post_fn(
                        f'{other_host}/3/device/{token}', headers_template, body,
                    )
                except Exception:
                    logger.exception('ApnsPushSink: fallback to %s also failed', other_env)
                    continue
                if 200 <= status2 < 300:
                    info['environment'] = other_env
                    env_updated = True
                    logger.info(
                        'ApnsPushSink: token %s is %s-issued; cached',
                        token[:8], other_env,
                    )
                    continue
                # fallback also failed → fall through to terminal handling
                status, body_text = status2, body_text2

            if status in _TERMINAL_STATUSES:
                logger.info(
                    'ApnsPushSink: deactivating token %s (status %s)',
                    token[:8], status,
                )
                deactivated.append(token)
            else:
                logger.warning(
                    'ApnsPushSink: unexpected status %s for token %s: %s',
                    status, token[:8], (body_text or '')[:120],
                )

        if deactivated or env_updated:
            self._mark_inactive(tokens, deactivated)

    # ── Token registry I/O ────────────────────────────────────────────

    def _load_tokens(self) -> dict:
        if not self.tokens_path or not self.tokens_path.exists():
            return {}
        try:
            return json.loads(self.tokens_path.read_text())
        except Exception:
            logger.exception('ApnsPushSink: tokens file unreadable')
            return {}

    def _mark_inactive(self, tokens: dict, deactivated: list[str]) -> None:
        for t in deactivated:
            if t in tokens:
                tokens[t]['active'] = False
        try:
            tmp = self.tokens_path.with_suffix(self.tokens_path.suffix + '.tmp')
            tmp.write_text(json.dumps(tokens, indent=2))
            os.replace(tmp, self.tokens_path)
        except Exception:
            logger.exception('ApnsPushSink: failed to write deactivations')
