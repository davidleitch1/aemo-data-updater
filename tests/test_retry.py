"""Test _get_with_retry against the failures nemweb actually throws."""
import sys, types
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

import requests
from aemo_updater.collectors import unified_collector as uc
from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector


class Shim:
    headers = {'User-Agent': 'test'}
    TRANSIENT_STATUS = UnifiedAEMOCollector.TRANSIENT_STATUS
    _get_with_retry = UnifiedAEMOCollector._get_with_retry


class Resp:
    def __init__(self, code):
        self.status_code = code
    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} Client Error")


failures = []
def check(label, cond):
    print(f"{'PASS' if cond else 'FAIL'}  {label}")
    if not cond:
        failures.append(label)


slept = []
uc.time = types.SimpleNamespace(sleep=lambda s: slept.append(s))

# 1. 403 twice then 200 -> recovers, does not raise.
seq = [Resp(403), Resp(403), Resp(200)]
calls = []
uc.requests.get = lambda u, **k: (calls.append(u), seq.pop(0))[1]
r = Shim()._get_with_retry('https://nemweb/x.zip', timeout=60)
check('403 -> 403 -> 200 recovers', r.status_code == 200)
check('403 path made 3 attempts', len(calls) == 3)
check('403 path backed off 1s then 2s', slept == [1.0, 2.0])

# 2. Persistent 403 -> raises after the attempt budget (no silent empty result).
seq = [Resp(403), Resp(403), Resp(403)]
uc.requests.get = lambda u, **k: seq.pop(0)
try:
    Shim()._get_with_retry('https://nemweb/x.zip', timeout=60)
    check('persistent 403 raises', False)
except Exception:
    check('persistent 403 raises', True)

# 3. RemoteDisconnected then 200 -> recovers (the single-interval-hole case).
state = {'n': 0}
def flaky(u, **k):
    state['n'] += 1
    if state['n'] == 1:
        raise requests.exceptions.ConnectionError('Connection aborted, RemoteDisconnected')
    return Resp(200)
uc.requests.get = flaky
r = Shim()._get_with_retry('https://nemweb/x.zip', timeout=60)
check('connection drop then 200 recovers', r.status_code == 200)

# 4. 404 is NOT transient -> fails fast, no wasted retries.
calls404 = []
uc.requests.get = lambda u, **k: (calls404.append(u), Resp(404))[1]
try:
    Shim()._get_with_retry('https://nemweb/missing.zip', timeout=60)
    check('404 raises', False)
except Exception:
    check('404 raises', True)
check('404 does not retry', len(calls404) == 1)

print()
if failures:
    print(f"{len(failures)} FAILED: {failures}")
    sys.exit(1)
print('all checks passed')
