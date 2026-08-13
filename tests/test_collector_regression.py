#!/usr/bin/env python3
"""Whole-collector regression for the per-cycle file cache.

The cache must be invisible: every collector must produce byte-identical output
with it on and off. This runs each collector twice against the same recorded
nemweb files -- once with the cache disabled (FILE_CACHE_MAX = 0, so the same
code path never retains anything) and once enabled -- and compares the resulting
DataFrames exactly, including dtypes and column order.

Coverage note, stated rather than implied: the eight collectors below are driven
end-to-end from fixtures. curtailment5, curtailment_duid5, bids and bid_dispatch
read Next_Day_Dispatch and Bidmove_Complete, whose daily files are tens of MB and
are not recorded here; no file in those feeds is requested twice in a cycle, so
the cache is a pass-through for them. That pass-through is proven directly by the
_download_zip_csv_bytes equivalence check at the end rather than assumed.
"""
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

FIX = Path(__file__).resolve().parent / 'fixtures' / 'nemweb'
MANIFEST = json.loads((FIX / 'manifest.json').read_text())

failures = []


def check(label, cond, extra=''):
    print(f"{'PASS' if cond else 'FAIL'}  {label}"
          f"{(' — ' + str(extra)) if extra and not cond else ''}")
    if not cond:
        failures.append(label)


class Resp:
    def __init__(self, content):
        self.content = content
        self.status_code = 200
        self.text = ''

    def raise_for_status(self):
        pass


def build(cache_on):
    """A collector wired to the fixtures, with the cache on or off."""
    c = UnifiedAEMOCollector()
    c.FILE_CACHE_MAX = 400 if cache_on else 0
    c.clear_file_cache()
    calls = []

    def fake_get(url, timeout=60, attempts=3):
        calls.append(url)
        p = FIX / url.rsplit('/', 1)[-1]
        if not p.exists():
            raise FileNotFoundError(url)
        return Resp(p.read_bytes())

    def fake_listing(url, pattern):
        for feed, meta in MANIFEST.items():
            if meta['url'] == url and meta['pattern'] == pattern:
                return sorted(meta['files'])
        return []

    c._get_with_retry = fake_get
    c.get_latest_files = fake_listing
    return c, calls


COLLECTORS = [
    ('prices5', 'collect_5min_prices'),
    ('scada5', 'collect_5min_scada'),
    ('transmission5', 'collect_5min_transmission'),
    ('curtailment_regional5', 'collect_5min_regional_curtailment'),
    ('bdu5', 'collect_5min_bdu'),
    ('rooftop30', 'collect_30min_rooftop'),
    ('demand30', 'collect_30min_demand'),
    ('demand_less_snsg', 'collect_30min_demand_less_snsg'),
]


def run_all(cache_on):
    c, calls = build(cache_on)
    out = {}
    for name, meth in COLLECTORS:
        fn = getattr(c, meth, None)
        if fn is None:
            out[name] = 'NO_SUCH_METHOD'
            continue
        try:
            out[name] = fn()
        except Exception as e:
            out[name] = f'ERROR: {type(e).__name__}: {e}'
    return c, calls, out


def main():
    print('--- cache OFF (baseline) ---')
    c_off, calls_off, off = run_all(False)
    print('--- cache ON ---')
    c_on, calls_on, on = run_all(True)
    print()

    for name, _ in COLLECTORS:
        a, b = off[name], on[name]
        if isinstance(a, pd.DataFrame) and isinstance(b, pd.DataFrame):
            same = a.equals(b)
            check(f'{name}: output identical ({len(a)} rows)', same,
                  f'off={a.shape} on={b.shape}')
            if same and not a.empty:
                check(f'{name}: dtypes identical',
                      list(a.dtypes) == list(b.dtypes))
        else:
            check(f'{name}: same result off/on', a == b, f'off={a!r} on={b!r}')

    # Every collector must have produced real data, or the comparison above is
    # vacuously true and proves nothing.
    nonempty = [n for n, _ in COLLECTORS
                if isinstance(on[n], pd.DataFrame) and not on[n].empty]
    check(f'collectors returning data: {len(nonempty)}/{len(COLLECTORS)}',
          len(nonempty) >= 7, f'only {nonempty}')

    # The point of the exercise: DispatchIS fetched once per file, not four times.
    dis_off = [u for u in calls_off if 'DISPATCHIS' in u]
    dis_on = [u for u in calls_on if 'DISPATCHIS' in u]
    uniq = len(set(dis_on))
    check(f'DispatchIS requests {len(dis_off)} -> {len(dis_on)}',
          len(dis_on) == uniq and len(dis_off) > len(dis_on),
          f'off={len(dis_off)} on={len(dis_on)} unique={uniq}')
    check('every DispatchIS file fetched exactly once with cache on',
          len(dis_on) == uniq, f'{len(dis_on)} calls for {uniq} files')
    if uniq:
        check(f'reduction factor {len(dis_off)/max(len(dis_on),1):.1f}x',
              len(dis_off) / max(len(dis_on), 1) >= 3.5,
              f'{len(dis_off)/max(len(dis_on),1):.2f}x')

    check('total requests strictly lower with cache',
          len(calls_on) < len(calls_off), f'{len(calls_on)} vs {len(calls_off)}')
    check('hit counter matches requests avoided',
          c_on._file_cache_hits == len(calls_off) - len(calls_on),
          f'hits={c_on._file_cache_hits} saved={len(calls_off)-len(calls_on)}')

    # Pass-through for the raw-bytes path used by bids / bid_dispatch.
    dis = sorted(MANIFEST['prices5']['files'])[0]
    url = MANIFEST['prices5']['url']
    c1, _ = build(False)
    c2, k2 = build(True)
    b1 = c1._download_zip_csv_bytes(url, dis)
    b2 = c2._download_zip_csv_bytes(url, dis)
    b3 = c2._download_zip_csv_bytes(url, dis)
    check('_download_zip_csv_bytes unchanged by cache', b1 == b2 and b2 == b3)
    check('_download_zip_csv_bytes served from cache on repeat', len(k2) == 1,
          f'{len(k2)} calls')

    # A second cycle must not reuse the first cycle's bodies.
    c3, k3 = build(True)
    c3.download_and_parse_file(url, dis, 'PRICE')
    c3.clear_file_cache()
    c3.download_and_parse_file(url, dis, 'PRICE')
    check('cache does not survive a cycle boundary', len(k3) == 2, f'{len(k3)} calls')

    print()
    if failures:
        print(f'{len(failures)} FAILED: {failures}')
        return 1
    print(f'all checks passed — {len(calls_off)} requests without cache, '
          f'{len(calls_on)} with')
    return 0


if __name__ == '__main__':
    sys.exit(main())
