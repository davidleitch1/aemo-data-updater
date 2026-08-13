#!/usr/bin/env python3
"""Cache behaviour for the per-cycle DispatchIS content cache.

Four tables read the same DispatchIS zip every cycle -- PRICE, INTERCONNECTORRES
and REGIONSUM twice (regional curtailment and bdu5) -- so each file was fetched
four times. At 12 files per cycle that is 48 requests where 12 suffice, and it
was that burst that tripped nemweb rate limiting (403s) on 13-Aug-2026 when the
per-cycle window was raised.

Caching is safe because a nemweb filename carries a serial and is immutable once
published, so the same name can never denote different content.
"""
import io
import sys
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

FIX = Path(__file__).resolve().parent / 'fixtures' / 'nemweb'
DIS = sorted(p.name for p in FIX.glob('PUBLIC_DISPATCHIS_*.zip'))
URL = 'https://nemweb.com.au/Reports/CURRENT/DispatchIS_Reports/'

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


def make_collector(fail_on=()):
    """Collector whose network layer serves fixtures and counts calls."""
    c = UnifiedAEMOCollector()
    calls = []

    def fake_get(url, timeout=60, attempts=3):
        calls.append(url)
        name = url.rsplit('/', 1)[-1]
        if name in fail_on:
            raise RuntimeError(f'simulated failure for {name}')
        p = FIX / name
        if not p.exists():
            raise FileNotFoundError(name)
        return Resp(p.read_bytes())

    c._get_with_retry = fake_get
    return c, calls


def main():
    if len(DIS) < 2:
        print('need at least 2 DispatchIS fixtures')
        return 1

    # 1. Same file, same table, twice -> one network call.
    c, calls = make_collector()
    a = c.download_and_parse_file(URL, DIS[0], 'PRICE')
    b = c.download_and_parse_file(URL, DIS[0], 'PRICE')
    check('same file+table twice = 1 fetch', len(calls) == 1, f'{len(calls)} calls')
    check('repeat returns equal data', a.equals(b))

    # 2. The real case: one file, the four tables a cycle actually asks for.
    c, calls = make_collector()
    for tbl in ('PRICE', 'INTERCONNECTORRES', 'REGIONSUM', 'REGIONSUM'):
        c.download_and_parse_file(URL, DIS[0], tbl)
    check('4 tables from one file = 1 fetch (was 4)', len(calls) == 1,
          f'{len(calls)} calls')

    # 3. Distinct files still fetched separately.
    c, calls = make_collector()
    for fn in DIS[:2]:
        c.download_and_parse_file(URL, fn, 'PRICE')
    check('distinct files fetched separately', len(calls) == 2, f'{len(calls)} calls')

    # 4. Parsing is per-table, not poisoned by the shared cache entry.
    c, _ = make_collector()
    price = c.download_and_parse_file(URL, DIS[0], 'PRICE')
    inter = c.download_and_parse_file(URL, DIS[0], 'INTERCONNECTORRES')
    check('PRICE parse non-empty', not price.empty)
    check('INTERCONNECTORRES parse non-empty', not inter.empty)
    check('the two tables differ', not price.equals(inter))
    check('PRICE has RRP', 'RRP' in price.columns, list(price.columns)[:6])
    check('INTERCONNECTORRES has INTERCONNECTORID',
          'INTERCONNECTORID' in inter.columns, list(inter.columns)[:6])

    # 5. Cache is per cycle: clearing forces a refetch.
    c, calls = make_collector()
    c.download_and_parse_file(URL, DIS[0], 'PRICE')
    c.clear_file_cache()
    c.download_and_parse_file(URL, DIS[0], 'PRICE')
    check('clear_file_cache() forces refetch', len(calls) == 2, f'{len(calls)} calls')

    # 6. Failures must NOT be cached -- a transient error must stay retryable,
    #    otherwise one blip would suppress the file for every later table.
    c, calls = make_collector(fail_on={DIS[0]})
    first = c.download_and_parse_file(URL, DIS[0], 'PRICE')
    second = c.download_and_parse_file(URL, DIS[0], 'PRICE')
    check('failure returns empty frame', first.empty and second.empty)
    check('failure is not cached (both attempts hit network)', len(calls) == 2,
          f'{len(calls)} calls')

    # 7. Cache is bounded, so a --backfill run cannot grow it without limit.
    c, _ = make_collector()
    check('cache exposes a size cap', hasattr(c, 'FILE_CACHE_MAX'))
    if hasattr(c, 'FILE_CACHE_MAX'):
        for i in range(c.FILE_CACHE_MAX + 25):
            c._file_cache[f'k{i}'] = b'x'
            c._trim_file_cache()
        check('cache stays within its cap',
              len(c._file_cache) <= c.FILE_CACHE_MAX, len(c._file_cache))

    # 8. Hit counter, so the saving is visible in the cycle log.
    c, _ = make_collector()
    for tbl in ('PRICE', 'INTERCONNECTORRES', 'REGIONSUM', 'REGIONSUM'):
        c.download_and_parse_file(URL, DIS[0], tbl)
    check('hit counter records the 3 saved fetches',
          getattr(c, '_file_cache_hits', None) == 3,
          getattr(c, '_file_cache_hits', 'missing'))

    print()
    if failures:
        print(f'{len(failures)} FAILED: {failures}')
        return 1
    print('all checks passed')
    return 0


if __name__ == '__main__':
    sys.exit(main())
