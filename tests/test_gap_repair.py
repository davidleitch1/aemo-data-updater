#!/usr/bin/env python3
"""End-to-end test of GapRepairer against a scratch database.

Builds a small DuckDB with the production schemas, copies in two days of real
data, punches holes of each kind, then runs the real stage+apply path and checks
every hole is closed. Exercises the CURRENT route, the ARCHIVE route, and the
midnight boundary rule (00:00 of day D lives in day D-1's archive file).
"""
import os
import shutil
import sys
from pathlib import Path
import tempfile
from datetime import datetime, timedelta

import duckdb

SCRATCH = tempfile.mkdtemp(prefix='gaprepair_')
os.environ['GAP_REPAIR_PRIMARY'] = f'{SCRATCH}/test.duckdb'
os.environ['GAP_REPAIR_REPLICA'] = f'{SCRATCH}/test.duckdb'
os.environ['GAP_REPAIR_STAGE'] = f'{SCRATCH}/stage'

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from gap_repair import GapRepairer  # noqa: E402

SRC = str(Path(os.environ.get('AEMO_DATA_PATH',
    '/Users/davidleitch/aemo_production/data')) / 'aemo_readonly.duckdb')
failures = []


def check(label, cond, extra=''):
    print(f"{'PASS' if cond else 'FAIL'}  {label}{(' — ' + extra) if extra and not cond else ''}")
    if not cond:
        failures.append(label)


def main():
    now = datetime.now()
    # Three recent whole days, safely inside the ARCHIVE window. The extra
    # leading day keeps the midnight hole interior to the enumeration range.
    hi = (now - timedelta(days=3)).replace(hour=23, minute=55, second=0, microsecond=0)
    lo = (hi - timedelta(days=2)).replace(hour=0, minute=5)

    con = duckdb.connect(os.environ['GAP_REPAIR_PRIMARY'])
    con.execute(f"ATTACH '{SRC}' AS src (READ_ONLY)")
    for t in ('prices5', 'scada5', 'transmission5',
              'prices30', 'transmission30', 'scada30'):
        con.execute(f"CREATE TABLE {t} AS SELECT * FROM src.{t} "
                    f"WHERE settlementdate BETWEEN TIMESTAMP '{lo}' AND TIMESTAMP '{hi}'")
    con.execute("DETACH src")

    # Punch three kinds of hole into the middle day.
    day = (hi - timedelta(days=1)).date()
    holes = {
        'prices5': [datetime.combine(day, datetime.min.time()) + timedelta(hours=13, minutes=25)],
        'transmission5': [datetime.combine(day, datetime.min.time()) + timedelta(hours=13, minutes=25)],
        # midnight: the hard case — lives in the PREVIOUS day's archive zip
        'scada5': [datetime.combine(day, datetime.min.time()),
                   datetime.combine(day, datetime.min.time()) + timedelta(hours=6, minutes=40)],
    }
    # The collector skips a 30-min label it cannot build from six intervals,
    # so in production the derived label is absent too. Mirror that.
    DERIVED = {'prices5': ('prices30', 'regionid', 'rrp'),
               'transmission5': ('transmission30', 'interconnectorid', 'mwflow'),
               'scada5': ('scada30', 'duid', 'scadavalue')}

    def label_of(ts):
        m = ts.minute % 30
        return ts if m == 0 else ts + timedelta(minutes=30 - m)

    for t, stamps in holes.items():
        for ts in stamps:
            con.execute(f"DELETE FROM {t} WHERE settlementdate = TIMESTAMP '{ts}'")
            con.execute(f"DELETE FROM {DERIVED[t][0]} WHERE settlementdate = TIMESTAMP '{label_of(ts)}'")
    con.close()

    r = GapRepairer(log=lambda m: print('   ' + str(m)))
    # fixture data ends 3 days ago; tell survey where its trailing edge is
    lag = int((now - hi).total_seconds() / 60) + 5
    survey = lambda: r.survey(('prices5', 'scada5', 'transmission5'),
                              trailing_lag_minutes=lag)

    found = survey()
    check('survey finds every punched hole',
          all(set(holes[t]) <= set(found[t]) for t in holes),
          str({t: len(found[t]) for t in found}))
    midnight = datetime.combine(day, datetime.min.time())
    check('survey sees the midnight interval', midnight in found['scada5'])

    print('   staging…')
    r.stage_all(found)
    inserted = r.apply()
    print(f'   inserted: {inserted}')

    after = survey()
    for t in holes:
        left = set(holes[t]) & set(after[t])
        check(f'{t}: all holes repaired', not left, f'still missing {sorted(left)}')
    check('midnight interval recovered via previous-day archive',
          midnight not in after['scada5'])
    check('no rows lost elsewhere', sum(len(v) for v in after.values()) == 0,
          str({t: len(v) for t, v in after.items()}))

    # Every derived 30-min label covering a hole must be rebuilt from all six
    # intervals. scada30 was missing from the rebuild list until 21-Sep-2026.
    c3 = duckdb.connect(os.environ['GAP_REPAIR_PRIMARY'], read_only=True)
    for src, (tgt, key, col) in DERIVED.items():
        for ts in holes[src]:
            lbl = label_of(ts)
            n = c3.execute(f"SELECT count(*) FROM {tgt} WHERE settlementdate = TIMESTAMP '{lbl}'").fetchone()[0]
            bad = c3.execute(f"""
                SELECT count(*) FROM {tgt} t JOIN (
                    SELECT {key}, AVG({col}) AS v FROM {src}
                    WHERE settlementdate > TIMESTAMP '{lbl}' - INTERVAL '30 minutes'
                      AND settlementdate <= TIMESTAMP '{lbl}'
                    GROUP BY {key} HAVING count(DISTINCT settlementdate) = 6) s USING ({key})
                WHERE t.settlementdate = TIMESTAMP '{lbl}' AND abs(t.{col} - s.v) > 1e-6
            """).fetchone()[0]
            check(f'{tgt} label {lbl:%d %H:%M} rebuilt from six {src} intervals',
                  n > 0 and bad == 0, f'rows={n} mismatched={bad}')
    c3.close()

    # Trailing-edge detection: a hole at the newest end must be visible.
    c2 = duckdb.connect(os.environ['GAP_REPAIR_PRIMARY'])
    c2.execute(f"DELETE FROM prices5 WHERE settlementdate > TIMESTAMP '{hi - timedelta(hours=2)}'")
    c2.close()
    trailing = survey()['prices5']
    # 24 intervals were deleted; the newest sits inside the lag allowance and is
    # deliberately not counted, so 23 is the correct answer. The old MAX-bounded
    # enumeration found 0 here, which is the defect this replaces.
    check('trailing hole at the newest end is detected', len(trailing) == 23,
          f'found {len(trailing)}, expected 23')

    # Idempotency: a second apply must insert nothing.
    r.clear_stage()
    r.stage_all(found)
    again = r.apply()
    check('re-apply is idempotent (0 new rows)',
          all(v == 0 for v in again.values()), str(again))

    shutil.rmtree(SCRATCH, ignore_errors=True)
    print()
    if failures:
        print(f'{len(failures)} FAILED: {failures}')
        return 1
    print('all checks passed')
    return 0


if __name__ == '__main__':
    sys.exit(main())
