#!/usr/bin/env python3
"""Run the integrity repair when the collector has left holes behind it.

Added 13-Nov-2026 after a power outage. When the collector restarts after
downtime longer than one batch it fetches the newest 12 files per feed, so the
tables fill up to the present but leave a hole behind those files. The daily
04:17 integrity run repairs such holes, but only the next morning.

This script runs from cron every 15 minutes. It does not touch the collector
and opens only the read-only replica (aemo_readonly.duckdb), read-only:

  1. If the newest prices5 row is more than BEHIND_MINUTES old, the collector is
     still catching up (or AEMO has stalled): do nothing and look again later.
  2. Otherwise list missing labels over the last WINDOW_HOURS in the core
     tables, ignoring the newest labels that are still arriving.
  3. If any hole has not been tried before, run weekly_integrity.py with
     --email-policy always. That script takes the DuckDB lock in the collector's
     gap between cycles, repairs every table, and emails the summary.
     A single missing label is left to the daily run (MIN_HOLES = 2): a
     catch-up hole is always several labels, and a lone miss is usually an
     interval AEMO never published.
  4. Each hole is tried once (state file, entries expire after a day), so an
     interval AEMO never published cannot cause repeated runs or emails.

It stays quiet from 04:00 to 05:00 host time, around the daily run.
"""
import fcntl
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

HOME = Path.home()
REPLICA = HOME / 'aemo_production' / 'data' / 'aemo_readonly.duckdb'
STATE = HOME / 'aemo_production' / 'logs' / 'catchup_repair_state.json'
LOCK = HOME / 'aemo_production' / 'logs' / 'catchup_repair.lock'
INTEGRITY = Path(__file__).resolve().parent / 'weekly_integrity.py'

# Tables the collector keeps current every cycle, with their step in minutes.
# Lagging feeds (curtailment5, rooftop30, demand30) are left to the integrity
# run itself, which surveys all twelve tables once triggered.
TABLES = {
    'prices5': 5, 'scada5': 5, 'transmission5': 5, 'bdu5': 5,
    'prices30': 30, 'scada30': 30, 'transmission30': 30,
}
BEHIND_MINUTES = 20
WINDOW_HOURS = 12
ATTEMPT_TTL_HOURS = 24
MIN_HOLES = 2


def nem_now():
    """NEM time (UTC+10, no daylight saving), naive, like settlementdate."""
    return (datetime.now(timezone.utc) + timedelta(hours=10)).replace(tzinfo=None)


def _floor(ts, step):
    return ts.replace(second=0, microsecond=0) - timedelta(minutes=ts.minute % step)


def find_holes(db_path, now):
    """Return ('behind'|'ok', {(table, iso_ts)}) for the replica at db_path."""
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        latest = con.execute('SELECT max(settlementdate) FROM prices5').fetchone()[0]
        if latest is None or now - latest > timedelta(minutes=BEHIND_MINUTES):
            return 'behind', set()
        holes = set()
        for table, step in TABLES.items():
            start = _floor(now - timedelta(hours=WINDOW_HOURS), step) + timedelta(minutes=step)
            end = _floor(latest - timedelta(minutes=step), step)
            if end < start:
                continue
            rows = con.execute(
                f"""
                WITH g AS (SELECT unnest(generate_series(?::TIMESTAMP, ?::TIMESTAMP,
                                                         INTERVAL {step} MINUTE)) AS ts)
                SELECT ts FROM g
                WHERE ts NOT IN (SELECT DISTINCT settlementdate FROM {table}
                                 WHERE settlementdate BETWEEN ? AND ?)
                ORDER BY ts
                """,
                [start, end, start, end],
            ).fetchall()
            holes.update((table, r[0].isoformat()) for r in rows)
        return 'ok', holes
    finally:
        con.close()


def _key(hole):
    return f'{hole[0]}|{hole[1]}'


def new_holes(holes, attempted):
    return {h for h in holes if _key(h) not in attempted}


def record_attempt(attempted, holes, now):
    out = dict(attempted)
    for h in holes:
        out[_key(h)] = now.isoformat()
    return out


def prune(attempted, now):
    cutoff = now - timedelta(hours=ATTEMPT_TTL_HOURS)
    return {k: v for k, v in attempted.items() if datetime.fromisoformat(v) >= cutoff}


def should_repair(todo):
    """A catch-up hole is several labels; a lone missed interval (often one
    AEMO never published) is left to the daily 04:17 run."""
    return len(todo) >= MIN_HOLES


def in_daily_window(host_now):
    return host_now.hour == 4


def _load_state():
    try:
        return json.loads(STATE.read_text())
    except (FileNotFoundError, ValueError):
        return {}


def _summary(holes):
    by_table = {}
    for table, ts in sorted(holes):
        by_table.setdefault(table, []).append(ts[5:16].replace('T', ' '))
    return '; '.join(f'{t} {len(v)} ({v[0]}..{v[-1]})' for t, v in by_table.items())


def main():
    host_now = datetime.now()
    if in_daily_window(host_now):
        return 0
    LOCK.parent.mkdir(parents=True, exist_ok=True)
    lock = open(LOCK, 'w')
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return 0  # a previous run (and its repair) is still going

    now = nem_now()
    try:
        status, holes = find_holes(REPLICA, now)
    except Exception as e:  # replica mid-swap or locked: try next run
        print(f'{host_now:%Y-%m-%d %H:%M} replica not readable: {e}', flush=True)
        return 0
    if status == 'behind':
        return 0

    attempted = prune(_load_state(), now)
    todo = new_holes(holes, attempted)
    if not should_repair(todo):
        if len(attempted) != len(_load_state()):
            STATE.write_text(json.dumps(attempted, indent=1))
        return 0

    # Record first, so a crash in the repair cannot cause a loop of runs.
    STATE.write_text(json.dumps(record_attempt(attempted, todo, now), indent=1))
    print(f'=== {host_now:%Y-%m-%d %H:%M} catch-up holes (NEM time): {_summary(todo)}', flush=True)
    print(f'running {INTEGRITY.name} --email-policy always', flush=True)
    rc = subprocess.call([sys.executable, str(INTEGRITY), '--email-policy', 'always'])
    print(f'{datetime.now():%Y-%m-%d %H:%M} integrity run exited {rc}', flush=True)
    return rc


if __name__ == '__main__':
    sys.exit(main())
