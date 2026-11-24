#!/usr/bin/env python3
"""Survey and repair constraint_intervals (the per-interval ledger in constraints.duckdb).

The survey enumerates every 5-minute interval from the ledger's first row up to NEM time now
minus 20 minutes and lists those with no ledger row. It never infers gaps from adjacent rows and
never bounds the range by MAX(settlementdate), so a single missing interval and a hole at the
newest end are both visible.

Missing intervals are repaired from DispatchIS CURRENT (individual files, ~2 days) and then from
the ARCHIVE daily zips (a zip of 5-minute zips; the zip for date D holds 00:05 on D to 00:00 on
D+1, so 00:00 belongs to the previous day's file). Stores go through store_dispatchis on a
short-lived connection, so the collector is not blocked while a zip downloads.

    .venv/bin/python scripts/constraints_repair.py [--check-only] [--db PATH] [--max-days 40]
                                                   [--pause 10] [--no-email]

Emails (weekly_integrity's helper and recipient) only when intervals older than 3 hours stay
missing after repair. Exit status is 0 unless the script crashed.
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from gap_repair import ARCHIVE_ROOT, CURRENT_ROOT, nem_now  # noqa: E402
from aemo_updater import constraints_store as store  # noqa: E402

FEED_DIR = "DispatchIS_Reports/"
PREFIX = "PUBLIC_DISPATCHIS_"
LAG_MINUTES = 20
EMAIL_AGE = timedelta(hours=3)
STAMP = re.compile(r"_(\d{12})_")


class Net:
    """The three network calls the repair makes; tests substitute their own."""

    def __init__(self):
        from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector
        self.c = UnifiedAEMOCollector()

    def current_files(self) -> list[str]:
        return self.c.get_latest_files(CURRENT_ROOT + FEED_DIR, PREFIX)

    def archive_listing(self) -> str:
        return self.c._get_with_retry(ARCHIVE_ROOT + FEED_DIR, timeout=120).text

    def get(self, url: str, timeout: int = 120) -> bytes:
        return self.c._get_with_retry(url, timeout=timeout).content


# ---------------------------------------------------------------- survey

def survey(con, now: datetime, lag_minutes: int = LAG_MINUTES) -> list[datetime]:
    """Missing 5-minute intervals from the first ledger row to floor5(now - lag), by enumeration."""
    mn = con.execute("SELECT min(settlementdate) FROM constraint_intervals").fetchone()[0]
    if mn is None:
        return []
    end = now - timedelta(minutes=lag_minutes)
    end -= timedelta(minutes=end.minute % 5, seconds=end.second, microseconds=end.microsecond)
    if end < mn:
        return []
    rows = con.execute(f"""
        WITH g AS (SELECT generate_series AS d FROM generate_series(
                     TIMESTAMP '{mn}', TIMESTAMP '{end}', INTERVAL 5 MINUTE)),
             i AS (SELECT DISTINCT settlementdate AS d FROM constraint_intervals)
        SELECT g.d FROM g LEFT JOIN i USING (d) WHERE i.d IS NULL ORDER BY 1""").fetchall()
    return [r[0] for r in rows]


# ---------------------------------------------------------------- repair

def _inner_csv(blob: bytes) -> bytes | None:
    try:
        with zipfile.ZipFile(io.BytesIO(blob)) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            return z.read(csvs[0]) if csvs else None
    except zipfile.BadZipFile:
        return blob


def _store(db: Path, contents: list[bytes], log) -> int:
    """Store files on one short-lived connection; returns how many stored."""
    con = store.connect_with_retry(db, log=log)
    n = 0
    try:
        store.ensure_tables(con)
        for content in contents:
            try:
                store.store_dispatchis(con, content)
                n += 1
            except Exception as e:  # noqa: BLE001
                log(f"  store failed: {e}")
    finally:
        con.close()
    return n


def repair_current(db: Path, missing, net, sleep, log) -> int:
    by_stamp = {}
    for f in net.current_files():
        m = STAMP.search(f[len(PREFIX) - 1:])
        if m:
            by_stamp[m.group(1)] = f      # later serial wins
    fixed = 0
    for ts in missing:
        name = by_stamp.get(f"{ts:%Y%m%d%H%M}")
        if not name:
            continue
        try:
            content = _inner_csv(net.get(CURRENT_ROOT + FEED_DIR + name))
            if content:
                fixed += _store(db, [content], log)
        except Exception as e:  # noqa: BLE001
            log(f"  CURRENT {ts} failed: {e}")
        sleep(1.0)
    return fixed


def repair_archive(db: Path, missing, net, sleep, pause, max_days, log) -> int:
    if not missing:
        return 0
    by_date = defaultdict(set)
    for ts in missing:
        d = ts.date() - timedelta(days=1) if (ts.hour == 0 and ts.minute == 0) else ts.date()
        by_date[d].add(f"{ts:%Y%m%d%H%M}")
    listing = net.archive_listing()
    fixed, days = 0, 0
    for d in sorted(by_date, reverse=True):          # newest days first
        names = sorted(set(re.findall(rf"(PUBLIC_[A-Z_]*{d:%Y%m%d}[0-9_]*\.zip)", listing)))
        if not names:
            continue
        if days >= max_days:
            log(f"  --max-days {max_days} reached; older days left for the next run")
            break
        days += 1
        try:
            blob = net.get(ARCHIVE_ROOT + FEED_DIR + names[-1], timeout=600)
            contents = []
            with zipfile.ZipFile(io.BytesIO(blob)) as outer:
                for n in outer.namelist():
                    m = STAMP.search(n[len(PREFIX) - 1:] if n.startswith(PREFIX) else n)
                    if m and m.group(1) in by_date[d]:
                        c = _inner_csv(outer.read(n))
                        if c:
                            contents.append(c)
            got = _store(db, contents, log)
            fixed += got
            log(f"  ARCHIVE {d}: wanted {len(by_date[d])}, stored {got}")
        except Exception as e:  # noqa: BLE001
            log(f"  ARCHIVE {d} failed: {e}")
        sleep(pause)
    return fixed


def _survey_db(db: Path, now, log):
    con = store.connect_with_retry(db, log=log, read_only=True)
    try:
        return survey(con, now)
    finally:
        con.close()


def run(db: Path, *, check_only=False, max_days=40, pause=10.0, no_email=False, net=None,
        sleep=time.sleep, now=None, send=None, log=print) -> dict:
    now = now or nem_now()
    missing = _survey_db(db, now, log)
    found = len(missing)
    log(f"constraint_intervals: {found} missing interval(s) to {now - timedelta(minutes=LAG_MINUTES):%Y-%m-%d %H:%M}")
    fixed = 0
    if missing and not check_only:
        net = net or Net()
        fixed += repair_current(db, missing, net, sleep, log)
        still = _survey_db(db, now, log)
        fixed += repair_archive(db, still, net, sleep, pause, max_days, log)
        store.refresh_readonly(db)
        remaining = _survey_db(db, now, log)
    else:
        remaining = missing
    old = [t for t in remaining if now - t > EMAIL_AGE]
    log(f"found {found}, fixed {found - len(remaining)}, remaining {len(remaining)} "
        f"({len(old)} older than 3 hours)")
    emailed = False
    if old and not no_email:
        if send is None:
            from weekly_integrity import send_email as send
        rows = "".join(f"<li>{t}</li>" for t in old[:50])
        emailed = bool(send(f"constraint_intervals: {len(old)} intervals still missing",
                            f"<p>{len(old)} intervals older than 3 hours remain missing after repair "
                            f"(first 50 listed).</p><ul>{rows}</ul>"))
    return dict(found=found, fixed=found - len(remaining), remaining=len(remaining), old=len(old),
                emailed=emailed, missing=remaining)


def main(argv=None, **kw) -> int:
    ap = argparse.ArgumentParser(description="Survey and repair constraint_intervals")
    ap.add_argument("--db", type=Path, default=None)
    ap.add_argument("--check-only", action="store_true")
    ap.add_argument("--max-days", type=int, default=40)
    ap.add_argument("--pause", type=float, default=10.0, help="seconds after each ARCHIVE daily zip")
    ap.add_argument("--no-email", action="store_true")
    a = ap.parse_args(argv)
    run(a.db or store.default_db_path(), check_only=a.check_only, max_days=a.max_days, pause=a.pause,
        no_email=a.no_email, **kw)
    return 0


if __name__ == "__main__":
    sys.exit(main())
