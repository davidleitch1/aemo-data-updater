#!/usr/bin/env python3
"""Backfill AEMO network-constraint history from the MMSDM monthly archive into constraints.duckdb.

Tables (see src/aemo_updater/constraints_store.py for the schema and keys):
  interval tables   DISPATCHCONSTRAINT, DISPATCH_LOCAL_PRICE      --start .. --end
  definition tables GENCONDATA, SPD*CONSTRAINT, GENCONSET,
                    GENCONSETINVOKE                                --defs-from .. --end
                    (monthly files are incremental: each holds only the versions effective
                    that month, so the sweep must cover every month back to --defs-from)
  snapshot tables   NETWORK_OUTAGEDETAIL, NETWORK_OUTAGECONSTRAINTSET,
                    DUDETAILSUMMARY                                --snapshots, --end month only
                    (cumulative snapshots)

Months run newest first. One table-month's zip(s) are downloaded to a temp dir beside the
database, loaded, then deleted. Inserts are anti-joins on the key, and a table-month already in
_loaded is skipped without downloading, so the script can be re-run or extended backwards.
nemweb rate-limits bursts from the shared public IP: sleep --pause seconds after every download
over 5 MB and 1 s after small ones.

    .venv/bin/python scripts/backfill_constraints.py --start 2024-07 --end 2026-10 \\
        --defs-from 2015-01 --snapshots [--tables T1,T2] [--pause 20] [--db PATH] [--dry-run]

Scheduled use (--auto): end = the newest month whose MMSDM directory exists (last month, else the
month before); definition tables are swept from two months before end; interval tables load for
the end month only and --snapshots is implied. The end month's interval tables are what the live
collector also wrote, so each is logged as "inserted N, already present M": inserted > 0 means
live collection missed those rows and the archive supplied them.

    .venv/bin/python scripts/backfill_constraints.py --auto [--pause 20] [--db PATH]
"""
from __future__ import annotations

import argparse
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.parse
import zipfile
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from aemo_updater import constraints_store as store  # noqa: E402

HOST = "https://nemweb.com.au"
DIR_URL = (HOST + "/Data_Archive/Wholesale_Electricity/MMSDM/{y}/MMSDM_{y}_{m:02d}/"
           "MMSDM_Historical_Data_SQLLoader/DATA/")
UA = "Mozilla/5.0"
LARGE_BYTES = 5_000_000

INTERVAL_TABLES = ["DISPATCHCONSTRAINT", "DISPATCH_LOCAL_PRICE"]
DEFINITION_TABLES = ["GENCONDATA", "SPDCONNECTIONPOINTCONSTRAINT", "SPDINTERCONNECTORCONSTRAINT",
                     "SPDREGIONCONSTRAINT", "GENCONSET", "GENCONSETINVOKE"]
SNAPSHOT_TABLES = ["NETWORK_OUTAGEDETAIL", "NETWORK_OUTAGECONSTRAINTSET", "DUDETAILSUMMARY"]


class Unavailable(Exception):
    """The month's directory (or file) does not exist on nemweb (HTTP 404)."""


# ---------------------------------------------------------------- months and name matching

def parse_ym(s: str) -> tuple[int, int]:
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {s!r}")
    return int(m.group(1)), int(m.group(2))


def months_desc(start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    out, (y, m) = [], start
    while (y, m) <= end:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out[::-1]


def match(hrefs, table: str) -> list[tuple[int, str]]:
    """(part, href) for the table's zips: PUBLIC_ARCHIVE#T#FILEnn#YYYYMM010000.zip (part = nn)
    and the older PUBLIC_DVD_T_YYYYMM010000.zip (part 1). Sorted by part."""
    t = re.escape(table)
    out = []
    for h in hrefs:
        name = urllib.parse.unquote(h.rsplit("/", 1)[-1])
        a = re.fullmatch(rf"PUBLIC_ARCHIVE#{t}#FILE(\d+)#\d+\.zip", name, flags=re.I)
        if a:
            out.append((int(a.group(1)), h))
        elif re.fullmatch(rf"PUBLIC_DVD_{t}_\d+\.zip", name, flags=re.I):
            out.append((1, h))
    return sorted(out)


# ---------------------------------------------------------------- network (curl)

def _curl(url: str, dest: str) -> int:
    r = subprocess.run(["curl", "-sS", "--retry", "6", "--retry-all-errors", "--retry-delay", "5",
                        "-A", UA, "-o", dest, "-w", "%{http_code}", url],
                       capture_output=True, text=True, check=True)
    return int(r.stdout.strip() or 0)


def curl_listing(ym: str) -> list[str]:
    y, m = map(int, ym.split("-"))
    with tempfile.TemporaryDirectory() as td:
        dest = str(Path(td) / "list.html")
        status = _curl(DIR_URL.format(y=y, m=m), dest)
        if status == 404:
            raise Unavailable(ym)
        if status != 200:
            raise RuntimeError(f"listing {ym}: HTTP {status}")
        html = Path(dest).read_text(encoding="latin1")
    return re.findall(r'HREF="([^"]+\.zip)"', html, flags=re.I)


def curl_download(url: str, dest: str) -> None:
    status = _curl(url, dest)
    if status == 404:
        raise Unavailable(url)
    if status != 200:
        raise RuntimeError(f"download HTTP {status}: {url}")


# ---------------------------------------------------------------- planning

def plan(args) -> dict[str, list[str]]:
    """{ym: [tables]} for every month with work, built from the CLI selections."""
    end = args.end
    wanted = {t.upper() for t in args.tables.split(",")} if args.tables else None
    out: dict[tuple[int, int], list[str]] = {}

    def add(ym, tables):
        for t in tables:
            if wanted is None or t in wanted:
                out.setdefault(ym, []).append(t)

    if args.start:
        for ym in months_desc(args.start, end):
            add(ym, INTERVAL_TABLES)
    if args.defs_from:
        for ym in months_desc(args.defs_from, end):
            add(ym, DEFINITION_TABLES)
    if args.snapshots:
        add(end, SNAPSHOT_TABLES)
    return {f"{y}-{m:02d}": ts for (y, m), ts in sorted(out.items(), reverse=True)}


def previous_months(today, n: int = 2) -> list[tuple[int, int]]:
    """The n months before today's month, nearest first."""
    y, m = today.year, today.month
    out = []
    for _ in range(n):
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
        out.append((y, m))
    return out


def shift_month(ym: tuple[int, int], back: int) -> tuple[int, int]:
    y, m = ym
    for _ in range(back):
        y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return y, m


def pick_auto_end(listing, today):
    """Newest month (last month, then the month before) whose MMSDM directory exists."""
    for y, m in previous_months(today):
        try:
            listing(f"{y}-{m:02d}")
            return (y, m)
        except Unavailable:
            continue
    return None


def _fmt(results) -> str:
    return "; ".join(f"{t} inserted {i}, already present {p}" for t, i, p in results)


def _extract(zip_path: Path, dest_csv: Path) -> None:
    # '#' in AEMO names breaks the DuckDB sniffer, so the CSV is always written as data*.csv
    with zipfile.ZipFile(zip_path) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        with z.open(name) as src, open(dest_csv, "wb") as out:
            shutil.copyfileobj(src, out, 1 << 20)


# ---------------------------------------------------------------- main

def main(argv=None, *, listing=curl_listing, download=curl_download, sleep=time.sleep, log=None,
         today=None) -> int:
    log = log or (lambda s: print(s, flush=True))
    ap = argparse.ArgumentParser(description="Backfill constraint history from MMSDM")
    ap.add_argument("--start", type=parse_ym, help="first month of the interval tables (YYYY-MM)")
    ap.add_argument("--end", type=parse_ym, help="last month (YYYY-MM)")
    ap.add_argument("--auto", action="store_true",
                    help="scheduled mode: newest published month, 2-month definition sweep, snapshots")
    ap.add_argument("--defs-from", type=parse_ym, help="first month of the definition-table sweep")
    ap.add_argument("--snapshots", action="store_true", help="load snapshot tables from the --end month")
    ap.add_argument("--tables", help="comma-separated source tables to restrict to")
    ap.add_argument("--pause", type=float, default=20.0, help="seconds to sleep after a download over 5 MB")
    ap.add_argument("--db", type=Path, default=None)
    ap.add_argument("--dry-run", action="store_true", help="list what would be fetched")
    args = ap.parse_args(argv)
    if args.auto:
        if args.start or args.end or args.defs_from:
            ap.error("--auto picks its own months; do not combine with --start/--end/--defs-from")
        from datetime import date
        memo = {}

        def listing_once(ym, _inner=listing):
            if ym not in memo:
                memo[ym] = _inner(ym)
            return memo[ym]

        listing = listing_once
        args.end = pick_auto_end(listing, today or date.today())
        if args.end is None:
            log("auto: no MMSDM directory for the last two months")
            return 1
        args.start, args.defs_from, args.snapshots = args.end, shift_month(args.end, 2), True
        log(f"auto: end month {args.end[0]}-{args.end[1]:02d}, definitions from "
            f"{args.defs_from[0]}-{args.defs_from[1]:02d}")
    elif args.end is None:
        ap.error("--end is required (or use --auto)")
    if not (args.start or args.defs_from or args.snapshots):
        ap.error("nothing to do: give --start, --defs-from and/or --snapshots")
    if args.start and args.start > args.end:
        ap.error("--start is after --end")
    if args.defs_from and args.defs_from > args.end:
        ap.error("--defs-from is after --end")
    if args.tables:
        bad = [t for t in args.tables.upper().split(",") if t not in store.SOURCE_TABLES]
        if bad:
            ap.error(f"unknown table(s): {bad}")
    db = args.db or store.default_db_path()

    work = plan(args)
    con = None
    if not args.dry_run:
        db.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db))
        store.ensure_tables(con)
    elif db.exists():
        con = duckdb.connect(str(db), read_only=True)

    failed = 0
    log(f"constraints backfill: {len(work)} months -> {db}{' (dry run)' if args.dry_run else ''}")
    try:
        for ym, tables in work.items():            # newest first
            pending = []
            for t in tables:
                if con is not None and store.is_loaded(con, t, ym):
                    log(f"{ym} {t}: already loaded, skipped")
                else:
                    pending.append(t)
            if not pending:
                continue
            try:
                hrefs = listing(ym)
            except Unavailable:
                for t in pending:
                    log(f"{ym} {t}: unavailable (no archive directory for this month)")
                continue
            except Exception as e:  # noqa: BLE001
                failed += 1
                log(f"{ym}: FAILED listing: {e}")
                continue
            for t in pending:
                parts = match(hrefs, t)
                if not parts:
                    log(f"{ym} {t}: NOT FOUND")
                    continue
                if args.dry_run:
                    log(f"{ym} {t}: would fetch {len(parts)} file(s): "
                        + ", ".join(urllib.parse.unquote(h.rsplit('/', 1)[-1]) for _, h in parts))
                    continue
                tmp = Path(tempfile.mkdtemp(prefix="bf_", dir=str(db.parent)))
                try:
                    csvs = []
                    for part, href in parts:
                        zp = tmp / f"part{part}.zip"
                        download(urllib.parse.urljoin(HOST, href), str(zp))
                        big = zp.stat().st_size > LARGE_BYTES
                        sleep(args.pause if big else 1.0)
                        csv = tmp / f"data{part}.csv"
                        _extract(zp, csv)
                        zp.unlink()
                        csvs.append((part, csv))
                    results = store.load_month_table(con, t, ym, csvs)
                    log(f"{ym} {t}: {_fmt(results)}")
                    if args.auto and t in INTERVAL_TABLES and any(i for _, i, _ in results):
                        log(f"{ym} {t}: RECONCILE inserted rows the live collector did not hold "
                            f"({', '.join(f'{n} {i}' for n, i, _ in results if i)})")
                except Unavailable:
                    log(f"{ym} {t}: unavailable (file listed but not downloadable)")
                except Exception as e:  # noqa: BLE001
                    failed += 1
                    log(f"{ym} {t}: FAILED: {e}")
                finally:
                    shutil.rmtree(tmp, ignore_errors=True)
    finally:
        if con is not None:
            con.close()
    log(f"done: {failed} failure(s)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
