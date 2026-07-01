#!/usr/bin/env python3
"""Backfill historical AEMO bid data into bids.duckdb from NEMWEB.

Sources (NEMWEB retains ~13-14 months):
  * ARCHIVE/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_<YYYYMMDD>.zip  - monthly,
    each a zip-of-daily-zips
  * CURRENT/Bidmove_Complete/PUBLIC_BIDMOVE_COMPLETE_<YYYYMMDD>_<run>.zip - daily

Both ultimately contain the same daily CSV (BIDDAYOFFER_D + BIDPEROFFER_D). This
script downloads each, extracts every daily CSV, parses (quote-aware), and merges
into bids.duckdb with the same idempotent DELETE+INSERT as the live collector, so
re-runs are safe. By default it skips trading days already present.

Usage (on the collector host, with the venv):
    .venv/bin/python scripts/backfill_bids.py --db /path/bids.duckdb [--months N]
    .venv/bin/python scripts/backfill_bids.py --db ... --current-only
"""
from __future__ import annotations

import argparse
import io
import re
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterator, Optional

import duckdb
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.bids_parser import parse_bidperoffer, parse_biddayoffer  # noqa: E402
from aemo_updater.collectors.bids_store import ensure_bids_tables, merge_bids  # noqa: E402

ARCHIVE_URL = "https://nemweb.com.au/Reports/ARCHIVE/Bidmove_Complete/"
CURRENT_URL = "https://nemweb.com.au/Reports/CURRENT/Bidmove_Complete/"
HEADERS = {"User-Agent": "AEMO Dashboard Data Collector"}


def list_files(url: str) -> list[str]:
    r = requests.get(url, headers=HEADERS, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out = []
    for a in soup.find_all("a"):
        name = a.text.strip()
        if name.startswith("PUBLIC_BIDMOVE_COMPLETE_") and name.endswith(".zip"):
            out.append(name)
    return sorted(set(out))


def iter_daily_csv_bytes(zip_bytes: bytes) -> Iterator[tuple[str, bytes]]:
    """Yield (entry_name, csv_bytes) for each daily CSV in a Bidmove_Complete zip.

    Handles both a flat daily zip (contains one .CSV) and a monthly archive zip
    (contains many nested daily .zip files, each with one .CSV).
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            lower = name.lower()
            if lower.endswith(".csv"):
                yield name, z.read(name)
            elif lower.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(z.read(name))) as inner:
                    for iname in inner.namelist():
                        if iname.lower().endswith(".csv"):
                            yield iname, inner.read(iname)


def ingest_bids_csv(conn, csv_bytes: bytes) -> dict:
    """Parse one daily CSV and merge into bids.duckdb. Returns row counts."""
    vol = parse_bidperoffer(csv_bytes)
    price = parse_biddayoffer(csv_bytes)
    return merge_bids(conn, vol, price)


def _existing_days(conn) -> set:
    """Set of 'YYYYMMDD' trading days already present (keyed on price bands,
    whose settlementdate equals the source file's date)."""
    try:
        rows = conn.execute(
            "SELECT DISTINCT strftime(settlementdate, '%Y%m%d') FROM bid_price_bands"
        ).fetchall()
        return {r[0] for r in rows}
    except duckdb.Error:
        return set()


def _date_from_name(name: str) -> Optional[str]:
    # PUBLIC_BIDMOVE_COMPLETE_YYYYMMDD[_<run>].zip/.CSV  (archive has no run id)
    m = re.search(r"_(\d{8})(?:_|\.)", name)
    return m.group(1) if m else None


def download(url: str, name: str, retries: int = 3) -> Optional[bytes]:
    for attempt in range(retries):
        try:
            r = requests.get(f"{url}{name}", headers=HEADERS, timeout=300)
            r.raise_for_status()
            return r.content
        except Exception as e:
            print(f"  download error {name} (attempt {attempt+1}): {e}", flush=True)
            time.sleep(2 * (attempt + 1))
    return None


def backfill(db_path: str, months: Optional[int], current_only: bool,
             skip_existing: bool = True) -> None:
    conn = duckdb.connect(db_path)
    conn.execute("PRAGMA force_compression='Dictionary'")
    ensure_bids_tables(conn)

    sources = []
    if not current_only:
        archive = list_files(ARCHIVE_URL)
        if months is not None:
            archive = archive[-months:]
        sources += [(ARCHIVE_URL, n) for n in archive]
    sources += [(CURRENT_URL, n) for n in list_files(CURRENT_URL)]

    print(f"{len(sources)} source files to process -> {db_path}", flush=True)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS bids_backfill_sources "
        "(name VARCHAR PRIMARY KEY, processed_at TIMESTAMP)"
    )
    done_sources = {
        r[0] for r in conn.execute("SELECT name FROM bids_backfill_sources").fetchall()
    } if skip_existing else set()
    existing = _existing_days(conn) if skip_existing else set()

    total_days = 0
    for url, name in sources:
        # Whole-file skip: archive months are stable; CURRENT names carry a run id
        # so a re-published day is a new name and will be reprocessed.
        if name in done_sources:
            continue
        t0 = time.time()
        blob = download(url, name)
        if blob is None:
            print(f"SKIP (download failed): {name}", flush=True)
            continue
        day_count = 0
        for entry_name, csv_bytes in iter_daily_csv_bytes(blob):
            day = _date_from_name(entry_name)
            if skip_existing and day in existing:
                continue
            counts = ingest_bids_csv(conn, csv_bytes)
            if counts["bid_volume5"] or counts["bid_price_bands"]:
                day_count += 1
                total_days += 1
                if day:
                    existing.add(day)
        conn.execute(
            "INSERT OR REPLACE INTO bids_backfill_sources VALUES (?, now())", [name]
        )
        print(f"{name}: {day_count} days ingested in {time.time()-t0:.0f}s", flush=True)

    vol_n = conn.execute("SELECT COUNT(*) FROM bid_volume5").fetchone()[0]
    span = conn.execute(
        "SELECT MIN(settlementdate), MAX(settlementdate) FROM bid_volume5"
    ).fetchone()
    print(f"DONE: {total_days} day-files ingested; bid_volume5 rows={vol_n} span={span}", flush=True)
    conn.close()


def main():
    ap = argparse.ArgumentParser(description="Backfill AEMO bids into bids.duckdb")
    ap.add_argument("--db", required=True, help="Path to bids.duckdb")
    ap.add_argument("--months", type=int, default=None,
                    help="Limit to the last N monthly archive files (plus CURRENT)")
    ap.add_argument("--current-only", action="store_true",
                    help="Only CURRENT (last ~60 days), skip ARCHIVE")
    ap.add_argument("--no-skip-existing", action="store_true",
                    help="Re-ingest days already present (default: skip)")
    args = ap.parse_args()
    backfill(args.db, args.months, args.current_only, not args.no_skip_existing)


if __name__ == "__main__":
    main()
