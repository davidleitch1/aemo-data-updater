#!/usr/bin/env python3
"""Backfill 5 years of historical AEMO bids from the MMSDM Data_Archive.

Source (monthly, per-table DVD files):
  https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/<Y>/MMSDM_<Y>_<M>/
    MMSDM_Historical_Data_SQLLoader/DATA/
      PUBLIC_DVD_BIDDAYOFFER_<YYYYMM>010000.zip   (table BIDDAYOFFER, small)
      PUBLIC_DVD_BIDPEROFFER_<YYYYMM>010000.zip   (table BIDOFFERPERIOD, ~8 GB CSV)

Prices (BIDDAYOFFER) parse in-memory. Volumes (BIDOFFERPERIOD) STREAM the zip's CSV
line-by-line and stage into DuckDB in batches, then dedup (latest offer per
interval/duid/direction) and merge — memory-safe for the multi-GB files.

Idempotent: per-month DELETE+INSERT keyed on the month's dates, plus a source-skip
log. By default stops before the Bidmove_Complete coverage already in the DB, so the
two sources don't overlap. CHECKPOINTs at the end to compact the file.

Run on the collector host with its venv:
    .venv/bin/python scripts/backfill_bids_mmsdm.py --db /path/bids.duckdb [--start 2021-07] [--month 2021-07]
"""
from __future__ import annotations

import argparse
import io
import os
import sys
import tempfile
import time
import zipfile
from datetime import date

import duckdb
import requests

sys.path.insert(0, str(__file__.rsplit("/scripts/", 1)[0] + "/src"))

from aemo_updater.collectors.bids_mmsdm import (  # noqa: E402
    bidofferperiod_frame, iter_bidofferperiod_rows, parse_biddayoffer_mmsdm,
)
from aemo_updater.collectors.bids_store import ensure_bids_tables, merge_bids  # noqa: E402

BASE = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"
HEADERS = {"User-Agent": "AEMO Dashboard Data Collector"}
DEFAULT_START = (2021, 7)
BATCH = 1_000_000


def month_url(y: int, m: int, table_file: str) -> str:
    return (f"{BASE}/{y}/MMSDM_{y}_{m:02d}/MMSDM_Historical_Data_SQLLoader/DATA/"
            f"PUBLIC_DVD_{table_file}_{y}{m:02d}010000.zip")


def months(start: tuple[int, int], end: tuple[int, int]):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y


def _download(url: str, dest: str) -> bool:
    for attempt in range(3):
        try:
            with requests.get(url, headers=HEADERS, timeout=1200, stream=True) as r:
                if r.status_code == 404:
                    return False
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
            return True
        except Exception as e:
            print(f"  download error (attempt {attempt+1}): {e}", flush=True)
            time.sleep(3 * (attempt + 1))
    return False


def _csv_lines(zip_path: str):
    with zipfile.ZipFile(zip_path) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        with z.open(name) as raw:
            for line in io.TextIOWrapper(raw, encoding="utf-8", errors="ignore"):
                yield line


def ingest_prices(conn, zip_path: str) -> int:
    with zipfile.ZipFile(zip_path) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        content = z.read(name)
    price_df = parse_biddayoffer_mmsdm(content)
    return merge_bids(conn, None, price_df)["bid_price_bands"]


def ingest_volumes(conn, zip_path: str) -> int:
    """Stream BIDOFFERPERIOD -> staging -> dedup latest offer -> merge into bid_volume5."""
    conn.execute("CREATE OR REPLACE TEMP TABLE stg_vol AS SELECT * FROM bid_volume5 WHERE 1=0")
    batch, staged = [], 0
    for row in iter_bidofferperiod_rows(_csv_lines(zip_path)):
        batch.append(row)
        if len(batch) >= BATCH:
            staged += _flush(conn, batch); batch = []
    staged += _flush(conn, batch)
    if staged == 0:
        conn.execute("DROP TABLE IF EXISTS stg_vol")
        return 0
    conn.execute("BEGIN TRANSACTION")
    conn.execute("DELETE FROM bid_volume5 WHERE settlementdate IN "
                 "(SELECT DISTINCT settlementdate FROM stg_vol)")
    cols = ", ".join(["settlementdate", "duid", "direction", "offerdate", "maxavail",
                      "pasaavailability"] + [f"bandavail{i}" for i in range(1, 11)])
    conn.execute(f"""
        INSERT INTO bid_volume5 ({cols})
        SELECT {cols} FROM (
            SELECT *, row_number() OVER (
                PARTITION BY settlementdate, duid, direction ORDER BY offerdate DESC) rn
            FROM stg_vol) WHERE rn = 1
    """)
    n = conn.execute("SELECT COUNT(*) FROM bid_volume5 WHERE settlementdate IN "
                     "(SELECT DISTINCT settlementdate FROM stg_vol)").fetchone()[0]
    conn.execute("COMMIT")
    conn.execute("DROP TABLE stg_vol")
    return n


def _flush(conn, batch: list) -> int:
    if not batch:
        return 0
    df = bidofferperiod_frame(batch)
    conn.register("_b", df)
    conn.execute("INSERT INTO stg_vol SELECT * FROM _b")
    conn.unregister("_b")
    return len(df)


def _coverage_end(conn) -> tuple[int, int]:
    """Last MMSDM month to fill = month before existing Bidmove coverage."""
    row = conn.execute("SELECT MIN(settlementdate) FROM bid_price_bands").fetchone()
    if not row or row[0] is None:
        t = date.today()
        return (t.year, t.month)
    d = row[0]
    y, m = d.year, d.month
    return (y - 1, 12) if m == 1 else (y, m - 1)


def main():
    ap = argparse.ArgumentParser(description="Backfill historical bids from MMSDM")
    ap.add_argument("--db", required=True)
    ap.add_argument("--start", default="2021-07", help="YYYY-MM (default 2021-07)")
    ap.add_argument("--end", help="YYYY-MM last month to fill (default: month before "
                    "existing Bidmove coverage in --db)")
    ap.add_argument("--month", help="Only this YYYY-MM (for testing)")
    ap.add_argument("--prices-only", action="store_true")
    args = ap.parse_args()

    conn = duckdb.connect(args.db)
    conn.execute("PRAGMA force_compression='Dictionary'")
    ensure_bids_tables(conn)
    conn.execute("CREATE TABLE IF NOT EXISTS bids_backfill_sources "
                 "(name VARCHAR PRIMARY KEY, processed_at TIMESTAMP)")
    done = {r[0] for r in conn.execute("SELECT name FROM bids_backfill_sources").fetchall()}

    sy, sm = map(int, args.start.split("-"))
    if args.month:
        my, mm = map(int, args.month.split("-"))
        month_iter = [(my, mm)]
    else:
        end = tuple(map(int, args.end.split("-"))) if args.end else _coverage_end(conn)
        month_iter = list(months((sy, sm), end))
    print(f"MMSDM backfill: {len(month_iter)} months -> {args.db}", flush=True)

    tmpdir = tempfile.mkdtemp(prefix="mmsdm_")
    for y, m in month_iter:
        tag = f"MMSDM_{y}_{m:02d}"
        if tag in done:
            continue
        t0 = time.time()
        # prices
        dp = os.path.join(tmpdir, "day.zip")
        np_ = 0
        if _download(month_url(y, m, "BIDDAYOFFER"), dp):
            np_ = ingest_prices(conn, dp)
            os.remove(dp)
        # volumes
        nv = 0
        if not args.prices_only:
            vp = os.path.join(tmpdir, "per.zip")
            if _download(month_url(y, m, "BIDPEROFFER"), vp):
                nv = ingest_volumes(conn, vp)
                os.remove(vp)
        conn.execute("INSERT OR REPLACE INTO bids_backfill_sources VALUES (?, now())", [tag])
        print(f"{tag}: prices={np_:,} volumes={nv:,} in {time.time()-t0:.0f}s", flush=True)

    print("CHECKPOINT (compacting)…", flush=True)
    conn.execute("CHECKPOINT")
    span = conn.execute("SELECT MIN(settlementdate), MAX(settlementdate), COUNT(*) "
                        "FROM bid_volume5").fetchone()
    print(f"DONE. bid_volume5 span={span[0]}..{span[1]} rows={span[2]:,}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
