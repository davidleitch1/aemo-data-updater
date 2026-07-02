#!/usr/bin/env python3
"""Backfill historical AEMO bids from the MMSDM Data_Archive (DuckDB-native, fast).

Source (monthly, per-table DVD files):
  https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/<Y>/MMSDM_<Y>_<M>/
    MMSDM_Historical_Data_SQLLoader/DATA/
      PUBLIC_DVD_BIDDAYOFFER_<YYYYMM>010000.zip   -> table BIDDAYOFFER (prices)
      PUBLIC_DVD_BIDPEROFFER[_1|_2]_<YYYYMM>...zip -> table BIDOFFERPERIOD (5-min volumes)

AEMO archive quirks handled:
  * from 2022-06 the volume file is SPLIT into BIDPEROFFER1 + BIDPEROFFER2 (try single,
    then the two halves);
  * DIRECTION column absent before ~2024 -> default GEN, …L suffix -> LOAD;
  * BIDOFFERPERIOD has no interval timestamp -> reconstruct from TRADINGDATE + PERIODID
    (5-min: interval = TRADINGDATE 04:00 + PERIODID*5min).

Speed: each CSV is extracted and read with DuckDB's C++ read_csv; the ENERGY filter,
interval reconstruction, dedup (latest offer) and merge happen in SQL — ~5-8x faster
than row-by-row Python. Idempotent per month (DELETE+INSERT on the month's dates) with
a source-skip log. CHECKPOINTs at the end.

Run:  .venv/bin/python scripts/backfill_bids_mmsdm.py --db bids_history.duckdb \
          --start 2021-07 --end 2024-07 [--tmp /path]
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile
import time
import zipfile
from datetime import date

import duckdb
import requests

sys.path.insert(0, str(__file__.rsplit("/scripts/", 1)[0] + "/src"))
from aemo_updater.collectors.bids_store import ensure_bids_tables  # noqa: E402

BASE = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM"
HEADERS = {"User-Agent": "AEMO Dashboard Data Collector"}
VOL_TABLE = "BIDOFFERPERIOD"
PRICE_TABLE = "BIDDAYOFFER"
VBANDS = [f"bandavail{i}" for i in range(1, 11)]
PBANDS = [f"priceband{i}" for i in range(1, 11)]


def month_url(y: int, m: int, table_file: str) -> str:
    return (f"{BASE}/{y}/MMSDM_{y}_{m:02d}/MMSDM_Historical_Data_SQLLoader/DATA/"
            f"PUBLIC_DVD_{table_file}_{y}{m:02d}010000.zip")


def months(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        m = 1 if m == 12 else m + 1
        y = y + 1 if m == 1 else y


def _download(url, dest, retries=3):
    for a in range(retries):
        try:
            with requests.get(url, headers=HEADERS, timeout=1800, stream=True) as r:
                if r.status_code == 404:
                    return False
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_content(1 << 20):
                        fh.write(chunk)
            return True
        except Exception as e:
            print(f"  download error {os.path.basename(url)} ({a+1}): {e}", flush=True)
            time.sleep(3 * (a + 1))
    return False


def _extract_csv(zip_path, tmpdir):
    with zipfile.ZipFile(zip_path) as z:
        name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        out = os.path.join(tmpdir, os.path.basename(name))
        with z.open(name) as src, open(out, "wb") as dst:
            shutil.copyfileobj(src, dst, length=1 << 24)
    return out


def _header_cols(csv_path, table):
    """Column names for a table's I-row (peek only the first lines)."""
    with open(csv_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if line.startswith("I,"):
                parts = line.rstrip("\r\n").split(",")
                if len(parts) > 3 and parts[2] == table:
                    return parts[4:]
            if i > 1000:
                break
    return None


def _read_csv(csv_path, ncols):
    cols = ", ".join(f"'c{i}': 'VARCHAR'" for i in range(ncols))
    p = csv_path.replace("'", "''")
    return (f"read_csv('{p}', header=false, all_varchar=true, sep=',', quote='\"', "
            f"null_padding=true, ignore_errors=true, columns={{{cols}}})")


def _direction_expr(pos, duid):
    if "DIRECTION" in pos:
        d = pos["DIRECTION"]
        return (f"CASE WHEN upper(trim({d})) IN ('GEN','LOAD','BIDIRECTIONAL') "
                f"THEN upper(trim({d})) WHEN trim({duid}) LIKE '%L' THEN 'LOAD' ELSE 'GEN' END")
    return f"CASE WHEN trim({duid}) LIKE '%L' THEN 'LOAD' ELSE 'GEN' END"


def _ts(col):
    return f"try_strptime(trim(both '\"' from {col}), '%Y/%m/%d %H:%M:%S')"


def _merge_stage(conn, stg, target, key_cols, out_cols):
    n = conn.execute(f"SELECT COUNT(*) FROM {stg}").fetchone()[0]
    if n == 0:
        conn.execute(f"DROP TABLE IF EXISTS {stg}")
        return 0
    conn.execute("BEGIN TRANSACTION")
    conn.execute(f"DELETE FROM {target} WHERE settlementdate IN (SELECT DISTINCT settlementdate FROM {stg})")
    cols = ", ".join(out_cols)
    conn.execute(
        f"INSERT INTO {target} ({cols}) SELECT {cols} FROM "
        f"(SELECT *, row_number() OVER (PARTITION BY {', '.join(key_cols)} "
        f"ORDER BY offerdate DESC) rn FROM {stg}) WHERE rn=1")
    got = conn.execute(
        f"SELECT COUNT(*) FROM {target} WHERE settlementdate IN (SELECT DISTINCT settlementdate FROM {stg})"
    ).fetchone()[0]
    conn.execute("COMMIT")
    conn.execute(f"DROP TABLE {stg}")
    return got


def ingest_volumes(conn, csv_paths):
    conn.execute("CREATE OR REPLACE TEMP TABLE stgv AS SELECT * FROM bid_volume5 WHERE 1=0")
    for csv_path in csv_paths:
        hdr = _header_cols(csv_path, VOL_TABLE)
        if not hdr:
            continue
        ncols = 4 + len(hdr)
        pos = {name: f"c{4 + i}" for i, name in enumerate(hdr)}
        g = lambda *ns: next(pos[n] for n in ns if n in pos)  # noqa: E731
        tdate, pid = g("TRADINGDATE", "SETTLEMENTDATE"), g("PERIODID")
        settlement = f"{_ts(tdate)} + to_minutes(240 + 5*try_cast({pid} AS INTEGER))"
        bands = ", ".join(f"try_cast({g(f'BANDAVAIL{i}')} AS DOUBLE)" for i in range(1, 11))
        conn.execute(f"""
            INSERT INTO stgv
            SELECT {settlement}, trim({g('DUID')}), {_direction_expr(pos, g('DUID'))},
                   {_ts(g('OFFERDATETIME', 'OFFERDATE'))},
                   try_cast({g('MAXAVAIL')} AS DOUBLE), try_cast({g('PASAAVAILABILITY')} AS DOUBLE),
                   {bands}
            FROM {_read_csv(csv_path, ncols)}
            WHERE c0='D' AND c2='{VOL_TABLE}' AND trim({g('BIDTYPE')})='ENERGY'
        """)
    out = ["settlementdate", "duid", "direction", "offerdate", "maxavail", "pasaavailability"] + VBANDS
    return _merge_stage(conn, "stgv", "bid_volume5", ["settlementdate", "duid", "direction"], out)


def ingest_prices(conn, csv_path):
    hdr = _header_cols(csv_path, PRICE_TABLE)
    if not hdr:
        return 0
    ncols = 4 + len(hdr)
    pos = {name: f"c{4 + i}" for i, name in enumerate(hdr)}
    g = lambda *ns: next(pos[n] for n in ns if n in pos)  # noqa: E731
    bands = ", ".join(f"try_cast({g(f'PRICEBAND{i}')} AS DOUBLE)" for i in range(1, 11))
    conn.execute("CREATE OR REPLACE TEMP TABLE stgp AS SELECT * FROM bid_price_bands WHERE 1=0")
    conn.execute(f"""
        INSERT INTO stgp
        SELECT {_ts(g('SETTLEMENTDATE', 'TRADINGDATE'))}, trim({g('DUID')}),
               {_direction_expr(pos, g('DUID'))}, {_ts(g('OFFERDATE', 'OFFERDATETIME'))}, {bands}
        FROM {_read_csv(csv_path, ncols)}
        WHERE c0='D' AND c2='{PRICE_TABLE}' AND trim({g('BIDTYPE')})='ENERGY'
    """)
    out = ["settlementdate", "duid", "direction", "offerdate"] + PBANDS
    return _merge_stage(conn, "stgp", "bid_price_bands", ["settlementdate", "duid", "direction"], out)


def download_price_csv(y, m, tmp):
    zp = os.path.join(tmp, "price.zip")
    if _download(month_url(y, m, PRICE_TABLE), zp):
        p = _extract_csv(zp, tmp)
        os.remove(zp)
        return p
    return None


def download_volume_csvs(y, m, tmp):
    paths = []
    zp = os.path.join(tmp, "vol.zip")
    if _download(month_url(y, m, "BIDPEROFFER"), zp):   # single (2021-07..2022-05)
        paths.append(_extract_csv(zp, tmp))
        os.remove(zp)
    else:                                               # split (2022-06+)
        for suf in ("1", "2"):
            zp = os.path.join(tmp, f"vol{suf}.zip")
            if _download(month_url(y, m, f"BIDPEROFFER{suf}"), zp):
                paths.append(_extract_csv(zp, tmp))
                os.remove(zp)
    return paths


def _coverage_end(conn):
    row = conn.execute("SELECT MIN(settlementdate) FROM bid_price_bands").fetchone()
    if not row or row[0] is None:
        t = date.today()
        return (t.year, t.month)
    d = row[0]
    return (d.year - 1, 12) if d.month == 1 else (d.year, d.month - 1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--start", default="2021-07")
    ap.add_argument("--end", help="YYYY-MM (default: month before existing coverage)")
    ap.add_argument("--month", help="only this YYYY-MM (test)")
    ap.add_argument("--tmp", default=None, help="scratch dir for CSV extraction")
    args = ap.parse_args()

    conn = duckdb.connect(args.db)
    conn.execute("PRAGMA force_compression='Dictionary'")
    ensure_bids_tables(conn)
    conn.execute("CREATE TABLE IF NOT EXISTS bids_backfill_sources (name VARCHAR PRIMARY KEY, processed_at TIMESTAMP)")
    done = {r[0] for r in conn.execute("SELECT name FROM bids_backfill_sources").fetchall()}

    sy, sm = map(int, args.start.split("-"))
    if args.month:
        month_iter = [tuple(map(int, args.month.split("-")))]
    else:
        end = tuple(map(int, args.end.split("-"))) if args.end else _coverage_end(conn)
        month_iter = list(months((sy, sm), end))
    print(f"MMSDM backfill (duckdb-native): {len(month_iter)} months -> {args.db}", flush=True)

    tmp = args.tmp or tempfile.mkdtemp(prefix="mmsdm_")
    os.makedirs(tmp, exist_ok=True)
    for y, m in month_iter:
        tag = f"MMSDM_{y}_{m:02d}"
        if tag in done:
            continue
        t0 = time.time()
        np_ = nv = 0
        pc = download_price_csv(y, m, tmp)
        if pc:
            np_ = ingest_prices(conn, pc)
            os.remove(pc)
        vcs = download_volume_csvs(y, m, tmp)
        if vcs:
            nv = ingest_volumes(conn, vcs)
            for p in vcs:
                os.remove(p)
        conn.execute("INSERT OR REPLACE INTO bids_backfill_sources VALUES (?, now())", [tag])
        print(f"{tag}: prices={np_:,} volumes={nv:,} in {time.time()-t0:.0f}s", flush=True)

    print("CHECKPOINT…", flush=True)
    conn.execute("CHECKPOINT")
    v = conn.execute("SELECT MIN(settlementdate), MAX(settlementdate), COUNT(*) FROM bid_volume5").fetchone()
    print(f"DONE. bid_volume5 span={v[0]}..{v[1]} rows={v[2]:,}", flush=True)
    conn.close()


if __name__ == "__main__":
    main()
