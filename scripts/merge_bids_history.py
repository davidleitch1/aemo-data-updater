#!/usr/bin/env python3
"""Merge bids_history.duckdb (5-yr MMSDM backfill) into the production bids.duckdb.

Inserts historical rows strictly BEFORE the Bidmove_Complete coverage boundary, so
the two sources don't overlap and there's no gap (history must cover up to the
boundary). Idempotent: clears any prior history import (< boundary) and re-inserts.

Connects with retry because the live collector occasionally holds the write lock
(only when a new daily bid/dispatch file lands). Run at a quiet time; verify recent
data afterwards. Then refresh the readonly replica separately.

    .venv/bin/python scripts/merge_bids_history.py --db .../bids.duckdb \
        --history .../bids_history.duckdb [--boundary '2025-05-02 04:05:00']
"""
import argparse
import sys
import time

import duckdb

DEFAULT_BOUNDARY = "2025-05-02 04:05:00"  # Bidmove_Complete coverage start


def connect_with_retry(path: str, attempts: int = 12):
    for a in range(attempts):
        try:
            return duckdb.connect(path)
        except duckdb.IOException as e:
            print(f"bids.duckdb locked, retry {a+1}/{attempts}: {e}", flush=True)
            time.sleep(6)
    sys.exit("could not acquire write lock on bids.duckdb")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="production bids.duckdb")
    ap.add_argument("--history", required=True, help="bids_history.duckdb")
    ap.add_argument("--boundary", default=DEFAULT_BOUNDARY,
                    help="import history rows with settlementdate < this")
    args = ap.parse_args()

    conn = connect_with_retry(args.db)
    conn.execute("PRAGMA force_compression='Dictionary'")
    conn.execute(f"ATTACH '{args.history}' AS hist (READ_ONLY)")

    for tbl in ("bid_price_bands", "bid_volume5"):
        conn.execute("BEGIN TRANSACTION")
        conn.execute(f"DELETE FROM {tbl} WHERE settlementdate < ?", [args.boundary])
        conn.execute(f"INSERT INTO {tbl} SELECT * FROM hist.{tbl} "
                     f"WHERE settlementdate < ?", [args.boundary])
        conn.execute("COMMIT")
        n, mn, mx = conn.execute(
            f"SELECT COUNT(*), MIN(settlementdate), MAX(settlementdate) FROM {tbl}"
        ).fetchone()
        print(f"{tbl}: rows={n:,} span={mn}..{mx}", flush=True)

    print("CHECKPOINT (compacting)…", flush=True)
    conn.execute("CHECKPOINT")
    conn.close()
    print("MERGE DONE", flush=True)


if __name__ == "__main__":
    main()
