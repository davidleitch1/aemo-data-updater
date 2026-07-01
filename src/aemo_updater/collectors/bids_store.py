#!/usr/bin/env python3
"""DuckDB storage for AEMO bid data (separate ``bids.duckdb``).

Bids live in their own DuckDB file, not the 4.5-min ``aemo_test.duckdb``: bid data
updates ~daily, so folding 2-4 GB of it into the hot per-cycle readonly copy would
triple that I/O for no freshness benefit. This module owns the two tables and the
DELETE+INSERT merge (same idempotent pattern as the main collector), keyed on
(settlementdate, duid, direction) so re-ingesting a day replaces rather than dupes.
"""
from __future__ import annotations

from typing import List, Optional

import pandas as pd

from .bids_parser import PRICE_BANDS, VOL_BANDS

BID_KEYS: List[str] = ["settlementdate", "duid", "direction"]

_VOL_COLS_SQL = ",\n    ".join(
    ["settlementdate TIMESTAMP", "duid VARCHAR", "direction VARCHAR",
     "offerdate TIMESTAMP", "maxavail DOUBLE", "pasaavailability DOUBLE"]
    + [f"{b} DOUBLE" for b in VOL_BANDS]
)
_PRICE_COLS_SQL = ",\n    ".join(
    ["settlementdate TIMESTAMP", "duid VARCHAR", "direction VARCHAR",
     "offerdate TIMESTAMP"]
    + [f"{b} DOUBLE" for b in PRICE_BANDS]
)

CREATE_VOLUME_SQL = f"CREATE TABLE IF NOT EXISTS bid_volume5 (\n    {_VOL_COLS_SQL}\n)"
CREATE_PRICE_SQL = f"CREATE TABLE IF NOT EXISTS bid_price_bands (\n    {_PRICE_COLS_SQL}\n)"


def ensure_bids_tables(conn) -> None:
    """Create bid_volume5 and bid_price_bands if they don't exist."""
    conn.execute(CREATE_VOLUME_SQL)
    conn.execute(CREATE_PRICE_SQL)


def _merge(conn, table: str, df: Optional[pd.DataFrame], keys: List[str]) -> int:
    if df is None or df.empty:
        return 0
    conn.register("_bids_new", df)
    try:
        conn.execute("BEGIN TRANSACTION")
        key_list = ", ".join(keys)
        col_list = ", ".join(df.columns)
        conn.execute(
            f"DELETE FROM {table} WHERE ({key_list}) IN (SELECT {key_list} FROM _bids_new)"
        )
        conn.execute(f"INSERT INTO {table} ({col_list}) SELECT {col_list} FROM _bids_new")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.unregister("_bids_new")
    return len(df)


def merge_bids(conn, volume_df: Optional[pd.DataFrame],
               price_df: Optional[pd.DataFrame]) -> dict:
    """Merge parsed volume/price frames into bids.duckdb. Returns row counts."""
    ensure_bids_tables(conn)
    return {
        "bid_volume5": _merge(conn, "bid_volume5", volume_df, BID_KEYS),
        "bid_price_bands": _merge(conn, "bid_price_bands", price_df, BID_KEYS),
    }
