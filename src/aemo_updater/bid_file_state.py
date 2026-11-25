"""Remember which daily bid files the collector has merged, across restarts.

The collector picks files to process by diffing the NEMweb CURRENT listing against
``last_files[key]``, which lives in memory. After a restart it is empty, so every daily file
still on CURRENT looks new: ~58 Bidmove_Complete and ~396 Next_Day_Dispatch files, re-processed
5 a cycle for about 7 hours (no new rows, but long cycles and long write-lock holds).

The collector saves the two bid keys to a small JSON file after each successful merge and loads
it at start. ``seed_from_db`` rebuilds the file from what bids.duckdb already holds, for the
first deploy or if the file is lost (``scripts/seed_bid_file_state.py``).

A file named ``..._YYYYMMDD_<id>.zip`` holds trading day YYYYMMDD: intervals from YYYYMMDD 04:05
to the next day 04:00.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Optional, Set

logger = logging.getLogger(__name__)

KEYS = ("bids", "bid_dispatch")
# last_files key -> table in bids.duckdb whose trading days show the file was merged
TABLES = {"bids": "bid_volume5", "bid_dispatch": "bid_dispatch"}
KEEP = 2000   # newest names kept per key; CURRENT holds about 400

_DAY = re.compile(r"_(\d{8})_\d+\.zip$")


def trading_day(filename: str) -> Optional[date]:
    m = _DAY.search(filename)
    if not m:
        return None
    s = m.group(1)
    return date(int(s[:4]), int(s[4:6]), int(s[6:]))


def load(path) -> Dict[str, Set[str]]:
    """Return {key: set(names)} from ``path``; empty if missing or unreadable (logged)."""
    path = Path(path)
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
        return {k: set(raw[k]) for k in KEYS if isinstance(raw.get(k), list)}
    except Exception as e:  # corrupt file: fall back to re-processing, which is safe
        logger.warning(f"bid file state {path} unreadable, ignoring: {e}")
        return {}


def save(path, last_files: Dict[str, Iterable[str]], keep: int = KEEP) -> None:
    """Write the bid keys of ``last_files`` to ``path`` atomically (tmp, then rename)."""
    path = Path(path)
    data = {k: sorted(last_files.get(k, ()))[-keep:] for k in KEYS}
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=0))
    os.replace(tmp, path)


def seed_from_db(bids_db, listings: Dict[str, Iterable[str]]) -> Dict[str, Set[str]]:
    """Names in ``listings`` whose trading day already has rows in bids.duckdb."""
    import duckdb

    con = duckdb.connect(str(bids_db), read_only=True)
    try:
        out = {}
        for key, names in listings.items():
            days = {r[0] for r in con.execute(
                f"SELECT DISTINCT CAST(settlementdate - INTERVAL 245 MINUTE AS DATE) "
                f"FROM {TABLES[key]}").fetchall()}
            out[key] = {n for n in names if trading_day(n) in days}
        return out
    finally:
        con.close()
