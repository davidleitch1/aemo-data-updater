#!/usr/bin/env python3
"""Tests for TOTALCLEARED parsing (DISPATCH.UNIT_SOLUTION) and bid_dispatch storage.

The parser reads MMS columns by name, so this compact fixture carries only the
columns used. Verifies INTERVENTION=0 filtering, negative cleared (battery load),
and idempotent merge.
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.bids_parser import parse_dispatch_totalcleared  # noqa: E402
from aemo_updater.collectors.bids_store import ensure_bids_tables, merge_dispatch  # noqa: E402

UNIT_SOLUTION_CSV = (
    "C,NEMP.WORLD,NEXT_DAY_DISPATCH,AEMO,PUBLIC,2026/06/29 00:00:00,1,NEXT_DAY_DISPATCH,1\n"
    "I,DISPATCH,UNIT_SOLUTION,3,SETTLEMENTDATE,DUID,INTERVENTION,DISPATCHMODE,TOTALCLEARED\n"
    'D,DISPATCH,UNIT_SOLUTION,3,"2026/06/29 04:05:00",AAA1,0,0,50\n'
    'D,DISPATCH,UNIT_SOLUTION,3,"2026/06/29 04:05:00",AAA1,1,0,60\n'  # intervention -> drop
    'D,DISPATCH,UNIT_SOLUTION,3,"2026/06/29 04:05:00",BBB1,0,0,-20\n'  # battery load
    'D,DISPATCH,UNIT_SOLUTION,3,"2026/06/29 04:05:00",CCC1,0,0,0\n'
    "C,END OF REPORT,6\n"
).encode()


def test_parse_filters_intervention_and_values():
    df = parse_dispatch_totalcleared(UNIT_SOLUTION_CSV)
    assert list(df.columns) == ["settlementdate", "duid", "totalcleared"]
    assert len(df) == 3  # the INTERVENTION=1 duplicate for AAA1 is dropped
    vals = dict(zip(df["duid"], df["totalcleared"]))
    assert vals == {"AAA1": 50.0, "BBB1": -20.0, "CCC1": 0.0}


def test_merge_dispatch_roundtrip_and_idempotent():
    conn = duckdb.connect(":memory:")
    ensure_bids_tables(conn)
    df = parse_dispatch_totalcleared(UNIT_SOLUTION_CSV)
    assert merge_dispatch(conn, df) == 3
    merge_dispatch(conn, df)  # re-ingest
    assert conn.execute("SELECT COUNT(*) FROM bid_dispatch").fetchone()[0] == 3
    aaa = conn.execute(
        "SELECT totalcleared FROM bid_dispatch WHERE duid='AAA1'"
    ).fetchone()[0]
    assert aaa == 50.0
    conn.close()
