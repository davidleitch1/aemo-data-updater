#!/usr/bin/env python3
"""Integration tests for bids DuckDB storage (ensure_bids_tables + merge_bids).

Offline: parses the local fixture and writes to an in-memory DuckDB. Verifies
schema creation, row counts, value round-trip, and idempotent DELETE+INSERT
(re-ingest of the same day must not duplicate; changed bands must update in place).
"""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.bids_parser import parse_bids  # noqa: E402
from aemo_updater.collectors.bids_store import ensure_bids_tables, merge_bids  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "bids_sample.csv"


@pytest.fixture
def frames():
    return parse_bids(FIXTURE.read_bytes())  # (volume_df, price_df)


@pytest.fixture
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def test_tables_created(conn):
    ensure_bids_tables(conn)
    tables = {t[0] for t in conn.execute("SHOW TABLES").fetchall()}
    assert {"bid_volume5", "bid_price_bands"} <= tables


def test_merge_row_counts(conn, frames):
    vol, price = frames
    counts = merge_bids(conn, vol, price)
    assert counts == {"bid_volume5": 4, "bid_price_bands": 3}
    assert conn.execute("SELECT COUNT(*) FROM bid_volume5").fetchone()[0] == 4
    assert conn.execute("SELECT COUNT(*) FROM bid_price_bands").fetchone()[0] == 3


def test_value_roundtrip(conn, frames):
    vol, price = frames
    merge_bids(conn, vol, price)
    ccc = conn.execute(
        "SELECT direction, maxavail, bandavail10 FROM bid_volume5 WHERE duid='CCC1'"
    ).fetchone()
    assert ccc == ("BIDIRECTIONAL", 200.0, 200.0)
    aaa_price = conn.execute(
        "SELECT priceband1 FROM bid_price_bands WHERE duid='AAA1' AND direction='GEN'"
    ).fetchone()
    assert aaa_price[0] == -900.0


def test_merge_is_idempotent(conn, frames):
    vol, price = frames
    merge_bids(conn, vol, price)
    merge_bids(conn, vol, price)  # re-ingest same day
    assert conn.execute("SELECT COUNT(*) FROM bid_volume5").fetchone()[0] == 4
    assert conn.execute("SELECT COUNT(*) FROM bid_price_bands").fetchone()[0] == 3


def test_merge_updates_in_place(conn, frames):
    vol, price = frames
    merge_bids(conn, vol, price)
    vol2 = vol.copy()
    vol2.loc[vol2["duid"] == "CCC1", "bandavail10"] = 111.0
    merge_bids(conn, vol2, None)
    assert conn.execute("SELECT COUNT(*) FROM bid_volume5").fetchone()[0] == 4  # no dupe
    updated = conn.execute(
        "SELECT bandavail10 FROM bid_volume5 WHERE duid='CCC1'"
    ).fetchone()[0]
    assert updated == 111.0
