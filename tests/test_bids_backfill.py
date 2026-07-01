#!/usr/bin/env python3
"""Offline tests for the bids backfill machinery (scripts/backfill_bids.py).

Exercises the zip iteration (flat daily zip AND nested monthly archive zip) and the
ingest, using in-memory zips built from the committed CSV fixture. The real
multi-hundred-MB NEMWEB download is a manual/live step.
"""
import io
import sys
import zipfile
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import backfill_bids as bf  # noqa: E402

CSV = (Path(__file__).parent / "fixtures" / "bids_sample.csv").read_bytes()
DAILY_CSV_NAME = "PUBLIC_BIDMOVE_COMPLETE_20260629_0000000000000001.CSV"
DAILY_ZIP_NAME = "PUBLIC_BIDMOVE_COMPLETE_20260629_0000000000000001.zip"


def _flat_daily_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(DAILY_CSV_NAME, CSV)
    return buf.getvalue()


def _nested_monthly_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(DAILY_ZIP_NAME, _flat_daily_zip())
    return buf.getvalue()


def test_iter_flat_daily_zip():
    items = list(bf.iter_daily_csv_bytes(_flat_daily_zip()))
    assert len(items) == 1
    name, data = items[0]
    assert name == DAILY_CSV_NAME
    assert data == CSV


def test_iter_nested_monthly_zip():
    items = list(bf.iter_daily_csv_bytes(_nested_monthly_zip()))
    assert len(items) == 1
    name, data = items[0]
    assert name.endswith(".CSV")
    assert data == CSV


def test_date_from_name():
    assert bf._date_from_name(DAILY_ZIP_NAME) == "20260629"
    assert bf._date_from_name("PUBLIC_BIDMOVE_COMPLETE_20250502.zip") == "20250502"


def test_ingest_roundtrip():
    conn = duckdb.connect(":memory:")
    bf.ensure_bids_tables(conn)
    counts = bf.ingest_bids_csv(conn, CSV)
    assert counts == {"bid_volume5": 4, "bid_price_bands": 3}
    # existing-days key is derived from price bands (= file date)
    assert bf._existing_days(conn) == {"20260629"}
    conn.close()
