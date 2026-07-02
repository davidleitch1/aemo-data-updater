#!/usr/bin/env python3
"""Offline tests for the MMSDM backfill machinery (scripts/backfill_bids_mmsdm.py):
month enumeration, coverage stop, and stream->stage->dedup->merge into DuckDB."""
import io
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import backfill_bids_mmsdm as bf  # noqa: E402
from aemo_updater.collectors.bids_store import ensure_bids_tables  # noqa: E402

PER_COLS = ["DUID", "BIDTYPE", "TRADINGDATE", "OFFERDATETIME", "PERIODID", "MAXAVAIL"] + \
    [f"BANDAVAIL{i}" for i in range(1, 11)] + ["PASAAVAILABILITY"]
DAY_COLS = ["DUID", "BIDTYPE", "SETTLEMENTDATE", "OFFERDATE", "VERSIONNO"] + \
    [f"PRICEBAND{i}" for i in range(1, 11)]


def _zip(inner_name, text):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(inner_name, text)
    p = io.BytesIO(buf.getvalue())
    return p


def _write_zip(tmp_path, inner_name, text):
    p = tmp_path / (inner_name.replace(".CSV", ".zip"))
    with zipfile.ZipFile(p, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr(inner_name, text)
    return str(p)


def test_months():
    assert list(bf.months((2021, 7), (2021, 10))) == [
        (2021, 7), (2021, 8), (2021, 9), (2021, 10)]
    assert list(bf.months((2021, 12), (2022, 2))) == [(2021, 12), (2022, 1), (2022, 2)]


def test_coverage_end():
    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    assert bf._coverage_end(c)[:2]  # empty -> today (no crash)
    c.execute("INSERT INTO bid_price_bands (settlementdate,duid,direction,offerdate,"
              + ",".join(f"priceband{i}" for i in range(1, 11)) + ") VALUES "
              "('2025-05-02','X','GEN','2025-05-01'," + ",".join(["0"] * 10) + ")")
    assert bf._coverage_end(c) == (2025, 4)
    c.close()


def test_ingest_volumes_dedup(tmp_path):
    rows = [
        # two offers same interval/duid -> latest offer wins
        "D,BIDS,BIDOFFERPERIOD,1,G1,ENERGY,\"2021/06/02 00:00:00\",\"2021/06/01 12:00:00\",1,100,"
        + ",".join(["10"] + ["0"] * 9) + ",100",
        "D,BIDS,BIDOFFERPERIOD,1,G1,ENERGY,\"2021/06/02 00:00:00\",\"2021/06/02 09:00:00\",1,100,"
        + ",".join(["5"] + ["0"] * 9) + ",100",
        "D,BIDS,BIDOFFERPERIOD,1,F1,RAISE6SEC,\"2021/06/02 00:00:00\",\"2021/06/01 12:00:00\",1,9,"
        + ",".join(["9"] + ["0"] * 9) + ",9",
    ]
    text = ("C,x\n" + "I,BIDS,BIDOFFERPERIOD,1," + ",".join(PER_COLS) + "\n"
            + "\n".join(rows) + "\nC,end,0\n")
    zp = _write_zip(tmp_path, "PUBLIC_DVD_BIDPEROFFER_202106010000.CSV", text)

    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    n = bf.ingest_volumes(c, zp)
    assert n == 1  # dedup to a single row; RAISE6SEC excluded
    row = c.execute("SELECT bandavail1, direction, settlementdate FROM bid_volume5").fetchone()
    assert row[0] == 5.0 and row[1] == "GEN"
    assert row[2] == datetime(2021, 6, 2, 4, 5)
    c.close()


def test_ingest_prices(tmp_path):
    text = ("C,x\n" + "I,BIDS,BIDDAYOFFER,1," + ",".join(DAY_COLS) + "\n"
            + "D,BIDS,BIDDAYOFFER,1,G1,ENERGY,\"2021/06/02 00:00:00\",\"2021/06/01 12:00:00\",1,"
            + ",".join(["-50"] + ["0"] * 9) + "\nC,end,0\n")
    zp = _write_zip(tmp_path, "PUBLIC_DVD_BIDDAYOFFER_202106010000.CSV", text)
    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    n = bf.ingest_prices(c, zp)
    assert n == 1
    assert c.execute("SELECT priceband1 FROM bid_price_bands").fetchone()[0] == -50.0
    c.close()
