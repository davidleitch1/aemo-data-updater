#!/usr/bin/env python3
"""Offline tests for the DuckDB-native MMSDM backfill ingest (scripts/backfill_bids_mmsdm.py):
month enumeration, coverage stop, and read_csv -> transform -> dedup -> merge, including
the split-file (BIDPEROFFER1/2) union and the no-DIRECTION …L->LOAD heuristic."""
import sys
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import backfill_bids_mmsdm as bf  # noqa: E402
from aemo_updater.collectors.bids_store import ensure_bids_tables  # noqa: E402

VOL_HDR = (["DUID", "BIDTYPE", "TRADINGDATE", "OFFERDATETIME", "PERIODID", "MAXAVAIL",
            "FIXEDLOAD", "RAMPUPRATE", "RAMPDOWNRATE", "ENABLEMENTMIN", "ENABLEMENTMAX",
            "LOWBREAKPOINT", "HIGHBREAKPOINT"] + [f"BANDAVAIL{i}" for i in range(1, 11)]
           + ["LASTCHANGED", "PASAAVAILABILITY"])
DAY_HDR = (["DUID", "BIDTYPE", "SETTLEMENTDATE", "OFFERDATE", "VERSIONNO", "REBIDEXPLANATION"]
           + [f"PRICEBAND{i}" for i in range(1, 11)])


def _row(hdr, table, **vals):
    return "D,BIDS," + table + ",1," + ",".join(str(vals.get(c, "")) for c in hdr)


def _write(tmp_path, name, table, hdr, rows):
    p = tmp_path / name
    lines = ["C,x", "I,BIDS," + table + ",1," + ",".join(hdr)] + rows + ["C,end,0"]
    p.write_text("\n".join(lines) + "\n")
    return str(p)


def _vol(**kw):
    kw.setdefault("BIDTYPE", "ENERGY")
    kw.setdefault("TRADINGDATE", '"2021/06/02 00:00:00"')
    kw.setdefault("PASAAVAILABILITY", "100")
    return _row(VOL_HDR, "BIDOFFERPERIOD", **kw)


def test_months():
    assert list(bf.months((2021, 7), (2021, 10))) == [(2021, 7), (2021, 8), (2021, 9), (2021, 10)]
    assert list(bf.months((2021, 12), (2022, 2))) == [(2021, 12), (2022, 1), (2022, 2)]


def test_coverage_end():
    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    c.execute("INSERT INTO bid_price_bands (settlementdate,duid,direction,offerdate,"
              + ",".join(f"priceband{i}" for i in range(1, 11)) + ") VALUES "
              "('2025-05-02','X','GEN','2025-05-01'," + ",".join(["0"] * 10) + ")")
    assert bf._coverage_end(c) == (2025, 4)
    c.close()


def test_ingest_volumes_interval_dedup_direction(tmp_path):
    rows = [
        _vol(DUID="G1", OFFERDATETIME='"2021/06/01 12:00:00"', PERIODID=1, MAXAVAIL=100,
             BANDAVAIL1=10, BANDAVAIL2=20, BANDAVAIL3=30),
        _vol(DUID="G1", OFFERDATETIME='"2021/06/02 09:00:00"', PERIODID=1, MAXAVAIL=100,
             BANDAVAIL1=5, BANDAVAIL2=5),                       # later offer wins
        _vol(DUID="G1", OFFERDATETIME='"2021/06/02 09:00:00"', PERIODID=288, MAXAVAIL=100,
             BANDAVAIL1=7),                                     # -> next-day 04:00
        _vol(DUID="ADPBA1L", OFFERDATETIME='"2021/06/01 12:00:00"', PERIODID=1, BANDAVAIL1=50),
        _vol(DUID="F1", BIDTYPE="RAISE6SEC", OFFERDATETIME='"2021/06/01 12:00:00"',
             PERIODID=1, BANDAVAIL1=9),                         # non-ENERGY dropped
    ]
    csv = _write(tmp_path, "vol.csv", "BIDOFFERPERIOD", VOL_HDR, rows)
    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    n = bf.ingest_volumes(c, [csv])
    assert n == 3  # G1@04:05, G1@04:00(next), ADPBA1L@04:05 ; F1 dropped
    g1 = c.execute("SELECT settlementdate, bandavail1, direction FROM bid_volume5 "
                   "WHERE duid='G1' ORDER BY settlementdate").fetchall()
    assert g1[0] == (datetime(2021, 6, 2, 4, 5), 5.0, "GEN")   # dedup: latest offer
    assert g1[1][0] == datetime(2021, 6, 3, 4, 0)              # period 288 boundary
    assert c.execute("SELECT direction FROM bid_volume5 WHERE duid='ADPBA1L'").fetchone()[0] == "LOAD"
    c.close()


def test_ingest_volumes_split_union(tmp_path):
    a = _write(tmp_path, "v1.csv", "BIDOFFERPERIOD", VOL_HDR,
               [_vol(DUID="G1", OFFERDATETIME='"2021/06/01 12:00:00"', PERIODID=1, BANDAVAIL1=10)])
    b = _write(tmp_path, "v2.csv", "BIDOFFERPERIOD", VOL_HDR,
               [_vol(DUID="G2", OFFERDATETIME='"2021/06/01 12:00:00"', PERIODID=1, BANDAVAIL1=20)])
    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    assert bf.ingest_volumes(c, [a, b]) == 2  # both halves merged
    assert {r[0] for r in c.execute("SELECT DISTINCT duid FROM bid_volume5").fetchall()} == {"G1", "G2"}
    c.close()


def test_ingest_prices(tmp_path):
    rows = [_row(DAY_HDR, "BIDDAYOFFER", DUID="G1", BIDTYPE="ENERGY",
                 SETTLEMENTDATE='"2021/06/02 00:00:00"', OFFERDATE='"2021/06/01 12:00:00"',
                 VERSIONNO=1, REBIDEXPLANATION='"PLANT, REVIEW"', PRICEBAND1=-50, PRICEBAND2=60)]
    csv = _write(tmp_path, "day.csv", "BIDDAYOFFER", DAY_HDR, rows)
    c = duckdb.connect(":memory:")
    ensure_bids_tables(c)
    assert bf.ingest_prices(c, csv) == 1
    r = c.execute("SELECT priceband1, priceband2, direction FROM bid_price_bands").fetchone()
    assert r == (-50.0, 60.0, "GEN")   # comma in REBIDEXPLANATION handled by quote-aware read
    c.close()
