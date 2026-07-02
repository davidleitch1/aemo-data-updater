#!/usr/bin/env python3
"""Tests for the MMSDM historical bid parser (5-year backfill).

Covers both schema eras: pre-2024 (no DIRECTION, …L->LOAD heuristic) and 2024+
(DIRECTION present). Verifies table/column aliasing, PERIODID->interval
reconstruction, ENERGY filtering, latest-offer dedup, and quote-safe price parsing.
"""
import csv
import io
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.bids_mmsdm import (  # noqa: E402
    bidofferperiod_frame, interval_from_period, iter_bidofferperiod_rows,
    parse_biddayoffer_mmsdm,
)

# ---- BIDDAYOFFER fixture (pre-2024: no DIRECTION, comma in REBIDEXPLANATION) ----
DAY_COLS = ["DUID", "BIDTYPE", "SETTLEMENTDATE", "OFFERDATE", "VERSIONNO",
            "REBIDEXPLANATION"] + [f"PRICEBAND{i}" for i in range(1, 11)]


def _day_csv(rows, cols=DAY_COLS, direction=False) -> bytes:
    if direction:
        cols = cols + ["DIRECTION"]
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["C", "NEMP", "BIDDAYOFFER", "AEMO"])
    w.writerow(["I", "BIDS", "BIDDAYOFFER", "1"] + cols)
    for r in rows:
        w.writerow(["D", "BIDS", "BIDDAYOFFER", "1"] + [r.get(c, "") for c in cols])
    w.writerow(["C", "END", str(len(rows))])
    return buf.getvalue().encode()


def _pb(*v):
    return {f"PRICEBAND{i}": x for i, x in enumerate(v, 1)}


def test_biddayoffer_no_direction_heuristic_and_comma_safe():
    rows = [
        {"DUID": "G1", "BIDTYPE": "ENERGY", "SETTLEMENTDATE": "2021/06/02 00:00:00",
         "OFFERDATE": "2021/06/01 12:00:00", "VERSIONNO": "1",
         "REBIDEXPLANATION": "PLANT LIMIT, REVIEW", **_pb(-50, 60, 200)},
        {"DUID": "G1", "BIDTYPE": "ENERGY", "SETTLEMENTDATE": "2021/06/02 00:00:00",
         "OFFERDATE": "2021/06/02 09:00:00", "VERSIONNO": "2",
         "REBIDEXPLANATION": "RAMP, RECOVER", **_pb(-40, 62, 202)},
        {"DUID": "ADPBA1L", "BIDTYPE": "ENERGY", "SETTLEMENTDATE": "2021/06/02 00:00:00",
         "OFFERDATE": "2021/06/01 12:00:00", "VERSIONNO": "1", **_pb(0, 10, 20)},
        {"DUID": "F1", "BIDTYPE": "RAISE6SEC", "SETTLEMENTDATE": "2021/06/02 00:00:00",
         "OFFERDATE": "2021/06/01 12:00:00", "VERSIONNO": "1", **_pb(1, 2, 3)},
    ]
    df = parse_biddayoffer_mmsdm(_day_csv(rows))
    assert list(df.columns) == ["settlementdate", "duid", "direction", "offerdate"] + \
        [f"priceband{i}" for i in range(1, 11)]
    assert set(df["duid"]) == {"G1", "ADPBA1L"}  # RAISE6SEC dropped
    g1 = df[df["duid"] == "G1"].iloc[0]
    assert g1["direction"] == "GEN"
    assert g1["offerdate"] == pd.Timestamp("2021-06-02 09:00:00")  # latest wins
    assert g1["priceband1"] == -40.0  # comma-safe: not shifted
    assert df[df["duid"] == "ADPBA1L"].iloc[0]["direction"] == "LOAD"  # …L heuristic


def test_biddayoffer_uses_direction_when_present():
    rows = [
        {"DUID": "ADPBA1", "BIDTYPE": "ENERGY", "SETTLEMENTDATE": "2024/06/02 00:00:00",
         "OFFERDATE": "2024/06/01 12:00:00", "VERSIONNO": "1", "DIRECTION": "LOAD",
         **_pb(-100, 0, 50)},
    ]
    df = parse_biddayoffer_mmsdm(_day_csv(rows, direction=True))
    # merged DUID (no …L) but DIRECTION column says LOAD -> must use it
    assert df.iloc[0]["direction"] == "LOAD"


# ---- BIDOFFERPERIOD fixture (streaming) ----
PER_COLS = ["DUID", "BIDTYPE", "TRADINGDATE", "OFFERDATETIME", "PERIODID", "MAXAVAIL"] + \
    [f"BANDAVAIL{i}" for i in range(1, 11)] + ["PASAAVAILABILITY"]


def _per_lines(rows):
    yield "C,NEMP,BIDOFFERPERIOD,AEMO"
    yield "I,BIDS,BIDOFFERPERIOD,1," + ",".join(PER_COLS)
    for r in rows:
        yield "D,BIDS,BIDOFFERPERIOD,1," + ",".join(str(r.get(c, "")) for c in PER_COLS)
    yield "C,END,0"


def _ba(*v):
    return {f"BANDAVAIL{i}": x for i, x in enumerate(v, 1)}


def test_interval_from_period():
    t = pd.Timestamp("2021-06-02 00:00:00")
    assert interval_from_period(t, 1) == pd.Timestamp("2021-06-02 04:05:00")
    assert interval_from_period(t, 288) == pd.Timestamp("2021-06-03 04:00:00")


def test_bidofferperiod_stream():
    rows = [
        {"DUID": "G1", "BIDTYPE": "ENERGY", "TRADINGDATE": '"2021/06/02 00:00:00"',
         "OFFERDATETIME": '"2021/06/01 12:00:00"', "PERIODID": 1, "MAXAVAIL": 100,
         "PASAAVAILABILITY": 100, **_ba(10, 20, 30)},
        {"DUID": "G1", "BIDTYPE": "ENERGY", "TRADINGDATE": '"2021/06/02 00:00:00"',
         "OFFERDATETIME": '"2021/06/01 12:00:00"', "PERIODID": 288, "MAXAVAIL": 100,
         "PASAAVAILABILITY": 100, **_ba(5, 5)},
        {"DUID": "ADPBA1L", "BIDTYPE": "ENERGY", "TRADINGDATE": '"2021/06/02 00:00:00"',
         "OFFERDATETIME": '"2021/06/01 12:00:00"', "PERIODID": 1, "MAXAVAIL": 50,
         "PASAAVAILABILITY": 50, **_ba(50)},
        {"DUID": "F1", "BIDTYPE": "RAISE6SEC", "TRADINGDATE": '"2021/06/02 00:00:00"',
         "OFFERDATETIME": '"2021/06/01 12:00:00"', "PERIODID": 1, "MAXAVAIL": 9,
         "PASAAVAILABILITY": 9, **_ba(9)},
    ]
    df = bidofferperiod_frame(iter_bidofferperiod_rows(_per_lines(rows)))
    assert len(df) == 3  # RAISE6SEC dropped
    g1 = df[(df["duid"] == "G1")].sort_values("settlementdate")
    assert list(g1["settlementdate"]) == [
        pd.Timestamp("2021-06-02 04:05:00"), pd.Timestamp("2021-06-03 04:00:00")]
    assert g1.iloc[0]["bandavail1"] == 10.0 and g1.iloc[0]["direction"] == "GEN"
    assert df[df["duid"] == "ADPBA1L"].iloc[0]["direction"] == "LOAD"
    assert list(df.columns) == ["settlementdate", "duid", "direction", "offerdate",
                                "maxavail", "pasaavailability"] + \
        [f"bandavail{i}" for i in range(1, 11)]
