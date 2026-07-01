#!/usr/bin/env python3
"""Unit tests for the Bidmove_Complete parser (bid_volume5 / bid_price_bands).

Fixture: tests/fixtures/bids_sample.csv (built by make_bids_fixture.py). It exercises
ENERGY-only filtering, latest-OFFERDATE dedup, GEN/LOAD/BIDIRECTIONAL directions, and
a REBIDEXPLANATION containing a comma (which would corrupt a naive str.split(',')).

Red before Phase 1 (module aemo_updater.collectors.bids_parser does not exist yet);
green once the parser is implemented.
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.bids_parser import (  # noqa: E402
    parse_biddayoffer,
    parse_bidperoffer,
)

FIXTURE = Path(__file__).parent / "fixtures" / "bids_sample.csv"


@pytest.fixture
def content() -> bytes:
    return FIXTURE.read_bytes()


# ----------------------------- price bands -----------------------------------

def test_biddayoffer_schema(content):
    df = parse_biddayoffer(content)
    expected_cols = ["settlementdate", "duid", "direction", "offerdate"] + [
        f"priceband{i}" for i in range(1, 11)
    ]
    assert list(df.columns) == expected_cols


def test_biddayoffer_filters_non_energy(content):
    df = parse_biddayoffer(content)
    # DDD1 is RAISE6SEC -> must be dropped; only ENERGY DUIDs remain
    assert set(df["duid"]) == {"AAA1", "BBB1", "CCC1"}


def test_biddayoffer_latest_offer_wins_and_comma_safe(content):
    df = parse_biddayoffer(content)
    aaa = df[(df["duid"] == "AAA1") & (df["direction"] == "GEN")]
    assert len(aaa) == 1
    row = aaa.iloc[0]
    # The later offer (2026/06/29 09:00) must win over the 06/28 12:00 one.
    assert row["offerdate"] == pd.Timestamp("2026-06-29 09:00:00")
    # Both AAA1 rows carry a comma in REBIDEXPLANATION; a naive split would
    # shift the price bands. These exact values prove quote-aware parsing.
    assert row["priceband1"] == -900.0
    assert row["priceband3"] == 12.0
    assert row["priceband10"] == 16600.0


def test_biddayoffer_directions_and_types(content):
    df = parse_biddayoffer(content)
    assert set(df["direction"]) == {"GEN", "LOAD", "BIDIRECTIONAL"}
    assert df["settlementdate"].iloc[0] == pd.Timestamp("2026-06-29 00:00:00")
    for i in range(1, 11):
        assert pd.api.types.is_float_dtype(df[f"priceband{i}"])


# ----------------------------- volume bands ----------------------------------

def test_bidperoffer_schema(content):
    df = parse_bidperoffer(content)
    expected_cols = (
        ["settlementdate", "duid", "direction", "offerdate", "maxavail",
         "pasaavailability"]
        + [f"bandavail{i}" for i in range(1, 11)]
    )
    assert list(df.columns) == expected_cols


def test_bidperoffer_filters_non_energy(content):
    df = parse_bidperoffer(content)
    assert "DDD1" not in set(df["duid"])


def test_bidperoffer_interval_and_dedup(content):
    df = parse_bidperoffer(content)
    # AAA1 GEN has two intervals (04:05 twice via rebid -> deduped, and 04:10)
    aaa = df[(df["duid"] == "AAA1") & (df["direction"] == "GEN")].sort_values(
        "settlementdate"
    )
    assert list(aaa["settlementdate"]) == [
        pd.Timestamp("2026-06-29 04:05:00"),
        pd.Timestamp("2026-06-29 04:10:00"),
    ]
    row0405 = aaa.iloc[0]
    # later offer (09:00) wins for the 04:05 interval
    assert row0405["offerdate"] == pd.Timestamp("2026-06-29 09:00:00")
    assert row0405["bandavail1"] == 5.0
    assert row0405["bandavail10"] == 90.0


def test_bidperoffer_rowcount_and_values(content):
    df = parse_bidperoffer(content)
    # 4 surviving rows: AAA1/04:05, AAA1/04:10, BBB1/04:05, CCC1/04:05
    assert len(df) == 4
    ccc = df[df["duid"] == "CCC1"].iloc[0]
    assert ccc["direction"] == "BIDIRECTIONAL"
    assert ccc["maxavail"] == 200.0
    assert ccc["bandavail10"] == 200.0
