#!/usr/bin/env python3
"""Parser for AEMO MMSDM Data_Archive historical bid tables (5-year backfill).

The historical archive schema differs from the daily Bidmove_Complete:

  file PUBLIC_DVD_BIDDAYOFFER  -> table BIDDAYOFFER   (per trading-day price bands)
       columns: DUID, BIDTYPE, SETTLEMENTDATE, OFFERDATE, PRICEBAND1..10  (no _D)
  file PUBLIC_DVD_BIDPEROFFER  -> table BIDOFFERPERIOD (per 5-min volume bands)
       columns: DUID, BIDTYPE, TRADINGDATE, OFFERDATETIME, PERIODID,
                MAXAVAIL, ..., BANDAVAIL1..10, PASAAVAILABILITY  (NO INTERVAL_DATETIME)

Key differences handled here:
  * table + column names differ from BIDPEROFFER_D / BIDDAYOFFER_D
  * no INTERVAL_DATETIME -> reconstruct from TRADINGDATE + PERIODID (5-min:
    period p ends at TRADINGDATE 04:00 + p*5min; p=1 -> 04:05, p=288 -> next 04:00)
  * DIRECTION absent before ~2024-02 -> default GEN, map battery/pump load DUIDs
    (…L suffix) to LOAD; use DIRECTION when the column is present (2024+)

BIDDAYOFFER files are small (~200 MB CSV) and parsed in-memory (quote-aware, since
REBIDEXPLANATION contains commas). BIDOFFERPERIOD files are ~8 GB CSV and are
STREAMED line-by-line (no quoted commas in that table, so a plain split is safe).
"""
from __future__ import annotations

from datetime import timedelta
from typing import Iterable, Iterator

import pandas as pd

from .bids_parser import (
    PRICE_BANDS, PRICE_COLUMNS, VOL_BANDS, VOL_COLUMNS,
    _keep_latest_offer, _read_mms_table, _to_dt,
)

# AEMO trading day starts 04:00; BIDOFFERPERIOD is 5-minute (PERIODID 1..288).
_DAY_START_MIN = 4 * 60
_PERIOD_MIN = 5


def load_direction(duid: str) -> str:
    """Heuristic direction when the source has no DIRECTION column (pre-2024):
    battery/pump load DUIDs use the …L suffix; everything else is generation."""
    return "LOAD" if duid.endswith("L") else "GEN"


def _direction_col(raw: pd.DataFrame, duid: pd.Series) -> pd.Series:
    heuristic = duid.map(load_direction)
    if "DIRECTION" in raw.columns:
        d = raw["DIRECTION"].astype(str).str.strip().str.upper()
        return d.where(d.isin(["GEN", "LOAD", "BIDIRECTIONAL"]), heuristic)
    return heuristic


def parse_biddayoffer_mmsdm(content: bytes) -> pd.DataFrame:
    """MMSDM BIDDAYOFFER -> bid_price_bands schema (in-memory, quote-aware)."""
    raw = _read_mms_table(content, "BIDDAYOFFER")
    if raw.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)
    raw = raw[raw["BIDTYPE"].astype(str).str.strip() == "ENERGY"].copy()

    out = pd.DataFrame()
    out["settlementdate"] = _to_dt(raw["SETTLEMENTDATE"])
    out["duid"] = raw["DUID"].astype(str).str.strip()
    out["direction"] = _direction_col(raw, out["duid"]).values
    out["offerdate"] = _to_dt(raw["OFFERDATE"])
    for i, band in enumerate(PRICE_BANDS, 1):
        out[band] = pd.to_numeric(raw[f"PRICEBAND{i}"], errors="coerce").astype("float64")
    if "VERSIONNO" in raw.columns:
        out["_versionno"] = pd.to_numeric(raw["VERSIONNO"], errors="coerce")

    out = _keep_latest_offer(out, ["settlementdate", "duid", "direction"])
    return out[PRICE_COLUMNS].reset_index(drop=True)


def interval_from_period(trading_ts: pd.Timestamp, periodid: int) -> pd.Timestamp:
    """5-min interval-ending timestamp for a BIDOFFERPERIOD row."""
    return trading_ts + timedelta(minutes=_DAY_START_MIN + _PERIOD_MIN * periodid)


def iter_bidofferperiod_rows(lines: Iterable[str]) -> Iterator[dict]:
    """Stream MMSDM BIDOFFERPERIOD D-rows -> dicts in bid_volume5 schema.

    Reads the I header to locate columns by name, then processes D rows by index.
    Safe to split on ',' (this table has no quoted free-text fields). ENERGY only.
    Dedup (latest offer) is done downstream after staging.
    """
    idx: dict[str, int] | None = None
    has_direction = False
    for line in lines:
        if line.startswith("I,BIDS,BIDOFFERPERIOD,"):
            cols = line.rstrip("\r\n").split(",")[4:]
            idx = {name.strip(): i + 4 for i, name in enumerate(cols)}
            has_direction = "DIRECTION" in idx
            continue
        if idx is None or not line.startswith("D,BIDS,BIDOFFERPERIOD,"):
            continue
        if ",ENERGY," not in line:  # fast-skip FCAS rows before the expensive split
            continue
        parts = line.rstrip("\r\n").split(",")
        try:
            if parts[idx["BIDTYPE"]].strip() != "ENERGY":
                continue
            duid = parts[idx["DUID"]].strip()
            tdate = parts[idx["TRADINGDATE"]].strip().strip('"')
            periodid = int(parts[idx["PERIODID"]])
            trading_ts = pd.Timestamp(tdate.replace("/", "-"))
            settlementdate = interval_from_period(trading_ts, periodid)
            offerdate = parts[idx["OFFERDATETIME"]].strip().strip('"')
            if has_direction:
                d = parts[idx["DIRECTION"]].strip().upper()
                direction = d if d in ("GEN", "LOAD", "BIDIRECTIONAL") else load_direction(duid)
            else:
                direction = load_direction(duid)
            row = {
                "settlementdate": settlementdate,
                "duid": duid,
                "direction": direction,
                "offerdate": pd.Timestamp(offerdate.replace("/", "-")) if offerdate else pd.NaT,
                "maxavail": float(parts[idx["MAXAVAIL"]] or "nan"),
                "pasaavailability": float(parts[idx["PASAAVAILABILITY"]] or "nan"),
            }
            for i, band in enumerate(VOL_BANDS, 1):
                v = parts[idx[f"BANDAVAIL{i}"]]
                row[band] = float(v) if v not in ("", None) else 0.0
        except (KeyError, IndexError, ValueError):
            continue
        yield row


def bidofferperiod_frame(rows: Iterable[dict]) -> pd.DataFrame:
    """Materialise streamed rows into a VOL_COLUMNS DataFrame (for tests / batching)."""
    df = pd.DataFrame(list(rows))
    if df.empty:
        return pd.DataFrame(columns=VOL_COLUMNS)
    return df[VOL_COLUMNS]
