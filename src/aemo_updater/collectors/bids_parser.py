#!/usr/bin/env python3
"""Parser for AEMO NEMWEB Bidmove_Complete files.

A daily PUBLIC_BIDMOVE_COMPLETE_<date>.CSV holds two MMS tables:

  * BIDDAYOFFER_D  - per trading-day, per-DUID PRICEBAND1..10 ($/MWh)
  * BIDPEROFFER_D  - per 5-min interval, per-DUID BANDAVAIL1..10 (MW),
                     keyed on INTERVAL_DATETIME, with MAXAVAIL / PASAAVAILABILITY

We keep only BIDTYPE='ENERGY' and, per key, only the row with the latest OFFERDATE
(the most recent rebid). DIRECTION is one of GEN / LOAD / BIDIRECTIONAL (the last
introduced 2024-07-01 when AEMO merged battery L/G DUIDs).

Unlike the collector's generic ``parse_mms_csv`` (a naive ``line.split(',')``),
BIDDAYOFFER_D carries a free-text REBIDEXPLANATION that can contain commas, so we
parse with the quote-aware :mod:`csv` reader.
"""
from __future__ import annotations

import csv
import io

import pandas as pd

PRICE_BANDS = [f"priceband{i}" for i in range(1, 11)]
VOL_BANDS = [f"bandavail{i}" for i in range(1, 11)]

# Output column order (also the DuckDB table column order).
PRICE_COLUMNS = ["settlementdate", "duid", "direction", "offerdate"] + PRICE_BANDS
VOL_COLUMNS = (
    ["settlementdate", "duid", "direction", "offerdate", "maxavail", "pasaavailability"]
    + VOL_BANDS
)

_DT_FMT = "%Y/%m/%d %H:%M:%S"


def _read_mms_table(content: bytes, table_name: str) -> pd.DataFrame:
    """Extract one MMS table (I header + D rows) into a raw string DataFrame."""
    text = content.decode("utf-8", errors="ignore")
    reader = csv.reader(io.StringIO(text))

    header: list[str] | None = None
    rows: list[list[str]] = []
    for parts in reader:
        if len(parts) < 4:
            continue
        row_type = parts[0]
        if row_type == "I" and parts[2] == table_name:
            header = [c.strip().replace("\r", "") for c in parts[4:]]
        elif row_type == "D" and parts[2] == table_name:
            rows.append(parts[4:])

    if not header or not rows:
        return pd.DataFrame()

    ncol = len(header)
    fixed = [r[:ncol] + [""] * (ncol - len(r)) if len(r) != ncol else r for r in rows]
    return pd.DataFrame(fixed, columns=header)


def _to_dt(series: pd.Series) -> pd.Series:
    return pd.to_datetime(
        series.astype(str).str.strip().str.strip('"'), format=_DT_FMT, errors="coerce"
    )


def _direction(raw: pd.DataFrame) -> pd.Series:
    if "DIRECTION" in raw.columns:
        return raw["DIRECTION"].astype(str).str.strip()
    return pd.Series([""] * len(raw), index=raw.index)


def _keep_latest_offer(df: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Keep the row with the latest OFFERDATE (then VERSIONNO) per key."""
    order = ["offerdate"] + (["_versionno"] if "_versionno" in df.columns else [])
    df = df.sort_values(order, na_position="first")
    df = df.drop_duplicates(subset=keys, keep="last")
    return df.sort_values(keys).reset_index(drop=True)


def parse_biddayoffer(content: bytes) -> pd.DataFrame:
    """Parse BIDDAYOFFER_D -> price bands (bid_price_bands schema)."""
    raw = _read_mms_table(content, "BIDDAYOFFER_D")
    if raw.empty:
        return pd.DataFrame(columns=PRICE_COLUMNS)

    raw = raw[raw["BIDTYPE"].astype(str).str.strip() == "ENERGY"].copy()

    out = pd.DataFrame()
    out["settlementdate"] = _to_dt(raw["SETTLEMENTDATE"])
    out["duid"] = raw["DUID"].astype(str).str.strip()
    out["direction"] = _direction(raw)
    out["offerdate"] = _to_dt(raw["OFFERDATE"])
    for i, band in enumerate(PRICE_BANDS, 1):
        out[band] = pd.to_numeric(raw[f"PRICEBAND{i}"], errors="coerce").astype("float64")
    if "VERSIONNO" in raw.columns:
        out["_versionno"] = pd.to_numeric(raw["VERSIONNO"], errors="coerce")

    out = _keep_latest_offer(out, ["settlementdate", "duid", "direction"])
    return out[PRICE_COLUMNS].reset_index(drop=True)


def parse_bidperoffer(content: bytes) -> pd.DataFrame:
    """Parse BIDPEROFFER_D -> per-interval volume bands (bid_volume5 schema)."""
    raw = _read_mms_table(content, "BIDPEROFFER_D")
    if raw.empty:
        return pd.DataFrame(columns=VOL_COLUMNS)

    raw = raw[raw["BIDTYPE"].astype(str).str.strip() == "ENERGY"].copy()

    out = pd.DataFrame()
    out["settlementdate"] = _to_dt(raw["INTERVAL_DATETIME"])
    out["duid"] = raw["DUID"].astype(str).str.strip()
    out["direction"] = _direction(raw)
    out["offerdate"] = _to_dt(raw["OFFERDATE"])
    out["maxavail"] = pd.to_numeric(raw["MAXAVAIL"], errors="coerce").astype("float64")
    out["pasaavailability"] = pd.to_numeric(
        raw["PASAAVAILABILITY"], errors="coerce"
    ).astype("float64")
    for i, band in enumerate(VOL_BANDS, 1):
        out[band] = pd.to_numeric(raw[f"BANDAVAIL{i}"], errors="coerce").astype("float64")
    if "VERSIONNO" in raw.columns:
        out["_versionno"] = pd.to_numeric(raw["VERSIONNO"], errors="coerce")

    out = _keep_latest_offer(out, ["settlementdate", "duid", "direction"])
    return out[VOL_COLUMNS].reset_index(drop=True)


def parse_bids(content: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Convenience: return (volume_df, price_df) from one CSV's bytes."""
    return parse_bidperoffer(content), parse_biddayoffer(content)
