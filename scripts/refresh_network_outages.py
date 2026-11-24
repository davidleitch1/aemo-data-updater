#!/usr/bin/env python3
"""Refresh network_outagedetail / network_outageconstraintset from the newest PUBLIC_NETWORK file.

Reads https://nemweb.com.au/Reports/Current/Network/, takes the newest PUBLIC_NETWORK_*.zip, and
applies its D,NETWORK,OUTAGEDETAIL and D,NETWORK,OUTAGECONSTRAINTSET rows with
constraints_store.upsert_outages (revised end times and statuses replace the stored rows of that
outage; outages absent from the file are kept). The last file name processed is kept in
network_outages_last.txt beside the database; an unchanged name exits quietly without touching
the database.

    .venv/bin/python scripts/refresh_network_outages.py [--db PATH]
"""
from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from aemo_updater import constraints_store as store  # noqa: E402

DIR_URL = "https://nemweb.com.au/Reports/Current/Network/"
UA = "Mozilla/5.0"
STATE_NAME = "network_outages_last.txt"


def http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def newest_name(listing_html: str) -> str | None:
    names = sorted(set(re.findall(r"(PUBLIC_NETWORK_[A-Za-z0-9_]+\.zip)", listing_html, flags=re.I)))
    return names[-1] if names else None


def parse_network_csv(content: bytes) -> tuple[list[dict], list[dict]]:
    """(detail_rows, constraintset_rows) as dicts with lowercase keys; empty strings become None."""
    cols: dict[str, list[str]] = {}
    out = {"OUTAGEDETAIL": [], "OUTAGECONSTRAINTSET": []}
    for rec in csv.reader(io.StringIO(content.decode("utf-8", errors="replace"))):
        if len(rec) < 4 or rec[1] != "NETWORK" or rec[2] not in out:
            continue
        if rec[0] == "I":
            cols[rec[2]] = [c.lower() for c in rec[4:]]
        elif rec[0] == "D" and rec[2] in cols:
            out[rec[2]].append({c: (v if v != "" else None) for c, v in zip(cols[rec[2]], rec[4:])})
    return out["OUTAGEDETAIL"], out["OUTAGECONSTRAINTSET"]


def main(argv=None, *, get=http_get, log=None) -> int:
    log = log or (lambda s: print(s, flush=True))
    ap = argparse.ArgumentParser(description="Refresh network outage tables from nemweb")
    ap.add_argument("--db", type=Path, default=None)
    args = ap.parse_args(argv)
    db = args.db or store.default_db_path()
    state = db.parent / STATE_NAME

    name = newest_name(get(DIR_URL).decode("latin1"))
    if name is None:
        log("no PUBLIC_NETWORK file listed")
        return 1
    if state.exists() and state.read_text().strip() == name:
        return 0                                           # unchanged: quiet no-op
    blob = get(DIR_URL + name)
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        csv_name = next(n for n in z.namelist() if n.lower().endswith(".csv"))
        detail, sets = parse_network_csv(z.read(csv_name))
    if not detail and not sets:
        log(f"{name}: no OUTAGEDETAIL or OUTAGECONSTRAINTSET rows, state not updated")
        return 1
    con = store.connect_with_retry(db, log=log)
    try:
        store.ensure_tables(con)
        res = store.upsert_outages(con, detail, sets)
    finally:
        con.close()
    store.refresh_readonly(db)
    state.write_text(name + "\n")
    log(f"{name}: " + "; ".join(f"{t} new {n}, already present {p}" for t, (n, p) in res.items())
        + f" (file held {len(detail)} detail, {len(sets)} constraint-set rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
