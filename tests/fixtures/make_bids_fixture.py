#!/usr/bin/env python3
"""Generate a small, deterministic Bidmove_Complete fixture for bid-parser tests.

Real Bidmove_Complete daily files are ~132 MB CSVs; this builds a tiny MMS-format
CSV (and zip) with hand-chosen rows that exercise every case the parser must handle:

  * BIDDAYOFFER_D (trading-day price bands, PRICEBAND1..10)
  * BIDPEROFFER_D (5-min volume bands, BANDAVAIL1..10, keyed on INTERVAL_DATETIME)
  * BIDTYPE filtering  -> only ENERGY kept (a RAISE6SEC row must be dropped)
  * latest-OFFERDATE dedup per (interval/day, duid, direction)
  * DIRECTION values GEN / LOAD / BIDIRECTIONAL (post-2024-07 bidirectional units)
  * a REBIDEXPLANATION containing a comma -> naive str.split(',') would corrupt the
    price bands, so the parser must be quote-aware.

Column orders below are copied verbatim from the real
PUBLIC_BIDMOVE_COMPLETE_YYYYMMDD CSV I-rows (BIDDAYOFFER_D v3, BIDPEROFFER_D v4).

Run:  python tests/fixtures/make_bids_fixture.py
Produces: tests/fixtures/bids_sample.csv  and  tests/fixtures/bids_sample.zip
"""
import csv
import io
import zipfile
from pathlib import Path

DAYOFFER_COLS = [
    "SETTLEMENTDATE", "DUID", "BIDTYPE", "BIDSETTLEMENTDATE", "OFFERDATE",
    "VERSIONNO", "PARTICIPANTID", "DAILYENERGYCONSTRAINT", "REBIDEXPLANATION",
    "PRICEBAND1", "PRICEBAND2", "PRICEBAND3", "PRICEBAND4", "PRICEBAND5",
    "PRICEBAND6", "PRICEBAND7", "PRICEBAND8", "PRICEBAND9", "PRICEBAND10",
    "MINIMUMLOAD", "T1", "T2", "T3", "T4", "NORMALSTATUS", "LASTCHANGED",
    "ENTRYTYPE", "DIRECTION",
]

PEROFFER_COLS = [
    "SETTLEMENTDATE", "DUID", "BIDTYPE", "BIDSETTLEMENTDATE", "OFFERDATE",
    "PERIODID", "VERSIONNO", "MAXAVAIL", "FIXEDLOAD", "ROCUP", "ROCDOWN",
    "ENABLEMENTMIN", "ENABLEMENTMAX", "LOWBREAKPOINT", "HIGHBREAKPOINT",
    "BANDAVAIL1", "BANDAVAIL2", "BANDAVAIL3", "BANDAVAIL4", "BANDAVAIL5",
    "BANDAVAIL6", "BANDAVAIL7", "BANDAVAIL8", "BANDAVAIL9", "BANDAVAIL10",
    "LASTCHANGED", "PASAAVAILABILITY", "INTERVAL_DATETIME", "DIRECTION",
    "ENERGYLIMIT", "RECALL_PERIOD",
]

DAY = "2026/06/29 00:00:00"


def _day_row(**kw):
    row = {c: "" for c in DAYOFFER_COLS}
    row.update(SETTLEMENTDATE=DAY, BIDSETTLEMENTDATE=DAY, ENTRYTYPE="DAILY")
    row.update(kw)
    return row


def _per_row(**kw):
    row = {c: "" for c in PEROFFER_COLS}
    row.update(SETTLEMENTDATE=DAY, BIDSETTLEMENTDATE=DAY, PERIODID="1")
    row.update(kw)
    return row


def _pb(*vals):
    return {f"PRICEBAND{i}": str(v) for i, v in enumerate(vals, 1)}


def _ba(*vals):
    return {f"BANDAVAIL{i}": str(v) for i, v in enumerate(vals, 1)}


DAYOFFER_ROWS = [
    # AAA1 GEN - early offer, superseded (note the comma in REBIDEXPLANATION)
    _day_row(DUID="AAA1", BIDTYPE="ENERGY", OFFERDATE="2026/06/28 12:00:00",
             VERSIONNO="1", PARTICIPANTID="PARTA",
             REBIDEXPLANATION="PLANT LIMIT, PRICE REVIEW", DIRECTION="GEN",
             **_pb(-1000, 0, 10, 20, 50, 100, 200, 500, 5000, 16600)),
    # AAA1 GEN - later offer, WINS
    _day_row(DUID="AAA1", BIDTYPE="ENERGY", OFFERDATE="2026/06/29 09:00:00",
             VERSIONNO="2", PARTICIPANTID="PARTA",
             REBIDEXPLANATION="RAMP UP, THEN RECOVERY", DIRECTION="GEN",
             **_pb(-900, 0, 12, 22, 52, 102, 202, 502, 5002, 16600)),
    # BBB1 LOAD
    _day_row(DUID="BBB1", BIDTYPE="ENERGY", OFFERDATE="2026/06/28 12:00:00",
             VERSIONNO="1", PARTICIPANTID="PARTB", DIRECTION="LOAD",
             **_pb(0, 10, 20, 30, 40, 50, 60, 70, 80, 90)),
    # CCC1 BIDIRECTIONAL
    _day_row(DUID="CCC1", BIDTYPE="ENERGY", OFFERDATE="2026/06/28 15:00:00",
             VERSIONNO="1", PARTICIPANTID="PARTC", DIRECTION="BIDIRECTIONAL",
             **_pb(-500, -100, 0, 25, 75, 150, 300, 600, 6000, 15000)),
    # DDD1 RAISE6SEC - MUST be filtered out (non-ENERGY)
    _day_row(DUID="DDD1", BIDTYPE="RAISE6SEC", OFFERDATE="2026/06/28 12:00:00",
             VERSIONNO="1", PARTICIPANTID="PARTD", DIRECTION="GEN",
             **_pb(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)),
]

PEROFFER_ROWS = [
    # AAA1 GEN 04:05 - early, superseded
    _per_row(DUID="AAA1", BIDTYPE="ENERGY", OFFERDATE="2026/06/28 12:00:00",
             VERSIONNO="1", MAXAVAIL="100", PASAAVAILABILITY="100",
             INTERVAL_DATETIME="2026/06/29 04:05:00", DIRECTION="GEN",
             **_ba(10, 20, 0, 0, 0, 0, 0, 0, 0, 70)),
    # AAA1 GEN 04:05 - later, WINS
    _per_row(DUID="AAA1", BIDTYPE="ENERGY", OFFERDATE="2026/06/29 09:00:00",
             VERSIONNO="2", MAXAVAIL="100", PASAAVAILABILITY="100",
             INTERVAL_DATETIME="2026/06/29 04:05:00", DIRECTION="GEN",
             **_ba(5, 5, 0, 0, 0, 0, 0, 0, 0, 90)),
    # AAA1 GEN 04:10 - single offer
    _per_row(DUID="AAA1", BIDTYPE="ENERGY", OFFERDATE="2026/06/29 09:00:00",
             VERSIONNO="2", MAXAVAIL="100", PASAAVAILABILITY="100",
             INTERVAL_DATETIME="2026/06/29 04:10:00", DIRECTION="GEN",
             **_ba(5, 5, 0, 0, 0, 0, 0, 0, 0, 90)),
    # BBB1 LOAD 04:05
    _per_row(DUID="BBB1", BIDTYPE="ENERGY", OFFERDATE="2026/06/28 12:00:00",
             VERSIONNO="1", MAXAVAIL="50", PASAAVAILABILITY="50",
             INTERVAL_DATETIME="2026/06/29 04:05:00", DIRECTION="LOAD",
             **_ba(30, 20, 0, 0, 0, 0, 0, 0, 0, 0)),
    # CCC1 BIDIRECTIONAL 04:05
    _per_row(DUID="CCC1", BIDTYPE="ENERGY", OFFERDATE="2026/06/28 15:00:00",
             VERSIONNO="1", MAXAVAIL="200", PASAAVAILABILITY="200",
             INTERVAL_DATETIME="2026/06/29 04:05:00", DIRECTION="BIDIRECTIONAL",
             **_ba(0, 0, 0, 0, 0, 0, 0, 0, 0, 200)),
    # DDD1 RAISE6SEC 04:05 - MUST be filtered out
    _per_row(DUID="DDD1", BIDTYPE="RAISE6SEC", OFFERDATE="2026/06/28 12:00:00",
             VERSIONNO="1", MAXAVAIL="15", PASAAVAILABILITY="15",
             INTERVAL_DATETIME="2026/06/29 04:05:00", DIRECTION="GEN",
             **_ba(15, 0, 0, 0, 0, 0, 0, 0, 0, 0)),
]


def build_csv() -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["C", "NEMP.WORLD", "BIDMOVE_COMPLETE", "AEMO", "PUBLIC",
                DAY, "0000000000000001", "BIDMOVE_COMPLETE", "1"])
    w.writerow(["I", "BID", "BIDDAYOFFER_D", "3"] + DAYOFFER_COLS)
    for r in DAYOFFER_ROWS:
        w.writerow(["D", "BID", "BIDDAYOFFER_D", "3"] + [r[c] for c in DAYOFFER_COLS])
    w.writerow(["I", "BID", "BIDPEROFFER_D", "4"] + PEROFFER_COLS)
    for r in PEROFFER_ROWS:
        w.writerow(["D", "BID", "BIDPEROFFER_D", "4"] + [r[c] for c in PEROFFER_COLS])
    w.writerow(["C", "END OF REPORT", str(len(DAYOFFER_ROWS) + len(PEROFFER_ROWS) + 4)])
    return buf.getvalue()


def main():
    here = Path(__file__).parent
    csv_text = build_csv()
    csv_path = here / "bids_sample.csv"
    csv_path.write_text(csv_text)

    zip_path = here / "bids_sample.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("PUBLIC_BIDMOVE_COMPLETE_20260629_0000000000000001.CSV", csv_text)

    print(f"wrote {csv_path} ({len(csv_text)} bytes)")
    print(f"wrote {zip_path}")


if __name__ == "__main__":
    main()
