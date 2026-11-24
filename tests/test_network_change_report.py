#!/usr/bin/env python3
"""scripts/network_change_report.py on synthetic summaries."""
import html
import json
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "src"))

import network_change_report as ncr  # noqa: E402

IDS = [f"A{i:02d}" for i in range(1, 13)]


def _many(c, sql, rows):
    if rows:
        c.executemany(sql, rows)


def make_summary(path, ranks=None, hours=None, leads=None, stale=(), units=(), none_rules=(), created="2026-11-25",
                 r90=None, ids=IDS):
    """Areas A01..A12; rank order = list position unless overridden."""
    if Path(path).exists():
        Path(path).unlink()
    c = duckdb.connect(str(path))
    c.execute("create table meta (built_at timestamp, data_through timestamp)")
    c.execute("insert into meta values ('2026-11-25 09:00:00', '2026-11-25 08:55:00')")
    c.execute("create table area (area_id varchar, name varchar, lead_constraintid varchar, created varchar)")
    _many(c, "insert into area values (?, ?, ?, ?)",
                  [[a, f"Name {a}", (leads or {}).get(a, "L_" + a), created] for a in ids])
    c.execute('create table area_rank ("window" varchar, area_id varchar, gwh double, hours double, '
              "rank_energy bigint, rank_hours bigint)")
    order_e = ranks or ids
    order_h = hours or ids
    rows = [["12m", a, 100.0 - order_e.index(a), 1000.0 - order_h.index(a), order_e.index(a) + 1, order_h.index(a) + 1]
            for a in ids]
    o90 = r90 or ids
    rows += [["90d", a, 1.0, 1.0, o90.index(a) + 1, o90.index(a) + 1] for a in o90]
    _many(c, "insert into area_rank values (?, ?, ?, ?, ?, ?)", rows)
    c.execute("create table area_note (area_id varchar, constraintid varchar, written_effectivedate timestamp, "
              "is_stale boolean)")
    _many(c, "insert into area_note values (?, ?, ?, ?)", [list(s) for s in stale])
    c.execute("create table constraint_equation_meta (constraintid varchar, effectivedate timestamp)")
    c.execute("insert into constraint_equation_meta values ('L_A01', '2026-10-01')")
    c.execute("create table area_units (area_id varchar, duid varchar, site varchar, capacity_mw double)")
    _many(c, "insert into area_units values (?, ?, ?, ?)", [list(u) for u in units])
    c.execute("create table constraint_meta (constraintid varchar, definition_rule varchar)")
    _many(c, "insert into constraint_meta values (?, ?)", [[k, r] for k, r in none_rules] + [["OK1", "exact"]])
    c.execute("create table constraint_daily (day date, constraintid varchar, n_intervals bigint)")
    _many(c, "insert into constraint_daily values ('2026-11-01', ?, ?)", [[k, 24] for k, r in none_rules] + [["OK1", 5]])
    c.close()


@pytest.fixture
def geo(tmp_path):
    p = tmp_path / "geo.json"
    p.write_text(json.dumps({"units": {"U1": [1, 2, "x"]}, "area_lines": {f"A{i:02d}": [] for i in range(1, 26)},
                             "area_status": {}}))
    return p


def run(tmp_path, geo, *extra, send=None):
    sent = []
    rc = ncr.main(["--summary", str(tmp_path / "s.duckdb"), "--state", str(tmp_path / "state.json"), "--geo", str(geo),
                   *extra], send=send or (lambda subj, body: sent.append((subj, html.unescape(body))) or True))
    return rc, sent


def test_first_run_sends_baseline_and_saves_state(tmp_path, geo):
    make_summary(tmp_path / "s.duckdb")
    rc, sent = run(tmp_path, geo)
    assert rc == 0 and len(sent) == 1 and "baseline" in sent[0][0]
    assert "1. Name A01 [A01]" in sent[0][1]
    st = json.loads((tmp_path / "state.json").read_text())
    assert st["top10_energy"] == IDS[:10] and st["leads"]["A01"] == "L_A01" and st["areas"] == sorted(IDS)


def test_no_change_sends_nothing(tmp_path, geo):
    make_summary(tmp_path / "s.duckdb")
    run(tmp_path, geo)
    rc, sent = run(tmp_path, geo)
    assert rc == 0 and sent == []


def test_top_ten_entering_leaving_by_energy_and_hours(tmp_path, geo):
    make_summary(tmp_path / "s.duckdb")
    run(tmp_path, geo)
    new_e = ["A11"] + IDS[:10] + ["A12"]                       # A11 enters the energy top ten, A10 drops out
    new_h = IDS[:9] + ["A12", "A10", "A11"]                    # A12 enters the hours top ten, A10 drops to 11th
    make_summary(tmp_path / "s.duckdb", ranks=new_e, hours=new_h)
    rc, sent = run(tmp_path, geo)
    body = sent[0][1]
    assert "top ten by energy" in body and "entered: Name A11 [A11]" in body and "left: Name A10 [A10]" in body
    assert "top ten by binding hours" in body and "entered: Name A12 [A12]" in body
    assert "Edit: config/network_area_names.csv" in body


def test_rising_lead_change_and_stale_notes(tmp_path, geo):
    make_summary(tmp_path / "s.duckdb")
    run(tmp_path, geo)
    make_summary(tmp_path / "s.duckdb", r90=["A12", "A01", "A02", "A03", "A04"] + [a for a in IDS if a not in
                 ("A12", "A01", "A02", "A03", "A04")], leads={"A02": "L_NEW"},
                 stale=[("A01", "L_A01", "2026-01-01 00:00:00", True)])
    rc, sent = run(tmp_path, geo)
    body = sent[0][1]
    assert "Rising" in body and "Name A12 [A12]" in body
    assert "lead constraint changed" in body and "A02" in body and "L_A02 -> L_NEW" in body
    assert "Notes now stale" in body and "written for the version effective 2026-01-01; current version 2026-10-01" in body
    assert body.count("Edit: config/network_area_notes.json") >= 2


def test_unmapped_areas_and_units_and_none_rules_and_new_areas(tmp_path, geo):
    ids = IDS + ["A13", "A14", "A15", "A16", "A17", "A18", "A19", "A20", "A21", "A22", "A23", "A24", "A25", "A26",
                 "A27", "A28", "A29", "A30", "A31"]
    make_summary(tmp_path / "s.duckdb", ids=ids[:12])
    run(tmp_path, geo)
    # 31 areas; the geo file maps only the first 25: the top-30 by hours include A26..A30 unmapped
    make_summary(tmp_path / "s.duckdb", ids=ids, units=[("A01", "U1", "Mapped", 100.0), ("A01", "U2", "Unmapped", 120.0),
                                                        ("A02", "U3", "Small", 5.0)],
                 none_rules=[("N_NEW", "none"), ("N_QUIET", "none")])
    c = duckdb.connect(str(tmp_path / "s.duckdb"))
    c.execute("delete from constraint_daily where constraintid = 'N_QUIET'")
    c.close()
    rc, sent = run(tmp_path, geo)
    body = sent[0][1]
    sec = body.split("no line mapping")[1].split("Units of")[0]
    assert "Name A26 [A26]" in sec and "Name A25 [A25]" not in sec
    assert "A31" not in sec                                                              # 31st by hours: outside the top 30
    assert "U2 (Unmapped, 120 MW)" in body and "U1 (" not in body and "U3" not in body
    assert "definition_rule 'none'" in body and "N_NEW: 2 h" in body and "N_QUIET" not in body
    assert "New areas created in the registry" in body and "Name A13 [A13] (created 2026-11-25)" in body


def test_no_email_prints_and_still_saves_state(tmp_path, geo, capsys):
    make_summary(tmp_path / "s.duckdb")
    rc = ncr.main(["--summary", str(tmp_path / "s.duckdb"), "--state", str(tmp_path / "state.json"),
                   "--geo", str(geo), "--no-email"])
    out = capsys.readouterr().out
    assert rc == 0 and "Network baseline" in out and (tmp_path / "state.json").exists()


def test_email_failure_returns_nonzero_and_keeps_old_state(tmp_path, geo):
    make_summary(tmp_path / "s.duckdb")
    rc, _ = run(tmp_path, geo, send=lambda s, b: False)
    assert rc == 1 and not (tmp_path / "state.json").exists()
