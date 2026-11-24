#!/usr/bin/env python3
"""Equation tables, constraint-level energy, ranks, notes and the area registry (synthetic inputs)."""
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent))
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from aemo_updater import network_equation as ne  # noqa: E402
from aemo_updater import network_summary as ns  # noqa: E402
from test_network_summary import M, T0, V1, V2, Scn  # noqa: E402


def rhs_term(s, cid, termid, spd, ty, f=1.0, op=None, dv=None, eff=V1, ver=1, scope="DS"):
    s.ins("cn.generic_constraint_rhs", genconid=cid, effectivedate=eff, versionno=ver, scope=scope, termid=termid,
          spd_id=spd, spd_type=ty, factor=f, operation=op, defaultvalue=dv)


def ems(s, spd, ty, desc):
    s.ins("cn.emsmaster", spd_id=spd, spd_type=ty, description=desc)


# ------------------------------------------------------------------ pure helpers

def test_num_never_strips_integer_zeros():
    assert ne.num(1500, 0) == "1,500"
    assert ne.num(750.0, 3) == "750"
    assert ne.num(1.96200, 3) == "1.962"
    assert ne.num(0.5, 2) == "0.5"
    assert ne.num(100, 1) == "100"
    assert ne.num(10.0, 0) == "10"


def test_rhs_reading_templates():
    r = ne.rhs_reading
    assert r(1, "NRATCN_MOON94T", "E", None, None, 300) == (
        "The continuous rating of the protected equipment, in MVA, from AEMO's rating equation NRATCN_MOON94T. "
        "If the live value is unavailable AEMO uses 300.")
    assert "15-minute rating" in r(1, "NRAT15_X", "E", None, None, 10)
    assert r(-1, "Meas", "A", "line MVA") == "Minus a live measurement: line MVA."
    assert r(-0.5, "Meas", "A", "line MVA") == "Minus 0.5 × a live measurement: line MVA."
    assert r(1, "Sw", "S", "On status of x") == "A switch that is 1 or 0: On status of x."
    assert r(2, "Sw", "S", "On status of x").endswith("When on, it adds 2.")
    assert r(1, "V-SA", "I", "MW flow west") == "Plus the current flow on an interconnector: MW flow west."
    assert r(-15, "Operating_Margin", "C") == "An operating margin of 15 held back as a safety allowance."
    assert r(-1.5, "Confidence_x", "C") == "A statistical confidence allowance of 1.5, subtracted."
    assert r(1200, "Limit", "C") == "A fixed number set by AEMO: 1,200."
    assert r(1.962, "Scale", "U").startswith("Everything above is multiplied by 1.962.")
    assert r(1, "Choose", "U", None, "EXLEZ").startswith("A calculation step that chooses between the two ratings")
    assert r(1, "Other", "U", None, "PUSH") == "A calculation step named “Other” (operation PUSH)."
    assert r(1, "Grp", "G") == "A label for an intermediate result, “Grp”."
    assert r(3, "Grp", "G").endswith("weight 3.")
    assert r(1, "Z", "X") == "Term “Z” of a type not covered by these explanations (AEMO type X)."


def test_lhs_reading_wording():
    assert ne.lhs_reading("unit", 0.987, "<=") == "Each extra MW adds 0.99 to the left side"
    assert ne.lhs_reading("unit", -0.25, "<=") == "Each extra MW takes 0.25 off the left side"
    assert ne.lhs_reading("unit", 0.5, ">=") == "Each extra MW counts 0.5 towards the minimum"
    assert ne.lhs_reading("unit", -0.5, ">=") == "Each extra MW takes 0.5 off the minimum"
    assert ne.lhs_reading("interconnector", 0.3, "<=", "MW flow north on Vic to NSW") == \
        "Each extra MW of flow (MW flow north on Vic to NSW) adds 0.3 to the left side"


def test_term_kind_order():
    k = ne.term_kind
    assert [k("E", "n", None), k("A", "n", None), k("S", "n", None), k("C", "n", None)] == [
        "rating", "measurement", "status", "constant"]
    assert k("U", "Scale_x", None) == "scale" and k("U", "Other", None) == "step" and k("U", "Scale", "PUSH") == "scale"
    assert k("C", "Limit*2", "PUSH") == "constant"           # type is tested before the operation
    assert k("G", "g", "PUSH") == "step" and k("G", "g", None) == "label"
    assert k("I", "i", None) == "interconnector" and k("T", "t", None) == "unit" and k("X", "x", None) == "other"


def test_fingerprint_is_structure_only():
    a = [("Limit", "C", None), ("Scale", "U", "PUSH")]
    assert ne.fingerprint(a) == ne.fingerprint(list(a)) and len(ne.fingerprint(a)) == 12
    assert ne.fingerprint(a) != ne.fingerprint(a[::-1])
    assert ne.fingerprint(a) != ne.fingerprint([("Limit", "C", None), ("Scale", "U", None)])


def test_is_feedback():
    cp, ic = {"CP1": 0.5}, {"V-SA": -0.3}
    assert ne.is_feedback("T", "STN.CP1", 0.5 + 5e-7, cp, ic)
    assert not ne.is_feedback("T", "STN.CP1", 0.6, cp, ic)
    assert not ne.is_feedback("T", "STN.CP9", 0.5, cp, ic)
    assert ne.is_feedback("I", "V-SA", -0.3, cp, ic) and not ne.is_feedback("I", "V-SA", 0.3, cp, ic)
    assert not ne.is_feedback("A", "CP1", 0.5, cp, ic)


# ------------------------------------------------------------------ equation tables

def _eq_scn():
    s = Scn()
    s.constraint("N_A", ctype="<=", terms={"CP_U1": 0.5, "CP_U2": -0.25}, descr="Out= Nil, avoid O/L of Element Alpha")
    s.ins("cn.spd_interconnector", genconid="N_A", effectivedate=V1, versionno=1, interconnectorid="VIC1-NSW1", factor=0.3)
    s.unit("U1", site="Site One")
    s.unit("U2", site="Site Two", cap=50.0)
    ems(s, "VIC1-NSW1", "I", "MW flow north on the Vic to NSW AC Interconnector")
    ems(s, "Meas1", "A", "Line 5 MVA")
    rhs_term(s, "N_A", 1, "NRATCN_X", "E", 1.0, dv=300.0)
    rhs_term(s, "N_A", 2, "Meas1", "A", -1.0)
    rhs_term(s, "N_A", 3, "Operating_Margin", "C", -15.0)
    rhs_term(s, "N_A", 4, "Scale", "U", 2.0)
    rhs_term(s, "N_A", 5, "STN.CP_U1", "T", 0.5)            # feedback (same factor as the left-side term)
    rhs_term(s, "N_A", 6, "VIC1-NSW1", "I", 0.3)             # feedback
    rhs_term(s, "N_A", 7, "STN.CP_U2", "T", -0.2)            # factor differs: not feedback
    rhs_term(s, "N_A", 8, "Pick", "U", 1.0, op="EXLEZ")
    rhs_term(s, "N_A", 9, "N_A_PD", "C", 1.0, scope="PD")   # not dispatch scope: absent
    for i in range(4):
        s.bind("N_A", T0 + i * M, rhs=100.0 + 10 * i, lhs=90.0 + i)
    return s.build()


def test_equation_rows_and_readings():
    s = _eq_scn()
    lhs = s.q("select ord, kind, spd_id, spd_type, factor, duid, site, fuel, capacity_mw, aemo_description, reading, "
              "is_feedback from constraint_equation where side='lhs' order by ord")
    assert [r[1] for r in lhs] == ["unit", "unit", "interconnector"]
    u1 = next(r for r in lhs if r[2] == "CP_U1")
    assert u1[5:9] == ("U1", "Site One", "Wind", 100.0) and u1[10] == "Each extra MW adds 0.5 to the left side"
    u2 = next(r for r in lhs if r[2] == "CP_U2")
    assert u2[10] == "Each extra MW takes 0.25 off the left side" and u2[8] == 50.0
    ic = next(r for r in lhs if r[1] == "interconnector")
    assert ic[2] == "VIC1-NSW1" and ic[9].startswith("MW flow north")
    assert "of flow (MW flow north on the Vic to NSW AC Interconnector) adds 0.3" in ic[10]
    assert [r[0] for r in lhs] == [1, 2, 3]
    rhs = {r[0]: r[1:] for r in s.q("select ord, kind, spd_id, spd_type, factor, operation, defaultvalue, duid, "
                                    "aemo_description, reading, is_feedback from constraint_equation where side='rhs'")}
    assert sorted(rhs) == [1, 2, 3, 4, 5, 6, 7, 8]                       # PD scope dropped, termid order
    assert rhs[1][0] == "rating" and rhs[1][5] == 300.0 and "continuous rating" in rhs[1][8]
    assert rhs[2][0] == "measurement" and rhs[2][7] == "Line 5 MVA" and rhs[2][8] == "Minus a live measurement: Line 5 MVA."
    assert rhs[3][0] == "constant" and rhs[4][0] == "scale"
    assert rhs[5][0] == "unit" and rhs[5][9] is True and rhs[5][6] == "U1"          # T term mapped to its unit
    assert rhs[6][0] == "interconnector" and rhs[6][9] is True
    assert rhs[7][0] == "unit" and rhs[7][9] is False
    assert rhs[8][0] == "step" and rhs[8][4] == "EXLEZ"


def test_equation_meta():
    s = _eq_scn()
    m = s.q("select constraintid, effectivedate, versionno, version_rule, constrainttype, scale, n_lhs_units, "
            "n_lhs_interconnectors, n_feedback, has_operations, fingerprint, rhs_p10, rhs_med, rhs_p90, lhs_med, "
            "n_binding_intervals from constraint_equation_meta")[0]
    assert m[:5] == ("N_A", V1, 1, "exact", "<=")
    assert m[5] == 2.0 and m[6] == 2 and m[7] == 1 and m[8] == 2 and m[9] is True
    expect = ne.fingerprint([("NRATCN_X", "E", None), ("Meas1", "A", None), ("Operating_Margin", "C", None),
                             ("Scale", "U", None), ("STN.CP_U2", "T", None), ("Pick", "U", "EXLEZ")])
    assert m[10] == expect
    assert m[11] == pytest.approx(103.0) and m[12] == pytest.approx(115.0) and m[13] == pytest.approx(127.0)
    assert m[14] == pytest.approx(91.5) and m[15] == 4


def test_fingerprint_ignores_factors_but_not_structure():
    base = _eq_scn().q("select fingerprint from constraint_equation_meta")[0][0]
    s = Scn()
    s.constraint("N_A", ctype="<=", terms={"CP_U1": 0.5, "CP_U2": -0.25})
    s.unit("U1")
    s.unit("U2")
    rhs_term(s, "N_A", 1, "NRATCN_X", "E", 7.0, dv=1.0)                  # factor and default differ
    rhs_term(s, "N_A", 2, "Meas1", "A", -3.0)
    rhs_term(s, "N_A", 3, "Operating_Margin", "C", -99.0)
    rhs_term(s, "N_A", 4, "Scale", "U", 5.0)
    rhs_term(s, "N_A", 7, "STN.CP_U2", "T", -0.2)
    rhs_term(s, "N_A", 8, "Pick", "U", 1.0, op="EXLEZ")
    s.bind("N_A", T0)
    s.build()
    assert s.q("select fingerprint from constraint_equation_meta")[0][0] == base
    s.con.execute("update cn.generic_constraint_rhs set spd_id = 'Meas2' where spd_id = 'Meas1'")
    s.build()
    assert s.q("select fingerprint from constraint_equation_meta")[0][0] != base


def test_equation_version_fallback_and_window():
    s = Scn()
    for c in ["N_EX", "N_EARLY", "N_LATER", "N_NORHS", "N_OLD"]:
        s.constraint(c, versions=[(V1, 1), (V2, 1)], terms={"CP_U": 0.5})
    s.unit("U")
    rhs_term(s, "N_EX", 1, "L", "C", eff=V2)
    rhs_term(s, "N_EARLY", 1, "L", "C", eff=V1)
    rhs_term(s, "N_LATER", 1, "L", "C", eff=V2)
    rhs_term(s, "N_OLD", 1, "L", "C", eff=V2)
    s.bind("N_EX", T0, eff=V2)
    s.bind("N_EARLY", T0, eff=V2)                  # latest earlier RHS version is V1
    s.bind("N_LATER", T0, eff=V1)                  # only a later RHS version exists: none
    s.bind("N_NORHS", T0, eff=V2)
    s.bind("N_OLD", T0 - timedelta(days=400), eff=V2)   # not bound in the last 365 days
    s.build()
    got = {r[0]: r[1:] for r in s.q("select constraintid, version_rule, effectivedate from constraint_equation_meta")}
    assert got == {"N_EX": ("exact", V2), "N_EARLY": ("earlier", V1), "N_LATER": ("none", None),
                   "N_NORHS": ("none", None)}
    assert s.q("select count(*) from constraint_equation where constraintid in ('N_LATER','N_NORHS','N_OLD')") == [(0,)]
    # the version used for the left side follows the same row
    assert s.q("select count(*) from constraint_equation where constraintid='N_EARLY' and side='lhs'") == [(1,)]


# ------------------------------------------------------------------ constraint energy, hours, lead, ranks

def _energy_scn():
    s = Scn()
    # N_X and N_Y share units so they form one area; N_Z is its own area
    s.constraint("N_X", terms={"CP_U": 0.5, "CP_P": 0.5}, descr="avoid O/L of Element Xray on trip")
    s.constraint("N_Y", terms={"CP_U": 0.5, "CP_P": 0.5}, descr="avoid O/L of Element Yankee on trip")
    s.constraint("N_Z", terms={"CP_Q": 1.0}, descr="avoid O/L of Element Zulu on trip")
    s.unit("U", region="QLD1")
    s.unit("P", region="QLD1")
    s.unit("Q", region="QLD1")
    s.bind("N_X", T0, mv=100)
    s.bind("N_Y", T0, mv=200)                     # Y dominant at T0
    s.bind("N_X", T0 + M, mv=300)                 # X dominant at T0+M
    s.bind("N_X", T0 + 2 * M, mv=100)
    s.bind("N_Z", T0, mv=50)
    for t, mw in [(T0, 12), (T0 + M, 24), (T0 + 2 * M, 12)]:
        s.ins("am.curtailment_duid5", settlementdate=t, duid="U", uigf=1.0, totalcleared=0.0, curtailment=mw)
        s.ins("am.prices5", settlementdate=t, regionid="QLD1", rrp=100.0)
    s.ins("am.curtailment_duid5", settlementdate=T0, duid="Q", uigf=1.0, totalcleared=0.0, curtailment=60.0)
    return s.build()


def test_constraint_energy_sums_to_area_energy():
    s = _energy_scn()
    cd = {r[0]: r[1:] for r in s.q("select constraintid, sum(constrained_mwh), sum(constrained_value) "
                                   "from constraint_daily group by 1")}
    assert cd["N_Y"] == (pytest.approx(1.0), pytest.approx(100.0))
    assert cd["N_X"] == (pytest.approx(3.0), pytest.approx(300.0))
    assert cd["N_Z"][0] == pytest.approx(5.0)
    got = s.q("""select a.area_id, a.day, sum(c.constrained_mwh), sum(c.constrained_value), any_value(a.constrained_mwh),
                        any_value(a.constrained_value)
                 from constraint_daily c join constraint_meta m on m.constraintid = c.constraintid
                 join area_daily a on a.area_id = m.area_id and a.day = c.day group by 1, 2""")
    assert got and all(r[2] == pytest.approx(r[4]) and r[3] == pytest.approx(r[5]) for r in got)


def test_lead_constraint_is_the_one_holding_back_most_energy():
    s = _energy_scn()
    a = s.q("select area_id, lead_constraintid from area order by 1")
    lead = dict(a)
    xy = s.q("select area_id from constraint_meta where constraintid='N_X'")[0][0]
    assert lead[xy] == "N_X"                                            # 3 MWh against 1 MWh


def test_lead_falls_back_to_binding_intervals_when_no_energy():
    s = Scn()
    s.constraint("N_A", terms={"CP_U": 0.5})
    s.constraint("N_B", terms={"CP_U": 0.5})
    s.unit("U")
    s.bind("N_A", T0)
    for i in range(3):
        s.bind("N_B", T0 + i * M)
    s.build()
    assert s.q("select lead_constraintid from area") == [("N_B",)]


def test_constraint_hour_last_365_days_only():
    s = Scn()
    s.constraint("N_A", terms={"CP_U": 0.5})
    s.unit("U")
    s.bind("N_A", T0)                                     # 11:55 period -> hour 11
    s.bind("N_A", T0 + M)
    s.bind("N_A", T0 + timedelta(hours=2))
    s.bind("N_A", T0 - timedelta(days=400))               # outside the window
    s.build()
    assert s.q("select hour, n_intervals from constraint_hour order by hour") == [(11, 1), (12, 1), (13, 1)]


def test_area_rank_windows_and_dense_ranks():
    s = Scn()
    for c, cp in [("N_P", "CP_P"), ("N_Q", "CP_Q"), ("N_R", "CP_R")]:
        s.constraint(c, terms={cp: 1.0}, descr=f"avoid O/L of Element {c} on trip")
    for u in "PQR":
        s.unit(u, region="NSW1")
    d200, d400 = timedelta(days=200), timedelta(days=400)
    for i in range(3):
        s.bind("N_P", T0 + i * M)                          # 90d and 12m
    s.bind("N_P", T0 - d200)                               # 12m and prev
    for i in range(5):
        s.bind("N_Q", T0 - d200 + i * M)
    for i in range(2):
        s.bind("N_R", T0 - d400 + i * M)                   # prev only
    s.ins("am.curtailment_duid5", settlementdate=T0, duid="P", uigf=1.0, totalcleared=0.0, curtailment=12.0)
    s.ins("am.curtailment_duid5", settlementdate=T0 - d200, duid="Q", uigf=1.0, totalcleared=0.0, curtailment=24.0)
    s.build()
    rk = {(r[0], r[1]): r[2:] for r in s.q('select "window", area_id, gwh, hours, rank_energy, rank_hours '
                                           "from area_rank")}
    assert rk[("12m", "N_Q")] == (pytest.approx(0.002), pytest.approx(5 / 12), 1, 1)
    assert rk[("12m", "N_P")] == (pytest.approx(0.001), pytest.approx(4 / 12), 2, 2)
    assert ("12m", "N_R") not in rk
    assert [k for k in rk if k[0] == "90d"] == [("90d", "N_P")] and rk[("90d", "N_P")][2:] == (1, 1)
    assert rk[("12m_prev", "N_Q")][2:] == (1, 1)
    assert rk[("12m_prev", "N_P")][2:] == (2, 3) and rk[("12m_prev", "N_R")][2:] == (2, 2)   # dense, tied at 0


# ------------------------------------------------------------------ notes

def _note_file(tmp_path, entries):
    p = tmp_path / "notes.json"
    p.write_text(json.dumps(entries), encoding="utf-8")
    return p


def test_area_note_staleness(tmp_path):
    s = _eq_scn()
    fp = s.q("select fingerprint from constraint_equation_meta")[0][0]
    f = _note_file(tmp_path, {
        "N_A": {"constraintid": "N_A", "note": "ok", "written_effectivedate": "2026-01-01 00:00:00",
                "written_fingerprint": fp}})
    ns.build(s.con, notes=f)
    assert s.q("select area_id, constraintid, note, current_fingerprint, is_stale from area_note") == [
        ("N_A", "N_A", "ok", fp, False)]
    f = _note_file(tmp_path, {"N_A": {"constraintid": "N_A", "note": "x", "written_effectivedate": "2026-01-01 00:00:00",
                                      "written_fingerprint": "deadbeef0000"},
                              "N_GONE": {"constraintid": "N_GONE", "note": "y", "written_effectivedate": None,
                                         "written_fingerprint": "x"}})
    ns.build(s.con, notes=f)
    assert dict(s.q("select area_id, is_stale from area_note")) == {"N_A": True, "N_GONE": True}
    # a note whose constraint is no longer the area's lead is stale even if its fingerprint matches
    f = _note_file(tmp_path, {"N_A": {"constraintid": "N_OTHER", "note": "z", "written_effectivedate": None,
                                      "written_fingerprint": fp}})
    ns.build(s.con, notes=f)
    assert s.q("select is_stale from area_note") == [(True,)]
    ns.build(s.con, notes=tmp_path / "missing.json")
    assert s.q("select count(*) from area_note") == [(0,)]


# ------------------------------------------------------------------ registry

def test_registry_first_run_matches_plain_clustering_and_is_stable():
    sets = {"A": frozenset("abc"), "B": frozenset("abcd"), "C": frozenset("xyz")}
    order = ["B", "A", "C", "I"]
    m1, reg1 = ns.cluster_with_registry(order, sets, None, "2026-11-25")
    assert m1 == ns.cluster_areas(order, sets) == {"B": "B", "A": "B", "C": "C", "I": "I"}
    assert reg1["areas"] == {"B": {"seed": "B", "created": "2026-11-25"}, "C": {"seed": "C", "created": "2026-11-25"},
                             "I": {"seed": "I", "created": "2026-11-25"}}
    # the order of binding changes (A now first): registered constraints keep their area
    m2, reg2 = ns.cluster_with_registry(["A", "C", "B", "I"], sets, reg1, "2026-12-01")
    assert m2 == {"A": "B", "C": "C", "B": "B", "I": "I"} and reg2 == reg1


def test_registry_joins_best_match_or_founds_new_area():
    sets = {"S1": frozenset("abcd"), "S2": frozenset("wxyz"), "N1": frozenset("abcde"), "N2": frozenset("wxyabc"),
            "N3": frozenset("pqr"), "N4": frozenset()}
    _, reg = ns.cluster_with_registry(["S1", "S2"], sets, None, "2026-11-25")
    m, reg2 = ns.cluster_with_registry(["N1", "N2", "N3", "N4"], sets, reg, "2026-12-01")
    # N1 vs S1: 4/5 -> S1.  N2 vs S1: 3/7, vs S2: 3/7 (<0.5 both) -> founds its own area.  N3 new.  N4 no terms.
    assert m == {"N1": "S1", "N2": "N2", "N3": "N3", "N4": "N4"}
    assert reg2["areas"]["N2"] == {"seed": "N2", "created": "2026-12-01"}
    assert reg2["areas"]["S1"]["created"] == "2026-11-25"


def test_registry_file_atomic_save_keeps_bak(tmp_path):
    p = tmp_path / "reg.json"
    ns.save_registry(p, {"version": 1, "constraints": {"A": "A"}, "areas": {"A": {"seed": "A", "created": "d"}}})
    assert not (tmp_path / "reg.json.bak").exists() and not (tmp_path / "reg.json.tmp").exists()
    ns.save_registry(p, {"version": 1, "constraints": {"A": "A", "B": "A"}, "areas": {"A": {"seed": "A", "created": "d"}}})
    assert json.loads((tmp_path / "reg.json.bak").read_text())["constraints"] == {"A": "A"}
    assert ns.load_registry(p)["constraints"] == {"A": "A", "B": "A"}
    assert ns.load_registry(tmp_path / "none.json") is None


def test_build_with_registry_keeps_areas_when_ordering_changes(tmp_path):
    s = Scn()
    s.constraint("N_A", terms={"CP_U1": 1.0, "CP_U2": 1.0}, descr="avoid O/L of Element Alpha on trip")
    s.constraint("N_B", terms={"CP_U1": 1.0, "CP_U2": 1.0, "CP_U3": 0.6}, descr="avoid O/L of Element Bravo on trip")
    s.constraint("N_C", terms={"CP_U9": 1.0}, descr="avoid O/L of Element Charlie on trip")
    for u in ["U1", "U2", "U3", "U9"]:
        s.unit(u)
    for i in range(3):
        s.bind("N_A", T0 + i * M)
    s.bind("N_B", T0)
    s.bind("N_C", T0)
    reg = tmp_path / "reg.json"
    out = tmp_path / "out.duckdb"
    s.times.update([T0, T0 + M, T0 + 2 * M])
    for t in s.times:
        s.ins("cn.constraint_intervals", settlementdate=t, n_listed=1, n_binding=1)
    ns.run(s.con, out, registry=reg)
    first = dict(s.q("select constraintid, area_id from constraint_meta"))
    assert first["N_B"] == "N_A" and first["N_C"] == "N_C"
    # N_B starts binding far more often than N_A: without a registry it would seed the area
    for i in range(1, 12):
        s.bind("N_B", T0 + i * M)
    s.times.update([T0 + i * M for i in range(12)])
    for t in s.times:
        if not s.con.execute("select 1 from cn.constraint_intervals where settlementdate=?", [t]).fetchone():
            s.ins("cn.constraint_intervals", settlementdate=t, n_listed=1, n_binding=1)
    ns.build(s.con)
    assert dict(s.q("select constraintid, area_id from constraint_meta"))["N_A"] == "N_B"       # re-clustered
    ns.run(s.con, out, registry=reg)
    assert dict(s.q("select constraintid, area_id from constraint_meta")) == first              # stable
    # a new constraint joins by Jaccard against the seed; created dates come from the registry
    s.constraint("N_D", terms={"CP_U1": 1.0, "CP_U2": 1.0, "CP_U3": 0.9}, descr="avoid O/L of Element Delta on trip")
    s.bind("N_D", T0)
    ns.run(s.con, out, registry=reg)
    assert dict(s.q("select constraintid, area_id from constraint_meta"))["N_D"] == "N_A"
    assert s.q("select created from area where area_id='N_A'")[0][0] is not None
    assert (tmp_path / "reg.json.bak").exists()
    c = duckdb.connect(str(out), read_only=True)
    assert c.execute("select count(*) from area_rank").fetchone()[0] >= 1
