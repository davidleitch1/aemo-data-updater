#!/usr/bin/env python3
"""Offline tests for aemo_updater.network_summary on tiny synthetic inputs (no real DB)."""
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from aemo_updater import constraints_store as cs  # noqa: E402
from aemo_updater import network_summary as ns  # noqa: E402

T0 = datetime(2026, 10, 1, 12, 0)
V1 = datetime(2026, 1, 1)
V2 = datetime(2026, 6, 1)
V3 = datetime(2026, 8, 1)
M = timedelta(minutes=5)


class Scn:
    """A tiny constraints + aemo world attached as schemas cn and am."""

    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("create schema cn")
        self.con.execute("create schema am")
        for name, spec in cs.TABLES.items():
            cols = ", ".join(f"{c} {cs._SQL_TYPE[t]}" for c, t in spec.cols.items())
            self.con.execute(f"create table cn.{name} ({cols})")
        self.con.execute('create table am.duid_mapping (region varchar, "site name" varchar, owner varchar, '
                         'duid varchar, capacity_mw double, storage_mwh double, fuel varchar)')
        self.con.execute("create table am.curtailment_duid5 (settlementdate timestamp, duid varchar, uigf double, "
                         "totalcleared double, curtailment double)")
        self.con.execute("create table am.prices5 (settlementdate timestamp, regionid varchar, rrp double)")
        self.times = set()

    def ins(self, table, **kw):
        cols = ", ".join('"%s"' % k for k in kw)
        self.con.execute(f"insert into {table} ({cols}) values ({', '.join('?' for _ in kw)})", list(kw.values()))

    def constraint(self, cid, versions=((V1, 1),), ctype="<=", descr="Out= Some line", limittype="Thermal",
                   terms=None, impact="x"):
        for eff, ver in versions:
            self.ins("cn.gencondata", genconid=cid, effectivedate=eff, versionno=ver, constrainttype=ctype,
                     description=descr, limittype=limittype, impact=impact)
            for cp, f in (terms or {}).items():
                self.ins("cn.spd_connection_point", genconid=cid, effectivedate=eff, versionno=ver,
                         connectionpointid=cp, bidtype="ENERGY", factor=f)

    def unit(self, duid, cp=None, cap=100.0, region="NSW1", fuel="Wind", start=datetime(2020, 1, 1), end=None,
             site=None):
        cp = cp or "CP_" + duid
        self.ins("cn.dudetailsummary", duid=duid, start_date=start, end_date=end, connectionpointid=cp,
                 regionid=region, dispatchtype="GENERATOR")
        if not self.con.execute("select 1 from am.duid_mapping where duid=?", [duid]).fetchone():
            self.ins("am.duid_mapping", region=region, **{"site name": site or "Site " + duid}, owner="o", duid=duid,
                     capacity_mw=cap, storage_mwh=0.0, fuel=fuel)

    def bind(self, cid, t, mv=100.0, eff=V1, ver=1, interv=0, rhs=None, lhs=None):
        self.times.add(t)
        self.ins("cn.constraint_binding5", settlementdate=t, constraintid=cid, intervention=interv,
                 marginalvalue=mv, genconid_effectivedate=eff, genconid_versionno=ver, rhs=rhs, lhs=lhs)

    def build(self):
        for t in self.times:
            if not self.con.execute("select 1 from cn.constraint_intervals where settlementdate=?", [t]).fetchone():
                self.ins("cn.constraint_intervals", settlementdate=t, n_listed=1, n_binding=1)
        ns.build(self.con, built_at=datetime(2026, 11, 24))
        return self

    def q(self, sql, *a):
        return self.con.execute(sql, *a).fetchall()


# ------------------------------------------------------------------ class filter

def test_class_filter_and_binding_filters():
    s = Scn()
    for cid in ["N_A", "F_X", "F-Y", "#Q", "$U", "FOO"]:
        s.constraint(cid)
        s.bind(cid, T0)
    s.bind("N_A", T0 + M, interv=1)       # intervention row
    s.bind("N_A", T0 + 2 * M, mv=0.0)     # not binding
    s.build()
    assert {r[0] for r in s.q("select constraintid from constraint_meta")} == {"N_A", "FOO"}
    assert s.q("select sum(n_intervals) from constraint_daily where constraintid='N_A'")[0][0] == 1
    assert s.q("select count(*) from constraint_daily where constraintid in ('F_X','F-Y','#Q','$U')")[0][0] == 0
    assert ns.CLASS_SQL  # classes are documented in meta only for network
    assert s.q("select class from constraint_meta where constraintid='N_A'")[0][0] == "network"


# ------------------------------------------------------------------ day / hour boundary

def test_midnight_interval_belongs_to_previous_day():
    s = Scn()
    s.constraint("N_A", terms={"CP_U": 0.5})
    s.unit("U")
    s.bind("N_A", datetime(2026, 10, 2, 0, 0))     # period ending 00:00 -> 1 Oct, hour 23
    s.bind("N_A", datetime(2026, 10, 2, 0, 5))     # -> 2 Oct hour 0
    s.build()
    assert s.q("select day, n_intervals from constraint_daily order by day") == [
        (datetime(2026, 10, 1).date(), 1), (datetime(2026, 10, 2).date(), 1)]
    assert s.q("select hour, n_intervals from unit_hour_month order by hour") == [(0, 1), (23, 1)]
    assert s.q("select day, n_intervals from intervals_daily order by day") == [
        (datetime(2026, 10, 1).date(), 1), (datetime(2026, 10, 2).date(), 1)]


# ------------------------------------------------------------------ version map

def _version_scn():
    s = Scn()
    # V1 is '>=' (a positive factor holds output UP, so the unit is charge-limited), V2 is '<='
    s.constraint("N_A", versions=[(V1, 1)], ctype=">=", terms={"CP_U": 0.5})
    s.constraint("N_A", versions=[(V2, 1)], ctype="<=", terms={"CP_U": 0.5})
    s.unit("U")
    s.bind("N_A", T0, eff=V2, ver=1)            # exact -> V2 '<='  -> discharge
    s.bind("N_A", T0 + M, eff=V3, ver=1)        # earlier: latest earlier is V2 -> discharge
    s.bind("N_A", T0 + 2 * M, eff=datetime(2025, 1, 1), ver=1)   # later: earliest later is V1 '>=' -> charge
    s.bind("N_NODEF", T0 + 3 * M, eff=V1, ver=1)  # no definition at all
    return s


def test_version_map_rules_and_direction_flip():
    s = _version_scn().build()
    rules = dict(((r[0], r[1]), r[2]) for r in s.q("select cid, eff, rule from vmap"))
    assert rules[("N_A", V2)] == "exact"
    assert rules[("N_A", V3)] == "earlier"
    assert rules[("N_A", datetime(2025, 1, 1))] == "later"
    assert rules[("N_NODEF", V1)] == "none"
    assert s.q("select direction, sum(n_intervals) from unit_constraint_daily group by 1 order by 1") == [
        ("charge", 1), ("discharge", 2)]
    assert dict(s.q("select definition_rule, n_rows from definition_rule_monthly")) == {
        "exact": 1, "earlier": 1, "later": 1, "none": 1}
    # a constraint with no definition is still listed, unattributed, in its own area
    r = s.q("select definition_rule, area_id, cause from constraint_meta where constraintid='N_NODEF'")
    assert r == [("none", "N_NODEF", "Outage")]


def test_version_ordering_uses_effectivedate_then_versionno():
    s = Scn()
    s.constraint("N_A", versions=[(V1, 1), (V1, 2), (V2, 1)], terms={"CP_U": 0.5})
    s.unit("U")
    s.bind("N_A", T0, eff=V1, ver=3)       # earlier: (V1,2) beats (V1,1)
    s.bind("N_A", T0 + M, eff=V1, ver=0)   # later: (V1,1) is the earliest later
    s.build()
    got = {(r[1], r[2]): (r[3], r[4]) for r in s.q("select cid, eff, ver, d_eff, d_ver from vmap")}
    # keys are (eff, ver) tuples -> index by version number
    by_ver = {r[1]: (r[2], r[3]) for r in s.q("select eff, ver, d_eff, d_ver from vmap")}
    assert by_ver[3] == (V1, 2)
    assert by_ver[0] == (V1, 1)


# ------------------------------------------------------------------ date-ranged mapping

def test_connection_point_mapping_is_date_ranged():
    s = Scn()
    s.constraint("N_A", terms={"CPX": 0.5})
    boundary = T0
    s.unit("OLD", cp="CPX", end=boundary)
    s.unit("NEW", cp="CPX", start=boundary)
    s.bind("N_A", boundary - M)   # inside OLD
    s.bind("N_A", boundary)       # t == OLD.end_date (inclusive) and t == NEW.start_date (exclusive) -> OLD
    s.bind("N_A", boundary + M)   # NEW
    s.build()
    got = dict(s.q("select duid, sum(n_discharge_limited) from unit_daily group by 1"))
    assert got == {"OLD": 2, "NEW": 1}


# ------------------------------------------------------------------ cause

@pytest.mark.parametrize("cid,descr,lt,expected", [
    ("N_SS", "Out= Foo", "System Strength", "Plant stability limit"),
    ("N_ISL", "x", "Islanding - Unit", "Plant stability limit"),
    ("N_D", "x", "Discretionary", "Other plant cap"),
    ("N_UZ", "x", "Unit Zero", "Other plant cap"),
    ("N_NC", "x", "Non-Conformance", "Other plant cap"),
    ("C_V_LANC_ZERO", "x", "Unit Zero", "Commissioning"),            # C_ prefix wins over the limit type
    ("N_COM", "Out= Nil, Foo Solar Farm COMMISSIONING limit", "System Strength", "Commissioning"),
    ("N_COM2", "commissioning test", "Thermal", "Commissioning"),
    ("N_N1", "Out= Nil, avoid O/L X", "Thermal", "System normal"),
    ("N_N2", "Out=nil", "Thermal", "System normal"),
    ("N_N3", "NIL outage constraint on Y", "Thermal", "System normal"),
    ("N_NIL_ID", "something else", "Thermal", "System normal"),
    ("N_O", "Out= Wagga-Lower Tumut, avoid O/L X", "Thermal", "Outage"),
])
def test_cause_classification(cid, descr, lt, expected):
    assert ns.classify_cause(descr, lt, cid) == expected


def test_cause_in_constraint_meta_and_regions():
    s = Scn()
    s.constraint("Q_SS", limittype="System Strength", descr="Out= whatever")
    s.constraint("V_NIL", descr="Out= Nil, avoid O/L of Foo - Bar 220 kV line on the trip of X")
    s.constraint("T_OUT", descr="Out= A-B")
    s.constraint("L_X")
    s.constraint("X_Y")
    for c in ["Q_SS", "V_NIL", "T_OUT", "L_X", "X_Y"]:
        s.bind(c, T0)
    s.build()
    m = {r[0]: r[1:] for r in s.q("select constraintid, cause, region, element from constraint_meta")}
    assert m["Q_SS"][:2] == ("Plant stability limit", "QLD")
    assert m["V_NIL"][:2] == ("System normal", "VIC")
    assert m["V_NIL"][2] == "Foo - Bar"
    assert m["T_OUT"][:2] == ("Outage", "TAS")
    assert m["L_X"][1] == "NSW" and m["X_Y"][1] == "Other"


def test_element_name_chain():
    assert ns.element_name("Out= Nil avoid overloading Calliope River to Wurdong 275 kV line on trip of X", "C") \
        == "Calliope River to Wurdong"
    assert ns.element_name("limit Molong-Orange North flow to X", "C") == "Molong-Orange North"
    assert ns.element_name("no pattern here", "CID") == "No pattern here"
    assert ns.element_name("", "CID") == "CID"
    assert ns.element_name(None, "CID") == "CID"


# ------------------------------------------------------------------ clustering

def test_cluster_areas_pure():
    sets = {"A": frozenset("abc"), "B": frozenset("abcd"), "C": frozenset("xyz"), "E": frozenset("abcdefgh")}
    # B first (seed). A: J(A,B)=3/4 -> B. E: J(E,B)=4/8=0.5 -> B. C disjoint -> own. I: no units -> own.
    out = ns.cluster_areas(["B", "A", "C", "E", "I"], sets)
    assert out == {"B": "B", "A": "B", "C": "C", "E": "B", "I": "I"}
    # Jaccard is against the seed only: F overlaps A (a member) but not the seed B enough
    out = ns.cluster_areas(["B", "A", "F"], {"A": frozenset("abc"), "B": frozenset("abcd"), "F": frozenset("cde")})
    assert out["F"] == "F"


def _area_scn():
    s = Scn()
    # B binds most often, A overlaps it, C is disjoint, I is interconnector-only
    s.constraint("N_A", descr="Out= X, avoid O/L of Element Alpha on trip", terms={"CP_U1": 0.5, "CP_U2": 0.3, "CP_U3": 0.1})
    s.constraint("N_B", descr="Out= Y, avoid O/L of Element Bravo on trip",
                 terms={"CP_U1": 0.4, "CP_U2": 0.25, "CP_U3": 0.12, "CP_U4": 0.08, "CP_U5": 0.02})
    s.constraint("N_C", descr="Out= Z, avoid O/L of Element Charlie on trip", terms={"CP_U9": 0.9})
    s.constraint("N_I", descr="Out= Nil", terms={})
    for u, cap in [("U1", 100), ("U2", 50), ("U3", None), ("U4", 10), ("U5", 10), ("U9", 70)]:
        s.unit(u, cap=cap)
    for i in range(3):
        s.bind("N_A", T0 + i * M)
    for i in range(5):
        s.bind("N_B", T0 + i * M)
    s.bind("N_C", T0)
    s.bind("N_I", T0)
    return s.build()


def test_areas_merge_overlap_keep_disjoint_and_interconnector_alone():
    s = _area_scn()
    amap = dict(s.q("select constraintid, area_id from constraint_meta"))
    assert amap == {"N_B": "N_B", "N_A": "N_B", "N_C": "N_C", "N_I": "N_I"}
    areas = {r[0]: r[1:] for r in s.q("select area_id, name, n_constraints, n_units from area")}
    assert areas["N_B"][0] == "Element Bravo"            # most frequent constraint names the area
    assert areas["N_B"][1] == 2
    assert areas["N_I"][1:] == (1, 0)


def test_units_behind_units_mw_and_weighted_mw_use_relative_weights():
    s = _area_scn()
    # relative weights: N_A U1 1, U2 .6, U3 .2 ; N_B U1 1, U2 .625, U3 .3, U4 .2, U5 .05
    r = s.q("select n_units, units_mw, weighted_mw, missing_capacity_units from area where area_id='N_B'")[0]
    assert r[0] == 3                                  # U1, U2, U3 (max rel >= 0.25)
    assert r[1] == pytest.approx(150.0)               # 100 + 50 + 0 (U3 has no capacity)
    assert r[2] == pytest.approx(100 * 1 + 50 * 0.625 + 0 * 0.3 + 10 * 0.2 + 10 * 0.05)
    assert r[3] == 1
    rows = {r[0]: r[1:] for r in s.q("select duid, max_factor, max_rel_weight, direction, site from area_units "
                                     "where area_id='N_B'")}
    assert rows["U1"] == (0.5, 1.0, "discharge", "Site U1")      # raw factor kept
    assert rows["U2"][1] == pytest.approx(0.625) and len(rows) == 5


def test_weights_are_scale_free_and_cluster_set_uses_relative_weight():
    s = Scn()
    # N_T: weights scaled x10 and small tail; N_Q on a 1.0 scale. Relative weights: T {A:1, B:.05, C:.05}
    # -> unit set {A}; Q {A:1, B:.9, C:.9} -> {A, B, C}; Jaccard 1/3 -> separate areas.
    # (On raw factors >= 0.07 both would be {A, B, C} and merge.)
    s.constraint("N_T", terms={"CP_A": 10.0, "CP_B": 0.5, "CP_C": 0.5})
    s.constraint("N_Q", terms={"CP_A": 1.0, "CP_B": 0.9, "CP_C": 0.9})
    for u in "ABC":
        s.unit(u)
    for i in range(3):
        s.bind("N_T", T0 + i * M)
    s.bind("N_Q", T0)
    s.build()
    amap = dict(s.q("select constraintid, area_id from constraint_meta"))
    assert amap["N_T"] != amap["N_Q"]
    rw = {r[0]: r[1] for r in s.q("select duid, max_rel_weight from area_units where area_id='N_T'")}
    assert rw == {"A": 1.0, "B": pytest.approx(0.05), "C": pytest.approx(0.05)}


# ------------------------------------------------------------------ names

def test_element_name_rejects_short_parse_and_falls_back_to_description():
    assert ns.element_name("Out= Nil, avoid O/L of 80% on trip of X", "CID") == "O/L of 80% on trip of X"
    long = "Out = NIL, " + "word " * 30
    got = ns.element_name(long, "CID")
    assert len(got) <= 70 and got.startswith("Word word") and not got.endswith(" ") and "Nil" not in got
    assert got == ("Word" + " word" * 13)                    # 69 chars: cut back to the word boundary
    assert ns.element_name("Out= A-B", "CID") == "Out= A-B"  # only a leading Out= Nil prefix is stripped
    assert ns.element_name("avoid O/L of Wagga Wagga (818) on trip", "CID") == "Wagga Wagga (818)"   # codes kept


def test_area_name_from_sites_for_small_unit_specific_area():
    s = Scn()
    s.constraint("N_SS", limittype="System Strength", descr="Out= Nil, something about a farm",
                 terms={"CP_U1": 1.0, "CP_U2": 1.0, "CP_U3": 0.05})
    s.unit("U1", site="Mount Emerald")
    s.unit("U2", site="Mount Emerald")
    s.unit("U3", site="Elsewhere")
    s.bind("N_SS", T0)
    s.build()
    r = s.q("select name, n_units, top_constraintid, description from area")[0]
    assert r[0] == "Mount Emerald \u2014 system strength"      # sites de-duplicated; U3 is not behind
    assert r[1] == 2 and r[2] == "N_SS" and r[3] == "Out= Nil, something about a farm"


def test_area_name_falls_back_to_element_when_not_unit_specific_majority_or_too_many_units():
    s = Scn()
    # same units: one unit-specific constraint binds once, a thermal one three times -> element name
    s.constraint("N_US", limittype="System Strength", descr="Out= Nil, Not used", terms={"CP_U1": 1.0})
    s.constraint("N_TH", descr="Out= X, avoid O/L of Element Delta on trip", terms={"CP_U1": 1.0})
    s.unit("U1", site="Site One")
    s.bind("N_US", T0)
    for i in range(3):
        s.bind("N_TH", T0 + i * M)
    # 4 units behind a unit-specific constraint -> element name, not sites
    s.constraint("N_FOUR", limittype="Discretionary", descr="avoid O/L of Element Echo on trip",
                 terms={f"CP_V{i}": 1.0 for i in range(4)})
    for i in range(4):
        s.unit(f"V{i}")
    s.bind("N_FOUR", T0)
    s.build()
    names = {r[0]: r[1] for r in s.q("select top_constraintid, name from area")}
    assert names["N_TH"] == "Element Delta"
    assert names["N_FOUR"] == "Element Echo"


# ------------------------------------------------------------------ distinct intervals

def test_area_n_intervals_is_distinct_when_two_constraints_bind_together():
    s = Scn()
    s.constraint("N_A", terms={"CP_U1": 0.5, "CP_U2": 0.5}, descr="Out= Nil")
    s.constraint("N_B", terms={"CP_U1": 0.5, "CP_U2": 0.5}, descr="Out= Foo")
    s.constraint("N_S", terms={"CP_U1": 0.5, "CP_U2": 0.5}, descr="x", limittype="Discretionary")
    s.unit("U1")
    s.unit("U2")
    for c in ["N_A", "N_B", "N_S"]:
        s.bind(c, T0)
    s.bind("N_A", T0 + M)
    s.build()
    assert len(s.q("select distinct area_id from constraint_meta")) == 1
    r = s.q("select n_intervals, n_sysnormal, n_outage, n_unit_specific from area_daily")
    assert r == [(2, 2, 1, 1)]          # N_A in both intervals (system normal), N_B and N_S in the first
    # cause_daily counts constraint-intervals: 4 in total
    assert s.q("select sum(n_constraint_intervals) from cause_daily")[0][0] == 4
    assert s.q("select n_discharge_limited from unit_daily where duid='U1'")[0][0] == 2


# ------------------------------------------------------------------ curtailment

def test_curtailment_assigned_to_dominant_area_other_and_negative_price():
    s = Scn()
    # two areas (disjoint unit sets apart from U): X holds U with factor .5, Y with factor .5
    s.constraint("N_X", terms={"CP_U": 0.5, "CP_P": 0.5}, descr="avoid O/L of ElemX on trip")
    s.constraint("N_Y", terms={"CP_U": 0.5, "CP_Q": 0.5}, descr="avoid O/L of ElemY on trip")
    s.unit("U", region="QLD1")
    s.unit("P")
    s.unit("Q")
    # t1: both bind, Y dominant (200 > 100); curtailment 12 MW -> 1 MWh at $50
    s.bind("N_X", T0, mv=100)
    s.bind("N_Y", T0, mv=200)
    # t2: nothing binds; curtailment 24 MW -> 2 MWh other
    # t3: X binds, curtailment 12 MW at negative price -> 1 MWh, value 0
    s.bind("N_X", T0 + 2 * M, mv=100)
    # t4: X binds but zero curtailment -> counted limited, no MWh
    s.bind("N_X", T0 + 3 * M, mv=100)
    for t, mw in [(T0, 12), (T0 + M, 24), (T0 + 2 * M, 12), (T0 + 3 * M, 0)]:
        s.ins("am.curtailment_duid5", settlementdate=t, duid="U", uigf=1.0, totalcleared=0.0, curtailment=mw)
    for t, p in [(T0, 50.0), (T0 + M, 50.0), (T0 + 2 * M, -100.0), (T0 + 3 * M, 50.0)]:
        s.ins("am.prices5", settlementdate=t, regionid="QLD1", rrp=p)
        s.ins("am.prices5", settlementdate=t, regionid="NSW1", rrp=999.0)
    s.build()
    a = s.q("select area_id from constraint_meta where constraintid='N_Y'")[0][0]
    x = s.q("select area_id from constraint_meta where constraintid='N_X'")[0][0]
    assert a != x
    day = {r[0]: r[1:] for r in s.q("select area_id, constrained_mwh, constrained_value from area_daily")}
    assert day[a] == (pytest.approx(1.0), pytest.approx(50.0))     # t1 only
    assert day[x] == (pytest.approx(1.0), pytest.approx(0.0))      # t3, negative price valued at 0
    u = s.q("select n_discharge_limited, constrained_mwh, other_curtailed_mwh, constrained_value "
            "from unit_daily where duid='U'")[0]
    assert u[0] == 3
    assert u[1] == pytest.approx(2.0) and u[2] == pytest.approx(2.0) and u[3] == pytest.approx(50.0)


def test_curtailment_with_only_charge_direction_is_other():
    s = Scn()
    s.constraint("N_X", ctype=">=", terms={"CP_U": 0.5})     # >= with positive factor: charge
    s.unit("U")
    s.bind("N_X", T0)
    s.ins("am.curtailment_duid5", settlementdate=T0, duid="U", uigf=1.0, totalcleared=0.0, curtailment=12.0)
    s.build()
    u = s.q("select n_charge_limited, n_discharge_limited, constrained_mwh, other_curtailed_mwh from unit_daily")[0]
    assert u == (1, 0, 0, pytest.approx(1.0))


def test_local_price_columns():
    s = Scn()
    s.constraint("N_X", terms={"CP_U": 0.5})
    s.unit("U")
    s.bind("N_X", T0)
    for i, adj in enumerate([-10.0, -30.0, -20.0, 5.0, 0.0]):
        s.ins("cn.local_price5", settlementdate=T0 + i * M, duid="U", local_price_adjustment=adj, locally_constrained=1)
    s.build()
    r = s.q("select n_local_price, median_local_price_adj from unit_daily where duid='U'")[0]
    assert r == (3, -20.0)


# ------------------------------------------------------------------ outages

def test_outage_area_link():
    s = Scn()
    s.constraint("N_A", terms={"CP_U": 0.5}, descr="Out= L1")
    s.constraint("N_Z", terms={"CP_U": 0.5}, descr="Out= L2")
    s.unit("U")
    for i in range(3):
        s.bind("N_A", T0 + i * M)                 # 3 intervals inside the window below (2 within it)
    s.bind("N_A", T0 + 10 * M)                    # outside window
    # genconset S1: version 1 (Jan) has N_A, version 2 (Dec, after the window) has N_Z only
    s.ins("cn.genconset", genconsetid="S1", effectivedate=V1, versionno=1, genconid="N_A")
    s.ins("cn.genconset", genconsetid="S1", effectivedate=datetime(2026, 12, 1), versionno=1, genconid="N_Z")
    s.ins("cn.genconset", genconsetid="S2", effectivedate=V1, versionno=1, genconid="N_Z")
    s.ins("cn.network_outageconstraintset", outageid=1, genconsetid="S1", startinterval=T0 + M, endinterval=T0 + 5 * M)
    s.ins("cn.network_outageconstraintset", outageid=2, genconsetid="S2", startinterval=T0, endinterval=T0 + 5 * M)
    s.ins("cn.network_outageconstraintset", outageid=3, genconsetid="S1", startinterval=T0 + 9 * M,
          endinterval=T0 + 11 * M)
    for oid, eq in [(1, "L1"), (1, "L1B"), (2, "L2"), (3, "L1")]:
        s.ins("cn.network_outagedetail", outageid=oid, substationid="SUB", equipmenttype="LINE", equipmentid=eq,
              starttime=T0, endtime=T0 + 99 * M, outagestatuscode="A", reason="maintenance")
    s.build()
    rows = s.q("select outageid, equipmentid, n_binding_intervals, area_id from outage_area order by 1, 2")
    # outage 1: two equipment rows both kept, 2 intervals in window; outage 2: N_Z never binds -> absent;
    # outage 3: one interval in window
    assert [(r[0], r[1], r[2]) for r in rows] == [(1, "L1", 2), (1, "L1B", 2), (3, "L1", 1)]
    assert rows[0][3] == s.q("select area_id from constraint_meta where constraintid='N_A'")[0][0]


# ------------------------------------------------------------------ meta, output, atomic write

def test_meta_row_counts():
    s = Scn()
    s.constraint("N_A", terms={"CP_U": 0.5})
    s.constraint("F_X")
    s.unit("U")
    s.bind("N_A", T0)
    s.bind("F_X", T0)
    s.build()
    m = s.q("select data_from, data_through, n_binding_rows, build_seconds >= 0 from meta")[0]
    assert m[0] == T0 and m[1] == T0 and m[2] == 2 and m[3]


def test_write_output_is_atomic_and_complete(tmp_path):
    s = _version_scn().build()
    out = tmp_path / "network_summary.duckdb"
    seen = {}
    real = os.rename

    def spy(a, b):
        seen["src"], seen["dst"], seen["src_existed"] = a, b, os.path.exists(a)
        assert not os.path.exists(b)
        return real(a, b)

    os.rename = spy
    try:
        ns.write_output(s.con, out)
    finally:
        os.rename = real
    assert seen["src"] == str(out) + ".tmp" and seen["dst"] == str(out) and seen["src_existed"]
    assert not os.path.exists(str(out) + ".tmp")
    c = duckdb.connect(str(out), read_only=True)
    names = {r[0] for r in c.execute("show tables").fetchall()}
    assert names == set(ns.OUTPUT_TABLES)
    assert c.execute("select count(*) from constraint_meta").fetchone()[0] == 2


def test_failed_build_leaves_old_file_untouched(tmp_path):
    out = tmp_path / "network_summary.duckdb"
    c = duckdb.connect(str(out))
    c.execute("create table keep (x int)")
    c.execute("insert into keep values (42)")
    c.close()
    before = out.read_bytes()
    con = duckdb.connect()          # no cn / am schemas: build must fail
    with pytest.raises(Exception):
        ns.run(con, out)
    assert out.read_bytes() == before
    assert not os.path.exists(str(out) + ".tmp")
    # a failure during the write step also leaves it intact
    s = _version_scn().build()
    s.con.execute("drop table area_units")
    with pytest.raises(Exception):
        ns.write_output(s.con, out)
    assert out.read_bytes() == before
    assert not os.path.exists(str(out) + ".tmp")


# ------------------------------------------------------------------ overrides, regions, site names, interconnectors

def test_region_prefix_rules():
    for cid, r in [("NSA_S_PPT_42", "SA"), ("C_V_X", "VIC"), ("C_N_X", "NSW"), ("C_Q_X", "QLD"), ("C_S_X", "SA"),
                   ("C_T_X", "TAS"), ("SVML_ZERO", "SA"), ("VS_X", "VIC"), ("V_S_HEYWOOD_UFLS", "VIC"),
                   ("N_X", "NSW"), ("L_PEC", "NSW"), ("Q_X", "QLD"), ("V_X", "VIC"), ("S_X", "SA"), ("T_X", "TAS"),
                   ("Z_X", "Other")]:
        assert ns.region_of(cid) == r, cid


def test_region_from_units_behind_beats_prefix_and_override_beats_units(tmp_path):
    s = Scn()
    s.constraint("N_A", terms={"CP_U1": 1.0, "CP_U2": 1.0, "CP_U3": 0.01}, descr="avoid O/L of Element Alpha on trip")
    s.constraint("N_B", terms={"CP_W": 1.0}, descr="avoid O/L of Element Bravo on trip")
    s.unit("U1", region="QLD1")
    s.unit("U2", region="QLD1")
    s.unit("U3", region="SA1")                  # not behind (relative weight .01)
    s.unit("W", region="VIC1")
    s.bind("N_A", T0)
    s.bind("N_B", T0)
    csv = tmp_path / "names.csv"
    csv.write_text("area_id,name,region\nN_B,Bravo override \u2013 x,TAS\nGONE,Old name,SA\n", encoding="utf-8")
    ns.build(s.con, area_names=csv)
    assert s.q("select region from area where area_id='N_A'") == [("QLD",)]          # modal, not the N prefix
    assert s.q("select region from constraint_meta where constraintid='N_A'") == [("QLD",)]
    assert s.q("select region from cause_daily where limittype='Thermal' and region='QLD'")
    assert s.q("select name, name_source, region from area where area_id='N_B'") == [
        ("Bravo override \u2013 x", "override", "TAS")]
    assert s.q("select region from constraint_meta where constraintid='N_B'") == [("TAS",)]
    assert ns.stale_overrides(s.con) == ["GONE"]
    assert s.q("select name_source from area where area_id='N_A'") == [("element",)]


def test_missing_override_file_is_not_an_error(tmp_path):
    s = Scn()
    s.constraint("N_A", terms={"CP_U": 1.0})
    s.unit("U")
    s.bind("N_A", T0)
    ns.build(s.con, area_names=tmp_path / "nope.csv")
    assert ns.stale_overrides(s.con) == []
    assert s.q("select region from area") == [("NSW",)]


def test_name_source_values():
    s = Scn()
    s.constraint("N_E", descr="avoid O/L of Element Foxtrot on trip")                   # element
    s.constraint("N_D", descr="Out= Nil, something quite different here")                # description
    s.constraint("N_I", versions=[(V1, 1)])
    s.bind("N_E", T0)
    s.bind("N_D", T0)
    s.bind("N_NOPE", T0)                                                                 # id
    s.build()
    got = dict(s.q("select top_constraintid, name_source from area"))
    assert got == {"N_E": "element", "N_D": "description", "N_NOPE": "id"}


def test_tidy_and_station_names():
    assert ns.element_name("avoid voltage collapse at Foo Place", "C") == "Voltage collapse at Foo Place"
    assert ns.element_name("on Dundonnell WF (1 + 2 + 3)", "C") == "Dundonnell WF (1 + 2 + 3)"
    assert ns.element_name("limit x flow to y", "C") == "Limit x flow to y"
    assert ns.station_name("Kennedy Energy Park - Phase 1 - Solar") == "Kennedy Energy Park"
    assert ns.station_name("Kennedy Energy Park - Phase 1 - Wind") == "Kennedy Energy Park"
    assert ns.station_name("Murra Warra Wind Farm - stage 1") == "Murra Warra Wind Farm"
    assert ns.station_name("Murra Warra Wind Farm - Stage 2") == "Murra Warra Wind Farm"
    assert ns.station_name("Haughton Solar Farm Stage 1") == "Haughton Solar Farm"
    assert ns.station_name("Foo Battery - BESS") == "Foo Battery"
    assert ns.station_name("Bar Wind Farm Unit 1") == "Bar Wind Farm"
    assert ns.station_name("Baz Solar 2") == "Baz Solar"
    assert ns.station_name("Plain Name") == "Plain Name"
    assert ns.limit_label("Islanding - Unit") == "islanding risk"
    assert ns.limit_label("Discretionary") == "discretionary cap"
    assert ns.limit_label("System Strength") == "system strength"


def test_site_name_collapses_stations_and_suffix_is_dominant_unit_specific_limit():
    s = Scn()
    # two unit-specific constraints on the same units; the Islanding one binds more often
    s.constraint("N_ISL", limittype="Islanding - Unit", descr="x", terms={"CP_U1": 1.0, "CP_U2": 1.0})
    s.constraint("N_SS", limittype="System Strength", descr="x", terms={"CP_U1": 1.0, "CP_U2": 1.0})
    s.unit("U1", site="Kennedy Energy Park - Phase 1 - Solar")
    s.unit("U2", site="Kennedy Energy Park - Phase 1 - Wind")
    for i in range(3):
        s.bind("N_ISL", T0 + i * M)
    s.bind("N_SS", T0)
    s.build()
    assert s.q("select name, name_source from area") == [("Kennedy Energy Park \u2014 islanding risk", "site")]


def test_interconnectors_column():
    s = Scn()
    s.constraint("N_IC", descr="Out= Nil, interconnector only limit")
    s.ins("cn.spd_interconnector", genconid="N_IC", effectivedate=V1, versionno=1, interconnectorid="V-SA", factor=1.0)
    s.ins("cn.spd_interconnector", genconid="N_IC", effectivedate=V1, versionno=1, interconnectorid="VIC1-NSW1", factor=-1.0)
    s.ins("cn.spd_interconnector", genconid="N_IC", effectivedate=V1, versionno=1, interconnectorid="T-V-MNSP1", factor=0.0)
    s.constraint("N_U", terms={"CP_U": 1.0})
    s.unit("U")
    s.bind("N_IC", T0)
    s.bind("N_U", T0)
    s.build()
    got = dict(s.q("select area_id, interconnectors from area"))
    assert got == {"N_IC": "V-SA,VIC1-NSW1", "N_U": ""}


# ------------------------------------------------------------------ five causes

def test_area_daily_splits_plant_causes_and_keeps_n_unit_specific():
    s = Scn()
    terms = {"CP_U1": 0.5, "CP_U2": 0.5}
    s.constraint("N_A", terms=terms, descr="Out= Nil")                                   # system normal
    s.constraint("N_SS", terms=terms, descr="x", limittype="System Strength")           # plant stability
    s.constraint("N_ISL", terms=terms, descr="x", limittype="Islanding - Unit")         # plant stability
    s.constraint("N_DC", terms=terms, descr="x", limittype="Discretionary")             # other cap
    s.constraint("C_N_COM", terms=terms, descr="x", limittype="Unit Zero")              # commissioning
    s.unit("U1")
    s.unit("U2")
    s.bind("N_A", T0)
    s.bind("N_SS", T0)
    s.bind("N_ISL", T0)                       # same interval as N_SS: counts once for plant stability
    s.bind("N_DC", T0 + M)
    s.bind("C_N_COM", T0 + M)
    s.bind("C_N_COM", T0 + 2 * M)
    s.build()
    assert len(s.q("select distinct area_id from constraint_meta")) == 1
    r = s.q("select n_intervals, n_sysnormal, n_plant_stability, n_commissioning, n_other_cap, n_unit_specific "
            "from area_daily")
    assert r == [(3, 1, 1, 2, 1, 3)]          # unit-specific = distinct intervals with any of the three
    assert {x[0]: x[1] for x in s.q("select cause, sum(n_constraint_intervals) from cause_daily group by 1")} == {
        "System normal": 1, "Plant stability limit": 2, "Commissioning": 2, "Other plant cap": 1}


def test_site_name_suffix_commissioning_for_commissioning_majority_area():
    s = Scn()
    s.constraint("C_V_LANC_ZERO", limittype="Unit Zero", descr="x", terms={"CP_U1": 1.0})
    s.constraint("N_SS", limittype="System Strength", descr="x", terms={"CP_U1": 1.0})
    s.unit("U1", site="Lancaster Solar Farm")
    for i in range(3):
        s.bind("C_V_LANC_ZERO", T0 + i * M)
    s.bind("N_SS", T0)
    s.build()
    assert s.q("select name, name_source from area") == [("Lancaster Solar Farm — commissioning", "site")]


def test_site_name_uses_plant_limit_when_commissioning_is_a_minority():
    s = Scn()
    s.constraint("C_V_X_ZERO", limittype="Unit Zero", descr="x", terms={"CP_U1": 1.0})
    s.constraint("N_SS", limittype="System Strength", descr="x", terms={"CP_U1": 1.0})
    s.unit("U1", site="Foo Wind Farm")
    s.bind("C_V_X_ZERO", T0)
    for i in range(3):
        s.bind("N_SS", T0 + i * M)
    s.build()
    assert s.q("select name from area") == [("Foo Wind Farm — system strength",)]
