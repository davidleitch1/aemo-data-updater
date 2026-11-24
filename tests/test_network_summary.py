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

    def unit(self, duid, cp=None, cap=100.0, region="NSW1", fuel="Wind", start=datetime(2020, 1, 1), end=None):
        cp = cp or "CP_" + duid
        self.ins("cn.dudetailsummary", duid=duid, start_date=start, end_date=end, connectionpointid=cp,
                 regionid=region, dispatchtype="GENERATOR")
        if not self.con.execute("select 1 from am.duid_mapping where duid=?", [duid]).fetchone():
            self.ins("am.duid_mapping", region=region, **{"site name": "Site " + duid}, owner="o", duid=duid,
                     capacity_mw=cap, storage_mwh=0.0, fuel=fuel)

    def bind(self, cid, t, mv=100.0, eff=V1, ver=1, interv=0):
        self.times.add(t)
        self.ins("cn.constraint_binding5", settlementdate=t, constraintid=cid, intervention=interv,
                 marginalvalue=mv, genconid_effectivedate=eff, genconid_versionno=ver)

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
    ("N_SS", "Out= Foo", "System Strength", "Unit-specific limit"),
    ("N_D", "x", "Discretionary", "Unit-specific limit"),
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
    assert m["Q_SS"][:2] == ("Unit-specific limit", "QLD")
    assert m["V_NIL"][:2] == ("System normal", "VIC")
    assert m["V_NIL"][2] == "Foo - Bar"
    assert m["T_OUT"][:2] == ("Outage", "TAS")
    assert m["L_X"][1] == "NSW" and m["X_Y"][1] == "Other"


def test_element_name_chain():
    assert ns.element_name("Out= Nil avoid overloading Calliope River to Wurdong 275 kV line on trip of X", "C") \
        == "Calliope River to Wurdong"
    assert ns.element_name("limit Molong-Orange North flow to X", "C") == "Molong-Orange North"
    assert ns.element_name("no pattern here", "CID") == "CID"


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
    s.constraint("N_A", descr="Out= X, avoid O/L of Elem A on trip", terms={"CP_U1": 0.5, "CP_U2": 0.3, "CP_U3": 0.1})
    s.constraint("N_B", descr="Out= Y, avoid O/L of Elem B on trip",
                 terms={"CP_U1": 0.4, "CP_U2": 0.25, "CP_U3": 0.1, "CP_U4": 0.08, "CP_U5": 0.02})
    s.constraint("N_C", descr="Out= Z, avoid O/L of Elem C on trip", terms={"CP_U9": 0.9})
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
    assert areas["N_B"][0] == "Elem B"            # most frequent constraint names the area
    assert areas["N_B"][1] == 2
    assert areas["N_I"][1:] == (1, 0)


def test_units_behind_and_weighted_mw():
    s = _area_scn()
    # N_B discharge units (all '<=', positive factors): U1 max .5 (from N_A), U2 .3, U3 .1 , U4 .08, U5 .02
    behind = s.q("select n_units, weighted_mw, missing_capacity_units from area where area_id='N_B'")[0]
    assert behind[0] == 2                                       # |f| >= 0.2: U1, U2
    assert behind[1] == pytest.approx(100 * 0.5 + 50 * 0.3 + 0 * 0.1 + 10 * 0.08 + 10 * 0.02)
    assert behind[2] == 1                                       # U3 has no capacity
    rows = s.q("select duid, max_factor, direction, site from area_units where area_id='N_B' order by duid")
    assert rows[0] == ("U1", 0.5, "discharge", "Site U1")
    assert len(rows) == 5


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
    assert r == [(2, 1, 1, 1)] or r == [(2, 2, 1, 1)]
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
