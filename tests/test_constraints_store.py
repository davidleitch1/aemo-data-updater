#!/usr/bin/env python3
"""Offline tests for aemo_updater.constraints_store: schema, filters, anti-join
inserts, missing columns, and the local-price views. Synthetic AEMO-format CSVs."""
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from aemo_updater import constraints_store as cs  # noqa: E402

DC_COLS = ["SETTLEMENTDATE", "RUNNO", "CONSTRAINTID", "DISPATCHINTERVAL", "INTERVENTION", "RHS",
           "MARGINALVALUE", "VIOLATIONDEGREE", "LASTCHANGED", "DUID", "GENCONID_EFFECTIVEDATE",
           "GENCONID_VERSIONNO", "LHS"]


def aemo_csv(table, cols, rows, path, group="DISPATCH"):
    """C header, I line, D rows, C footer (all values quoted as AEMO does)."""
    lines = ['C,NEMP.WORLD,' + table + ',AEMO,PUBLIC,2024/10/01,00:00:00,1,' + table + ',1',
             "I," + group + "," + table + ",1," + ",".join(cols)]
    for r in rows:
        lines.append("D," + group + "," + table + ",1," + ",".join(
            "" if v is None else f'"{v}"' for v in r))
    lines.append("C,END OF REPORT,%d" % (len(rows) + 2))
    Path(path).write_text("\n".join(lines) + "\n")
    return Path(path)


def dc_row(ts, cid, mv, vd=0, interv=0, rhs=10, eff="2024/09/01 00:00:00", ver=1, lhs=5):
    return [ts, 1, cid, 1, interv, rhs, mv, vd, ts, "", eff, ver, lhs]


@pytest.fixture
def con(tmp_path):
    c = duckdb.connect(str(tmp_path / "c.duckdb"))
    cs.ensure_tables(c)
    yield c
    c.close()


def test_default_path_env(monkeypatch):
    monkeypatch.setenv("AEMO_CONSTRAINTS_DUCKDB_PATH", "/x/y.duckdb")
    assert cs.default_db_path() == Path("/x/y.duckdb")
    monkeypatch.delenv("AEMO_CONSTRAINTS_DUCKDB_PATH")
    assert cs.default_db_path() == Path.home() / "aemo_production/data/constraints.duckdb"


def test_schema_has_all_tables_and_views(con):
    names = {r[0] for r in con.execute("select table_name from information_schema.tables").fetchall()}
    for t in ["constraint_binding5", "constraint_intervals", "local_price5", "gencondata",
              "spd_connection_point", "spd_interconnector", "spd_region", "genconset",
              "genconsetinvoke", "network_outagedetail", "network_outageconstraintset",
              "dudetailsummary", "_loaded", "unit_binding", "local_price_computed"]:
        assert t in names, t
    cs.ensure_tables(con)  # idempotent


def test_binding_filter_and_ledger(con, tmp_path):
    ts1, ts2 = "2024/10/01 00:05:00", "2024/10/01 00:10:00"
    rows = [dc_row(ts1, "A", 100), dc_row(ts1, "B", 0), dc_row(ts1, "C", 0, vd=3),
            dc_row(ts1, "D", 50, interv=1),     # intervention=1: kept if binding, not in ledger
            dc_row(ts2, "A", 0), dc_row(ts2, "B", 0)]
    p = aemo_csv("CONSTRAINT", DC_COLS, rows, tmp_path / "d.csv")
    res = cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p)])
    assert dict((t, i) for t, i, _ in res) == {"constraint_binding5": 3, "constraint_intervals": 2}
    got = con.execute("select constraintid, intervention from constraint_binding5 order by 1").fetchall()
    assert got == [("A", 0), ("C", 0), ("D", 1)]
    led = con.execute("select settlementdate, n_listed, n_binding from constraint_intervals order by 1").fetchall()
    assert led == [(datetime(2024, 10, 1, 0, 5), 3, 2), (datetime(2024, 10, 1, 0, 10), 2, 0)]
    # all columns carried through with the right types
    r = con.execute("select rhs, marginalvalue, lhs, genconid_effectivedate, genconid_versionno "
                    "from constraint_binding5 where constraintid='A'").fetchone()
    assert r == (10.0, 100.0, 5.0, datetime(2024, 9, 1), 1)


def test_idempotent_rerun(con, tmp_path):
    p = aemo_csv("CONSTRAINT", DC_COLS, [dc_row("2024/10/01 00:05:00", "A", 100)], tmp_path / "d.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p)])
    n1 = con.execute("select count(*) from constraint_binding5").fetchone()[0]
    res = cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p)])
    assert dict((t, i) for t, i, _ in res) == {"constraint_binding5": 0, "constraint_intervals": 0}
    assert dict((t, k) for t, _, k in res) == {"constraint_binding5": 1, "constraint_intervals": 1}
    assert con.execute("select count(*) from constraint_binding5").fetchone()[0] == n1


def test_loaded_ledger(con, tmp_path):
    assert not cs.is_loaded(con, "DISPATCHCONSTRAINT", "2024-10")
    p = aemo_csv("CONSTRAINT", DC_COLS, [dc_row("2024/10/01 00:05:00", "A", 100)], tmp_path / "d.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p)])
    assert cs.is_loaded(con, "DISPATCHCONSTRAINT", "2024-10")
    assert not cs.is_loaded(con, "DISPATCHCONSTRAINT", "2024-11")
    assert con.execute("select tbl, ym, part, n_rows from _loaded").fetchall() == [
        ("DISPATCHCONSTRAINT", "2024-10", 1, 1)]


def test_existing_rows_never_changed(con, tmp_path):
    ts = "2024/10/01 00:05:00"
    p1 = aemo_csv("CONSTRAINT", DC_COLS, [dc_row(ts, "A", 100)], tmp_path / "d1.csv")
    p2 = aemo_csv("CONSTRAINT", DC_COLS, [dc_row(ts, "A", 999), dc_row(ts, "B", 7)], tmp_path / "d2.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p1)])
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-11", [(1, p2)])
    assert con.execute("select marginalvalue from constraint_binding5 where constraintid='A'").fetchall() == [(100.0,)]
    assert con.execute("select count(*) from constraint_binding5").fetchone()[0] == 2


def test_multipart_loaded_together(con, tmp_path):
    ts = "2024/10/01 00:05:00"
    p1 = aemo_csv("CONSTRAINT", DC_COLS, [dc_row(ts, "A", 100), dc_row(ts, "B", 0)], tmp_path / "a.csv")
    p2 = aemo_csv("CONSTRAINT", DC_COLS, [dc_row(ts, "C", 5), dc_row(ts, "E", 0)], tmp_path / "b.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p1), (2, p2)])
    assert con.execute("select count(*) from constraint_binding5").fetchone()[0] == 2
    # ledger counts span both parts
    assert con.execute("select n_listed, n_binding from constraint_intervals").fetchall() == [(4, 2)]
    assert con.execute("select part, n_rows from _loaded order by part").fetchall() == [(1, 2), (2, 2)]


def test_missing_newer_columns_are_null(con, tmp_path):
    cols = ["SETTLEMENTDATE", "CONSTRAINTID", "INTERVENTION", "RHS", "MARGINALVALUE", "VIOLATIONDEGREE"]
    p = aemo_csv("CONSTRAINT", cols, [["2016/07/01 00:05:00", "A", 0, 1, 50, 0]], tmp_path / "d.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2016-07", [(1, p)])
    r = con.execute("select marginalvalue, lhs, genconid_effectivedate, genconid_versionno "
                    "from constraint_binding5").fetchone()
    assert r == (50.0, None, None, None)


def test_local_price(con, tmp_path):
    p = aemo_csv("LOCAL_PRICE", ["SETTLEMENTDATE", "DUID", "LOCAL_PRICE_ADJUSTMENT", "LOCALLY_CONSTRAINED"],
                 [["2024/10/01 00:05:00", "U1", -12.5, 1], ["2024/10/01 00:05:00", "U2", 0, 0]], tmp_path / "l.csv")
    res = cs.load_month_table(con, "DISPATCH_LOCAL_PRICE", "2024-10", [(1, p)])
    assert res == [("local_price5", 2, 0)]
    assert con.execute("select local_price_adjustment, locally_constrained from local_price5 "
                       "where duid='U1'").fetchone() == (-12.5, 1)


def test_definition_tables_key_and_factor(con, tmp_path):
    cols = ["CONNECTIONPOINTID", "EFFECTIVEDATE", "VERSIONNO", "GENCONID", "FACTOR", "LASTCHANGED", "BIDTYPE"]
    e = "2024/09/01 00:00:00"
    p = aemo_csv("SPDCONNECTIONPOINTCONSTRAINT", cols,
                 [["CP1", e, 1, "X", 0.5, e, "ENERGY"], ["CP1", e, 1, "X", 0.25, e, "ENERGY"],
                  ["CP1", e, 1, "X", 0.1, e, "RAISE6SEC"]], tmp_path / "s.csv", group="GENERIC_CONSTRAINT")
    res = cs.load_month_table(con, "SPDCONNECTIONPOINTCONSTRAINT", "2024-09", [(1, p)])
    assert res == [("spd_connection_point", 2, 0)]  # duplicate on key collapsed, bidtype distinguishes


def test_genconsetinvoke_key_with_and_without_invocation_id(con, tmp_path):
    base = ["STARTDATE", "STARTPERIOD", "GENCONSETID", "ENDDATE", "ENDPERIOD", "INTERVENTION",
            "ASCONSTRAINTTYPE", "STARTINTERVALDATETIME", "ENDINTERVALDATETIME", "SYSTEMNORMAL", "LASTCHANGED"]
    d = "2024/10/01 00:00:00"
    row = lambda s: [d, 5, s, d, 9, "0", "", d, d, 1, d]
    p_old = aemo_csv("GENCONSETINVOKE", base, [row("S1"), row("S2")], tmp_path / "o.csv")
    r = cs.load_month_table(con, "GENCONSETINVOKE", "2015-01", [(1, p_old)])
    assert r == [("genconsetinvoke", 2, 0)]
    assert cs.load_month_table(con, "GENCONSETINVOKE", "2015-02", [(1, p_old)]) == [("genconsetinvoke", 0, 2)]
    p_new = aemo_csv("GENCONSETINVOKE", base + ["INVOCATION_ID"],
                     [row("S1") + [11], row("S1") + [12]], tmp_path / "n.csv")
    r = cs.load_month_table(con, "GENCONSETINVOKE", "2024-10", [(1, p_new)])
    assert r == [("genconsetinvoke", 2, 0)]
    assert cs.load_month_table(con, "GENCONSETINVOKE", "2024-11", [(1, p_new)]) == [("genconsetinvoke", 0, 2)]


def test_outage_tables_and_dudetail(con, tmp_path):
    cols = ["OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "STARTTIME", "ENDTIME", "LASTCHANGED",
            "REASON"]
    t = "2024/10/01 00:00:00"
    p = aemo_csv("NETWORK_OUTAGEDETAIL", cols, [[1, "SS", "LINE", "L1", t, t, t, "work"]], tmp_path / "n.csv",
                 group="NETWORK")
    assert cs.load_month_table(con, "NETWORK_OUTAGEDETAIL", "2024-10", [(1, p)]) == [("network_outagedetail", 1, 0)]
    p = aemo_csv("NETWORK_OUTAGECONSTRAINTSET", ["OUTAGEID", "GENCONSETID", "STARTINTERVAL", "ENDINTERVAL"],
                 [[1, "S1", t, t]], tmp_path / "o.csv", group="NETWORK")
    assert cs.load_month_table(con, "NETWORK_OUTAGECONSTRAINTSET", "2024-10", [(1, p)]) == [
        ("network_outageconstraintset", 1, 0)]
    p = aemo_csv("DUDETAILSUMMARY", ["DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID",
                                      "REGIONID", "STATIONID", "PARTICIPANTID"],
                 [["U1", t, "2999/12/31 00:00:00", "GENERATOR", "CP1", "NSW1", "ST", "P"]],
                 tmp_path / "u.csv", group="PARTICIPANT_REGISTRATION")
    assert cs.load_month_table(con, "DUDETAILSUMMARY", "2024-10", [(1, p)]) == [("dudetailsummary", 1, 0)]


def test_local_price_computed_view_with_dated_mapping(con, tmp_path):
    ts = "2024/10/01 00:05:00"
    e1, e2 = "2024/09/01 00:00:00", "2024/11/01 00:00:00"
    # constraints X (mv 100, v1) and Y (mv -20, v1); a non-energy factor and a later version must not count
    p = aemo_csv("CONSTRAINT", DC_COLS, [dc_row(ts, "X", 100, eff=e1, ver=1), dc_row(ts, "Y", -20, eff=e1, ver=1),
                                         dc_row(ts, "Z", 0, eff=e1, ver=1)], tmp_path / "d.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p)])
    cols = ["CONNECTIONPOINTID", "EFFECTIVEDATE", "VERSIONNO", "GENCONID", "FACTOR", "LASTCHANGED", "BIDTYPE"]
    sp = aemo_csv("SPD", cols, [
        ["CP1", e1, 1, "X", 0.5, e1, "ENERGY"], ["CP2", e1, 1, "X", 0.2, e1, "ENERGY"],
        ["CP1", e1, 1, "Y", 1.0, e1, "ENERGY"], ["CP1", e1, 1, "X", 7, e1, "RAISE6SEC"],
        ["CP1", e2, 2, "X", 9.0, e2, "ENERGY"], ["CP1", e1, 1, "Z", 3.0, e1, "ENERGY"]], tmp_path / "s.csv")
    cs.load_month_table(con, "SPDCONNECTIONPOINTCONSTRAINT", "2024-09", [(1, sp)])
    dcols = ["DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID", "REGIONID", "STATIONID"]
    du = aemo_csv("DUDETAILSUMMARY", dcols, [
        ["OLD_A", "2020/01/01 00:00:00", "2024/09/30 00:00:00", "GENERATOR", "CP1", "NSW1", "S"],
        ["NEW_B", "2024/09/30 00:00:00", "2999/12/31 00:00:00", "GENERATOR", "CP1", "NSW1", "S"],
        ["UNIT_C", "2020/01/01 00:00:00", "2999/12/31 00:00:00", "GENERATOR", "CP2", "NSW1", "S"]], tmp_path / "u.csv")
    cs.load_month_table(con, "DUDETAILSUMMARY", "2024-10", [(1, du)])
    got = dict(con.execute("select duid, local_price_computed from local_price_computed").fetchall())
    assert got == {"NEW_B": pytest.approx(100 * 0.5 + -20 * 1.0), "UNIT_C": pytest.approx(100 * 0.2)}
    ub = con.execute("select count(*) from unit_binding").fetchone()[0]
    assert ub == 4  # X: CP1 energy, CP1 raise, CP2 energy; Y: CP1 energy; OLD_A excluded by date; Z not binding
