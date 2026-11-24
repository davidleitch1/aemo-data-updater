#!/usr/bin/env python3
"""generic_constraint_rhs / emsmaster: store filter and snapshot semantics, and the --fill-rhs
mode of scripts/backfill_constraints.py. Synthetic AEMO-format zips, injected network."""
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_constraints_backfill import World, add_dc, arch, db_of, q  # noqa: E402,F401
from test_constraints_store import aemo_csv, dc_row  # noqa: E402

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
from aemo_updater import constraints_store as cs  # noqa: E402

RHS_COLS = ["GENCONID", "EFFECTIVEDATE", "VERSIONNO", "SCOPE", "TERMID", "GROUPID", "SPD_ID", "SPD_TYPE", "FACTOR",
            "OPERATION", "DEFAULTVALUE", "PARAMETERTERM1", "PARAMETERTERM2", "PARAMETERTERM3", "LASTCHANGED"]
EMS_COLS = ["SPD_ID", "SPD_TYPE", "DESCRIPTION", "GROUPING_ID", "LASTCHANGED"]
TODAY = date(2026, 11, 24)
L = "2024/09/01 00:00:00"


def rhs(cid, eff, ver, termid, spd="Limit", ty="C", f=1.0, scope="DS", op=None):
    return [cid, eff, ver, scope, termid, "", spd, ty, f, op, 0, "", "", "", L]


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def test_store_filters_scope_and_binding_constraints(tmp_path):
    con = duckdb.connect(str(tmp_path / "c.duckdb"))
    cs.ensure_tables(con)
    p = aemo_csv("CONSTRAINT", ["SETTLEMENTDATE", "RUNNO", "CONSTRAINTID", "DISPATCHINTERVAL", "INTERVENTION", "RHS",
                                "MARGINALVALUE", "VIOLATIONDEGREE", "LASTCHANGED", "DUID", "GENCONID_EFFECTIVEDATE",
                                "GENCONID_VERSIONNO", "LHS"],
                 [dc_row("2024/10/01 00:05:00", "A", 5)], tmp_path / "d.csv")
    cs.load_month_table(con, "DISPATCHCONSTRAINT", "2024-10", [(1, p)])
    rows = [rhs("A", L, 1, 1), rhs("A", L, 1, 2, "X", "T", 0.5), rhs("A", L, 1, 3, scope="PD"),
            rhs("B", L, 1, 1)]                       # B never bound; PD scope is not dispatch
    p = aemo_csv("GENERICCONSTRAINTRHS", RHS_COLS, rows, tmp_path / "r.csv", group="GCRHS")
    r = cs.load_month_table(con, "GENERICCONSTRAINTRHS", "2024-09", [(1, p)], record=False)
    assert r == [("generic_constraint_rhs", 2, 0)]
    assert con.execute("select count(*) from _loaded where tbl='GENERICCONSTRAINTRHS'").fetchone()[0] == 0
    assert cs.load_month_table(con, "GENERICCONSTRAINTRHS", "2024-09", [(1, p)], record=False) == [
        ("generic_constraint_rhs", 0, 2)]
    assert con.execute("select spd_id, factor, defaultvalue from generic_constraint_rhs order by termid").fetchall()[1] \
        == ("X", 0.5, 0.0)


def test_emsmaster_is_replaced_whole(tmp_path):
    con = duckdb.connect(str(tmp_path / "c.duckdb"))
    cs.ensure_tables(con)
    p = aemo_csv("EMSMASTER", EMS_COLS, [["A", "A", "old a", "g", L], ["B", "A", "old b", "g", L]], tmp_path / "e.csv",
                 group="GENERIC_CONSTRAINT")
    cs.load_month_table(con, "EMSMASTER", "2026-09", [(1, p)], record=False)
    p = aemo_csv("EMSMASTER", EMS_COLS, [["A", "A", "new a", "g", L]], tmp_path / "e2.csv", group="GENERIC_CONSTRAINT")
    cs.load_month_table(con, "EMSMASTER", "2026-10", [(1, p)], record=False)
    assert con.execute("select spd_id, description from emsmaster").fetchall() == [("A", "new a")]


def seed(w, tmp_path):
    """Binding: A (2024-09 v1), D (2024-10 v1), OLD (2014-03 v1), NEW (2026-11 v1), F (2024-09 v1, no rhs ever)."""
    rows = [dc_row("2024/10/01 00:05:00", "A", 5, eff="2024/09/01 00:00:00", ver=1),
            dc_row("2024/10/01 00:05:00", "D", 5, eff="2024/10/01 00:00:00", ver=1),
            dc_row("2024/10/01 00:05:00", "OLD", 5, eff="2014/03/01 00:00:00", ver=1),
            dc_row("2024/10/01 00:05:00", "NEW", 5, eff="2026/11/01 00:00:00", ver=1),
            dc_row("2024/10/01 00:05:00", "F", 5, eff="2024/09/01 00:00:00", ver=1)]
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT", 1, "202410"), rows)
    w.add("2024-09", arch("GENERICCONSTRAINTRHS", 1, "202409"), "GENERICCONSTRAINTRHS", RHS_COLS,
          [rhs("A", "2024/09/01 00:00:00", 1, 1), rhs("A", "2024/09/01 00:00:00", 1, 2, "T1.CP", "T", 0.3),
           rhs("A", "2024/09/01 00:00:00", 1, 3, scope="PD"), rhs("E", "2024/09/01 00:00:00", 1, 1)], group="GCRHS")
    w.add("2024-10", arch("GENERICCONSTRAINTRHS", 1, "202410"), "GENERICCONSTRAINTRHS", RHS_COLS,
          [rhs("D", "2024/10/01 00:00:00", 1, 1)], group="GCRHS")
    w.add("2026-10", arch("EMSMASTER", 1, "202610"), "EMSMASTER", EMS_COLS, [["Limit", "C", "a limit", "g", L]],
          group="GENERIC_CONSTRAINT")
    w.unavailable = {"2026-11"}
    # real run order: binding rows exist before the fill; load them with the interval tables
    assert w.run(["--start", "2024-10", "--end", "2024-10", "--tables", "DISPATCHCONSTRAINT",
                  "--db", str(db_of(tmp_path)), "--pause", "0"]) == 0


def fill(w, tmp_path, today=TODAY):
    return w.run(["--fill-rhs", "--db", str(db_of(tmp_path)), "--pause", "0"], today=today)


def test_fill_rhs_fetches_missing_months_only(w, tmp_path):
    seed(w, tmp_path)
    w.downloads.clear()
    assert fill(w, tmp_path) == 0
    assert sorted(w.downloads) == sorted([arch("GENERICCONSTRAINTRHS", 1, "202409"),
                                          arch("GENERICCONSTRAINTRHS", 1, "202410"), arch("EMSMASTER", 1, "202610")])
    got = q(tmp_path, "select genconid, termid, spd_id from generic_constraint_rhs order by 1, 2")
    assert got == [("A", 1, "Limit"), ("A", 2, "T1.CP"), ("D", 1, "Limit")]      # E not bound, PD scope dropped
    assert q(tmp_path, "select description from emsmaster") == [("a limit",)]
    text = "\n".join(w.logs)
    assert "rhs 2014-03" in text and "before 2015" in text
    assert "rhs 2026-11" in text and "not yet in the archive" in text
    assert q(tmp_path, "select count(*) from _loaded where tbl='GENERICCONSTRAINTRHS'") == [(0,)]
    assert "2 month(s) fetched, 3 row(s) inserted, 3 version(s) still without rows" in text   # F, OLD, NEW


def test_fill_rhs_second_run_refetches_only_months_with_missing_versions(w, tmp_path):
    seed(w, tmp_path)
    fill(w, tmp_path)
    w.downloads.clear()
    assert fill(w, tmp_path) == 0
    # F (2024-09) still lacks rows, so that month is tried again; 2024-10 is complete
    assert arch("GENERICCONSTRAINTRHS", 1, "202409") in w.downloads
    assert arch("GENERICCONSTRAINTRHS", 1, "202410") not in w.downloads
    assert q(tmp_path, "select count(*) from generic_constraint_rhs") == [(3,)]      # anti-join, no duplicates


def test_fill_rhs_picks_up_a_constraint_that_starts_binding_later(w, tmp_path):
    seed(w, tmp_path)
    fill(w, tmp_path)
    c = duckdb.connect(str(db_of(tmp_path)))
    c.execute("insert into constraint_binding5 values ('2024-11-01 00:05:00','E',0,1,2,0,1,'2024-09-01 00:00:00',1)")
    c.close()
    fill(w, tmp_path)
    assert q(tmp_path, "select count(*) from generic_constraint_rhs where genconid='E'") == [(1,)]


def test_auto_runs_the_fill_at_its_end(w, tmp_path):
    seed(w, tmp_path)
    w.downloads.clear()
    assert w.run(["--auto", "--tables", "DISPATCHCONSTRAINT", "--db", str(db_of(tmp_path)), "--pause", "0"],
                 today=TODAY) == 0
    assert any("rhs fill:" in l for l in w.logs)
    assert q(tmp_path, "select count(*) from generic_constraint_rhs") == [(3,)]
