#!/usr/bin/env python3
"""--auto mode of scripts/backfill_constraints.py: month selection and plan, injected listing."""
import sys
from datetime import date
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from test_constraints_backfill import (GCD_COLS, LP_COLS, World, add_dc, arch, bf, db_of, dvd, q)  # noqa: E402,F401
from test_constraints_store import dc_row  # noqa: E402

TODAY = date(2026, 11, 24)


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def run_auto(w, tmp_path, *extra, today=TODAY):
    return w.run(["--auto", "--db", str(db_of(tmp_path)), "--pause", "0", *extra], today=today)


def seed_months(w, months):
    for ym in months:
        k = ym.replace("-", "")
        add_dc(w, ym, arch("DISPATCHCONSTRAINT", 1, k), [dc_row(f"{ym.replace('-', '/')}/01 00:05:00", "A" + ym[-2:], 5)])
        w.add(ym, arch("GENCONDATA", 1, k), "GENCONDATA", GCD_COLS,
              [[f"{ym.replace('-', '/')}/01 00:00:00", 1, "C" + ym[-2:], "LE", "d", "Thermal"]], group="GENERIC_CONSTRAINT")
        w.add(ym, arch("NETWORK_OUTAGEDETAIL", 1, k), "NETWORK_OUTAGEDETAIL",
              ["OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "STARTTIME"],
              [[int(ym[-2:]), "S", "LINE", "L", "2026/10/01 00:00:00"]], group="NETWORK")


def test_auto_picks_last_month_and_plans(w, tmp_path):
    seed_months(w, ["2026-08", "2026-09", "2026-10"])
    assert run_auto(w, tmp_path, "--tables", "DISPATCHCONSTRAINT,GENCONDATA,NETWORK_OUTAGEDETAIL") == 0
    assert any("end month 2026-10" in l and "definitions from 2026-08" in l for l in w.logs)
    assert q(tmp_path, "select constraintid from constraint_binding5") == [("A10",)]       # end month only
    assert q(tmp_path, "select genconid from gencondata order by 1") == [("C08",), ("C09",), ("C10",)]
    assert q(tmp_path, "select outageid from network_outagedetail") == [(10,)]             # snapshots implied


def test_auto_falls_back_to_month_before(w, tmp_path):
    seed_months(w, ["2026-09"])
    w.unavailable = {"2026-10"}
    assert run_auto(w, tmp_path, "--tables", "DISPATCHCONSTRAINT") == 0
    assert any("end month 2026-09" in l for l in w.logs)
    assert q(tmp_path, "select constraintid from constraint_binding5") == [("A09",)]


def test_auto_nothing_published_exits_nonzero(w, tmp_path):
    w.unavailable = {"2026-10", "2026-09"}
    assert run_auto(w, tmp_path) == 1


def test_auto_year_boundary(w, tmp_path):
    seed_months(w, ["2025-12"])
    assert run_auto(w, tmp_path, "--tables", "DISPATCHCONSTRAINT", today=date(2026, 1, 15)) == 0
    assert any("end month 2025-12" in l for l in w.logs)


def test_auto_reports_reconciliation_and_skips_loaded(w, tmp_path):
    seed_months(w, ["2026-10"])
    assert run_auto(w, tmp_path, "--tables", "DISPATCHCONSTRAINT") == 0
    assert any("RECONCILE" in l for l in w.logs)              # empty DB: archive supplied everything
    n = len(w.downloads)
    w.logs.clear()
    assert run_auto(w, tmp_path, "--tables", "DISPATCHCONSTRAINT") == 0
    assert len(w.downloads) == n and any("already loaded, skipped" in l for l in w.logs)
    assert not any("RECONCILE" in l for l in w.logs)


def test_auto_logs_already_present_when_live_data_exists(w, tmp_path):
    seed_months(w, ["2026-10"])
    first = w.run(["--start", "2026-10", "--end", "2026-10", "--tables", "DISPATCHCONSTRAINT", "--pause", "0",
                   "--db", str(db_of(tmp_path))])
    assert first == 0
    c = duckdb.connect(str(db_of(tmp_path)))
    c.execute("delete from _loaded")                         # as if the month had only been live-collected
    c.close()
    w.logs.clear()
    assert run_auto(w, tmp_path, "--tables", "DISPATCHCONSTRAINT") == 0
    assert any("inserted 0, already present 1" in l for l in w.logs)
    assert not any("RECONCILE" in l for l in w.logs)


def test_auto_rejects_manual_months(w, tmp_path):
    with pytest.raises(SystemExit):
        w.run(["--auto", "--end", "2026-10", "--db", str(db_of(tmp_path))])
