#!/usr/bin/env python3
"""Offline tests for live constraint collection: store_dispatchis, connect_with_retry and the
snapshot semantics of dudetailsummary / network outage tables. Real DispatchIS fixtures."""
import sys
import zipfile
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).parent))

from aemo_updater import constraints_store as cs  # noqa: E402
from test_constraints_store import aemo_csv  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "nemweb"
DIS = sorted(FIX.glob("PUBLIC_DISPATCHIS_*.zip"))


def dis_bytes(i=0) -> bytes:
    with zipfile.ZipFile(DIS[i]) as z:
        return z.read([n for n in z.namelist() if n.lower().endswith(".csv")][0])


def section_lines(raw: bytes, table: str):
    return [l for l in raw.decode().splitlines()
            if l.startswith(f"I,DISPATCH,{table},") or l.startswith(f"D,DISPATCH,{table},")]


@pytest.fixture
def con(tmp_path):
    c = duckdb.connect(str(tmp_path / "c.duckdb"))
    cs.ensure_tables(c)
    yield c
    c.close()


def counts(con):
    return {t: con.execute(f"select count(*) from {t}").fetchone()[0]
            for t in ("constraint_binding5", "local_price5", "constraint_intervals")}


# ---------------------------------------------------------------- store_dispatchis

def test_store_counts_match_file(con):
    raw = dis_bytes()
    n_con = len(section_lines(raw, "CONSTRAINT")) - 1
    n_lp = len(section_lines(raw, "LOCAL_PRICE")) - 1
    res = cs.store_dispatchis(con, raw)
    c = counts(con)
    assert c["local_price5"] == n_lp and n_lp > 0
    assert c["constraint_intervals"] == 1
    n_listed, n_binding = con.execute("select n_listed, n_binding from constraint_intervals").fetchone()
    assert n_listed <= n_con and n_binding == c["constraint_binding5"] > 0
    assert res["ledger"] == (1, 0) and res["binding"][0] == n_binding and res["local_price"][0] == n_lp


def test_store_is_idempotent(con):
    raw = dis_bytes()
    cs.store_dispatchis(con, raw)
    before = counts(con)
    res = cs.store_dispatchis(con, raw)
    assert counts(con) == before
    assert res["binding"][0] == 0 and res["local_price"][0] == 0 and res["ledger"][0] == 0


def test_no_constraint_rows_raises_and_writes_nothing(con):
    raw = b"\n".join(l.encode() for l in dis_bytes().decode().splitlines()
                     if "CONSTRAINT" not in l or "INTERCONNECTORRES" in l)
    with pytest.raises(ValueError):
        cs.store_dispatchis(con, raw)
    assert counts(con) == {"constraint_binding5": 0, "local_price5": 0, "constraint_intervals": 0}


def test_ledger_not_written_when_insert_fails(con, monkeypatch):
    real = cs.insert_new

    def boom(c, target, *a, **k):
        if target == "local_price5":
            raise RuntimeError("disk full")
        return real(c, target, *a, **k)

    monkeypatch.setattr(cs, "insert_new", boom)
    with pytest.raises(RuntimeError):
        cs.store_dispatchis(con, dis_bytes())
    assert counts(con) == {"constraint_binding5": 0, "local_price5": 0, "constraint_intervals": 0}
    monkeypatch.setattr(cs, "insert_new", real)
    cs.store_dispatchis(con, dis_bytes())            # connection still usable, retry works
    assert counts(con)["constraint_intervals"] == 1


def test_live_rows_identical_to_mmsdm_loader(tmp_path):
    raw = dis_bytes()
    live, mm = duckdb.connect(str(tmp_path / "live.duckdb")), duckdb.connect(str(tmp_path / "mm.duckdb"))
    for c in (live, mm):
        cs.ensure_tables(c)
    cs.store_dispatchis(live, raw)
    for table, src in (("CONSTRAINT", "DISPATCHCONSTRAINT"), ("LOCAL_PRICE", "DISPATCH_LOCAL_PRICE")):
        p = tmp_path / f"{table}.csv"
        p.write_text("C,NEMP.WORLD,x\n" + "\n".join(section_lines(raw, table)) + "\nC,END OF REPORT,9\n")
        cs.load_month_table(mm, src, "2026-08", [(1, p)])
    for t in ("constraint_binding5", "local_price5", "constraint_intervals"):
        order = ", ".join(cs.TABLES[t].cols)
        a = live.execute(f"select * from {t} order by {order}").fetchall()
        b = mm.execute(f"select * from {t} order by {order}").fetchall()
        assert a == b and len(a) > 0, t
        assert live.execute(f"describe {t}").fetchall() == mm.execute(f"describe {t}").fetchall()
        assert [type(x) for x in a[0]] == [type(x) for x in b[0]]
    live.close(); mm.close()


# ---------------------------------------------------------------- connect_with_retry

def test_connect_with_retry_backs_off_then_succeeds(monkeypatch, tmp_path):
    calls, sleeps = [], []

    def fake(path):
        calls.append(path)
        if len(calls) < 3:
            raise duckdb.IOException("Could not set lock on file")
        return "conn"

    monkeypatch.setattr(cs.duckdb, "connect", fake)
    assert cs.connect_with_retry(tmp_path / "x", sleep=sleeps.append) == "conn"
    assert sleeps == [2.0, 4.0]


def test_connect_with_retry_gives_up(monkeypatch, tmp_path):
    def fake(path):
        raise duckdb.IOException("locked")

    monkeypatch.setattr(cs.duckdb, "connect", fake)
    sleeps = []
    with pytest.raises(duckdb.IOException):
        cs.connect_with_retry(tmp_path / "x", attempts=3, sleep=sleeps.append)
    assert len(sleeps) == 2


# ---------------------------------------------------------------- snapshot semantics

T = "2024/10/01 00:00:00"
OD = ["OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "STARTTIME", "ENDTIME", "OUTAGESTATUSCODE"]


def detail(oid, end, status="APPR"):
    return dict(outageid=oid, substationid="SS", equipmenttype="LINE", equipmentid="L1", starttime=T,
                endtime=end, outagestatuscode=status)


def test_upsert_outages_updates_revision_and_keeps_absent(con):
    cs.upsert_outages(con, [detail(1, "2024/10/02 00:00:00"), detail(2, "2024/10/03 00:00:00")],
                      [dict(outageid=1, genconsetid="S1", startinterval=T, endinterval=T),
                       dict(outageid=2, genconsetid="S2", startinterval=T, endinterval=T)])
    res = cs.upsert_outages(con, [detail(1, "2024/10/05 00:00:00", "CANC")],
                            [dict(outageid=1, genconsetid="S9", startinterval=T, endinterval=T)])
    assert con.execute("select outageid, endtime, outagestatuscode from network_outagedetail order by 1").fetchall() \
        == [(1, __import__("datetime").datetime(2024, 10, 5), "CANC"),
            (2, __import__("datetime").datetime(2024, 10, 3), "APPR")]
    assert con.execute("select outageid, genconsetid from network_outageconstraintset order by 1").fetchall() \
        == [(1, "S9"), (2, "S2")]
    assert res["network_outagedetail"] == (0, 1)   # same key, new values


def test_upsert_outages_unchanged_is_stable_and_empty_noop(con):
    rows = [detail(1, "2024/10/02 00:00:00")]
    cs.upsert_outages(con, rows, [])
    res = cs.upsert_outages(con, rows, [])
    assert res["network_outagedetail"] == (0, 1)
    assert con.execute("select count(*) from network_outagedetail").fetchone()[0] == 1
    cs.upsert_outages(con, [], [])
    assert con.execute("select count(*) from network_outagedetail").fetchone()[0] == 1


def test_upsert_rolls_back_on_failure(con):
    cs.upsert_outages(con, [detail(1, "2024/10/02 00:00:00")], [])
    with pytest.raises(ValueError):    # constraintset rows without the key column outageid
        cs.upsert_outages(con, [detail(1, "2024/10/09 00:00:00")], [dict(genconsetid="S1")])
    assert con.execute("select endtime from network_outagedetail").fetchone()[0].day == 2


def test_dudetailsummary_replace_removes_stale_row(con, tmp_path):
    cols = ["DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID", "REGIONID", "STATIONID"]
    old = aemo_csv("DUDETAILSUMMARY", cols, [["U1", T, "2024/12/31 00:00:00", "GENERATOR", "CP1", "NSW1", "S"],
                                              ["GONE", T, "2999/12/31 00:00:00", "GENERATOR", "CP9", "NSW1", "S"]],
                   tmp_path / "o.csv")
    cs.load_month_table(con, "DUDETAILSUMMARY", "2024-10", [(1, old)])
    new = aemo_csv("DUDETAILSUMMARY", cols, [["U1", T, "2999/12/31 00:00:00", "GENERATOR", "CP1", "NSW1", "S"]],
                   tmp_path / "n.csv")
    res = cs.load_month_table(con, "DUDETAILSUMMARY", "2024-11", [(1, new)])
    assert con.execute("select duid, end_date from dudetailsummary").fetchall() \
        == [("U1", __import__("datetime").datetime(2999, 12, 31))]
    assert res == [("dudetailsummary", 0, 1)]


def test_empty_snapshot_does_not_wipe_table(con, tmp_path):
    cols = ["DUID", "START_DATE", "END_DATE", "DISPATCHTYPE", "CONNECTIONPOINTID", "REGIONID", "STATIONID"]
    old = aemo_csv("DUDETAILSUMMARY", cols, [["U1", T, "2999/12/31 00:00:00", "GENERATOR", "CP1", "NSW1", "S"]],
                   tmp_path / "o.csv")
    cs.load_month_table(con, "DUDETAILSUMMARY", "2024-10", [(1, old)])
    empty = aemo_csv("DUDETAILSUMMARY", cols, [], tmp_path / "e.csv")
    with pytest.raises(Exception):
        cs.load_month_table(con, "DUDETAILSUMMARY", "2024-11", [(1, empty)])
    assert con.execute("select count(*) from dudetailsummary").fetchone()[0] == 1
    assert not cs.is_loaded(con, "DUDETAILSUMMARY", "2024-11")


def test_outage_month_load_upserts(con, tmp_path):
    cols = ["OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "STARTTIME", "ENDTIME"]
    p1 = aemo_csv("NETWORK_OUTAGEDETAIL", cols, [[1, "SS", "LINE", "L1", T, T], [2, "SS", "LINE", "L2", T, T]],
                  tmp_path / "a.csv", group="NETWORK")
    cs.load_month_table(con, "NETWORK_OUTAGEDETAIL", "2024-10", [(1, p1)])
    p2 = aemo_csv("NETWORK_OUTAGEDETAIL", cols, [[1, "SS", "LINE", "L1", T, "2024/10/09 00:00:00"]],
                  tmp_path / "b.csv", group="NETWORK")
    res = cs.load_month_table(con, "NETWORK_OUTAGEDETAIL", "2024-11", [(1, p2)])
    assert res == [("network_outagedetail", 0, 1)]
    assert con.execute("select outageid, day(endtime) from network_outagedetail order by 1").fetchall() == [(1, 9), (2, 1)]
