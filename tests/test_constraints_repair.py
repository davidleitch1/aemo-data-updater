#!/usr/bin/env python3
"""Offline tests for scripts/constraints_repair.py: absolute-enumeration survey, CURRENT before
ARCHIVE, lag, email policy. Synthetic DispatchIS files; network injected."""
import io
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from aemo_updater import constraints_store as cs  # noqa: E402
import constraints_repair as cr  # noqa: E402

NOW = datetime(2026, 11, 24, 12, 3)          # NEM time; survey end = floor5(11:43) = 11:40
CON_HDR = ("I,DISPATCH,CONSTRAINT,5,SETTLEMENTDATE,RUNNO,CONSTRAINTID,DISPATCHINTERVAL,INTERVENTION,RHS,"
           "MARGINALVALUE,VIOLATIONDEGREE,LASTCHANGED,DUID,GENCONID_EFFECTIVEDATE,GENCONID_VERSIONNO,LHS")
LP_HDR = "I,DISPATCH,LOCAL_PRICE,1,SETTLEMENTDATE,DUID,LOCAL_PRICE_ADJUSTMENT,LOCALLY_CONSTRAINED"


def dis_csv(ts: datetime) -> bytes:
    t = f'"{ts:%Y/%m/%d %H:%M:%S}"'
    return "\n".join([
        "C,NEMP.WORLD,DISPATCHIS", CON_HDR,
        f'D,DISPATCH,CONSTRAINT,5,{t},1,CON_A,1,0,10,50,0,{t},,"2024/01/01 00:00:00",1,5',
        f'D,DISPATCH,CONSTRAINT,5,{t},1,CON_B,1,0,10,0,0,{t},,"2024/01/01 00:00:00",1,5',
        LP_HDR, f"D,DISPATCH,LOCAL_PRICE,1,{t},U1,-1.5,1", "C,END OF REPORT,9"]).encode()


def zip_of(name, data):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        z.writestr(name, data)
    return b.getvalue()


def cur_name(ts):
    return f"PUBLIC_DISPATCHIS_{ts:%Y%m%d%H%M}_0000000000000001.zip"


class FakeNet:
    def __init__(self, current=(), archive_days=None):
        self.current = {cur_name(t): t for t in current}
        self.archive = archive_days or {}          # date -> list of ts held in that day's zip
        self.calls = []

    def current_files(self):
        return sorted(self.current)

    def archive_listing(self):
        return "".join(f'<A HREF="/Reports/ARCHIVE/DispatchIS_Reports/PUBLIC_DISPATCHIS_{d:%Y%m%d}.zip">x</A>'
                       for d in self.archive)

    def get(self, url, timeout=120):
        name = url.rsplit("/", 1)[-1]
        self.calls.append(name)
        if name in self.current:
            return zip_of("x.CSV", dis_csv(self.current[name]))
        d = datetime.strptime(name[len("PUBLIC_DISPATCHIS_"):][:8], "%Y%m%d").date()
        b = io.BytesIO()
        with zipfile.ZipFile(b, "w") as z:
            for t in self.archive[d]:
                z.writestr(cur_name(t), zip_of("x.CSV", dis_csv(t)))
        return b.getvalue()


def seed(db, stamps):
    con = cs.connect_with_retry(db)
    cs.ensure_tables(con)
    for t in stamps:
        cs.store_dispatchis(con, dis_csv(t))
    con.close()


def ledger(db):
    con = duckdb.connect(str(db), read_only=True)
    try:
        return [r[0] for r in con.execute("select settlementdate from constraint_intervals order by 1").fetchall()]
    finally:
        con.close()


def span(a, b):
    out, t = [], a
    while t <= b:
        out.append(t)
        t += timedelta(minutes=5)
    return out


@pytest.fixture
def db(tmp_path):
    return tmp_path / "c.duckdb"


def test_survey_finds_middle_hole_and_tail_and_respects_lag(db):
    full = span(datetime(2026, 11, 24, 10, 0), datetime(2026, 11, 24, 11, 0))
    seed(db, [t for t in full if t != datetime(2026, 11, 24, 10, 30)])
    con = duckdb.connect(str(db), read_only=True)
    got = cr.survey(con, NOW)
    con.close()
    assert got[0] == datetime(2026, 11, 24, 10, 30)
    assert got[1:] == span(datetime(2026, 11, 24, 11, 5), datetime(2026, 11, 24, 11, 40))   # not 11:45+
    assert datetime(2026, 11, 24, 11, 45) not in got


def test_survey_tail_hole_not_hidden_by_max(db):
    seed(db, [datetime(2026, 11, 24, 8, 0)])
    con = duckdb.connect(str(db), read_only=True)
    got = cr.survey(con, NOW)
    con.close()
    assert len(got) == 12 * 3 + 8      # 08:05 .. 11:40


def test_survey_empty_ledger_and_future_start(db):
    seed(db, [])
    con = duckdb.connect(str(db), read_only=True)
    assert cr.survey(con, NOW) == []
    con.close()
    seed(db, [datetime(2026, 11, 24, 12, 0)])
    con = duckdb.connect(str(db), read_only=True)
    assert cr.survey(con, NOW) == []
    con.close()


def test_repair_uses_current_before_archive_and_fixes_all(db):
    full = span(datetime(2026, 11, 24, 10, 0), datetime(2026, 11, 24, 11, 0))
    hole = datetime(2026, 11, 24, 10, 30)
    seed(db, [t for t in full if t != hole])
    net = FakeNet(current=span(datetime(2026, 11, 24, 11, 5), datetime(2026, 11, 24, 11, 40)),
                  archive_days={hole.date(): [hole]})
    sleeps, logs, mails = [], [], []
    r = cr.run(db, net=net, sleep=sleeps.append, pause=7.0, now=NOW, send=lambda s, h: mails.append(s), log=logs.append)
    assert (r["found"], r["fixed"], r["remaining"]) == (9, 9, 0)
    kinds = ["cur" if "_0000000000000001" in c else "arc" for c in net.calls]
    assert kinds == ["cur"] * 8 + ["arc"]                     # CURRENT first, ARCHIVE only for the rest
    assert sleeps == [1.0] * 8 + [7.0]
    assert hole in ledger(db) and len(ledger(db)) == 7 * 0 + len(full) + 8
    assert mails == []
    assert (db.parent / "constraints_readonly.duckdb").exists()
    assert any("found 9, fixed 9, remaining 0" in l for l in logs)


def test_midnight_interval_comes_from_previous_days_archive(db):
    mid = datetime(2026, 11, 24, 0, 0)
    seed(db, [datetime(2026, 11, 23, 23, 55), datetime(2026, 11, 24, 0, 5)])
    net = FakeNet(archive_days={mid.date() - timedelta(days=1): [mid]})
    r = cr.run(db, net=net, sleep=lambda s: None, now=datetime(2026, 11, 24, 0, 25), no_email=True, log=lambda s: None)
    assert r["found"] == 1 and r["remaining"] == 0


def test_check_only_makes_no_network_calls_or_writes(db):
    seed(db, [datetime(2026, 11, 24, 8, 0)])
    before = ledger(db)
    net = FakeNet(current=span(datetime(2026, 11, 24, 8, 5), datetime(2026, 11, 24, 11, 40)))
    r = cr.run(db, check_only=True, net=net, now=NOW, no_email=True, log=lambda s: None)
    assert net.calls == [] and ledger(db) == before and r["remaining"] == r["found"] == 44


def test_max_days_limits_archive_fetches_newest_first(db):
    base = datetime(2026, 11, 20, 12, 0)
    seed(db, [base])
    days = {}
    for k in range(4):
        d = (base + timedelta(days=k)).date()
        days[d] = span(datetime(d.year, d.month, d.day, 0, 5), datetime(d.year, d.month, d.day, 0, 10))
    # only the two midnight-adjacent slots per day are held; the rest of each day stays missing
    net = FakeNet(archive_days={d: v for d, v in days.items()})
    sleeps = []
    cr.run(db, net=net, sleep=sleeps.append, max_days=2, pause=3.0, now=NOW, no_email=True, log=lambda s: None)
    fetched = [c for c in net.calls]
    assert len(fetched) == 2 and fetched == [f"PUBLIC_DISPATCHIS_{d:%Y%m%d}.zip" for d in sorted(days, reverse=True)[:2]]
    assert sleeps.count(3.0) == 2


def test_email_only_when_old_intervals_remain(db):
    seed(db, [datetime(2026, 11, 24, 8, 0)])
    mails = []
    r = cr.run(db, net=FakeNet(), sleep=lambda s: None, now=NOW, send=lambda s, h: mails.append((s, h)) or True,
               log=lambda s: None)
    assert r["old"] > 0 and r["emailed"] and len(mails) == 1
    r = cr.run(db, net=FakeNet(), sleep=lambda s: None, now=NOW, no_email=True,
               send=lambda s, h: mails.append(s), log=lambda s: None)
    assert len(mails) == 1
    # only recent holes (< 3 h) remain: no email
    db2 = db.parent / "d2.duckdb"
    seed(db2, span(datetime(2026, 11, 24, 9, 30), datetime(2026, 11, 24, 11, 0)))
    r = cr.run(db2, net=FakeNet(), sleep=lambda s: None, now=NOW, send=lambda s, h: mails.append(s), log=lambda s: None)
    assert r["remaining"] == 8 and r["old"] == 0 and len(mails) == 1


def test_main_returns_zero_and_accepts_flags(db, capsys):
    seed(db, span(datetime(2026, 11, 24, 10, 0), datetime(2026, 11, 24, 11, 40)))
    assert cr.main(["--db", str(db), "--check-only", "--no-email", "--max-days", "3", "--pause", "0"],
                   now=NOW) == 0
    assert "found 0, fixed 0, remaining 0" in capsys.readouterr().out
