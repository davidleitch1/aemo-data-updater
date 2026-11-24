#!/usr/bin/env python3
"""Offline tests for scripts/refresh_network_outages.py with an injected fetcher."""
import io
import sys
import zipfile
from pathlib import Path

import duckdb

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import refresh_network_outages as rn  # noqa: E402

NAME1, NAME2 = "PUBLIC_NETWORK_20261124010000_0000000001.zip", "PUBLIC_NETWORK_20261124020000_0000000002.zip"
T = "2026/11/24 00:00:00"


def netcsv(end1):
    return "\n".join([
        "C,NEMP.WORLD,NETWORK",
        "I,NETWORK,OUTAGEDETAIL,2,OUTAGEID,SUBSTATIONID,EQUIPMENTTYPE,EQUIPMENTID,STARTTIME,ENDTIME,OUTAGESTATUSCODE",
        f'D,NETWORK,OUTAGEDETAIL,2,"1","SS","LINE","L1","{T}","{end1}","APPR"',
        f'D,NETWORK,OUTAGEDETAIL,2,"2","SS","LINE","L2","{T}",,"APPR"',
        "I,NETWORK,OUTAGECONSTRAINTSET,1,OUTAGEID,GENCONSETID,STARTINTERVAL,ENDINTERVAL",
        f'D,NETWORK,OUTAGECONSTRAINTSET,1,"1","S1","{T}","{T}"',
        "C,END OF REPORT,9"])


def zipped(text):
    b = io.BytesIO()
    with zipfile.ZipFile(b, "w") as z:
        z.writestr("x.CSV", text)
    return b.getvalue()


class Fake:
    def __init__(self):
        self.names, self.files, self.urls = [NAME1], {NAME1: zipped(netcsv("2026/11/25 00:00:00"))}, []

    def __call__(self, url):
        self.urls.append(url)
        if url == rn.DIR_URL:
            return "".join(f'<A HREF="/x/{n}">{n}</A>' for n in self.names).encode()
        return self.files[url.rsplit("/", 1)[-1]]


def q(db, sql):
    c = duckdb.connect(str(db), read_only=True)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def test_parse():
    d, s = rn.parse_network_csv(netcsv("2026/11/25 00:00:00").encode())
    assert len(d) == 2 and d[1]["endtime"] is None and d[0]["outageid"] == "1" and len(s) == 1


def test_loads_then_noop_when_unchanged_then_picks_up_new_file(tmp_path):
    db, f, logs = tmp_path / "c.duckdb", Fake(), []
    assert rn.main(["--db", str(db)], get=f, log=logs.append) == 0
    assert q(db, "select count(*) from network_outagedetail") == [(2,)]
    assert (tmp_path / rn.STATE_NAME).read_text().strip() == NAME1
    n_urls, mtime = len(f.urls), db.stat().st_mtime_ns
    logs.clear()
    assert rn.main(["--db", str(db)], get=f, log=logs.append) == 0
    assert logs == [] and len(f.urls) == n_urls + 1 and db.stat().st_mtime_ns == mtime   # listing only
    f.names.append(NAME2)
    f.files[NAME2] = zipped(netcsv("2026/11/30 00:00:00"))
    assert rn.main(["--db", str(db)], get=f, log=logs.append) == 0
    assert q(db, "select day(endtime) from network_outagedetail where outageid = 1") == [(30,)]
    assert (tmp_path / rn.STATE_NAME).read_text().strip() == NAME2


def test_failure_leaves_state_unchanged(tmp_path):
    db, f = tmp_path / "c.duckdb", Fake()
    f.files[NAME1] = zipped("C,nothing")
    assert rn.main(["--db", str(db)], get=f, log=lambda s: None) == 1
    assert not (tmp_path / rn.STATE_NAME).exists()
