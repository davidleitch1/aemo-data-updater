#!/usr/bin/env python3
"""Offline tests for scripts/backfill_constraints.py. Listing, download and sleep are injected;
no network. Zips are synthetic AEMO-format files."""
import sys
import zipfile
from pathlib import Path
from urllib.parse import quote, unquote

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).parent))

import backfill_constraints as bf  # noqa: E402
from test_constraints_store import DC_COLS, aemo_csv, dc_row  # noqa: E402

LP_COLS = ["SETTLEMENTDATE", "DUID", "LOCAL_PRICE_ADJUSTMENT", "LOCALLY_CONSTRAINED"]
GCD_COLS = ["EFFECTIVEDATE", "VERSIONNO", "GENCONID", "CONSTRAINTTYPE", "DESCRIPTION", "LIMITTYPE"]


class World:
    """Fake nemweb: {ym: {filename: local zip path}}; records listings, downloads, sleeps."""

    def __init__(self, tmp):
        self.tmp, self.files = tmp, {}
        self.downloads, self.listings, self.sleeps, self.logs = [], [], [], []
        self.fail_download = set()   # substrings of filenames that raise
        self.unavailable = set()     # ym that 404 on listing

    def add(self, ym, name, table, cols, rows, group="DISPATCH"):
        d = self.tmp / "world" / ym
        d.mkdir(parents=True, exist_ok=True)
        csv = aemo_csv(table, cols, rows, d / "x.csv", group=group)
        zp = d / name
        with zipfile.ZipFile(zp, "w") as z:
            z.write(csv, name.replace(".zip", ".CSV"))
        csv.unlink()
        self.files.setdefault(ym, {})[name] = zp

    def listing(self, ym):
        self.listings.append(ym)
        if ym in self.unavailable:
            raise bf.Unavailable(ym)
        return [f"/Data_Archive/Wholesale_Electricity/MMSDM/{ym[:4]}/x/DATA/{quote(n)}"
                for n in self.files.get(ym, {})]

    def download(self, url, dest):
        name = unquote(url.rsplit("/", 1)[-1])
        ym = next(k for k, v in self.files.items() if name in v)
        if any(s in name for s in self.fail_download):
            raise RuntimeError("boom " + name)
        self.downloads.append(name)
        Path(dest).write_bytes(self.files[ym][name].read_bytes())

    def run(self, argv, **kw):
        return bf.main(argv, listing=self.listing, download=self.download, sleep=self.sleeps.append,
                       log=self.logs.append, **kw)


def arch(table, n=1, ym="202410"):
    return f"PUBLIC_ARCHIVE#{table}#FILE{n:02d}#{ym}010000.zip"


def dvd(table, ym="201701"):
    return f"PUBLIC_DVD_{table}_{ym}010000.zip"


@pytest.fixture
def w(tmp_path):
    return World(tmp_path)


def db_of(tmp_path):
    return tmp_path / "db" / "c.duckdb"


def q(tmp_path, sql):
    c = duckdb.connect(str(db_of(tmp_path)), read_only=True)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def argv(tmp_path, *a):
    return ["--db", str(db_of(tmp_path)), *a]


def add_dc(w, ym, name, rows):
    w.add(ym, name, "DISPATCHCONSTRAINT", DC_COLS, rows)


def test_match_both_name_patterns():
    hrefs = ["/a/" + quote(n) for n in [arch("DISPATCHCONSTRAINT"), dvd("GENCONSET"),
                                         dvd("GENCONSETINVOKE"), arch("DISPATCHCONSTRAINT", 2)]]
    assert [p for p, _ in bf.match(hrefs, "DISPATCHCONSTRAINT")] == [1, 2]
    assert [p for p, _ in bf.match(hrefs, "GENCONSET")] == [1]       # not GENCONSETINVOKE
    assert [p for p, _ in bf.match(hrefs, "GENCONSETINVOKE")] == [1]
    assert bf.match(hrefs, "DUDETAILSUMMARY") == []


def test_month_range_newest_first():
    assert bf.months_desc((2024, 11), (2025, 2)) == [(2025, 2), (2025, 1), (2024, 12), (2024, 11)]


def test_loads_both_patterns_and_multipart(w, tmp_path):
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT", 1), [dc_row("2024/10/01 00:05:00", "A", 5)])
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT", 2), [dc_row("2024/10/01 00:10:00", "B", 6)])
    w.add("2024-10", dvd("DISPATCH_LOCAL_PRICE", "202410"), "LOCAL_PRICE", LP_COLS,
          [["2024/10/01 00:05:00", "U1", 1.5, 1]])
    assert w.run(argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--pause", "0")) == 0
    assert q(tmp_path, "select constraintid from constraint_binding5 order by 1") == [("A",), ("B",)]
    assert q(tmp_path, "select count(*) from local_price5") == [(1,)]
    assert len(w.downloads) == 3


def test_second_run_no_download_and_no_change(w, tmp_path):
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT"), [dc_row("2024/10/01 00:05:00", "A", 5)])
    a = argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--tables", "DISPATCHCONSTRAINT", "--pause", "0")
    assert w.run(a) == 0
    n = len(w.downloads)
    before = q(tmp_path, "select count(*) from constraint_binding5")
    assert w.run(a) == 0
    assert len(w.downloads) == n
    assert q(tmp_path, "select count(*) from constraint_binding5") == before


def test_rerun_after_ledger_cleared_inserts_zero(w, tmp_path):
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT"), [dc_row("2024/10/01 00:05:00", "A", 5)])
    a = argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--tables", "DISPATCHCONSTRAINT", "--pause", "0")
    w.run(a)
    c = duckdb.connect(str(db_of(tmp_path)))
    c.execute("delete from _loaded")
    c.close()
    w.logs.clear()
    assert w.run(a) == 0
    assert len(w.downloads) == 2
    assert any("inserted 0" in l and "already present 1" in l for l in w.logs)
    assert q(tmp_path, "select count(*) from constraint_binding5") == [(1,)]


def test_missing_table_not_found_is_skipped(w, tmp_path):
    w.add("2024-10", dvd("DISPATCH_LOCAL_PRICE", "202410"), "LOCAL_PRICE", LP_COLS,
          [["2024/10/01 00:05:00", "U1", 1.5, 1]])
    assert w.run(argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--pause", "0")) == 0
    assert any("DISPATCHCONSTRAINT" in l and "NOT FOUND" in l for l in w.logs)
    assert q(tmp_path, "select count(*) from local_price5") == [(1,)]


def test_defs_sweep_covers_every_month_and_snapshots_end_month_only(w, tmp_path):
    for ym in ["2024-08", "2024-09", "2024-10"]:
        w.add(ym, arch("GENCONDATA", 1, ym.replace("-", "")), "GENCONDATA", GCD_COLS,
              [[f"{ym.replace('-', '/')}/01 00:00:00", 1, "C_" + ym.replace("-", "_"), "LE", "d", "Thermal"]],
              group="GENERIC_CONSTRAINT")
        w.add(ym, arch("NETWORK_OUTAGEDETAIL", 1, ym.replace("-", "")), "NETWORK_OUTAGEDETAIL",
              ["OUTAGEID", "SUBSTATIONID", "EQUIPMENTTYPE", "EQUIPMENTID", "STARTTIME"],
              [[int(ym[-2:]), "S", "LINE", "L", "2024/10/01 00:00:00"]], group="NETWORK")
    rc = w.run(argv(tmp_path, "--defs-from", "2024-08", "--end", "2024-10", "--snapshots",
                    "--tables", "GENCONDATA,NETWORK_OUTAGEDETAIL", "--pause", "0"))
    assert rc == 0
    assert q(tmp_path, "select genconid from gencondata order by 1") == [("C_2024_08",), ("C_2024_09",), ("C_2024_10",)]
    assert q(tmp_path, "select outageid from network_outagedetail") == [(10,)]
    # newest first
    assert [d for d in w.downloads if "GENCONDATA" in d] == [arch("GENCONDATA", 1, m) for m in ["202410", "202409", "202408"]]


def test_failed_month_does_not_stop_later_months(w, tmp_path):
    for ym in ["2024-09", "2024-10"]:
        add_dc(w, ym, arch("DISPATCHCONSTRAINT", 1, ym.replace("-", "")),
               [dc_row(f"{ym.replace('-', '/')}/01 00:05:00", "A" + ym[-2:], 5)])
    w.fail_download = {"202410"}   # newest month fails first
    rc = w.run(argv(tmp_path, "--start", "2024-09", "--end", "2024-10", "--tables", "DISPATCHCONSTRAINT",
                    "--pause", "0"))
    assert rc != 0
    assert q(tmp_path, "select constraintid from constraint_binding5") == [("A09",)]
    assert any("FAILED" in l for l in w.logs)
    # failed month is not marked loaded, so a retry picks it up
    w.fail_download = set()
    assert w.run(argv(tmp_path, "--start", "2024-09", "--end", "2024-10", "--tables", "DISPATCHCONSTRAINT",
                      "--pause", "0")) == 0
    assert q(tmp_path, "select count(*) from constraint_binding5") == [(2,)]


def test_unavailable_month_logged_not_failed(w, tmp_path):
    w.unavailable = {"2014-05"}
    rc = w.run(argv(tmp_path, "--defs-from", "2014-05", "--end", "2014-05", "--tables", "GENCONDATA", "--pause", "0"))
    assert rc == 0
    assert any("unavailable" in l for l in w.logs)


@pytest.mark.parametrize("threshold,expected", [(1, 20.0), (10**9, 1.0)])
def test_pause_after_large_vs_small_download(w, tmp_path, monkeypatch, threshold, expected):
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT"), [dc_row("2024/10/01 00:05:00", "A", 5)])
    w.add("2024-10", dvd("DISPATCH_LOCAL_PRICE", "202410"), "LOCAL_PRICE", LP_COLS,
          [["2024/10/01 00:05:00", "U1", 1.5, 1]])
    monkeypatch.setattr(bf, "LARGE_BYTES", threshold)
    w.run(argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--pause", "20"))
    assert w.sleeps == [expected, expected]


def test_dry_run_downloads_and_writes_nothing(w, tmp_path):
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT"), [dc_row("2024/10/01 00:05:00", "A", 5)])
    assert w.run(argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--dry-run")) == 0
    assert w.downloads == [] and not db_of(tmp_path).exists()
    assert any("would fetch" in l and "DISPATCHCONSTRAINT" in l for l in w.logs)


def test_temp_dir_removed_after_each_month(w, tmp_path):
    add_dc(w, "2024-10", arch("DISPATCHCONSTRAINT"), [dc_row("2024/10/01 00:05:00", "A", 5)])
    w.run(argv(tmp_path, "--start", "2024-10", "--end", "2024-10", "--tables", "DISPATCHCONSTRAINT", "--pause", "0"))
    assert [p.name for p in (tmp_path / "db").iterdir() if p.is_dir()] == []
    assert not any(p.suffix == ".zip" for p in (tmp_path / "db").iterdir())


def test_bad_args(w, tmp_path):
    with pytest.raises(SystemExit):
        w.run(argv(tmp_path, "--end", "2024-10"))   # nothing selected
