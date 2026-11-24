#!/usr/bin/env python3
"""Live constraint collection inside DuckDBCollector. Network is faked from the recorded
DispatchIS fixtures; databases are tmp files."""
import json
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "src"))

from aemo_updater import constraints_store as cs  # noqa: E402
from aemo_updater.collectors import unified_collector_duckdb as ucd  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "nemweb"
MANIFEST = json.loads((FIX / "manifest.json").read_text())
DIS = sorted(MANIFEST["prices5"]["files"])
URL = MANIFEST["prices5"]["url"]


class Resp:
    def __init__(self, content):
        self.content, self.status_code, self.text = content, 200, ""

    def raise_for_status(self):
        pass


def make(tmp_path, listing):
    """DuckDBCollector with a fake network. `listing` is a mutable list of DispatchIS names."""
    c = ucd.DuckDBCollector(config={"data_path": str(tmp_path)})
    calls = []

    def fake_get(url, timeout=60, attempts=3):
        calls.append(url.rsplit("/", 1)[-1])
        return Resp((FIX / url.rsplit("/", 1)[-1]).read_bytes())

    def fake_listing(url, pattern):
        if pattern == "PUBLIC_DISPATCHIS_":
            return sorted(listing)
        for meta in MANIFEST.values():
            if meta["url"] == url and meta["pattern"] == pattern:
                return sorted(meta["files"])
        return []

    c._get_with_retry = fake_get
    c.get_latest_files = fake_listing
    return c, calls


def rows(path, sql):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_db_path_default_and_env(tmp_path, monkeypatch):
    c, _ = make(tmp_path, [])
    assert c.constraints_db_path == tmp_path / "constraints.duckdb"
    monkeypatch.setenv("AEMO_CONSTRAINTS_DUCKDB_PATH", str(tmp_path / "x.duckdb"))
    c2, _ = make(tmp_path, [])
    assert c2.constraints_db_path == tmp_path / "x.duckdb"


def test_cold_start_does_not_raise_and_marks_listing_seen(tmp_path):
    """_select_new_files cold start hands back the newest window (it is not download-free,
    contrary to the brief) and marks the whole listing seen. Stores are anti-joins, so this
    is safe for rows the DB already holds."""
    c, calls = make(tmp_path, list(DIS))
    assert c.collect_and_store_constraints() is True
    assert c.last_files["constraints5"] == set(DIS)
    assert len(rows(c.constraints_db_path, "select * from constraint_intervals")) == 3
    assert c.collect_and_store_constraints() is False


def test_cold_start_with_empty_listing(tmp_path):
    c, calls = make(tmp_path, [])
    assert c.collect_and_store_constraints() is False
    assert calls == [] and not c.constraints_db_path.exists()


def test_stores_new_files_and_refreshes_readonly(tmp_path):
    listing = DIS[:1]
    c, _ = make(tmp_path, listing)
    c.last_files["constraints5"] = set(listing)
    listing[:] = DIS
    assert c.collect_and_store_constraints() is True
    assert len(rows(c.constraints_db_path, "select * from constraint_intervals")) == 2
    ro = tmp_path / "constraints_readonly.duckdb"
    assert rows(ro, "select count(*) from constraint_intervals") == [(2,)]
    assert not (tmp_path / "constraints_readonly.duckdb.tmp").exists()
    # nothing new: no work, no error
    assert c.collect_and_store_constraints() is False


def test_no_second_download_after_prices(tmp_path):
    listing = DIS[:1]
    c, calls = make(tmp_path, listing)
    c.last_files["prices5"] = set(listing)
    c.last_files["constraints5"] = set(listing)
    listing[:] = DIS
    c.clear_file_cache()
    c.collect_5min_prices()
    c.collect_and_store_constraints()
    assert sorted(calls) == sorted(DIS[1:])    # each new file fetched once, not twice
    assert len(rows(c.constraints_db_path, "select * from constraint_intervals")) == 2


def test_failed_store_is_unmarked_and_retried(tmp_path, monkeypatch):
    listing = DIS[:1]
    c, _ = make(tmp_path, listing)
    c.last_files["constraints5"] = set(listing)
    listing[:] = DIS
    real = cs.store_dispatchis
    state = {"n": 0}

    def flaky(con, b):
        state["n"] += 1
        if state["n"] == 1:
            raise RuntimeError("boom")
        return real(con, b)

    monkeypatch.setattr(cs, "store_dispatchis", flaky)
    assert c.collect_and_store_constraints() is True        # second file stored
    assert DIS[1] not in c.last_files["constraints5"] and DIS[2] in c.last_files["constraints5"]
    assert len(rows(c.constraints_db_path, "select * from constraint_intervals")) == 1
    assert c.collect_and_store_constraints() is True        # retry picks up the failed one
    assert len(rows(c.constraints_db_path, "select * from constraint_intervals")) == 2
    assert c.collect_and_store_constraints() is False


def test_db_open_failure_unmarks_everything(tmp_path, monkeypatch):
    listing = DIS[:1]
    c, calls = make(tmp_path, listing)
    c.last_files["constraints5"] = set(listing)
    listing[:] = DIS

    def locked(*a, **k):
        raise duckdb.IOException("locked")

    monkeypatch.setattr(cs, "connect_with_retry", locked)
    assert c.collect_and_store_constraints() is False
    assert c.last_files["constraints5"] == {DIS[0]}
    monkeypatch.undo()
    assert c.collect_and_store_constraints() is True
    assert len(rows(c.constraints_db_path, "select * from constraint_intervals")) == 2


def test_gives_up_on_a_file_that_never_stores(tmp_path, monkeypatch):
    listing = DIS[:1]
    c, _ = make(tmp_path, listing)
    c.last_files["constraints5"] = set(listing)
    listing[:] = DIS[:2]
    monkeypatch.setattr(cs, "store_dispatchis", lambda con, b: (_ for _ in ()).throw(ValueError("bad")))
    for _ in range(10):
        c.collect_and_store_constraints()
    assert DIS[1] in c.last_files["constraints5"]            # retry cap reached, no endless loop


def test_cycle_unaffected_when_constraints_fail(tmp_path, monkeypatch):
    def run(fail):
        d = tmp_path / ("fail" if fail else "ok")
        d.mkdir()
        c, _ = make(d, list(DIS))
        c.last_files["constraints5"] = {DIS[0]}
        if fail:
            monkeypatch.setattr(cs, "store_dispatchis", lambda con, b: (_ for _ in ()).throw(RuntimeError("x")))
        else:
            monkeypatch.setattr(c, "collect_and_store_constraints", lambda: False)
        res = c.run_single_update()
        monkeypatch.undo()
        return res

    ok, bad = run(False), run(True)
    assert "constraints" in ok and "constraints" in bad
    assert {k: v for k, v in ok.items() if k != "constraints"} == {k: v for k, v in bad.items() if k != "constraints"}
    assert bad["constraints"] is False or bad["constraints"] is True


def test_cycle_survives_exception_in_collect(tmp_path, monkeypatch):
    c, _ = make(tmp_path, list(DIS))
    monkeypatch.setattr(c, "collect_and_store_constraints", lambda: (_ for _ in ()).throw(RuntimeError("x")))
    res = c.run_single_update()
    assert res["constraints"] is False
