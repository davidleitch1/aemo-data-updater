"""Bid file state: the collector remembers which daily bid files it has merged, across restarts.

Without it, last_files['bids'] / ['bid_dispatch'] start empty after every restart and the
collector re-processes every daily file still on NEMweb CURRENT (~58 BIDMOVE_COMPLETE and
~396 NEXT_DAY_DISPATCH files, 5 a cycle: about 7 hours of long cycles). All tests use tmp_path.
"""
import json
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater import bid_file_state as bfs  # noqa: E402
from aemo_updater.collectors import unified_collector_duckdb as ucd  # noqa: E402
from aemo_updater.collectors import bids_store  # noqa: E402

FIXTURE_ZIP_CSV = (Path(__file__).parent / "fixtures" / "bids_sample.csv").read_bytes()
SAMPLE_FILE = "PUBLIC_BIDMOVE_COMPLETE_20260629_0000000000000001.zip"


# ---------------------------------------------------------------- module

def test_save_then_load_round_trip(tmp_path):
    p = tmp_path / "state.json"
    bfs.save(p, {"bids": {"b2", "b1"}, "bid_dispatch": {"d1"}, "prices5": {"x"}})
    assert bfs.load(p) == {"bids": {"b1", "b2"}, "bid_dispatch": {"d1"}}
    assert not (tmp_path / "state.json.tmp").exists()


def test_load_missing_file_is_empty(tmp_path):
    assert bfs.load(tmp_path / "nope.json") == {}


def test_load_corrupt_file_is_empty(tmp_path):
    p = tmp_path / "state.json"
    p.write_text("{not json")
    assert bfs.load(p) == {}


def test_save_keeps_newest_names_only(tmp_path):
    p = tmp_path / "state.json"
    names = {f"PUBLIC_NEXT_DAY_DISPATCH_2026{m:02d}01_1.zip" for m in range(1, 13)}
    bfs.save(p, {"bid_dispatch": names}, keep=5)
    kept = bfs.load(p)["bid_dispatch"]
    assert kept == set(sorted(names)[-5:])


def test_trading_day_from_filename():
    assert bfs.trading_day("PUBLIC_BIDMOVE_COMPLETE_20261124_0000000550454788.zip") == date(2026, 11, 24)
    assert bfs.trading_day("PUBLIC_NEXT_DAY_DISPATCH_20251026_0000000486606278.zip") == date(2025, 10, 26)
    assert bfs.trading_day("something_else.zip") is None


def _bids_db(path, vol_times, disp_times):
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE bid_volume5 (settlementdate TIMESTAMP)")
    con.execute("CREATE TABLE bid_dispatch (settlementdate TIMESTAMP)")
    for t in vol_times:
        con.execute("INSERT INTO bid_volume5 VALUES (?)", [t])
    for t in disp_times:
        con.execute("INSERT INTO bid_dispatch VALUES (?)", [t])
    con.close()


def test_seed_marks_only_trading_days_present_in_db(tmp_path):
    db = tmp_path / "bids.duckdb"
    # trading day 23 Nov runs 23 Nov 04:05 to 24 Nov 04:00
    _bids_db(db, [datetime(2026, 11, 23, 4, 5), datetime(2026, 11, 24, 4, 0)],
             [datetime(2026, 11, 24, 4, 0)])
    listings = {
        "bids": ["PUBLIC_BIDMOVE_COMPLETE_20261123_1.zip", "PUBLIC_BIDMOVE_COMPLETE_20261124_2.zip"],
        "bid_dispatch": ["PUBLIC_NEXT_DAY_DISPATCH_20261123_1.zip",
                         "PUBLIC_NEXT_DAY_DISPATCH_20261124_2.zip"],
    }
    seeded = bfs.seed_from_db(db, listings)
    assert seeded == {"bids": {"PUBLIC_BIDMOVE_COMPLETE_20261123_1.zip"},
                      "bid_dispatch": {"PUBLIC_NEXT_DAY_DISPATCH_20261123_1.zip"}}


# ---------------------------------------------------------------- collector wiring

def _collector(tmp_path, listing):
    c = ucd.DuckDBCollector(config={"data_path": str(tmp_path)})
    c.get_latest_files = lambda url, pattern: list(listing) if pattern == "PUBLIC_BIDMOVE_COMPLETE_" else []
    c._download_zip_csv_bytes = lambda url, fn: FIXTURE_ZIP_CSV
    c._copy_bids_to_readonly = lambda: None
    return c


def test_state_path_defaults_beside_bids_db(tmp_path):
    c = ucd.DuckDBCollector(config={"data_path": str(tmp_path)})
    assert c.bid_file_state_path == c.bids_db_path.parent / "bid_file_state.json"


def test_merged_files_survive_restart(tmp_path):
    c1 = _collector(tmp_path, [SAMPLE_FILE])
    assert c1.collect_and_store_bids() is True
    state = json.loads((tmp_path / "bid_file_state.json").read_text())
    assert SAMPLE_FILE in state["bids"]

    c2 = _collector(tmp_path, [SAMPLE_FILE])       # a restart
    assert SAMPLE_FILE in c2.last_files["bids"]
    assert c2.collect_and_store_bids() is False     # nothing re-processed


def test_failed_merge_is_not_remembered(tmp_path, monkeypatch):
    c1 = _collector(tmp_path, [SAMPLE_FILE])

    def boom(*a, **k):
        raise RuntimeError("merge failed")

    monkeypatch.setattr(bids_store, "merge_bids", boom)
    with pytest.raises(RuntimeError):
        c1.collect_and_store_bids()
    monkeypatch.undo()
    assert not (tmp_path / "bid_file_state.json").exists()

    c2 = _collector(tmp_path, [SAMPLE_FILE])       # a restart retries the file
    assert c2.collect_and_store_bids() is True


def test_state_save_failure_does_not_break_cycle(tmp_path, monkeypatch):
    c = _collector(tmp_path, [SAMPLE_FILE])
    monkeypatch.setattr(bfs, "save", lambda *a, **k: (_ for _ in ()).throw(OSError("disk")))
    assert c.collect_and_store_bids() is True
