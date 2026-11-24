"""Tests for aemo_updater.replica: refresh a read-only replica by APFS clone, falling back to copy.

All tests work in tmp_path; none touch the production databases.
"""
import os
import sys
from pathlib import Path
from unittest import mock

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater import replica  # noqa: E402


def _make_db(path: Path, rows: int = 1000) -> None:
    con = duckdb.connect(str(path))
    con.execute(f"CREATE TABLE t AS SELECT range AS i FROM range({rows})")
    con.close()


def _count(path: Path, table: str = "t") -> int:
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        con.close()


def test_refresh_creates_identical_replica(tmp_path):
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    replica.refresh_replica(src, dst)
    assert dst.read_bytes() == src.read_bytes()
    assert _count(dst) == 1000
    assert not (tmp_path / "a_readonly.duckdb.tmp").exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="clonefile is macOS only")
def test_uses_clone_on_apfs(tmp_path):
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    assert replica.refresh_replica(src, dst) == "clone"


def test_replica_independent_of_later_writes(tmp_path):
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    replica.refresh_replica(src, dst)
    con = duckdb.connect(str(src))
    con.execute("INSERT INTO t SELECT range FROM range(500)")
    con.close()
    assert _count(src) == 1500
    assert _count(dst) == 1000


def test_falls_back_to_copy_when_clone_fails(tmp_path):
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    with mock.patch.object(replica, "_clone", side_effect=OSError("not supported")):
        assert replica.refresh_replica(src, dst) == "copy"
    assert dst.read_bytes() == src.read_bytes()


def test_stale_tmp_is_replaced(tmp_path):
    """A .tmp left by a crash must not block the clone (clonefile refuses an existing target)."""
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    (tmp_path / "a_readonly.duckdb.tmp").write_bytes(b"junk")
    replica.refresh_replica(src, dst)
    assert _count(dst) == 1000
    assert not (tmp_path / "a_readonly.duckdb.tmp").exists()


def test_failure_leaves_old_replica_and_no_tmp(tmp_path):
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    replica.refresh_replica(src, dst)
    before = dst.read_bytes()
    with mock.patch.object(replica, "_clone", side_effect=OSError("x")), \
         mock.patch.object(replica.shutil, "copy2", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            replica.refresh_replica(src, dst)
    assert dst.read_bytes() == before
    assert not (tmp_path / "a_readonly.duckdb.tmp").exists()


def test_open_reader_survives_refresh(tmp_path):
    """A reader holding the old replica keeps reading it after the rename, as with copy2."""
    src, dst = tmp_path / "a.duckdb", tmp_path / "a_readonly.duckdb"
    _make_db(src)
    replica.refresh_replica(src, dst)
    reader = duckdb.connect(str(dst), read_only=True)
    try:
        con = duckdb.connect(str(src))
        con.execute("INSERT INTO t SELECT range FROM range(500)")
        con.close()
        replica.refresh_replica(src, dst)
        assert reader.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 1000
    finally:
        reader.close()
    assert _count(dst) == 1500


def test_collector_and_store_use_refresh_replica():
    """All four replica refreshes go through replica.refresh_replica, none through shutil.copy2."""
    root = Path(__file__).parent.parent / "src" / "aemo_updater"
    collector = (root / "collectors" / "unified_collector_duckdb.py").read_text()
    store = (root / "constraints_store.py").read_text()
    for text in (collector, store):
        assert "shutil.copy2" not in text
    assert collector.count("refresh_replica(") == 3
    assert store.count("refresh_replica(") == 1
