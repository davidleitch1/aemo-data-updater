"""
Tests for DuckDB read-only replica mechanism.

Verifies that:
1. The replica file is readable
2. The atomic copy leaves no .tmp files
3. Failed copies clean up partial files
4. No lock contention between writer and replica reader
"""

import os
import sys
import time
import shutil
import tempfile
from pathlib import Path
from unittest import mock

import duckdb
import pytest

# Add src to path so we can import the collector
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

DB_PATH = Path('/Users/davidleitch/aemo_production/data/aemo_test.duckdb')
READONLY_PATH = Path('/Users/davidleitch/aemo_production/data/aemo_readonly.duckdb')


def test_replica_is_readable():
    """Verify aemo_readonly.duckdb exists and can be opened read-only."""
    assert READONLY_PATH.exists(), f"Replica not found at {READONLY_PATH}"

    conn = duckdb.connect(str(READONLY_PATH), read_only=True)
    try:
        result = conn.execute("SELECT COUNT(*) FROM generation_5min").fetchone()
        assert result[0] > 0, "Replica has no data in generation_5min"
        print(f"  Replica has {result[0]:,} rows in generation_5min")
    finally:
        conn.close()


def test_atomic_copy():
    """Call _copy_to_readonly() and verify no .tmp file remains."""
    from aemo_updater.collectors.unified_collector_duckdb import DuckDBCollector

    collector = DuckDBCollector()
    collector._copy_to_readonly()

    tmp_path = DB_PATH.parent / 'aemo_readonly.duckdb.tmp'
    assert not tmp_path.exists(), f"Temporary file still exists: {tmp_path}"
    assert READONLY_PATH.exists(), f"Replica missing after copy: {READONLY_PATH}"

    # Verify the replica is a valid DuckDB file
    conn = duckdb.connect(str(READONLY_PATH), read_only=True)
    try:
        tables = [t[0] for t in conn.execute("SHOW TABLES").fetchall()]
        assert len(tables) > 0, "Replica has no tables"
        print(f"  Replica has {len(tables)} tables after atomic copy")
    finally:
        conn.close()


def test_cleanup_on_failure():
    """Verify .tmp file is cleaned up when copy fails."""
    from aemo_updater.collectors.unified_collector_duckdb import DuckDBCollector

    collector = DuckDBCollector()
    tmp_path = DB_PATH.parent / 'aemo_readonly.duckdb.tmp'

    # Mock shutil.copy2 to raise an error
    with mock.patch('shutil.copy2', side_effect=OSError("Simulated disk error")):
        collector._copy_to_readonly()

    assert not tmp_path.exists(), f"Temporary file not cleaned up: {tmp_path}"
    print("  .tmp file cleaned up after simulated failure")


def test_no_lock_contention():
    """Key test: writer on aemo_test.duckdb, reader on aemo_readonly.duckdb.

    This test verifies the core benefit of the read replica approach:
    a write lock on the source file does NOT block reads on the replica.
    """
    # Open a write connection to the source (simulates collector)
    write_conn = duckdb.connect(str(DB_PATH))
    try:
        # While write lock is held, open read-only connection to replica
        read_conn = duckdb.connect(str(READONLY_PATH), read_only=True)
        try:
            # This query should succeed without IOException
            result = read_conn.execute(
                "SELECT COUNT(*) FROM generation_5min"
            ).fetchone()
            assert result[0] > 0, "Read from replica failed while source is write-locked"
            print(f"  Read {result[0]:,} rows from replica while source write-locked")
        finally:
            read_conn.close()
    finally:
        write_conn.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
