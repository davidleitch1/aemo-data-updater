"""Tests for scripts/catchup_repair.py: decide when to run the integrity repair."""
import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pytest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts' / 'catchup_repair.py'
spec = importlib.util.spec_from_file_location('catchup_repair', SCRIPT)
cr = importlib.util.module_from_spec(spec)
sys.modules['catchup_repair'] = cr
spec.loader.exec_module(cr)

NOW = datetime(2026, 11, 13, 8, 0)  # NEM time


def make_db(tmp_path, drop=None, latest=NOW - timedelta(minutes=5)):
    """Replica with every label for 14 h up to `latest`, minus `drop`."""
    drop = drop or {}
    path = tmp_path / 'replica.duckdb'
    con = duckdb.connect(str(path))
    start = latest - timedelta(hours=14)
    for table, step in cr.TABLES.items():
        con.execute(f'CREATE TABLE {table} (settlementdate TIMESTAMP)')
        ts = start - timedelta(minutes=start.minute % step)
        rows = []
        while ts <= latest:
            if ts not in drop.get(table, ()):
                rows.append((ts,))
            ts += timedelta(minutes=step)
        con.executemany(f'INSERT INTO {table} VALUES (?)', rows)
    con.close()
    return path


def test_no_holes_means_nothing_to_do(tmp_path):
    db = make_db(tmp_path)
    status, holes = cr.find_holes(db, NOW)
    assert status == 'ok'
    assert holes == set()


def test_finds_hole_behind_newest_data(tmp_path):
    gap = [datetime(2026, 11, 13, 6, 15) + timedelta(minutes=5 * i) for i in range(6)]
    db = make_db(tmp_path, drop={'prices5': gap, 'scada30': [datetime(2026, 11, 13, 6, 30)]})
    status, holes = cr.find_holes(db, NOW)
    assert status == 'ok'
    assert {('prices5', t.isoformat()) for t in gap} <= holes
    assert ('scada30', '2026-11-13T06:30:00') in holes
    assert len(holes) == 7


def test_waits_while_collector_is_behind(tmp_path):
    db = make_db(tmp_path, latest=NOW - timedelta(minutes=60),
                 drop={'prices5': [datetime(2026, 11, 13, 5, 0)]})
    status, holes = cr.find_holes(db, NOW)
    assert status == 'behind'
    assert holes == set()


def test_ignores_newest_labels_still_arriving(tmp_path):
    # The newest 5-min label of scada5 lags prices5 by one interval routinely,
    # and the newest 30-min label is built a cycle later. Neither is a hole.
    latest = NOW - timedelta(minutes=5)
    db = make_db(tmp_path, latest=latest,
                 drop={'scada5': [latest], 'scada30': [datetime(2026, 11, 13, 7, 30)]})
    status, holes = cr.find_holes(db, NOW)
    assert status == 'ok'
    assert holes == set()


def test_ignores_holes_older_than_window(tmp_path):
    old = NOW - timedelta(hours=13)
    old = old - timedelta(minutes=old.minute % 5)
    db = make_db(tmp_path, drop={'prices5': [old]})
    status, holes = cr.find_holes(db, NOW)
    assert holes == set()


def test_decide_runs_once_per_hole():
    holes = {('prices5', '2026-11-13T06:15:00')}
    assert cr.new_holes(holes, attempted={}) == holes
    attempted = cr.record_attempt({}, holes, NOW)
    assert cr.new_holes(holes, attempted) == set()
    # a different hole still triggers
    other = {('scada5', '2026-11-13T06:10:00')}
    assert cr.new_holes(holes | other, attempted) == other


def test_attempted_entries_expire():
    holes = {('prices5', '2026-11-13T06:15:00')}
    attempted = cr.record_attempt({}, holes, NOW - timedelta(hours=30))
    pruned = cr.prune(attempted, NOW)
    assert pruned == {}


@pytest.mark.parametrize('hhmm,quiet', [('04:10', True), ('04:59', True),
                                        ('05:00', False), ('03:59', False)])
def test_quiet_around_daily_job(hhmm, quiet):
    h, m = map(int, hhmm.split(':'))
    assert cr.in_daily_window(datetime(2026, 11, 13, h, m)) is quiet


def test_single_hole_left_to_daily_job():
    one = {('scada5', '2026-11-12T02:45:00')}
    two = one | {('scada5', '2026-11-12T02:50:00')}
    assert cr.should_repair(one) is False
    assert cr.should_repair(two) is True


def test_main_runs_integrity_once_per_hole(tmp_path, monkeypatch):
    gap = [datetime(2026, 11, 13, 6, 15) + timedelta(minutes=5 * i) for i in range(3)]
    db = make_db(tmp_path, drop={'prices5': gap})
    marker = tmp_path / 'calls.txt'
    fake = tmp_path / 'fake_integrity.py'
    fake.write_text(f"import sys\nopen({str(marker)!r}, 'a').write(' '.join(sys.argv[1:]) + '\\n')\n")
    monkeypatch.setattr(cr, 'REPLICA', db)
    monkeypatch.setattr(cr, 'STATE', tmp_path / 'state.json')
    monkeypatch.setattr(cr, 'LOCK', tmp_path / 'x.lock')
    monkeypatch.setattr(cr, 'INTEGRITY', fake)
    monkeypatch.setattr(cr, 'nem_now', lambda: NOW)
    monkeypatch.setattr(cr, 'in_daily_window', lambda _: False)
    assert cr.main() == 0
    assert cr.main() == 0
    assert marker.read_text().splitlines() == ['--email-policy always']
