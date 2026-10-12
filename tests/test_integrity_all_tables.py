"""Offline tests for the all-tables integrity survey, repair and scheduling.

No network and no write to the production databases: scratch DuckDB files are
built with the real schemas (LIMIT 0 from a read-only attach of the replica), and
nemweb is replaced by in-memory zips (recorded fixtures in tests/fixtures/nemweb
plus small synthetic files for the feeds not recorded: rooftop MEASUREMENT,
Next_Day_Dispatch, weekly ARCHIVE zips).
"""
import io
import json
import os
import shutil
import sys
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(os.environ.get('GAP_SCRIPTS_DIR', ROOT / 'scripts'))
sys.path.insert(0, str(SCRIPTS))
sys.path.insert(0, str(ROOT / 'src'))

import gap_repair as gr  # noqa: E402
import weekly_integrity as wi  # noqa: E402

FIX = ROOT / 'tests' / 'fixtures' / 'nemweb'
SCHEMA_SRC = Path(os.environ.get(
    'AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data')) / 'aemo_readonly.duckdb'
NOW = datetime(2026, 10, 13, 9, 7)
REGIONS = ['NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1']
ALL12 = ['prices5', 'scada5', 'transmission5', 'bdu5', 'curtailment_regional5',
         'curtailment5', 'curtailment_duid5', 'prices30', 'scada30',
         'transmission30', 'demand30', 'rooftop30']

pytestmark = pytest.mark.skipif(not SCHEMA_SRC.exists(),
                                reason='schema source database not present')


# ------------------------------------------------------------------ fixtures

@pytest.fixture(scope='session')
def template_db(tmp_path_factory):
    p = tmp_path_factory.mktemp('schema') / 'template.duckdb'
    con = duckdb.connect(str(p))
    con.execute(f"ATTACH '{SCHEMA_SRC}' AS src (READ_ONLY)")
    for t in ALL12:
        con.execute(f'CREATE TABLE {t} AS SELECT * FROM src.{t} LIMIT 0')
    con.execute('DETACH src')
    con.close()
    return p


@pytest.fixture
def db(tmp_path, template_db, monkeypatch):
    p = tmp_path / 'scratch.duckdb'
    shutil.copy(template_db, p)
    stage = tmp_path / 'stage'
    stage.mkdir()
    for mod in (gr,):
        monkeypatch.setattr(mod, 'PRIMARY', str(p))
        monkeypatch.setattr(mod, 'REPLICA', str(p))
        monkeypatch.setattr(mod, 'STAGE', stage)
    monkeypatch.setattr(gr, '_sleep', lambda s: None)
    return p


def keys_for(table):
    spec = gr.TABLES[table]
    if 'regionid' in spec.keys:
        return REGIONS
    if 'duid' in spec.keys:
        return ['DUID1', 'DUID2']
    return ['NSW1-QLD1']


def seed(path, table, start, end, drop=(), drop_keys=()):
    """Insert key-only rows for every interval start..end (inclusive)."""
    spec = gr.TABLES[table]
    stamps = pd.date_range(start, end, freq=f'{spec.interval}min')
    kcol = spec.keys[1]
    rows = [(s, k) for s in stamps if s not in set(drop) for k in keys_for(table)
            if (s, k) not in set(drop_keys)]
    df = pd.DataFrame(rows, columns=['settlementdate', kcol])
    con = duckdb.connect(str(path))
    con.register('df', df)
    con.execute(f'INSERT INTO {table} (settlementdate, {kcol}) SELECT * FROM df')
    con.close()


def bound(table, now=NOW, lag=None):
    spec = gr.TABLES[table]
    return gr._floor(now - timedelta(minutes=spec.lag if lag is None else lag),
                     spec.interval)


def survey(path, tables=None, **kw):
    r = gr.GapRepairer(log=lambda m: None)
    return r.survey(tables, db=str(path), now=NOW, **kw)


# ------------------------------------------------------------------ survey

def test_every_table_is_configured():
    assert set(gr.TABLES) == set(ALL12)
    for name, s in gr.TABLES.items():
        assert s.interval in (5, 30) and s.lag > 0
        assert s.keys[0] == 'settlementdate'
        assert s.feed or s.derived_from, f'{name} is neither repairable nor derived'
    # the Next_Day_Dispatch tables normally trail by about a day
    assert gr.TABLES['curtailment5'].lag >= 24 * 60
    assert gr.TABLES['curtailment_duid5'].lag >= 24 * 60


@pytest.mark.parametrize('table', ALL12)
def test_complete_table_reports_nothing(db, table):
    b = bound(table)
    seed(db, table, b - timedelta(minutes=10 * gr.TABLES[table].interval), b)
    assert survey(db, [table])[table] == []


@pytest.mark.parametrize('table', ALL12)
def test_trailing_hole_detected(db, table):
    s = gr.TABLES[table]
    b = bound(table)
    step = timedelta(minutes=s.interval)
    seed(db, table, b - 12 * step, b - 3 * step)
    assert survey(db, [table])[table] == [b - 2 * step, b - step, b]


@pytest.mark.parametrize('table', ALL12)
def test_single_missing_interval_detected(db, table):
    s = gr.TABLES[table]
    b = bound(table)
    step = timedelta(minutes=s.interval)
    hole = b - 5 * step
    seed(db, table, b - 12 * step, b, drop=[hole])
    assert survey(db, [table])[table] == [hole]


@pytest.mark.parametrize('table', ALL12)
def test_normal_publication_lag_is_not_a_gap(db, table):
    s = gr.TABLES[table]
    step = timedelta(minutes=s.interval)
    newest = gr._floor(NOW - timedelta(minutes=s.lag * 0.75), s.interval)
    seed(db, table, newest - 12 * step, newest)
    assert survey(db, [table])[table] == []


@pytest.mark.parametrize('table', ALL12)
def test_lag_beyond_allowance_is_a_gap(db, table):
    s = gr.TABLES[table]
    step = timedelta(minutes=s.interval)
    newest = gr._floor(NOW - timedelta(minutes=s.lag * 1.5), s.interval)
    seed(db, table, newest - 12 * step, newest)
    assert len(survey(db, [table])[table]) >= 1


def test_curtailment_not_flagged_at_one_day_lag(db):
    # at 09:07 the newest Next_Day_Dispatch interval is 04:00 the same day
    for t in ('curtailment5', 'curtailment_duid5'):
        seed(db, t, datetime(2026, 10, 12, 4, 5), datetime(2026, 10, 13, 4, 0))
    got = survey(db, ['curtailment5', 'curtailment_duid5'])
    assert got == {'curtailment5': [], 'curtailment_duid5': []}


def test_each_table_surveyed_from_its_own_start(db):
    # a common start would flag bdu5 from 06:00, before it existed
    seed(db, 'prices5', datetime(2026, 10, 13, 6, 0), bound('prices5'))
    seed(db, 'bdu5', datetime(2026, 10, 13, 8, 0), bound('bdu5'))
    assert survey(db, ['prices5', 'bdu5']) == {'prices5': [], 'bdu5': []}


@pytest.mark.parametrize('table', [t for t in ALL12 if gr.TABLES[t].regional])
def test_missing_region_at_a_timestamp_is_a_gap(db, table):
    s = gr.TABLES[table]
    b = bound(table)
    mid = b - 4 * timedelta(minutes=s.interval)
    seed(db, table, b - timedelta(minutes=10 * s.interval), b, drop_keys=[(mid, 'SA1')])
    assert survey(db, [table])[table] == [mid]


def test_rooftop_sub_regions_do_not_count(db):
    b = bound('rooftop30')
    seed(db, 'rooftop30', b - timedelta(hours=3), b)
    con = duckdb.connect(str(db))
    con.execute("INSERT INTO rooftop30 (settlementdate, regionid) "
                f"VALUES (TIMESTAMP '{b}', 'QLDC')")
    con.close()
    assert survey(db, ['rooftop30'])['rooftop30'] == []


def test_recent_null_snsg_is_not_yet_a_gap(db):
    b = bound('demand30')
    seed(db, 'demand30', b - timedelta(hours=5), b)
    con = duckdb.connect(str(db))
    con.execute(f"UPDATE demand30 SET demand_less_snsg = 1.0 "
                f"WHERE settlementdate < TIMESTAMP '{b}'")      # newest label still NULL
    con.close()
    assert survey(db, ['demand30'])[gr.SNSG] == []


def test_null_demand_less_snsg_detected_from_first_value(db):
    b = bound('demand30')
    seed(db, 'demand30', b - timedelta(hours=5), b)
    con = duckdb.connect(str(db))
    start = b - timedelta(hours=4)
    con.execute(f"UPDATE demand30 SET demand_less_snsg = 1.0 "
                f"WHERE settlementdate >= TIMESTAMP '{start}'")
    bad = start + timedelta(hours=1)
    con.execute(f"UPDATE demand30 SET demand_less_snsg = NULL WHERE regionid = 'VIC1' "
                f"AND settlementdate = TIMESTAMP '{bad}'")
    con.close()
    got = survey(db, ['demand30'])
    # rows before the column starts (feed did not exist) are not gaps
    assert got[gr.SNSG] == [bad]
    assert got['demand30'] == []


# ------------------------------------------------------------------ fake nemweb

class Resp:
    def __init__(self, content=b'', text=''):
        self.content, self.text, self.status_code = content, text, 200

    def raise_for_status(self):
        pass


class FakeNemweb:
    """Replaces the collector's two network methods with dictionaries."""

    def __init__(self, repairer):
        self.files = {}      # full url -> bytes
        self.current = {}    # CURRENT dir url -> [filenames]
        self.archive = {}    # ARCHIVE dir url -> [filenames]
        c = repairer.collector
        c.get_latest_files = lambda url, pattern: sorted(
            n for n in self.current.get(url, []) if pattern in n)
        c._get_with_retry = self._get

    @staticmethod
    def _norm(u):
        return u.lower().replace('https://', '').replace('http://', '').replace('www.', '')

    def _get(self, url, timeout=60, attempts=3):
        n = self._norm(url)
        for k, v in self.files.items():
            if self._norm(k) == n:
                return Resp(content=v)
        for k, v in self.archive.items():
            if self._norm(k) == n:
                return Resp(text='\n'.join(v))
        raise FileNotFoundError(url)

    def add_current(self, feed, name, blob):
        d = gr.CURRENT_ROOT + gr.FEEDS[feed]['cur']
        self.current.setdefault(d, []).append(name)
        self.files[d + name] = blob

    def add_archive(self, feed, name, blob):
        d = gr.ARCHIVE_ROOT + gr.FEEDS[feed]['arc']
        self.archive.setdefault(d, []).append(name)
        self.files[d + name] = blob


def fixture_zip(name):
    return (FIX / name).read_bytes()


def outer_zip(members):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        for n, b in members.items():
            z.writestr(n, b)
    return buf.getvalue()


def one_file_zip(inner_name, text):
    return outer_zip({inner_name: text})


def rooftop_zip(interval, stamp14):
    lines = ['C,NEMP.WORLD,ROOFTOP_PV_ACTUAL_MEASUREMENT,AEMO,PUBLIC,x,x,1,DEMAND,1',
             'I,ROOFTOP,ACTUAL,2,INTERVAL_DATETIME,REGIONID,POWER,QI,TYPE,LASTCHANGED']
    for i, r in enumerate(REGIONS + ['QLDC', 'QLDN']):
        lines.append(f'D,ROOFTOP,ACTUAL,2,"{interval:%Y/%m/%d %H:%M:%S}",{r},'
                     f'{100 + i},1,MEASUREMENT,"2026/08/13 00:00:00"')
    lines.append('C,"END OF REPORT",9')
    return one_file_zip(f'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_{stamp14}_1.CSV',
                        '\n'.join(lines))


def nextday_csv(day_stamps, duids=(('WINDWF1', 40.0, 55.0, 1, 60.0),
                                   ('BIGSF1', 80.0, 100.0, 1, 100.0),
                                   ('GASPS1', 10.0, 0.0, 0, 0.0))):
    """Synthetic Next_Day_Dispatch UNIT_SOLUTION rows.

    tuple = (duid, totalcleared, availability, semidispatchcap, uigf)
    """
    lines = ['C,NEMP.WORLD,NEXT_DAY_DISPATCH,AEMO,PUBLIC,x,x,1,x,1',
             'I,DISPATCH,UNIT_SOLUTION,6,SETTLEMENTDATE,RUNNO,DUID']
    for ts in day_stamps:
        for duid, cleared, avail, cap, uigf in duids:
            p = [''] * 75
            p[0], p[1], p[2], p[3] = 'D', 'DISPATCH', 'UNIT_SOLUTION', '6'
            p[4], p[5], p[6] = f'"{ts:%Y/%m/%d %H:%M:%S}"', '1', duid
            p[14], p[36], p[59], p[68] = str(cleared), str(avail), str(cap), str(uigf)
            lines.append(','.join(p))
    lines.append('C,"END OF REPORT",1')
    return '\n'.join(lines)


@pytest.fixture
def rep(db):
    r = gr.GapRepairer(log=lambda m: None)
    return r, FakeNemweb(r)


def table_df(path, sql):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchdf()
    finally:
        con.close()


T0 = datetime(2026, 8, 13, 13, 30)
SOON = datetime(2026, 8, 13, 14, 0)         # "now" for fixtures: they are all CURRENT


# ------------------------------------------------------------------ repair: 5-minute

def load_dispatchis(fake, archive=False):
    for n in ('PUBLIC_DISPATCHIS_202608131325_0000000532431384.zip',
              'PUBLIC_DISPATCHIS_202608131330_0000000532431882.zip',
              'PUBLIC_DISPATCHIS_202608131335_0000000532432469.zip'):
        fake.add_current('dispatchis', n, fixture_zip(n))


def test_repairs_bdu5_and_regional_curtailment_from_current(db, rep):
    r, fake = rep
    load_dispatchis(fake)
    r.stage_all({'bdu5': [T0], 'curtailment_regional5': [T0]}, now=SOON)
    ins = r.apply()
    assert ins['bdu5'] == 5 and ins['curtailment_regional5'] == 5
    got = table_df(db, "SELECT * FROM curtailment_regional5 ORDER BY regionid")
    assert list(got['regionid']) == REGIONS
    assert (got['total_curtailment'] == got['solar_curtailment'] + got['wind_curtailment']).all()
    assert table_df(db, "SELECT count(*) n FROM bdu5").n[0] == 5


def test_only_tables_with_a_gap_are_staged(db, rep):
    r, fake = rep
    load_dispatchis(fake)
    r.stage_all({'prices5': [T0]}, now=SOON)
    staged = {p.name.split('_cur_')[0] for p in gr.STAGE.glob('*.parquet')}
    assert staged == {'prices5'}


def test_dispatchis_parse_matches_the_collectors_own_output(db, rep):
    """The repair parser must produce the rows the collector would have written."""
    r, fake = rep
    load_dispatchis(fake)
    c = r.collector
    url = c.current_urls['prices5']
    c.get_latest_files = lambda u, p: sorted(
        n for n in fake.current[gr.CURRENT_ROOT + 'DispatchIS_Reports/'] if p in n)
    c._get_with_retry = fake._get
    c.clear_file_cache()
    for prim, theirs in (('bdu5', c.collect_5min_bdu),
                         ('curtailment_regional5', c.collect_5min_regional_curtailment)):
        c.last_files['bdu5'] = set()
        c.last_files['curtailment_regional5'] = set()
        want = theirs().sort_values(['settlementdate', 'regionid']).reset_index(drop=True)
        frames = []
        for n in fake.current[gr.CURRENT_ROOT + 'DispatchIS_Reports/']:
            blob = fake.files[gr.CURRENT_ROOT + 'DispatchIS_Reports/' + n]
            frames.append(r._parse(r._inner_csv(blob), 'dispatchis')[prim])
        have = pd.concat(frames).drop_duplicates(['settlementdate', 'regionid']) \
            .sort_values(['settlementdate', 'regionid']).reset_index(drop=True)
        # column order is irrelevant: apply() inserts with explicit column lists
        assert set(have.columns) == set(want.columns)
        pd.testing.assert_frame_equal(have[list(want.columns)], want, check_dtype=False)


def test_prices5_scada5_transmission5_still_repaired(db, rep):
    r, fake = rep
    load_dispatchis(fake)
    for n in ('PUBLIC_DISPATCHSCADA_202608131330_0000000532431887.zip',):
        fake.add_current('scada', n, fixture_zip(n))
    r.stage_all({'prices5': [T0], 'transmission5': [T0], 'scada5': [T0]}, now=SOON)
    ins = r.apply()
    assert ins['prices5'] == 5 and ins['transmission5'] > 0 and ins['scada5'] > 0


def test_dispatchis_archive_route(db, rep):
    r, fake = rep
    members = {n: fixture_zip(n) for n in (
        'PUBLIC_DISPATCHIS_202608131325_0000000532431384.zip',
        'PUBLIC_DISPATCHIS_202608131330_0000000532431882.zip',
        'PUBLIC_DISPATCHIS_202608131335_0000000532432469.zip')}
    fake.add_archive('dispatchis', 'PUBLIC_DISPATCHIS_20260813.zip', outer_zip(members))
    r.stage_all({'bdu5': [T0]}, now=T0 + timedelta(days=30))
    assert r.apply()['bdu5'] == 5


def test_nextday_repair_matches_collector_logic(db, rep):
    r, fake = rep
    stamps = [datetime(2026, 8, 12, 4, 5), datetime(2026, 8, 12, 4, 10),
              datetime(2026, 8, 13, 4, 0)]
    blob = one_file_zip('PUBLIC_NEXT_DAY_DISPATCH_20260812_1.CSV', nextday_csv(stamps))
    fake.add_current('nextday', 'PUBLIC_NEXT_DAY_DISPATCH_20260812_0000000000000001.zip', blob)
    # the collector's own methods on the same file
    c = r.collector
    c.get_latest_files = lambda u, p: ['PUBLIC_NEXT_DAY_DISPATCH_20260812_0000000000000001.zip']
    c._get_with_retry = lambda u, timeout=60, attempts=3: Resp(content=blob)
    c.last_files['curtailment5'] = set()
    c.last_files['curtailment_duid5'] = set()
    want5 = c.collect_5min_curtailment().sort_values(['settlementdate', 'duid']).reset_index(drop=True)
    wantd = c.collect_5min_duid_curtailment().sort_values(['settlementdate', 'duid']).reset_index(drop=True)
    assert len(want5) == 6 and len(wantd) == 6   # wind + solar x 3 stamps; gas has uigf 0

    fake2 = FakeNemweb(r)
    fake2.add_current('nextday', 'PUBLIC_NEXT_DAY_DISPATCH_20260812_0000000000000001.zip', blob)
    r.stage_all({'curtailment5': stamps, 'curtailment_duid5': stamps},
                now=datetime(2026, 8, 14, 12, 0))
    ins = r.apply()
    assert ins['curtailment5'] == 6 and ins['curtailment_duid5'] == 6
    have5 = table_df(db, "SELECT * FROM curtailment5 ORDER BY settlementdate, duid")
    haved = table_df(db, "SELECT * FROM curtailment_duid5 ORDER BY settlementdate, duid")
    pd.testing.assert_frame_equal(have5, want5, check_dtype=False)
    pd.testing.assert_frame_equal(haved, wantd, check_dtype=False)
    # the 04:00 stamp of the NEXT calendar day lives in the file for trading day D
    assert datetime(2026, 8, 13, 4, 0) in set(have5['settlementdate'])


def test_nextday_file_chosen_by_trading_day(db, rep):
    r, fake = rep
    for d in (12, 13):
        stamps = [datetime(2026, 8, d, 4, 5)]
        fake.add_current('nextday', f'PUBLIC_NEXT_DAY_DISPATCH_202608{d}_1.zip',
                         one_file_zip('x.CSV', nextday_csv(stamps)))
    r.stage_all({'curtailment5': [datetime(2026, 8, 13, 4, 5)]}, now=datetime(2026, 8, 20))
    assert r.apply()['curtailment5'] == 2
    assert set(table_df(db, "SELECT DISTINCT settlementdate d FROM curtailment5").d) == \
        {pd.Timestamp('2026-08-13 04:05')}


# ------------------------------------------------------------------ repair: 30-minute

def test_demand_from_current_then_snsg_fill(db, rep):
    r, fake = rep
    for n in ('PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_202608131300_20260813130006.zip',):
        fake.add_current('demand', n, fixture_zip(n))
    n = 'PUBLIC_ACTUAL_OPERATIONAL_DEM_LESS_SNSG_HH_202608131300_20260813130006.zip'
    fake.add_current('snsg', n, fixture_zip(n))
    t = datetime(2026, 8, 13, 13, 0)
    r.stage_all({'demand30': [t]}, now=SOON)
    ins = r.apply()
    assert ins['demand30'] == 5 and ins['demand_less_snsg'] == 5
    got = table_df(db, "SELECT * FROM demand30 ORDER BY regionid")
    assert got['demand_less_snsg'].notna().all() and got['demand'].notna().all()


def test_null_snsg_is_filled_but_existing_values_are_not_overwritten(db, rep):
    r, fake = rep
    t = datetime(2026, 8, 13, 13, 0)
    seed(db, 'demand30', t, t)
    con = duckdb.connect(str(db))
    con.execute("UPDATE demand30 SET demand = 7, demand_less_snsg = 111 WHERE regionid = 'NSW1'")
    con.execute("UPDATE demand30 SET demand = 7 WHERE regionid != 'NSW1'")
    con.close()
    n = 'PUBLIC_ACTUAL_OPERATIONAL_DEM_LESS_SNSG_HH_202608131300_20260813130006.zip'
    fake.add_current('snsg', n, fixture_zip(n))
    r.stage_all({gr.SNSG: [t]}, now=SOON)
    assert r.apply()['demand_less_snsg'] == 4
    got = table_df(db, "SELECT regionid, demand, demand_less_snsg s FROM demand30 ORDER BY 1")
    assert got.set_index('regionid').loc['NSW1', 's'] == 111       # untouched
    assert got['s'].notna().all() and (got['demand'] == 7).all()   # demand untouched


def test_demand_weekly_archive_route(db, rep):
    r, fake = rep
    n = 'PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_202608131300_20260813130006.zip'
    fake.add_archive('demand', 'PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_20260813.zip',
                     outer_zip({n: fixture_zip(n)}))
    r.stage_all({'demand30': [datetime(2026, 8, 13, 13, 0)]}, now=datetime(2026, 11, 1))
    assert r.apply()['demand30'] == 5


def test_rooftop_current_file_is_stamped_after_its_interval(db, rep):
    r, fake = rep
    t = datetime(2026, 8, 13, 13, 0)
    fake.add_current('rooftop', 'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20260813133000_0000000000000001.zip',
                     rooftop_zip(t, '20260813133000'))
    r.stage_all({'rooftop30': [t]}, now=SOON)
    assert r.apply()['rooftop30'] == 5               # main regions only, QLDC/QLDN dropped
    got = table_df(db, "SELECT * FROM rooftop30")
    assert set(got['regionid']) == set(REGIONS)
    assert set(got['type']) == {'MEASUREMENT'}


def test_rooftop_weekly_archive_route(db, rep):
    r, fake = rep
    t = datetime(2026, 8, 13, 13, 0)
    inner = {'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20260813133000_1.zip':
             rooftop_zip(t, '20260813133000')}
    fake.add_archive('rooftop', 'PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_20260813.zip',
                     outer_zip(inner))
    r.stage_all({'rooftop30': [t]}, now=datetime(2026, 11, 1))
    assert r.apply()['rooftop30'] == 5


def test_unreachable_interval_stages_nothing(db, rep):
    r, fake = rep
    r.stage_all({'rooftop30': [datetime(2024, 8, 1, 12, 0)]}, now=NOW)
    assert list(gr.STAGE.glob('*.parquet')) == []
    assert r.apply()['rooftop30'] == 0


def test_tables_without_a_feed_are_survey_only(db, rep):
    r, fake = rep
    seed(db, 'prices30', datetime(2026, 8, 13, 12, 0), datetime(2026, 8, 13, 12, 0))
    r.stage_all({'prices30': [datetime(2026, 8, 13, 12, 30)]}, now=SOON)
    assert list(gr.STAGE.glob('*.parquet')) == []


# ------------------------------------------------------------------ anti-join

def test_apply_never_deletes_or_changes_existing_rows(db, rep):
    r, fake = rep
    # a good bdu5 row for NSW1 with a value the staged file would contradict
    con = duckdb.connect(str(db))
    con.execute(f"INSERT INTO bdu5 VALUES (TIMESTAMP '{T0}', 'NSW1', 123.0, 4.0, 5.0)")
    # and a neighbour interval the staged range spans, which must survive
    con.execute("INSERT INTO bdu5 VALUES (TIMESTAMP '2026-08-13 13:00:00', 'NSW1', 9.0, 9.0, 9.0)")
    con.execute("INSERT INTO bdu5 VALUES (TIMESTAMP '2026-08-13 14:00:00', 'QLD1', 8.0, 8.0, 8.0)")
    con.close()
    load_dispatchis(fake)
    r.stage_all({'bdu5': [T0]}, now=SOON)
    ins = r.apply()
    assert ins['bdu5'] == 4                           # four regions absent, NSW1 kept
    got = table_df(db, "SELECT * FROM bdu5").set_index(['settlementdate', 'regionid'])
    assert got.loc[(pd.Timestamp(T0), 'NSW1'), 'bdu_energy_storage'] == 123.0
    assert got.loc[(pd.Timestamp('2026-08-13 13:00:00'), 'NSW1'), 'bdu_energy_storage'] == 9.0
    assert got.loc[(pd.Timestamp('2026-08-13 14:00:00'), 'QLD1'), 'bdu_energy_storage'] == 8.0
    assert len(got) == 7
    # idempotent
    r2 = gr.GapRepairer(log=lambda m: None)
    assert r2.apply()['bdu5'] == 0


# ------------------------------------------------------------------ 30-minute derived

def test_nem_now_is_utc_plus_10_whatever_the_host_zone():
    from datetime import timezone
    want = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=10)
    assert abs((gr.nem_now() - want).total_seconds()) < 5


def test_label_is_interval_end():
    assert gr.label_of(pd.Timestamp('2026-01-01 12:05')) == pd.Timestamp('2026-01-01 12:30')
    assert gr.label_of(pd.Timestamp('2026-01-01 12:30')) == pd.Timestamp('2026-01-01 12:30')
    assert gr.label_of(pd.Timestamp('2026-01-01 12:35')) == pd.Timestamp('2026-01-01 13:00')
    assert gr.label_of(pd.Timestamp('2026-01-01 13:00')) == pd.Timestamp('2026-01-01 13:00')
    assert gr.label_of(pd.Timestamp('2026-01-01 00:05')) == pd.Timestamp('2026-01-01 00:30')


def put_prices5(path, stamps_values, region='NSW1'):
    con = duckdb.connect(str(path))
    for ts, v in stamps_values:
        con.execute(f"INSERT INTO prices5 VALUES (TIMESTAMP '{ts}', '{region}', {v})")
    con.close()


def test_prices30_rebuilt_with_end_label_bucket(db, rep):
    r, fake = rep
    base = datetime(2026, 8, 13, 12, 0)
    present = [(base + timedelta(minutes=m), float(i))
               for i, m in enumerate((10, 15, 20, 25, 30), start=2)]          # 12:10..12:30
    present.append((base + timedelta(minutes=35), 1000.0))                    # next bucket
    put_prices5(db, present)
    # the missing 12:05 interval comes in through staging
    pd.DataFrame({'settlementdate': [pd.Timestamp(base + timedelta(minutes=5))],
                  'regionid': ['NSW1'], 'rrp': [1.0]}).to_parquet(gr.STAGE / 'prices5_x.parquet')
    # a stale partial label the collector could have written
    con = duckdb.connect(str(db))
    con.execute("INSERT INTO prices30 VALUES (TIMESTAMP '2026-08-13 12:30:00', 'NSW1', -1.0)")
    con.close()
    ins = r.apply()
    assert ins['prices5'] == 1
    got = table_df(db, "SELECT * FROM prices30").set_index('settlementdate')['rrp']
    assert got[pd.Timestamp('2026-08-13 12:30')] == pytest.approx(3.5)   # mean of 1..6
    assert pd.Timestamp('2026-08-13 13:00') not in got.index              # only 1 of 6 intervals


def test_derived_label_needs_all_six_intervals(db, rep):
    r, fake = rep
    base = datetime(2026, 8, 13, 12, 0)
    put_prices5(db, [(base + timedelta(minutes=m), 1.0) for m in (10, 15, 20, 25, 30)])
    pd.DataFrame({'settlementdate': [pd.Timestamp(base + timedelta(minutes=10))],
                  'regionid': ['QLD1'], 'rrp': [1.0]}).to_parquet(gr.STAGE / 'prices5_x.parquet')
    r.apply()
    assert table_df(db, "SELECT count(*) n FROM prices30").n[0] == 0


def test_absent_derived_label_rebuilt_by_antijoin_only(db, rep):
    r, fake = rep
    base = datetime(2026, 8, 13, 12, 0)
    put_prices5(db, [(base + timedelta(minutes=m), float(m)) for m in range(5, 31, 5)])
    put_prices5(db, [(base + timedelta(minutes=m), 50.0) for m in range(35, 61, 5)])
    con = duckdb.connect(str(db))
    con.execute("INSERT INTO prices30 VALUES (TIMESTAMP '2026-08-13 13:00:00', 'NSW1', 777.0)")
    con.close()
    both = {'prices30': [datetime(2026, 8, 13, 12, 30), datetime(2026, 8, 13, 13, 0)]}
    ins = r.apply(gaps=both)
    assert ins['prices30'] == 1                                          # only 12:30 was absent
    got = table_df(db, "SELECT * FROM prices30").set_index('settlementdate')['rrp']
    assert got[pd.Timestamp('2026-08-13 12:30')] == pytest.approx(17.5)
    assert got[pd.Timestamp('2026-08-13 13:00')] == 777.0                # existing row untouched


def test_scada30_replace_keeps_keys_that_cannot_be_rebuilt(db, rep):
    r, fake = rep
    base = datetime(2026, 8, 13, 12, 0)
    con = duckdb.connect(str(db))
    for m in range(10, 31, 5):                    # DUID1 has 12:10..12:30
        con.execute(f"INSERT INTO scada5 VALUES (TIMESTAMP '{base + timedelta(minutes=m)}', 'DUID1', 10)")
    for m in range(5, 31, 5):
        con.execute(f"INSERT INTO scada5 VALUES (TIMESTAMP '{base + timedelta(minutes=m)}', 'DUID2', 20)")
    # collector-style partial rows already in scada30 for both units
    con.execute("INSERT INTO scada30 VALUES (TIMESTAMP '2026-08-13 12:30:00', 'DUID1', 10.0)")
    con.execute("INSERT INTO scada30 VALUES (TIMESTAMP '2026-08-13 12:30:00', 'DUID2', 19.0)")
    con.close()
    pd.DataFrame({'settlementdate': [pd.Timestamp(base + timedelta(minutes=5))],
                  'duid': ['DUID1'], 'scadavalue': [10.0]}).to_parquet(gr.STAGE / 'scada5_x.parquet')
    r.apply()
    got = table_df(db, "SELECT duid, scadavalue FROM scada30 ORDER BY duid").set_index('duid').scadavalue
    assert got['DUID1'] == pytest.approx(10.0)        # now built from six intervals
    assert got['DUID2'] == pytest.approx(19.0)        # no new DUID2 interval: row not rewritten


# ------------------------------------------------------------------ lock handling

class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_acquire_retries_until_the_lock_frees():
    clock, calls = FakeClock(), []

    def connect(path):
        calls.append(path)
        if len(calls) < 4:
            raise duckdb.IOException('Could not set lock on file')
        return 'CON'

    assert gr.acquire_primary(timeout=60, poll=3, sleep=clock.sleep, clock=clock,
                              connect=connect, settled=lambda: True) == 'CON'
    assert len(calls) == 4


def test_acquire_times_out_instead_of_stopping_anything():
    clock = FakeClock()

    def connect(path):
        raise duckdb.IOException('Could not set lock on file')

    with pytest.raises(gr.LockTimeout):
        gr.acquire_primary(timeout=30, poll=3, sleep=clock.sleep, clock=clock,
                           connect=connect, settled=lambda: True)


def test_acquire_does_not_connect_while_the_replica_copy_is_pending():
    clock, calls = FakeClock(), []
    state = {'n': 0}

    def settled():
        state['n'] += 1
        return state['n'] > 5

    def connect(path):
        calls.append(1)
        return 'CON'

    gr.acquire_primary(timeout=60, poll=3, sleep=clock.sleep, clock=clock,
                       connect=connect, settled=settled)
    assert state['n'] > 5 and len(calls) == 1


def test_replica_settled_follows_copy_state(tmp_path):
    p, rp = tmp_path / 'p.duckdb', tmp_path / 'r.duckdb'
    p.write_bytes(b'a')
    rp.write_bytes(b'a')
    shutil.copystat(p, rp)
    assert gr.replica_settled(p, rp)
    os.utime(p, (p.stat().st_atime, p.stat().st_mtime + 100))     # collector wrote
    assert not gr.replica_settled(p, rp)
    shutil.copystat(p, rp)
    (tmp_path / 'r.duckdb.tmp').write_bytes(b'x')                 # copy in flight
    assert not gr.replica_settled(p, rp)


# ------------------------------------------------------------------ collector detection

PS = """\
  101 /sbin/launchd
  202 -zsh
  303 ssh davidleitch@192.168.68.71 cd /x && .venv/bin/python -m aemo_updater.collectors.unified_collector_duckdb
  404 /bin/zsh -c .venv/bin/python scripts/weekly_integrity.py # unified_collector_duckdb
  505 bash -c cd repo && .venv/bin/python -m aemo_updater.collectors.unified_collector_duckdb
  606 grep unified_collector_duckdb
  707 .venv/bin/python -m aemo_updater.collectors.unified_collector_duckdb
  808 /Library/Frameworks/Python.framework/Versions/3.12/Resources/Python.app/Contents/MacOS/Python -m aemo_updater.collectors.unified_collector_duckdb
  909 .venv/bin/python scripts/weekly_integrity.py --check-only
"""


def test_collector_pids_match_only_the_collector_process():
    assert wi.collector_pids(PS) == [707, 808]


def test_collector_pids_ignore_unrelated_processes():
    no_collector = "\n".join(l for l in PS.splitlines()
                             if not l.strip().startswith(('707', '808')))
    assert wi.collector_pids(no_collector) == []


def test_collector_pids_exclude_given_pids():
    assert wi.collector_pids(PS, exclude={707}) == [808]


def test_collector_running_ignores_this_scripts_own_ancestry(monkeypatch):
    me = os.getpid()
    ps = f"{me} .venv/bin/python -m aemo_updater.collectors.unified_collector_duckdb\n"
    monkeypatch.setattr(wi.subprocess, 'run', lambda cmd, **k: type(
        'R', (), {'stdout': ps, 'returncode': 0})())
    assert wi.collector_running() is False


# ------------------------------------------------------------------ stop / start

def test_failed_stop_never_sends_the_start_command():
    sent = []
    with pytest.raises(wi.CollectorWontStop):
        wi.run_with_collector_stopped(
            lambda: sent.append('work'),
            stop=lambda: False, running=lambda: True, start=lambda: sent.append('START'))
    assert sent == []


def test_failed_stop_restarts_only_if_the_interrupt_landed_late():
    sent, state = [], iter([True, False])         # running at first look, gone afterwards
    with pytest.raises(wi.CollectorWontStop):
        wi.run_with_collector_stopped(
            lambda: sent.append('work'),
            stop=lambda: False, running=lambda: next(state), start=lambda: sent.append('START'))
    assert sent == ['START']


def test_successful_stop_restarts_after_the_work_even_on_error():
    sent = []

    def work():
        sent.append('work')
        raise RuntimeError('boom')

    with pytest.raises(RuntimeError):
        wi.run_with_collector_stopped(work, stop=lambda: True, running=lambda: True,
                                      start=lambda: sent.append('START'))
    assert sent == ['work', 'START']


def test_collector_that_was_not_running_is_left_alone():
    sent = []
    wi.run_with_collector_stopped(lambda: sent.append('work'), stop=lambda: sent.append('STOP'),
                                  running=lambda: False, start=lambda: sent.append('START'))
    assert sent == ['work']


def test_stop_waits_longer_than_one_collector_cycle(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(wi.subprocess, 'run', lambda *a, **k: None)
    monkeypatch.setattr(wi.time, 'time', clock)
    monkeypatch.setattr(wi.time, 'sleep', clock.sleep)
    monkeypatch.setattr(wi, 'collector_running', lambda: clock.t < 270)   # a 4.5 min cycle
    assert wi.stop_collector() is True


# ------------------------------------------------------------------ ledger and email

def test_ledger_round_trip_and_exclusion(tmp_path):
    p = tmp_path / 'ledger.json'
    a, b = datetime(2024, 8, 1, 12, 0), datetime(2024, 8, 1, 12, 30)
    wi.save_ledger({'rooftop30': {a, b}}, p)
    led = wi.load_ledger(p)
    assert led == {'rooftop30': {a, b}}
    got = wi.exclude_known({'rooftop30': [a, b, datetime(2026, 1, 1)], 'bdu5': [a]}, led)
    assert got == {'rooftop30': [datetime(2026, 1, 1)], 'bdu5': [a]}
    assert wi.load_ledger(tmp_path / 'absent.json') == {}


def test_unreachable_excludes_the_seam():
    now = datetime(2026, 10, 13, 4, 17)
    got = wi.unreachable({'bdu5': [now - timedelta(days=1), now - timedelta(days=9)]}, now)
    assert got == {'bdu5': {now - timedelta(days=9)}}


@pytest.mark.parametrize('day,before,after,policy,send', [
    (datetime(2026, 10, 11), 0, 0, 'auto', True),       # Sunday, all clean: full report
    (datetime(2026, 10, 11), 5, 0, 'auto', True),
    (datetime(2026, 10, 13), 0, 0, 'auto', False),      # Tuesday, nothing to say
    (datetime(2026, 10, 13), 3, 0, 'auto', True),       # missing before, all repaired
    (datetime(2026, 10, 13), 0, 2, 'auto', True),       # remains after
    (datetime(2026, 10, 13), 0, 0, 'always', True),
    (datetime(2026, 10, 11), 4, 4, 'never', False),
])
def test_email_decision(day, before, after, policy, send):
    assert wi.email_decision(day, before, after, policy) is send


class StubRepairer:
    gaps = {}

    def __init__(self, log=print):
        pass

    def survey(self, *a, **k):
        return {t: list(self.gaps.get(t, [])) for t in wi.TABLES}

    def clear_stage(self):
        pass

    def stage_all(self, gaps):
        pass


def run_main(monkeypatch, tmp_path, now, gaps, *argv, cls=None):
    sent = []
    cls = cls or StubRepairer
    cls.gaps = gaps
    monkeypatch.setattr(wi, 'GapRepairer', cls)
    monkeypatch.setattr(wi, 'LEDGER_PATH', str(tmp_path / 'ledger.json'))
    monkeypatch.setattr(wi, 'LOG_PATH', str(tmp_path / 'log.txt'))
    monkeypatch.setattr(wi, 'STAGE', tmp_path / 'emptystage')
    (tmp_path / 'emptystage').mkdir(exist_ok=True)
    for f in (tmp_path / 'emptystage').glob('*.parquet'):
        f.unlink()
    monkeypatch.setattr(wi, 'send_email', lambda s, h: sent.append(s) or True)
    wi.main(list(argv), now=now)
    return sent


def test_main_tuesday_clean_sends_nothing(monkeypatch, tmp_path):
    assert run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17), {}) == []


def test_main_sunday_clean_sends_full_report(monkeypatch, tmp_path):
    sent = run_main(monkeypatch, tmp_path, datetime(2026, 10, 11, 4, 17), {})
    assert sent == ['NEM data integrity: all complete']


def test_main_tuesday_with_gaps_sends(monkeypatch, tmp_path):
    gap = {'bdu5': [datetime(2026, 10, 12, 20, 0)]}
    assert len(run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17), gap)) == 1


def test_main_known_unreachable_gaps_do_not_alarm_daily(monkeypatch, tmp_path):
    old = datetime(2024, 8, 1, 12, 0)
    wi.save_ledger({'rooftop30': {old}}, tmp_path / 'ledger.json')
    sent = run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17), {'rooftop30': [old]})
    assert sent == []
    # ... but the Sunday run retries them and reports
    sent = run_main(monkeypatch, tmp_path, datetime(2026, 10, 11, 4, 17), {'rooftop30': [old]})
    assert len(sent) == 1


def test_main_check_only_never_promotes_gaps_to_the_ledger(monkeypatch, tmp_path):
    old = datetime(2024, 8, 1, 12, 0)
    run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17), {'rooftop30': [old]},
             '--check-only')
    assert wi.load_ledger(tmp_path / 'ledger.json') == {}


def test_main_nothing_stageable_promotes_old_gaps_and_sends_one_followup(monkeypatch, tmp_path):
    old = datetime(2024, 8, 1, 12, 0)
    sent = run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17), {'rooftop30': [old]})
    assert len(sent) == 2 and 'FOLLOW-UP' in sent[1]
    assert wi.load_ledger(tmp_path / 'ledger.json') == {'rooftop30': {old}}
    # the next day it is quiet
    assert run_main(monkeypatch, tmp_path, datetime(2026, 10, 14, 4, 17),
                    {'rooftop30': [old]}) == []


class ApplyingStub(StubRepairer):
    """Repairs one of two gaps; the other stays missing."""

    def stage_all(self, gaps):
        pd.DataFrame({'x': [1]}).to_parquet(wi.STAGE / 'bdu5_x.parquet')

    def apply(self, con=None, gaps=None):
        return {'bdu5': 1}

    def survey(self, *a, con=None, **k):
        if con is None:
            return {t: list(self.gaps.get(t, [])) for t in wi.TABLES}
        return {t: ([] if t == 'bdu5' else list(self.gaps.get(t, []))) for t in wi.TABLES}


class FakeCon:
    closed = False

    def close(self):
        self.closed = True


def test_main_apply_path_uses_the_lock_and_records_what_stays_missing(monkeypatch, tmp_path):
    old = datetime(2024, 8, 1, 12, 0)
    recent = datetime(2026, 10, 12, 20, 0)
    con = FakeCon()
    started = []
    monkeypatch.setattr(wi, 'acquire_primary', lambda **k: con)
    monkeypatch.setattr(wi, 'start_collector', lambda: started.append(1))
    monkeypatch.setattr(wi, 'run_with_collector_stopped',
                        lambda f: started.append('stopped') or f())
    sent = run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17),
                    {'bdu5': [recent], 'rooftop30': [old]}, cls=ApplyingStub)
    assert con.closed and started == []                 # lock path: collector never touched
    assert wi.load_ledger(tmp_path / 'ledger.json') == {'rooftop30': {old}}
    assert len(sent) == 2 and 'FOLLOW-UP' in sent[1]


def test_main_stop_collector_flag_uses_the_stop_start_route(monkeypatch, tmp_path):
    started = []
    monkeypatch.setattr(wi, 'run_with_collector_stopped',
                        lambda f: started.append('stopped') or f())
    run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17),
             {'bdu5': [datetime(2026, 10, 12, 20, 0)]}, '--stop-collector', cls=ApplyingStub)
    assert started == ['stopped']


def test_main_lock_timeout_leaves_the_ledger_alone(monkeypatch, tmp_path):
    def boom(**k):
        raise wi.LockTimeout('held')
    monkeypatch.setattr(wi, 'acquire_primary', boom)
    run_main(monkeypatch, tmp_path, datetime(2026, 10, 13, 4, 17),
             {'rooftop30': [datetime(2024, 8, 1, 12, 0)]}, cls=ApplyingStub)
    assert wi.load_ledger(tmp_path / 'ledger.json') == {}
