"""Tests for aemo_updater.duid_registry. Synthetic data only: no network, no real DB."""
import datetime as dt

import duckdb
import numpy as np
import pandas as pd
import pytest

from aemo_updater import duid_registry as dr

REG_COLS = ['DUID', 'Station Name', 'Participant', 'Region', 'Dispatch Type',
            'Fuel Source - Primary', 'Fuel Source - Descriptor',
            'Technology Type - Primary', 'Technology Type - Descriptor',
            'Reg Cap generation (MW)', 'Max Cap generation (MW)',
            'Maximum storage capacity ']


def reg_raw(rows):
    """Raw registration sheet as read from Excel; rows are dicts keyed by short names."""
    out = []
    for r in rows:
        out.append([r.get('duid'), r.get('station', 'Stn'), r.get('part', 'Part Pty Ltd'),
                    r.get('region', 'NSW1'), r.get('dtype', 'Generator'),
                    r.get('fuel', 'Solar'), r.get('fueld', 'Solar'),
                    r.get('tech', 'Photovoltaic'), r.get('techd', 'Photovoltaic Flat Panel'),
                    r.get('regcap', 100.0), r.get('maxcap', 100.0), r.get('storage', np.nan)])
    return pd.DataFrame(out, columns=REG_COLS)


def mms_summary(rows):
    return pd.DataFrame(rows, columns=['DUID', 'START_DATE', 'REGIONID', 'DISPATCHTYPE',
                                       'STATIONID', 'PARTICIPANTID'])


def mms_detail(rows):
    return pd.DataFrame(rows, columns=['DUID', 'EFFECTIVEDATE', 'VERSIONNO', 'REGISTEREDCAPACITY',
                                       'MAXCAPACITY', 'MAXSTORAGECAPACITY'])


def make_ref(reg_rows=(), summary_rows=(), detail_rows=()):
    reg = dr.parse_registration(reg_raw(list(reg_rows))) if reg_rows else None
    ds = dr.latest_dudetailsummary(mms_summary(list(summary_rows))) if summary_rows else None
    dd = dr.latest_dudetail(mms_detail(list(detail_rows))) if detail_rows else None
    return dr.build_reference(reg, ds, dd)


def mapping_df(rows):
    return pd.DataFrame(rows, columns=['region', 'site name', 'owner', 'duid', 'capacity_mw',
                                       'storage_mwh', 'fuel'])


# ------------------------------------------------------------------ fuel mapping

@pytest.mark.parametrize('primary,desc,tech,dtype,expected', [
    ('Battery Storage', 'Battery Storage', 'Battery', None, 'Battery Storage'),
    ('Solar', 'Solar', 'Photovoltaic', None, 'Solar'),
    ('Wind', 'Wind', 'Wind Turbine', None, 'Wind'),
    ('Hydro', 'Hydro', 'Hydro - Gravity', 'GENERATOR', 'Water'),
    ('Hydro', 'Hydro', 'Pumped Hydro', 'LOAD', dr.PUMP_LOAD_FUEL),
    ('Fossil', 'Black Coal', 'Steam Sub Critical', None, 'Coal'),
    ('Fossil', 'Brown Coal', 'Steam Sub Critical', None, 'Coal'),
    ('Fossil', 'Natural Gas / Fuel Oil', 'Combined Cycle Gas Turbine (CCGT)', None, 'CCGT'),
    ('Fossil', 'Natural Gas', 'Open Cycle Gas Turbine (OCGT)', None, 'OCGT'),
    ('Fossil', 'Natural Gas', 'OCGT', None, 'OCGT'),
    ('Fossil', 'Diesel', 'Reciprocating Engine', None, 'Other'),
    ('Fossil', 'Natural Gas', 'Steam Sub Critical', None, 'Gas other'),
    ('Fossil', 'Coal Seam Methane', 'Reciprocating Engine', None, 'Gas other'),
    ('Fossil', 'Waste Coal Mine Gas', 'Reciprocating Engine', None, 'Gas other'),
    ('Renewable/ Biomass / Waste', 'Bagasse', 'Steam', None, 'Biomass'),
    ('Other', 'Whatever', 'Whatever', None, 'Other'),
    (None, None, None, None, 'Other'),
])
def test_map_fuel(primary, desc, tech, dtype, expected):
    assert dr.map_fuel(primary, desc, tech, dtype) == expected


def test_pump_load_fuel_is_null():
    assert dr.PUMP_LOAD_FUEL is None


# ------------------------------------------------------------------ owner cleaning

@pytest.mark.parametrize('raw,expected', [
    ('Foo Pty Ltd as trustee for the Foo Trust', 'Foo Pty Ltd'),
    ('Foo Pty Ltd AS THE TRUSTEE of Foo', 'Foo Pty Ltd'),
    ('Foo Pty Ltd in its capacity as trustee', 'Foo Pty Ltd'),
    ('Bar Limited (ACN 123 456 789)', 'Bar Limited'),
    ('Bar Limited (ABN 12 345)', 'Bar Limited'),
    ('Bar Limited ACN 123', 'Bar Limited'),
    ('Baz Energy,', 'Baz Energy'),
    ('Plain Name Pty Ltd', 'Plain Name Pty Ltd'),
    ('  ', None),
    (None, None),
    (np.nan, None),
])
def test_clean_owner(raw, expected):
    assert dr.clean_owner(raw) == expected


# ------------------------------------------------------------------ source parsing

MMS_CSV = '\n'.join([
    'C,NEMP.WORLD,DUDETAILSUMMARY,AEMO,PUBLIC',
    'I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,5,DUID,START_DATE,END_DATE,DISPATCHTYPE,REGIONID,STATIONID,PARTICIPANTID',
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,5,"AAA1","2020/01/01 00:00:00","2021/01/01 00:00:00",GENERATOR,NSW1,ST1,P1',
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,5,"AAA1","2021/01/01 00:00:00","2999/12/31 00:00:00",GENERATOR,VIC1,ST1,P1',
    'D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,5,"PMP1","2020/01/01 00:00:00","2999/12/31 00:00:00",LOAD,QLD1,ST2,P2',
    'C,END OF REPORT,6',
])

DETAIL_CSV = '\n'.join([
    'I,PARTICIPANT_REGISTRATION,DUDETAIL,3,DUID,EFFECTIVEDATE,VERSIONNO,REGISTEREDCAPACITY,MAXCAPACITY,MAXSTORAGECAPACITY',
    'D,PARTICIPANT_REGISTRATION,DUDETAIL,3,"AAA1","2020/01/01 00:00:00",2,50,55,',
    'D,PARTICIPANT_REGISTRATION,DUDETAIL,3,"AAA1","2021/01/01 00:00:00",9,60,65,',
    'D,PARTICIPANT_REGISTRATION,DUDETAIL,3,"AAA1","2021/01/01 00:00:00",10,70,75,12.5',
])


def test_parse_mms_csv_header_and_rows():
    df = dr.parse_mms_csv(MMS_CSV)
    assert list(df.columns)[:3] == ['DUID', 'START_DATE', 'END_DATE']
    assert len(df) == 3
    assert set(df.DUID) == {'AAA1', 'PMP1'}


def test_dudetailsummary_latest_start_date_wins():
    s = dr.latest_dudetailsummary(dr.parse_mms_csv(MMS_CSV)).set_index('DUID')
    assert s.loc['AAA1', 'REGIONID'] == 'VIC1'
    assert s.loc['PMP1', 'DISPATCHTYPE'] == 'LOAD'
    assert len(s) == 2


def test_dudetail_latest_effective_then_numeric_version():
    d = dr.latest_dudetail(dr.parse_mms_csv(DETAIL_CSV)).set_index('DUID')
    # version 10 beats 9 numerically (string sort would pick 9)
    assert d.loc['AAA1', 'REGISTEREDCAPACITY'] == 70
    assert d.loc['AAA1', 'MAXSTORAGECAPACITY'] == 12.5


def test_parse_registration_cleans_and_aggregates():
    raw = reg_raw([
        dict(duid='X1', regcap=40.0, maxcap=45.0, storage=10.0),
        dict(duid='X1', regcap=60.0, maxcap=55.0, storage=30.0),
        dict(duid='-'), dict(duid=None), dict(duid='  '),
        dict(duid=' Y1 ', regcap='1,200', maxcap=1200.0),
    ])
    reg = dr.parse_registration(raw).set_index('duid')
    assert set(reg.index) == {'X1', 'Y1'}
    assert reg.loc['X1', 'r_regcap'] == 100.0
    assert reg.loc['X1', 'r_maxcap'] == 100.0
    assert reg.loc['X1', 'r_storage'] == 30.0
    assert reg.loc['Y1', 'r_regcap'] == 1200.0


def test_candidate_months_and_urls():
    months = dr.candidate_months(dt.date(2026, 10, 8), 4)
    assert months[0] == (2026, 10) and months[-1] == (2026, 6) and len(months) == 5
    assert dr.candidate_months(dt.date(2026, 2, 3), 4)[-1] == (2025, 10)
    url = dr.mmsdm_url(2026, 9, 'DUDETAIL')
    assert 'MMSDM_2026_09' in url and 'PUBLIC_ARCHIVE%23DUDETAIL%23FILE01%23202609010000.zip' in url


# ------------------------------------------------------------------ plan builder

TODAY_REF = dict(
    reg_rows=[
        dict(duid='NEW1', station='New Solar Farm', part='Newco Pty Ltd as trustee for Newco Trust',
             region='NSW1', fuel='Solar', regcap=120.0, maxcap=125.0),
        dict(duid='BLNK1', station='Blank Wind', part='Blankco (ACN 1)', region='SA1',
             fuel='Wind', fueld='Wind', regcap=90.0, maxcap=95.0),
        dict(duid='MLB01', station='Melb Batt', part='Melco', region='NSW1', fuel='Battery Storage',
             fueld='Battery Storage', regcap=200.0, maxcap=210.0, storage=400.0),
        dict(duid='ACDC1', station='Acdc', part='Acdc Co', region='VIC1', fuel='Solar',
             regcap=100.0, maxcap=100.0),
        dict(duid='PMP1', station='Pump Station', part='Pumpco', region='QLD1', fuel='Hydro',
             fueld='Hydro', tech='Hydro', techd='Pumped Hydro', regcap=250.0, maxcap=250.0),
    ],
    summary_rows=[
        ('NEW1', '2020/01/01 00:00:00', 'NSW1', 'GENERATOR', 'S1', 'PA'),
        ('MLB01', '2020/01/01 00:00:00', 'VIC1', 'BIDIRECTIONAL', 'S2', 'PB'),
        ('PMP1', '2020/01/01 00:00:00', 'QLD1', 'LOAD', 'S3', 'PC'),
        ('MMSONLY', '2020/01/01 00:00:00', 'TAS1', 'BIDIRECTIONAL', 'STX', 'PD'),
        ('MMSLOAD', '2020/01/01 00:00:00', 'TAS1', 'LOAD', 'STY', 'PD'),
    ],
    detail_rows=[
        ('MMSONLY', '2020/01/01 00:00:00', 1, 30, 31, 60),
        ('MMSLOAD', '2020/01/01 00:00:00', 1, 0, 0, 0),
    ],
)


def plan_for(mapping_rows, active, **kw):
    ref = make_ref(**TODAY_REF)
    return dr.build_plan(mapping_df(mapping_rows), ref, set(active), **kw)


def edits_of(plan, duid):
    return {(e['field']): e for e in plan.edits if e['duid'] == duid}


def test_insert_active_unmapped_duid():
    plan = plan_for([], ['NEW1'])
    assert [r['duid'] for r in plan.inserts] == ['NEW1']
    row = plan.inserts[0]
    assert row['region'] == 'NSW1'
    assert row['site name'] == 'New Solar Farm'
    assert row['owner'] == 'Newco Pty Ltd'
    assert row['capacity_mw'] == 120.0          # Reg Cap, not Max Cap, for non-battery
    assert row['storage_mwh'] == 0.0
    assert row['fuel'] == 'Solar'
    assert {e['action'] for e in plan.edits} == {'insert'}


def test_inactive_unmapped_duid_not_inserted():
    plan = plan_for([], [])
    assert plan.inserts == []


def test_battery_insert_uses_max_cap_and_registration_storage_and_mms_region():
    plan = plan_for([], ['MLB01'])
    row = plan.inserts[0]
    assert row['fuel'] == 'Battery Storage'
    assert row['capacity_mw'] == 210.0
    assert row['storage_mwh'] == 400.0
    assert row['region'] == 'VIC1'              # MMS beats registration NSW1


def test_pump_load_insert_gets_pump_fuel_and_zero_capacity():
    plan = plan_for([], ['PMP1'])
    row = plan.inserts[0]
    assert row['fuel'] is dr.PUMP_LOAD_FUEL and row['fuel'] is None
    assert row['capacity_mw'] == 0.0
    assert 'PMP1' not in [u['duid'] for u in plan.unclassified]   # NULL fuel is intended


def test_mms_only_bidirectional_becomes_battery_and_load_is_unclassified():
    plan = plan_for([], ['MMSONLY', 'MMSLOAD'])
    by = {r['duid']: r for r in plan.inserts}
    assert by['MMSONLY']['fuel'] == 'Battery Storage'
    assert by['MMSONLY']['capacity_mw'] == 31.0     # MMS MAXCAPACITY for battery
    assert by['MMSONLY']['storage_mwh'] == 60.0
    assert by['MMSONLY']['region'] == 'TAS1'
    assert by['MMSONLY']['site name'].startswith('[auto-classified')
    assert 'MMSLOAD' not in by
    assert 'MMSLOAD' in [u['duid'] for u in plan.unclassified]


def test_skip_rt_and_dg_aggregates():
    ref = make_ref(**TODAY_REF)
    plan = dr.build_plan(mapping_df([]), ref, {'RT_NSW1', 'DG_X', 'NEW1'})
    assert [r['duid'] for r in plan.inserts] == ['NEW1']
    assert plan.unfound == []


def test_fill_blank_region_only_touches_blank_fields():
    m = [('', 'Kept Name', 'Kept Owner', 'BLNK1', 77.0, 0.0, 'Wind')]
    plan = plan_for(m, ['BLNK1'])
    e = edits_of(plan, 'BLNK1')
    assert set(e) == {'region'}
    assert e['region']['new'] == 'SA1' and e['region']['action'] == 'fill'
    assert plan.fills['BLNK1'] == {'region': 'SA1'}
    assert plan.conflicts == []   # capacity 77 is within the band, name/owner untouched


def test_fill_zero_capacity_and_placeholder_name_and_owner():
    m = [('SA1', '[auto-classified 2026-10-06]', '', 'BLNK1', 0.0, 0.0, 'Wind')]
    plan = plan_for(m, [])        # fill applies to all rows, active or not
    f = plan.fills['BLNK1']
    assert f == {'site name': 'Blank Wind', 'owner': 'Blankco', 'capacity_mw': 90.0}


def test_fill_battery_storage_when_blank():
    m = [('VIC1', 'Melb Batt', 'Melco', 'MLB01', 210.0, 0.0, 'Battery Storage')]
    plan = plan_for(m, [])
    assert plan.fills['MLB01'] == {'storage_mwh': 400.0}


def test_pump_load_zero_capacity_is_not_filled():
    m = [('QLD1', 'Pump Station', 'Pumpco', 'PMP1', 0.0, 0.0, dr.PUMP_LOAD_FUEL)]
    plan = plan_for(m, ['PMP1'])
    assert 'PMP1' not in plan.fills
    # also when the mapping still labels the pump 'Water' (MMS says LOAD)
    m = [('QLD1', 'Pump Station', 'Pumpco', 'PMP1', 0.0, 0.0, 'Water')]
    assert 'PMP1' not in plan_for(m, ['PMP1']).fills


def test_null_fuel_on_pump_load_is_not_filled_or_flagged():
    m = [('QLD1', 'Pump Station', 'Pumpco', 'PMP1', 0.0, 0.0, None)]
    plan = plan_for(m, ['PMP1'])
    assert plan.fills == {} and plan.edits == [] and plan.conflicts == []
    # NaN fuel as read from a DataFrame behaves the same
    m = [('QLD1', 'Pump Station', 'Pumpco', 'PMP1', 0.0, 0.0, np.nan)]
    plan = plan_for(m, ['PMP1'])
    assert plan.fills == {} and plan.conflicts == []


def test_region_conflict_reported_not_applied():
    m = [('NSW1', 'Melb Batt', 'Melco', 'MLB01', 210.0, 400.0, 'Battery Storage')]
    plan = plan_for(m, ['MLB01'])
    assert plan.fills == {} and plan.edits == []
    c = [x for x in plan.conflicts if x['kind'] == 'region']
    assert len(c) == 1
    assert c[0]['duid'] == 'MLB01' and c[0]['mapping'] == 'NSW1' and c[0]['source'] == 'VIC1'


def test_conflict_only_for_active_duids():
    m = [('NSW1', 'Melb Batt', 'Melco', 'MLB01', 210.0, 400.0, 'Battery Storage')]
    plan = plan_for(m, [])
    assert plan.conflicts == []


def test_capacity_band_tolerates_ac_vs_regcap_difference():
    # 85 MW against registration 100 MW: inside [0.8x, 1.2x]
    m = [('VIC1', 'Acdc', 'Acdc Co', 'ACDC1', 85.0, 0.0, 'Solar')]
    assert plan_for(m, ['ACDC1']).conflicts == []
    # 70 MW is outside
    m = [('VIC1', 'Acdc', 'Acdc Co', 'ACDC1', 70.0, 0.0, 'Solar')]
    c = plan_for(m, ['ACDC1']).conflicts
    assert [x['kind'] for x in c] == ['capacity']
    # 130 MW is outside the upper bound
    m = [('VIC1', 'Acdc', 'Acdc Co', 'ACDC1', 130.0, 0.0, 'Solar')]
    assert [x['kind'] for x in plan_for(m, ['ACDC1']).conflicts] == ['capacity']


def test_storage_conflict_over_ten_percent():
    ok = [('VIC1', 'Melb Batt', 'Melco', 'MLB01', 210.0, 430.0, 'Battery Storage')]   # +7.5%
    assert plan_for(ok, ['MLB01']).conflicts == []
    bad = [('VIC1', 'Melb Batt', 'Melco', 'MLB01', 210.0, 500.0, 'Battery Storage')]
    assert [x['kind'] for x in plan_for(bad, ['MLB01']).conflicts] == ['storage']


def test_fuel_contradiction_reported():
    m = [('VIC1', 'Acdc', 'Acdc Co', 'ACDC1', 100.0, 0.0, 'Battery Storage')]
    c = plan_for(m, ['ACDC1']).conflicts
    assert [x['kind'] for x in c if x['kind'] == 'fuel'] == ['fuel']
    m = [('VIC1', 'Acdc', 'Acdc Co', 'ACDC1', 100.0, 0.0, 'Solar')]
    assert plan_for(m, ['ACDC1']).conflicts == []


def test_fuel_family_rules():
    assert dr.fuel_contradicts('Water', 'Fossil', 'Natural Gas')
    assert dr.fuel_contradicts('Coal', 'Fossil', 'Coal Seam Methane')
    assert dr.fuel_contradicts('Coal', 'Fossil', 'Waste Coal Mine Gas')
    assert not dr.fuel_contradicts('Gas other', 'Fossil', 'Coal Seam Methane')
    assert dr.fuel_contradicts('OCGT', 'Fossil', 'Black Coal')
    assert not dr.fuel_contradicts('Coal', 'Fossil', 'Black Coal')
    assert not dr.fuel_contradicts('Other', 'Fossil', 'Diesel')
    assert not dr.fuel_contradicts('Water', 'Hydro', 'Hydro')
    assert not dr.fuel_contradicts(None, 'Hydro', 'Hydro')
    assert not dr.fuel_contradicts(np.nan, 'Solar', 'Solar')
    assert not dr.fuel_contradicts('Biomass', 'Renewable/ Biomass / Waste', 'Bagasse')
    assert dr.fuel_contradicts('Wind', 'Solar', 'Solar')


def test_active_duid_in_no_source_is_reported():
    m = [('VIC1', 'Ghost', 'Ghost Co', 'GHOST1', 10.0, 0.0, 'Wind')]
    plan = plan_for(m, ['GHOST1'])
    assert [u['duid'] for u in plan.unfound] == ['GHOST1']
    # unmapped and unknown too
    plan = plan_for([], ['NOWHERE'])
    assert [u['duid'] for u in plan.unfound] == ['NOWHERE']


def test_single_source_only_still_plans():
    reg_only = dr.build_reference(dr.parse_registration(reg_raw(TODAY_REF['reg_rows'])), None, None)
    plan = dr.build_plan(mapping_df([]), reg_only, {'NEW1'})
    assert plan.inserts[0]['region'] == 'NSW1'


# ------------------------------------------------------------------ DB write

def make_db(path, rows):
    con = duckdb.connect(str(path))
    con.execute('CREATE TABLE duid_mapping (region VARCHAR, "site name" VARCHAR, owner VARCHAR, '
                'duid VARCHAR, capacity_mw DOUBLE, storage_mwh DOUBLE, fuel VARCHAR)')
    for r in rows:
        con.execute('INSERT INTO duid_mapping VALUES (?,?,?,?,?,?,?)', list(r))
    return con


def test_apply_plan_writes_inserts_and_fills_in_one_transaction(tmp_path):
    con = make_db(tmp_path / 't.duckdb', [
        ('', 'Kept Name', 'Kept Owner', 'BLNK1', 77.0, 0.0, 'Wind'),
        ('VIC1', 'Melb Batt', 'Melco', 'MLB01', 210.0, 400.0, 'Battery Storage'),
    ])
    current = con.execute('SELECT * FROM duid_mapping').df()
    plan = dr.build_plan(current, make_ref(**TODAY_REF), {'BLNK1', 'NEW1', 'PMP1'})
    dr.apply_plan(con, plan)
    got = con.execute('SELECT * FROM duid_mapping ORDER BY duid').df().set_index('duid')
    assert got.loc['BLNK1', 'region'] == 'SA1'
    assert got.loc['BLNK1', 'site name'] == 'Kept Name' and got.loc['BLNK1', 'capacity_mw'] == 77.0
    assert got.loc['NEW1', 'owner'] == 'Newco Pty Ltd'
    assert pd.isna(got.loc['PMP1', 'fuel']) and got.loc['PMP1', 'capacity_mw'] == 0.0
    assert len(got) == 4
    con.close()


def test_apply_plan_rolls_back_everything_on_error(tmp_path):
    con = make_db(tmp_path / 't.duckdb', [('', 'N', 'O', 'BLNK1', 77.0, 0.0, 'Wind')])
    plan = dr.Plan(inserts=[{'region': 'X', 'site name': 's', 'owner': 'o', 'duid': 'BAD1',
                             'capacity_mw': 'not-a-number', 'storage_mwh': 0.0, 'fuel': 'Wind'}],
                   fills={'BLNK1': {'region': 'SA1'}})
    with pytest.raises(Exception):
        dr.apply_plan(con, plan)
    got = con.execute('SELECT region, duid FROM duid_mapping').fetchall()
    assert got == [('', 'BLNK1')]
    con.close()


def test_connect_with_retry_retries_on_io_error(tmp_path, monkeypatch):
    calls = {'n': 0}
    real = duckdb.connect

    def flaky(path, *a, **k):
        calls['n'] += 1
        if calls['n'] < 3:
            raise duckdb.IOException('Could not set lock on file')
        return real(path, *a, **k)

    monkeypatch.setattr(dr.duckdb, 'connect', flaky)
    slept = []
    con = dr.connect_with_retry(str(tmp_path / 'r.duckdb'), interval=5, timeout=300,
                                sleep=slept.append)
    assert calls['n'] == 3 and slept == [5, 5]
    con.close()


def test_connect_with_retry_gives_up(tmp_path, monkeypatch):
    def always(*a, **k):
        raise duckdb.IOException('locked')
    monkeypatch.setattr(dr.duckdb, 'connect', always)
    with pytest.raises(duckdb.IOException):
        dr.connect_with_retry('x.duckdb', interval=5, timeout=10, sleep=lambda s: None,
                              clock=iter([0, 4, 8, 12, 16]).__next__)


def test_audit_files(tmp_path):
    cur = mapping_df([('VIC1', 'A', 'B', 'D1', 1.0, 0.0, 'Wind')])
    snap = dr.write_snapshot(cur, tmp_path, dt.date(2026, 10, 8))
    assert snap.name == 'duid_mapping_snapshot_20261008.csv'
    dr.write_snapshot(cur, tmp_path, dt.date(2026, 10, 8))     # appends on rerun
    assert len(pd.read_csv(snap)) == 2
    edits = [dict(duid='D1', action='fill', field='region', old='', new='VIC1')]
    ep = dr.write_edits(edits, tmp_path, dt.date(2026, 10, 8))
    assert ep.name == 'refresh_edits_20261008.csv'
    assert list(pd.read_csv(ep).columns) == ['duid', 'action', 'field', 'old', 'new']
