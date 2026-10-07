"""Keep the duid_mapping table in step with AEMO registration data.

Pure functions (parsing, owner cleaning, fuel mapping, plan building) plus two
small DuckDB helpers (connect_with_retry, apply_plan). Downloads, e-mail and
the CLI live in scripts/refresh_duid_mapping.py.

Sources: the AEMO NEM Registration and Exemption List (registration), and the
MMSDM monthly archive tables DUDETAILSUMMARY and DUDETAIL (MMS).
Precedence: region MMS > registration; capacity registration > MMS.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# Fuel label for pump loads (MMS DISPATCHTYPE == 'LOAD' hydro). NULL in the table:
# nothing else in the codebase should hard-code this.
PUMP_LOAD_FUEL = None

SKIP_PREFIXES = ('RT_', 'DG_')
PLACEHOLDER_PREFIX = '[auto-classified'
MAPPING_COLS = ['region', 'site name', 'owner', 'duid', 'capacity_mw', 'storage_mwh', 'fuel']

CAP_BAND_LOW = 0.8
CAP_BAND_HIGH = 1.2
STORAGE_TOL = 0.10

_OWNER_SPLIT = re.compile(r'\s+(?:as (?:the )?trustee|in its capacity|\(ACN|\(ABN|ACN\b)',
                          flags=re.I)
_COAL = re.compile(r'\b(?:black|brown)\s+coal\b', flags=re.I)

MMSDM_BASE = ('https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{y}/MMSDM_{y}_{m:02d}/'
              'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE%23{table}%23FILE01%23{y}{m:02d}010000.zip')
REGISTRATION_URL = ('https://www.aemo.com.au/-/media/Files/Electricity/NEM/Participant_Information/'
                    'NEM-Registration-and-Exemption-List.xls')


# ------------------------------------------------------------------ small helpers

def _blank(x) -> bool:
    if x is None:
        return True
    try:
        if pd.isna(x):
            return True
    except (TypeError, ValueError):
        pass
    return str(x).strip() in ('', '-')


def _s(x) -> str:
    return '' if _blank(x) else str(x).strip()


def _pos(*vals):
    """First value that is a number > 0, else NaN."""
    for v in vals:
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f == f and f > 0:
            return f
    return np.nan


def _clean(v):
    """numpy/NaN -> plain Python for DB parameters."""
    if v is None:
        return None
    if isinstance(v, (np.floating, float)):
        return None if v != v else float(v)
    if isinstance(v, np.integer):
        return int(v)
    if v is pd.NA or v is pd.NaT:
        return None
    return v


def is_skipped(duid: str) -> bool:
    return str(duid).startswith(SKIP_PREFIXES)


# ------------------------------------------------------------------ owner / fuel

def clean_owner(raw):
    """Drop trustee/ACN clauses from a participant name; None if nothing is left."""
    if _blank(raw):
        return None
    out = _OWNER_SPLIT.split(str(raw), maxsplit=1)[0].strip(' ,').strip()
    return out or None


def map_fuel(primary, descriptor, tech_descriptor, dispatchtype=None):
    """Mapping fuel label from registration fields. Hydro with MMS DISPATCHTYPE
    'LOAD' returns PUMP_LOAD_FUEL."""
    p, d, t = _s(primary).lower(), _s(descriptor).lower(), _s(tech_descriptor).lower()
    if 'battery' in p:
        return 'Battery Storage'
    if 'solar' in p:
        return 'Solar'
    if 'wind' in p:
        return 'Wind'
    if 'hydro' in p:
        return PUMP_LOAD_FUEL if _s(dispatchtype).upper() == 'LOAD' else 'Water'
    if 'fossil' in p:
        if _COAL.search(d):
            return 'Coal'
        if 'combined cycle' in t:
            return 'CCGT'
        if 'open cycle' in t or 'ocgt' in t:
            return 'OCGT'
        if 'diesel' in d and 'reciprocating' in t:
            return 'Other'
        return 'Gas other'
    if 'biomass' in p:
        return 'Biomass'
    return 'Other'


_MAPPING_FAMILY = {'Solar': 'solar', 'Wind': 'wind', 'Battery Storage': 'battery', 'Water': 'water',
                   'Biomass': 'biomass', 'Coal': 'coal', 'OCGT': 'gas', 'CCGT': 'gas',
                   'Gas other': 'gas'}


def _registration_family(primary, descriptor):
    p, d = _s(primary).lower(), _s(descriptor).lower()
    if 'battery' in p:
        return 'battery'
    if 'solar' in p:
        return 'solar'
    if 'wind' in p:
        return 'wind'
    if 'hydro' in p:
        return 'water'
    if 'biomass' in p:
        return 'biomass'
    if 'fossil' in p:
        if _COAL.search(d):
            return 'coal'
        if 'diesel' in d:
            return None
        return 'gas'          # includes Coal Seam Methane and Waste Coal Mine Gas
    return None


def fuel_contradicts(mapping_fuel, reg_primary, reg_descriptor) -> bool:
    """True when the mapping fuel clearly contradicts the registration. A NULL
    mapping fuel (pump loads) never contradicts."""
    if _blank(mapping_fuel):
        return False
    a = _MAPPING_FAMILY.get(str(mapping_fuel).strip())
    b = _registration_family(reg_primary, reg_descriptor)
    return a is not None and b is not None and a != b


# ------------------------------------------------------------------ source parsing

def _num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(',', '', regex=False), errors='coerce')


def parse_registration(raw: pd.DataFrame) -> pd.DataFrame:
    """Sheet 'PU and Scheduled Loads' -> one row per DUID (r_* columns)."""
    p = raw.copy()
    p.columns = [str(c).strip() for c in p.columns]
    p = p[p['DUID'].notna()].copy()
    p['DUID'] = p['DUID'].astype(str).str.strip()
    p = p[~p['DUID'].isin(['', '-', 'nan'])]

    def col(name):
        return p[name] if name in p.columns else pd.Series(np.nan, index=p.index)

    reg = pd.DataFrame({
        'duid': p['DUID'],
        'r_station': col('Station Name'), 'r_participant': col('Participant'),
        'r_region': col('Region'), 'r_dtype': col('Dispatch Type'),
        'r_fuel': col('Fuel Source - Primary'), 'r_fueld': col('Fuel Source - Descriptor'),
        'r_tech': col('Technology Type - Primary'), 'r_techd': col('Technology Type - Descriptor'),
        'r_regcap': _num(col('Reg Cap generation (MW)')),
        'r_maxcap': _num(col('Max Cap generation (MW)')),
        'r_storage': _num(col('Maximum storage capacity')),
    })
    firsts = {c: 'first' for c in reg.columns if c not in ('duid', 'r_regcap', 'r_maxcap', 'r_storage')}
    agg = reg.groupby('duid', sort=False).agg(
        {**firsts, 'r_regcap': lambda s: s.sum(min_count=1), 'r_maxcap': lambda s: s.sum(min_count=1),
         'r_storage': 'max'})
    return agg.reset_index()


def parse_mms_csv(text: str) -> pd.DataFrame:
    """AEMO MMS CSV: 'I' row = header (columns from index 4), 'D' rows = data."""
    header = None
    rows = []
    for r in csv.reader(io.StringIO(text)):
        if not r:
            continue
        if r[0] == 'I' and header is None:
            header = r[4:]
        elif r[0] == 'D' and header is not None:
            vals = r[4:]
            vals = (vals + [''] * len(header))[:len(header)]
            rows.append(vals)
    if header is None:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=header)
    if 'DUID' in df.columns:
        df['DUID'] = df['DUID'].str.strip()
    return df


def latest_dudetailsummary(df: pd.DataFrame) -> pd.DataFrame:
    keep = [c for c in ('DUID', 'START_DATE', 'END_DATE', 'REGIONID', 'DISPATCHTYPE', 'STATIONID',
                        'PARTICIPANTID') if c in df.columns]
    out = df.sort_values('START_DATE', kind='stable').groupby('DUID', sort=False).tail(1)
    return out[keep].reset_index(drop=True)


def latest_dudetail(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d['VERSIONNO'] = pd.to_numeric(d['VERSIONNO'], errors='coerce')
    for c in ('REGISTEREDCAPACITY', 'MAXCAPACITY', 'MAXSTORAGECAPACITY'):
        d[c] = pd.to_numeric(d[c], errors='coerce')
    d = d.sort_values(['EFFECTIVEDATE', 'VERSIONNO'], kind='stable').groupby('DUID', sort=False).tail(1)
    return d[['DUID', 'REGISTEREDCAPACITY', 'MAXCAPACITY', 'MAXSTORAGECAPACITY']].reset_index(drop=True)


def candidate_months(today: dt.date, back: int = 4):
    """(year, month) from the current month back `back` months, newest first."""
    y, m = today.year, today.month
    out = []
    for _ in range(back + 1):
        out.append((y, m))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return out


def mmsdm_url(year: int, month: int, table: str) -> str:
    return MMSDM_BASE.format(y=year, m=month, table=table)


REF_COLS = ['r_station', 'r_participant', 'r_region', 'r_dtype', 'r_fuel', 'r_fueld', 'r_tech',
            'r_techd', 'r_regcap', 'r_maxcap', 'r_storage',
            'm_region', 'm_dtype', 'm_station', 'm_participant', 'm_regcap', 'm_maxcap', 'm_storage']


def build_reference(reg, summary, detail) -> pd.DataFrame:
    """Merge the sources into one frame indexed by duid. Any source may be None."""
    frames = []
    if reg is not None and len(reg):
        frames.append(reg.assign(in_reg=True))
    if summary is not None and len(summary):
        s = summary.rename(columns={'DUID': 'duid', 'REGIONID': 'm_region', 'DISPATCHTYPE': 'm_dtype',
                                    'STATIONID': 'm_station', 'PARTICIPANTID': 'm_participant'})
        s = s[[c for c in ('duid', 'm_region', 'm_dtype', 'm_station', 'm_participant')
               if c in s.columns]]
        frames.append(s.assign(in_mms_s=True))
    if detail is not None and len(detail):
        d = detail.rename(columns={'DUID': 'duid', 'REGISTEREDCAPACITY': 'm_regcap',
                                   'MAXCAPACITY': 'm_maxcap', 'MAXSTORAGECAPACITY': 'm_storage'})
        frames.append(d.assign(in_mms_d=True))
    if not frames:
        out = pd.DataFrame(columns=['duid'])
    else:
        out = frames[0]
        for f in frames[1:]:
            out = out.merge(f, on='duid', how='outer')
    for c in REF_COLS:
        if c not in out.columns:
            out[c] = np.nan
    flag = lambda c: out[c].eq(True) if c in out.columns else pd.Series(False, index=out.index)
    out['in_reg'] = flag('in_reg')
    out['in_mms'] = flag('in_mms_s') | flag('in_mms_d')
    out = out.drop(columns=[c for c in ('in_mms_s', 'in_mms_d') if c in out.columns])
    return out.drop_duplicates('duid').set_index('duid', drop=False)


# ------------------------------------------------------------------ plan

@dataclass
class Plan:
    inserts: list = field(default_factory=list)      # full mapping rows
    fills: dict = field(default_factory=dict)        # duid -> {column: new value}
    edits: list = field(default_factory=list)        # duid, action, field, old, new
    conflicts: list = field(default_factory=list)    # duid, kind, mapping, source, detail
    unfound: list = field(default_factory=list)      # active, in no source
    unclassified: list = field(default_factory=list)  # active, found only in MMS, fuel unknown

    @property
    def changed(self) -> bool:
        return bool(self.edits)


def _region_of(r):
    for v in (r['m_region'], r['r_region']):
        if not _blank(v):
            return str(v).strip()
    return None


def _ref_capacity(r, battery: bool):
    return _pos(r['r_maxcap'], r['m_maxcap']) if battery else _pos(r['r_regcap'], r['m_regcap'])


def _ref_storage(r):
    return _pos(r['r_storage'], r['m_storage'])


def _is_load(r) -> bool:
    return _s(r['m_dtype']).upper() == 'LOAD'


def _new_row(duid, r, today):
    """Mapping row for an unmapped active DUID, or (None, reason) if fuel unknown."""
    if r['in_reg']:
        fuel = map_fuel(r['r_fuel'], r['r_fueld'], r['r_techd'], r['m_dtype'])
    elif _s(r['m_dtype']).upper() == 'BIDIRECTIONAL':
        fuel = 'Battery Storage'
    else:
        return None
    battery = fuel == 'Battery Storage'
    pump = _is_load(r) and fuel is PUMP_LOAD_FUEL
    if pump:
        cap = 0.0
    else:
        cap = _ref_capacity(r, battery)
    storage = _ref_storage(r) if battery else 0.0
    name = _s(r['r_station']) or f'{PLACEHOLDER_PREFIX} {today.isoformat()}]'
    owner = clean_owner(r['r_participant'])
    return {'region': _region_of(r), 'site name': name, 'owner': owner, 'duid': duid,
            'capacity_mw': _clean(cap), 'storage_mwh': _clean(0.0 if storage != storage and not battery
                                                              else storage),
            'fuel': fuel}


def _conflicts_for(duid, row, r):
    out = []
    fuel = None if _blank(row['fuel']) else str(row['fuel']).strip()
    load = _is_load(r)
    # region
    reg_region = _region_of(r)
    if not _blank(row['region']) and reg_region and str(row['region']).strip() != reg_region:
        out.append(dict(duid=duid, kind='region', mapping=row['region'], source=reg_region,
                        detail='MMS REGIONID' if not _blank(r['m_region']) else 'registration Region'))
    # capacity
    cap = row['capacity_mw']
    if not load and _pos(cap) == _pos(cap):
        vals = [v for v in (r['r_regcap'], r['r_maxcap']) if _pos(v) == _pos(v)]
        src = 'registration'
        if not vals:
            vals = [v for v in (r['m_regcap'], r['m_maxcap']) if _pos(v) == _pos(v)]
            src = 'MMS'
        if vals:
            lo, hi = CAP_BAND_LOW * min(vals), CAP_BAND_HIGH * max(vals)
            if not (lo <= float(cap) <= hi):
                out.append(dict(duid=duid, kind='capacity', mapping=float(cap),
                                source=' / '.join(f'{float(v):g}' for v in vals),
                                detail=f'{src} Reg/Max Cap; allowed {lo:g} to {hi:g} MW'))
    # storage
    if fuel == 'Battery Storage' and _pos(row['storage_mwh']) == _pos(row['storage_mwh']):
        ref = _ref_storage(r)
        if ref == ref and abs(float(row['storage_mwh']) - ref) / ref > STORAGE_TOL:
            out.append(dict(duid=duid, kind='storage', mapping=float(row['storage_mwh']), source=ref,
                            detail=f'differs by {abs(float(row["storage_mwh"]) - ref) / ref:.0%}'))
    # fuel (NULL fuel on an MMS LOAD row is intentional)
    if fuel is not None and r['in_reg'] and fuel_contradicts(fuel, r['r_fuel'], r['r_fueld']):
        out.append(dict(duid=duid, kind='fuel', mapping=fuel,
                        source=f"{_s(r['r_fuel'])} / {_s(r['r_fueld'])}",
                        detail='registration fuel source'))
    return out


def build_plan(mapping: pd.DataFrame, ref: pd.DataFrame, active, today: dt.date | None = None) -> Plan:
    today = today or dt.date.today()
    plan = Plan()
    active = {d for d in active if not is_skipped(d)}
    mapping = mapping.drop_duplicates('duid')
    mapped = set(mapping['duid'])

    # INSERT
    for duid in sorted(active - mapped):
        if duid not in ref.index:
            plan.unfound.append(dict(duid=duid, note='not in duid_mapping or any source'))
            continue
        row = _new_row(duid, ref.loc[duid], today)
        if row is None:
            plan.unclassified.append(dict(duid=duid, note='found in MMS only; fuel cannot be determined',
                                          region=_region_of(ref.loc[duid]),
                                          dispatchtype=_s(ref.loc[duid]['m_dtype'])))
            continue
        plan.inserts.append(row)
        for col in MAPPING_COLS:
            if col != 'duid':
                plan.edits.append(dict(duid=duid, action='insert', field=col, old=None, new=row[col]))

    # FILL and conflicts
    for _, row in mapping.iterrows():
        duid = row['duid']
        if is_skipped(duid):
            continue
        if duid not in ref.index:
            if duid in active:
                plan.unfound.append(dict(duid=duid, note='mapped, but in no source'))
            continue
        r = ref.loc[duid]
        fuel = None if _blank(row['fuel']) else str(row['fuel']).strip()
        pump = _is_load(r)
        fill = {}
        if _blank(row['region']) and _region_of(r):
            fill['region'] = _region_of(r)
        if not pump and (pd.isna(row['capacity_mw']) or float(row['capacity_mw']) == 0):
            cap = _ref_capacity(r, fuel == 'Battery Storage')
            if cap == cap:
                fill['capacity_mw'] = cap
        if fuel == 'Battery Storage' and (pd.isna(row['storage_mwh']) or float(row['storage_mwh']) == 0):
            sto = _ref_storage(r)
            if sto == sto:
                fill['storage_mwh'] = sto
        if _s(row['site name']).startswith(PLACEHOLDER_PREFIX) or str(row['site name']).startswith(PLACEHOLDER_PREFIX):
            station = _s(r['r_station'])
            if station:
                fill['site name'] = station
            owner = clean_owner(r['r_participant'])
            if _blank(row['owner']) and owner:
                fill['owner'] = owner
        if fill:
            plan.fills[duid] = fill
            for k, v in fill.items():
                plan.edits.append(dict(duid=duid, action='fill', field=k,
                                       old=_clean(row[k]), new=_clean(v)))
        if duid in active:
            plan.conflicts.extend(_conflicts_for(duid, row, r))
    return plan


# ------------------------------------------------------------------ DuckDB

def connect_with_retry(path, interval=5, timeout=300, sleep=time.sleep, clock=time.monotonic):
    """duckdb.connect, retrying on IOException (collector write lock)."""
    start = clock()
    while True:
        try:
            return duckdb.connect(str(path))
        except duckdb.IOException:
            if clock() - start >= timeout:
                raise
            sleep(interval)


def apply_plan(con, plan: Plan) -> None:
    """Apply fills and inserts in one transaction; roll everything back on error."""
    con.execute('BEGIN TRANSACTION')
    try:
        for duid, fill in plan.fills.items():
            cols = list(fill)
            sets = ', '.join(f'"{c}" = ?' for c in cols)
            con.execute(f'UPDATE duid_mapping SET {sets} WHERE duid = ?',
                        [_clean(fill[c]) for c in cols] + [duid])
        for row in plan.inserts:
            con.execute('INSERT INTO duid_mapping (region, "site name", owner, duid, capacity_mw, '
                        'storage_mwh, fuel) VALUES (?,?,?,?,?,?,?)',
                        [_clean(row[c]) for c in MAPPING_COLS])
        con.execute('COMMIT')
    except Exception:
        con.execute('ROLLBACK')
        raise


# ------------------------------------------------------------------ audit files

def write_snapshot(df: pd.DataFrame, directory, date: dt.date) -> Path:
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f'duid_mapping_snapshot_{date:%Y%m%d}.csv'
    out = df.copy()
    out['snapshot_ts'] = dt.datetime.now().isoformat(timespec='seconds')
    out.to_csv(path, mode='a', header=not path.exists(), index=False)
    return path


def write_edits(edits, directory, date: dt.date) -> Path:
    d = Path(directory)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f'refresh_edits_{date:%Y%m%d}.csv'
    df = pd.DataFrame(edits, columns=['duid', 'action', 'field', 'old', 'new'])
    df.to_csv(path, mode='a', header=not path.exists(), index=False)
    return path
