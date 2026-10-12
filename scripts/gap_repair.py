#!/usr/bin/env python3
"""Gap survey and repair for every collector-written base table.

Written 13-Aug-2026 after a 3-hour nemweb publishing outage exposed that the
weekly check found holes it could not fix: it only ever called the CURRENT-based
scripts, so anything older than ~2 days was logged as "manual ARCHIVE" and left.
1,687 intervals had accumulated that way, the oldest from July 2024.

Extended 13-Oct-2026 from three tables to all twelve after a power outage left
bdu5, curtailment_regional5, demand30 and rooftop30 holes that nothing surveyed
(see TABLES for the per-table interval, lag, key and repair source).

Sources, chosen by age and feed:

  CURRENT  individual files; ~2 days for the 5-minute feeds, ~60 days of
           Operational_Demand, ~14 days of rooftop, every file since 30-Jan-2025
           for Operational_Demand_Less_SNSG, one daily Next_Day_Dispatch zip
           back to Sep-2025
  ARCHIVE  DispatchIS and Dispatch_SCADA: one daily zip per date holding ~288
           nested zips (~12 months). Operational_Demand, ROOFTOP_PV: one zip per
           week of 30-minute zips (from Sep-2025)
  MMSDM    prices5 and scada5 only, one monthly zip per table

Boundary rules that cost real intervals when missed:

  * A daily ARCHIVE zip for date D spans 00:05 on D through 00:00 on D+1, so the
    00:00 interval of day D lives in day D-1's file. Same at month boundaries in
    MMSDM. Ignoring this leaves exactly one interval short per affected date.
  * A file can age out of CURRENT (~2 days) before ARCHIVE publishes it (ARCHIVE
    lags 1-2 days). Intervals in that seam are briefly unreachable and simply
    need retrying on a later run -- they are not permanent losses.
  * 30-minute labels are interval-END: label 12:30 is the mean of the 5-minute
    intervals 12:05..12:30, i.e. (label - 30 min, label].
  * Rooftop MEASUREMENT files are stamped 30 minutes after the interval they
    hold (the 00:00 file carries 23:30).
  * Next_Day_Dispatch file for trading day D holds D 04:05 .. D+1 04:00.

Staging is separated from applying so downloads run while the collector is live;
only the insert needs the write lock, and inserts are anti-joins on the table's
key, so a good row is never deleted or changed. The only UPDATE is
demand30.demand_less_snsg, and only where it is NULL.
"""
import io
import os
import re
import sys
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Tuple

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

# Paths follow the package convention -- env var with a machine default -- so
# this runs on .68/.69 or against a scratch copy without editing. The GAP_REPAIR_*
# overrides exist so tests can point the repair path at a throwaway database.
DATA_PATH = Path(os.environ.get(
    'AEMO_DATA_PATH', '/Users/davidleitch/aemo_production/data'))
PRIMARY = os.environ.get('GAP_REPAIR_PRIMARY', str(DATA_PATH / 'aemo_test.duckdb'))
REPLICA = os.environ.get('GAP_REPAIR_REPLICA', str(DATA_PATH / 'aemo_readonly.duckdb'))
STAGE = Path(os.environ.get('GAP_REPAIR_STAGE', str(DATA_PATH / 'gap_recovery')))

ARCHIVE_ROOT = 'https://nemweb.com.au/Reports/ARCHIVE/'
CURRENT_ROOT = 'https://nemweb.com.au/Reports/CURRENT/'
MMSDM_ROOT = 'https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM'

MAIN_REGIONS = {'NSW1', 'QLD1', 'SA1', 'TAS1', 'VIC1'}

SNSG = 'demand30.demand_less_snsg'      # column-completeness check key
SNSG_LAG = 180   # minutes: the SNSG feed is filled by the collector after the demand row


@dataclass(frozen=True)
class Spec:
    """Survey and repair configuration for one table."""
    name: str
    interval: int                      # minutes between expected timestamps
    lag: int                           # minutes the newest rows may trail "now"
    keys: Tuple[str, ...]
    cols: Tuple[str, ...]              # columns inserted on repair
    feed: Optional[str] = None         # repair source feed; None = not staged
    derived_from: Optional[str] = None  # 30-minute table rebuilt from this 5-minute one
    regional: bool = False             # expect all five main regions at every timestamp


TABLES = {s.name: s for s in (
    # --- 5-minute, DispatchIS / Dispatch_SCADA (lag: publication + cycle)
    Spec('prices5', 5, 20, ('settlementdate', 'regionid'),
         ('settlementdate', 'regionid', 'rrp'), 'dispatchis', regional=True),
    Spec('scada5', 5, 20, ('settlementdate', 'duid'),
         ('settlementdate', 'duid', 'scadavalue'), 'scada'),
    Spec('transmission5', 5, 20, ('settlementdate', 'interconnectorid'),
         ('settlementdate', 'interconnectorid', 'meteredmwflow', 'mwflow',
          'exportlimit', 'importlimit', 'mwlosses'), 'dispatchis'),
    Spec('bdu5', 5, 20, ('settlementdate', 'regionid'),
         ('settlementdate', 'regionid', 'bdu_energy_storage', 'bdu_clearedmw_gen',
          'bdu_clearedmw_load'), 'dispatchis', regional=True),
    Spec('curtailment_regional5', 5, 20, ('settlementdate', 'regionid'),
         ('settlementdate', 'regionid', 'solar_uigf', 'solar_cleared', 'wind_uigf',
          'wind_cleared', 'solar_curtailment', 'wind_curtailment', 'total_curtailment'),
         'dispatchis', regional=True),
    # --- 5-minute, Next_Day_Dispatch: published ~04:30 the day after, so the
    # newest row trails now by 5-24.5 h. 30 h covers it with margin.
    Spec('curtailment5', 5, 30 * 60, ('settlementdate', 'duid'),
         ('settlementdate', 'duid', 'availability', 'totalcleared', 'semidispatchcap',
          'curtailment'), 'nextday'),
    Spec('curtailment_duid5', 5, 30 * 60, ('settlementdate', 'duid'),
         ('settlementdate', 'duid', 'uigf', 'totalcleared', 'curtailment'), 'nextday'),
    # --- 30-minute, derived from the 5-minute tables (rebuilt, never downloaded)
    Spec('prices30', 30, 45, ('settlementdate', 'regionid'),
         ('settlementdate', 'regionid', 'rrp'), derived_from='prices5', regional=True),
    Spec('scada30', 30, 45, ('settlementdate', 'duid'),
         ('settlementdate', 'duid', 'scadavalue'), derived_from='scada5'),
    Spec('transmission30', 30, 45, ('settlementdate', 'interconnectorid'),
         ('settlementdate', 'interconnectorid', 'meteredmwflow', 'mwflow', 'mwlosses',
          'exportlimit', 'importlimit'), derived_from='transmission5'),
    # --- 30-minute, own feeds. Rooftop files land ~30 min after the demand ones.
    Spec('demand30', 30, 90, ('settlementdate', 'regionid'),
         ('settlementdate', 'regionid', 'demand'), 'demand', regional=True),
    Spec('rooftop30', 30, 120, ('settlementdate', 'regionid'),
         ('settlementdate', 'regionid', 'power', 'quality_indicator', 'type'),
         'rooftop', regional=True),
)}
ALL_TABLES = tuple(TABLES)
# Order matters for apply(): demand30 rows go in before the SNSG fill.
STAGED_TABLES = ('prices5', 'scada5', 'transmission5', 'bdu5', 'curtailment_regional5',
                 'curtailment5', 'curtailment_duid5', 'demand30', 'rooftop30')

# Back-compat names used by older callers.
KEYS = {t: list(s.keys) for t, s in TABLES.items()}
COLS = {t: list(s.cols) for t, s in TABLES.items()}
FEED = {t: s.feed for t, s in TABLES.items() if s.feed}

# Derived 30-minute tables: (target, source, key, aggregate SQL, columns, extra WHERE)
DERIVED = (
    ('prices30', 'prices5', 'regionid', 'AVG(s.rrp) AS rrp', 'rrp',
     f"AND s.regionid IN ({','.join(repr(r) for r in sorted(MAIN_REGIONS))})"),
    ('transmission30', 'transmission5', 'interconnectorid',
     'AVG(s.meteredmwflow) AS meteredmwflow, AVG(s.mwflow) AS mwflow, '
     'AVG(s.mwlosses) AS mwlosses, AVG(s.exportlimit) AS exportlimit, '
     'AVG(s.importlimit) AS importlimit',
     'meteredmwflow, mwflow, mwlosses, exportlimit, importlimit', ''),
    ('scada30', 'scada5', 'duid', 'AVG(s.scadavalue) AS scadavalue', 'scadavalue', ''),
)

MMSDM_TABLE = {'prices5': 'DISPATCHPRICE', 'scada5': 'DISPATCH_UNIT_SCADA'}

# Feed definitions. cur_days: how far back CURRENT reaches. archive: 'daily'
# (one zip per date), 'weekly' (one zip per week, named by its first date), or None.
# offsets: file stamp = interval stamp + offset (minutes) -- rooftop files trail.
FEEDS = {
    'dispatchis': dict(cur='DispatchIS_Reports/', prefix='PUBLIC_DISPATCHIS_',
                       cur_days=2, archive='daily', arc='DispatchIS_Reports/',
                       offsets=(0,)),
    'scada': dict(cur='Dispatch_SCADA/', prefix='PUBLIC_DISPATCHSCADA_',
                  cur_days=2, archive='daily', arc='Dispatch_SCADA/', offsets=(0,)),
    'demand': dict(cur='Operational_Demand/ACTUAL_HH/',
                   prefix='PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_', cur_days=50,
                   archive='weekly', arc='Operational_Demand/ACTUAL_HH/', offsets=(0,)),
    'snsg': dict(cur='Operational_Demand_Less_SNSG/ACTUAL_HH/',
                 prefix='PUBLIC_ACTUAL_OPERATIONAL_DEM_LESS_SNSG_HH_', cur_days=10000,
                 archive='weekly', arc='Operational_Demand_Less_SNSG/ACTUAL_HH/',
                 offsets=(0,)),
    'rooftop': dict(cur='ROOFTOP_PV/ACTUAL/',
                    prefix='PUBLIC_ROOFTOP_PV_ACTUAL_MEASUREMENT_', cur_days=12,
                    archive='weekly', arc='ROOFTOP_PV/ACTUAL/', offsets=(30, 0)),
    'nextday': dict(cur='Next_Day_Dispatch/', prefix='PUBLIC_NEXT_DAY_DISPATCH_',
                    cur_days=400, archive=None, arc=None, offsets=(0,)),
}
# Backwards-compatible aliases
FEED_DIR = {'dispatchis': 'DispatchIS_Reports', 'scada': 'Dispatch_SCADA'}
FEED_PREFIX = {'dispatchis': 'PUBLIC_DISPATCHIS_', 'scada': 'PUBLIC_DISPATCHSCADA_'}

def _sleep(sec):
    time.sleep(sec)


_STAMP_RE = re.compile(r'_(\d{12})(?:\d{2})?_')


# ------------------------------------------------------------ primary lock

class LockTimeout(RuntimeError):
    pass


def replica_settled(primary=None, replica=None):
    """True when the collector is not between "closed the DB" and "copied it".

    The collector closes its connection, then copies the primary to the replica
    (copy2 keeps the mtime, so a finished copy has the primary's mtime). A writer
    that connects inside that window changes the file the copy is reading.
    """
    p, r = Path(primary or PRIMARY), Path(replica or REPLICA)
    if Path(str(r) + '.tmp').exists():
        return False
    try:
        return r.stat().st_mtime >= p.stat().st_mtime - 1.0
    except OSError:
        return False


def acquire_primary(timeout=900, poll=3.0, log=print, sleep=time.sleep,
                    clock=time.monotonic, connect=None, settled=None):
    """Open the primary read-write, waiting for the collector to release it.

    The collector holds the file lock only while a cycle runs and closes it for
    the ~4.5 minute sleep, and its own _open_conn retries a conflict for ~30 s.
    So the safe order is: wait for the gap between cycles, take the lock, do the
    few-second insert, release. No process is stopped and no keystroke is sent
    to the collector's terminal. Raises LockTimeout rather than proceeding if the
    lock never frees or the replica copy never settles.
    """
    connect = connect or duckdb.connect
    settled = settled or replica_settled
    deadline = clock() + timeout
    last = 'replica not settled'
    while True:
        if settled():
            try:
                con = connect(PRIMARY)
            except duckdb.IOException as e:
                last = f'lock held: {str(e)[:120]}'
            else:
                if settled():
                    return con
                con.close()
                last = 'replica copy started while connecting'
        if clock() >= deadline:
            raise LockTimeout(f'could not take {PRIMARY} within {timeout}s ({last})')
        sleep(poll)


# ------------------------------------------------------------ helpers

NEM_TZ = timezone(timedelta(hours=10))


def nem_now():
    """Current NEM time: AEST, UTC+10 all year. Every settlementdate in the
    database is in this zone, but the host clock follows Sydney daylight saving,
    so datetime.now() runs an hour ahead from October to April. Comparing the two
    made every table look an hour behind (13-Oct-2026)."""
    return datetime.now(NEM_TZ).replace(tzinfo=None)


def _floor(ts, minutes):
    return ts - timedelta(minutes=ts.minute % minutes, seconds=ts.second,
                          microseconds=ts.microsecond)


def label_of(ts):
    """30-minute END label of a 5-minute interval: 12:05..12:30 -> 12:30."""
    m = ts.minute % 30
    return ts if m == 0 and ts.second == 0 else _floor(ts, 30) + timedelta(minutes=30)


class GapRepairer:
    def __init__(self, log=print, collector=None):
        self.log = log
        self.collector = collector or UnifiedAEMOCollector()
        self._listing_cache = {}
        self._wanted = None            # frame name -> stamps wanted, set by stage_all
        STAGE.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- survey

    def survey(self, tables=None, db=REPLICA, trailing_lag_minutes=None, now=None,
               con=None):
        """Return {table: [missing timestamps]} by absolute enumeration.

        Enumerates every expected interval rather than diffing adjacent rows, so
        a single missing interval is visible. The LAG-based detectors elsewhere
        in this tree test `> 600 seconds`, and one absent 5-minute interval is a
        gap of exactly 600 -- which is why isolated holes survived every pass.

        The upper bound is `now - lag`, not MAX(settlementdate). Bounding at MAX
        makes a hole at the newest end invisible, because the hole moves the
        bound with it -- the defect that let demand30 sit empty for 14 hours
        through a check that reported OK. The lag allowance keeps normal
        publication delay from reading as a gap. Lags are per table (TABLES);
        `trailing_lag_minutes` overrides all of them.

        Each table is surveyed from its own MIN(settlementdate). Tables on the
        five-region pattern also report timestamps where a main region is absent.
        demand30 also reports timestamps whose demand_less_snsg is NULL, under
        the key 'demand30.demand_less_snsg', from the first date the column holds
        a value (the AEMO feed starts 30-Jan-2025) and up to SNSG_LAG minutes
        before now (the collector fills the column a cycle or two after the row).
        """
        tables = tuple(tables) if tables else ALL_TABLES
        now = now or nem_now()
        own = con is None
        con = con or duckdb.connect(db, read_only=True)
        out = {}
        try:
            for t in tables:
                spec = TABLES[t]
                mn, mx = con.execute(
                    f'SELECT MIN(settlementdate), MAX(settlementdate) FROM "{t}"'
                ).fetchone()
                if mn is None:
                    out[t] = []
                    continue
                lag = spec.lag if trailing_lag_minutes is None else trailing_lag_minutes
                end = max(mx, _floor(now - timedelta(minutes=lag), spec.interval))
                rows = con.execute(f"""
                    WITH g AS (SELECT generate_series AS d FROM generate_series(
                                 TIMESTAMP '{mn}', TIMESTAMP '{end}',
                                 INTERVAL {spec.interval} MINUTE)),
                         i AS (SELECT DISTINCT settlementdate AS d FROM "{t}")
                    SELECT g.d FROM g LEFT JOIN i USING (d)
                    WHERE i.d IS NULL ORDER BY 1
                """).fetchall()
                missing = {r[0] for r in rows}
                if spec.regional:
                    regs = ','.join(repr(r) for r in sorted(MAIN_REGIONS))
                    missing |= {r[0] for r in con.execute(f"""
                        SELECT settlementdate FROM "{t}" WHERE regionid IN ({regs})
                        GROUP BY 1 HAVING count(DISTINCT regionid) < {len(MAIN_REGIONS)}
                    """).fetchall()}
                out[t] = sorted(missing)
            if 'demand30' in tables:
                cut = now - timedelta(minutes=SNSG_LAG)
                out[SNSG] = [r[0] for r in con.execute(f"""
                    SELECT DISTINCT settlementdate FROM demand30
                    WHERE demand_less_snsg IS NULL
                      AND settlementdate <= TIMESTAMP '{cut}'
                      AND settlementdate >= (SELECT MIN(settlementdate) FROM demand30
                                             WHERE demand_less_snsg IS NOT NULL)
                    ORDER BY 1""").fetchall()]
        finally:
            if own:
                con.close()
        return out

    # ---------------------------------------------------------------- parsing

    @staticmethod
    def _ts(series):
        return pd.to_datetime(series.str.strip('"'), format='%Y/%m/%d %H:%M:%S')

    def _parse(self, content, feed, wanted=None):
        """Parse one nemweb CSV into {table: DataFrame} using the collector's own
        column handling, so repaired rows match collected rows."""
        out = {}
        c = self.collector
        if feed == 'dispatchis':
            df = c.parse_mms_csv(content, 'PRICE')
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                d = pd.DataFrame()
                d['settlementdate'] = self._ts(df['SETTLEMENTDATE'])
                d['regionid'] = df['REGIONID'].str.strip()
                d['rrp'] = pd.to_numeric(df['RRP'], errors='coerce')
                out['prices5'] = d[d['regionid'].isin(MAIN_REGIONS)]
            df = c.parse_mms_csv(content, 'INTERCONNECTORRES')
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                d = pd.DataFrame()
                d['settlementdate'] = self._ts(df['SETTLEMENTDATE'])
                d['interconnectorid'] = df['INTERCONNECTORID'].str.strip()
                for src, tgt in (('METEREDMWFLOW', 'meteredmwflow'), ('MWFLOW', 'mwflow'),
                                 ('EXPORTLIMIT', 'exportlimit'),
                                 ('IMPORTLIMIT', 'importlimit'), ('MWLOSSES', 'mwlosses')):
                    d[tgt] = (pd.to_numeric(df[src], errors='coerce')
                              if src in df.columns else float('nan'))
                out['transmission5'] = d
            df = c.parse_mms_csv(content, 'REGIONSUM')
            if not df.empty and 'SETTLEMENTDATE' in df.columns and 'REGIONID' in df.columns:
                st = self._ts(df['SETTLEMENTDATE'])
                rid = df['REGIONID'].str.strip()
                b = pd.DataFrame({'settlementdate': st, 'regionid': rid})
                for raw, col in (('BDU_ENERGY_STORAGE', 'bdu_energy_storage'),
                                 ('BDU_CLEAREDMW_GEN', 'bdu_clearedmw_gen'),
                                 ('BDU_CLEAREDMW_LOAD', 'bdu_clearedmw_load')):
                    b[col] = (pd.to_numeric(df[raw].str.strip().replace('', pd.NA),
                                            errors='coerce')
                              if raw in df.columns else float('nan'))
                out['bdu5'] = b[b['regionid'].isin(MAIN_REGIONS)]
                r = pd.DataFrame({'settlementdate': st, 'regionid': rid})
                for raw, col in (('SS_SOLAR_UIGF', 'solar_uigf'),
                                 ('SS_SOLAR_CLEAREDMW', 'solar_cleared'),
                                 ('SS_WIND_UIGF', 'wind_uigf'),
                                 ('SS_WIND_CLEAREDMW', 'wind_cleared')):
                    r[col] = (pd.to_numeric(df[raw], errors='coerce').fillna(0)
                              if raw in df.columns else 0.0)
                r['solar_curtailment'] = (r['solar_uigf'] - r['solar_cleared']).clip(lower=0)
                r['wind_curtailment'] = (r['wind_uigf'] - r['wind_cleared']).clip(lower=0)
                r['total_curtailment'] = r['solar_curtailment'] + r['wind_curtailment']
                out['curtailment_regional5'] = r[r['regionid'].isin(MAIN_REGIONS)]
        elif feed == 'scada':
            df = c.parse_mms_csv(content, 'UNIT_SCADA')
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                d = pd.DataFrame()
                d['settlementdate'] = self._ts(df['SETTLEMENTDATE'])
                d['duid'] = df['DUID'].str.strip()
                d['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
                out['scada5'] = d[d['scadavalue'].notna()]
        elif feed == 'demand':
            d = c._parse_demand_csv(content.decode('utf-8', errors='ignore'))
            if not d.empty:
                out['demand30'] = d[d['regionid'].isin(MAIN_REGIONS)]
        elif feed == 'snsg':
            d = c._parse_demand_less_snsg_csv(content.decode('utf-8', errors='ignore'))
            if not d.empty:
                out['demand_less_snsg'] = d[d['regionid'].isin(MAIN_REGIONS)]
        elif feed == 'rooftop':
            df = c.parse_mms_csv(content, 'ACTUAL')
            if not df.empty and {'INTERVAL_DATETIME', 'REGIONID', 'POWER'} <= set(df.columns):
                d = pd.DataFrame()
                d['settlementdate'] = self._ts(df['INTERVAL_DATETIME'])
                d['regionid'] = df['REGIONID'].str.strip()
                d['power'] = pd.to_numeric(df['POWER'], errors='coerce')
                d['quality_indicator'] = (df['QI'].str.strip() if 'QI' in df.columns
                                          else pd.array([pd.NA] * len(df), dtype='string'))
                d['type'] = (df['TYPE'].str.strip() if 'TYPE' in df.columns
                             else 'MEASUREMENT')
                d = d[d['power'].notna() & (d['power'] >= 0)
                      & d['regionid'].isin(MAIN_REGIONS)]
                out['rooftop30'] = d
        elif feed == 'nextday':
            out.update(self._parse_nextday(content, wanted))
        return out

    # The two Next_Day_Dispatch parsers mirror UnifiedAEMOCollector
    # .collect_5min_curtailment / .collect_5min_duid_curtailment line for line
    # (positional UNIT_SOLUTION columns: 4 settlementdate, 6 duid, 14 totalcleared,
    # 36 availability, 59 semidispatchcap, 68 uigf). Those methods fuse download
    # and parse, so they cannot be called on a file already in hand; tests pin the
    # two together on a synthetic file.
    _WIND_SOLAR = re.compile(r'(WF|SF|SOLAR|WIND|PV)', re.IGNORECASE)

    def _parse_nextday(self, content, wanted=None):
        legacy, duid5 = [], []
        want = ({f'{t:%Y/%m/%d %H:%M:%S}' for t in wanted} if wanted else None)
        for line in content.decode('utf-8', errors='ignore').split('\n'):
            if not line.startswith('D,DISPATCH,UNIT_SOLUTION'):
                continue
            parts = line.split(',')
            if len(parts) <= 60:
                continue
            sd = parts[4].strip('"')
            if want is not None and sd not in want:
                continue
            duid = parts[6].strip('"')
            try:
                totalcleared = float(parts[14]) if parts[14] else 0.0
                if self._WIND_SOLAR.search(duid):
                    availability = float(parts[36]) if parts[36] else 0.0
                    semidispatchcap = int(parts[59]) if parts[59] else 0
                    curtailment = 0.0
                    if semidispatchcap == 1:
                        if 'SF' in duid or 'SOLAR' in duid.upper():
                            if availability > 1.0:
                                curtailment = max(0.0, availability - totalcleared)
                        else:
                            curtailment = max(0.0, availability - totalcleared)
                    legacy.append({'settlementdate': sd, 'duid': duid,
                                   'availability': availability,
                                   'totalcleared': totalcleared,
                                   'semidispatchcap': semidispatchcap,
                                   'curtailment': curtailment})
            except (ValueError, IndexError):
                pass
            if len(parts) > 68:
                try:
                    uigf = float(parts[68]) if parts[68] else 0.0
                    if uigf > 0:
                        duid5.append({'settlementdate': sd, 'duid': duid, 'uigf': uigf,
                                      'totalcleared': totalcleared,
                                      'curtailment': max(0.0, uigf - totalcleared)})
                except (ValueError, IndexError):
                    pass
        out = {}
        for name, rows in (('curtailment5', legacy), ('curtailment_duid5', duid5)):
            if rows:
                d = pd.DataFrame(rows)
                d['settlementdate'] = pd.to_datetime(d['settlementdate'],
                                                     format='%Y/%m/%d %H:%M:%S')
                out[name] = d.drop_duplicates(subset=['settlementdate', 'duid'])
        return out

    @staticmethod
    def _inner_csv(blob):
        try:
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                csvs = [n for n in z.namelist() if n.lower().endswith('.csv')]
                return z.read(csvs[0]) if csvs else None
        except zipfile.BadZipFile:
            return blob

    def _listing(self, url):
        if url not in self._listing_cache:
            self._listing_cache[url] = self.collector._get_with_retry(url, timeout=120).text
        return self._listing_cache[url]

    def _current_files(self, feed):
        key = ('cur', feed)
        if key not in self._listing_cache:
            f = FEEDS[feed]
            self._listing_cache[key] = self.collector.get_latest_files(
                CURRENT_ROOT + f['cur'], f['prefix'])
        return self._listing_cache[key]

    def _save(self, frames, wanted, tag):
        """Write the requested rows of each parsed frame to the stage.

        A DispatchIS file parses into four tables, but only the tables that have
        a gap at a given interval are wanted: staging bdu5 rows for a 2023 price
        gap would extend bdu5's history before it began.
        """
        n = 0
        for name, df in frames.items():
            if df is None or df.empty:
                continue
            if self._wanted is not None:
                if name not in self._wanted:
                    continue
                df = df[df['settlementdate'].isin(set(wanted) & self._wanted[name])]
            else:
                df = df[df['settlementdate'].isin(wanted)]
            if not df.empty:
                df.to_parquet(STAGE / f"{name}_{tag}.parquet", index=False)
                n += df['settlementdate'].nunique()
        return n

    # ---------------------------------------------------------------- staging

    def stage_current(self, feed, stamps):
        """Stage `stamps` from CURRENT files. Returns the stamps found."""
        if feed == 'nextday':
            return self._stage_nextday(stamps)
        cfg = FEEDS[feed]
        url = CURRENT_ROOT + cfg['cur']
        by_stamp = defaultdict(list)
        for f in self._current_files(feed):
            m = _STAMP_RE.search(f[len(cfg['prefix']) - 1:])
            if m:
                by_stamp[m.group(1)].append(f)
        done = []
        for ts in stamps:
            for off in cfg['offsets']:
                key = f"{ts + timedelta(minutes=off):%Y%m%d%H%M}"
                if key not in by_stamp:
                    continue
                try:
                    blob = self.collector._get_with_retry(
                        url + sorted(by_stamp[key])[-1], timeout=120).content
                    content = self._inner_csv(blob)
                    if content and self._save(self._parse(content, feed), {ts},
                                              f"cur_{feed}_{ts:%Y%m%d%H%M}"):
                        done.append(ts)
                        break
                except Exception as e:
                    self.log(f"    CURRENT {feed} {ts} failed: {e}")
                _sleep(0.5)
        return done

    def _stage_nextday(self, stamps):
        """One CURRENT zip per trading day D holds D 04:05 .. D+1 04:00."""
        cfg = FEEDS['nextday']
        url = CURRENT_ROOT + cfg['cur']
        files = self._current_files('nextday')
        by_day = defaultdict(set)
        for ts in stamps:
            by_day[(ts - timedelta(hours=4, minutes=5)).date()].add(ts)
        done = []
        for day, want in sorted(by_day.items()):
            names = [f for f in files if f'{cfg["prefix"]}{day:%Y%m%d}_' in f]
            if not names:
                continue
            try:
                blob = self.collector._get_with_retry(url + sorted(names)[-1],
                                                      timeout=300).content
                content = self._inner_csv(blob)
                frames = self._parse(content, 'nextday', wanted=want) if content else {}
                if self._save(frames, want, f"cur_nextday_{day:%Y%m%d}"):
                    got = set()
                    for v in frames.values():
                        got |= {pd.Timestamp(x) for x in v['settlementdate'].unique()}
                    done.extend(t for t in want if pd.Timestamp(t) in got)
            except Exception as e:
                self.log(f"    CURRENT nextday {day} failed: {e}")
            _sleep(1.0)
        return done

    def stage_archive(self, feed, stamps):
        """Daily ARCHIVE zips (dispatch feeds). Group by the date whose zip holds
        each interval; midnight belongs to the previous day's file."""
        cfg = FEEDS[feed]
        if cfg['archive'] == 'weekly':
            return self._stage_archive_weekly(feed, stamps)
        if cfg['archive'] != 'daily':
            return []
        url = ARCHIVE_ROOT + cfg['arc']
        listing = self._listing(url)
        by_file_date = defaultdict(set)
        for ts in stamps:
            fd = ts.date() - timedelta(days=1) if (ts.hour == 0 and ts.minute == 0) \
                else ts.date()
            by_file_date[fd].add(ts)

        done = []
        for fd, want in sorted(by_file_date.items()):
            m = sorted(set(re.findall(
                rf'(PUBLIC_[A-Z_]*{fd:%Y%m%d}[0-9_]*\.zip)', listing)))
            if not m:
                continue
            try:
                blob = self.collector._get_with_retry(url + m[-1], timeout=600).content
            except Exception as e:
                self.log(f"    ARCHIVE {feed} {fd} failed: {e}")
                continue
            targets = {f"{t:%Y%m%d%H%M}" for t in want}
            frames = defaultdict(list)
            with zipfile.ZipFile(io.BytesIO(blob)) as outer:
                for n in [x for x in outer.namelist() if any(t in x for t in targets)]:
                    content = self._inner_csv(outer.read(n))
                    if not content:
                        continue
                    for k, v in self._parse(content, feed).items():
                        frames[k].append(v)
            merged = {k: pd.concat(v, ignore_index=True) for k, v in frames.items() if v}
            if self._save(merged, want, f"arc_{feed}_{fd:%Y%m%d}"):
                got = set()
                for v in merged.values():
                    got |= set(v['settlementdate'].unique())
                done.extend(t for t in want if pd.Timestamp(t) in got)
            _sleep(1.0)
        return done

    def _stage_archive_weekly(self, feed, stamps):
        """Weekly ARCHIVE zips: named by their first date, each holds that week's
        30-minute zips. A zip can also carry the previous evening's last interval
        (rooftop) so both neighbours of a boundary are tried."""
        cfg = FEEDS[feed]
        url = ARCHIVE_ROOT + cfg['arc']
        listing = self._listing(url)
        names = {}
        for m in re.finditer(rf'{re.escape(cfg["prefix"])}(\d{{8}})\.zip', listing):
            names[datetime.strptime(m.group(1), '%Y%m%d').date()] = m.group(0)
        starts = sorted(names)
        by_zip = defaultdict(set)
        for ts in stamps:
            for probe in (ts - timedelta(hours=1), ts + timedelta(hours=1)):
                cand = [d for d in starts if d <= probe.date()]
                if cand:
                    by_zip[cand[-1]].add(ts)
        done = []
        for d, want in sorted(by_zip.items()):
            try:
                blob = self.collector._get_with_retry(url + names[d], timeout=600).content
            except Exception as e:
                self.log(f"    ARCHIVE {feed} {d} failed: {e}")
                continue
            frames = defaultdict(list)
            with zipfile.ZipFile(io.BytesIO(blob)) as outer:
                for n in outer.namelist():
                    content = self._inner_csv(outer.read(n))
                    if not content:
                        continue
                    for k, v in self._parse(content, feed).items():
                        frames[k].append(v[v['settlementdate'].isin(want)])
            merged = {k: pd.concat(v, ignore_index=True) for k, v in frames.items() if v}
            if self._save(merged, want, f"arc_{feed}_{d:%Y%m%d}"):
                got = set()
                for v in merged.values():
                    got |= {pd.Timestamp(x) for x in v['settlementdate'].unique()}
                done.extend(t for t in want if pd.Timestamp(t) in got)
            _sleep(1.0)
        return done

    def stage_mmsdm(self, table, stamps):
        by_month = defaultdict(list)
        for ts in stamps:
            # the first interval of a month sits in the previous month's file
            ref = ts - timedelta(minutes=5)
            by_month[(ref.year, ref.month)].append(ts)

        done = []
        for (y, mo), want in sorted(by_month.items()):
            stem = f"{y}{mo:02d}010000"
            name = MMSDM_TABLE[table]
            if y <= 2024:
                url = (f"{MMSDM_ROOT}/{y}/MMSDM_{y}_{mo:02d}/MMSDM_Historical_Data_SQLLoader/"
                       f"DATA/PUBLIC_DVD_{name}_{stem}.zip")
            else:
                url = (f"{MMSDM_ROOT}/{y}/MMSDM_{y}_{mo:02d}/MMSDM_Historical_Data_SQLLoader/"
                       f"DATA/PUBLIC_ARCHIVE%23{name}%23FILE01%23{stem}.zip")
            try:
                blob = self.collector._get_with_retry(url, timeout=900).content
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    fn = [n for n in z.namelist() if n.upper().endswith('.CSV')][0]
                    raw = z.read(fn).decode('utf-8', errors='replace')
            except Exception as e:
                self.log(f"    MMSDM {y}-{mo:02d} {name} failed: {e}")
                continue

            keys = {f'"{s:%Y/%m/%d %H:%M:%S}"' for s in want}
            header, kept = None, []
            for line in raw.splitlines():
                if line.startswith('I,'):
                    header = line
                elif line.startswith('D,') and any(k in line for k in keys):
                    kept.append(line)
            if not header or not kept:
                continue
            cols = [c.strip().strip('"') for c in header.split(',')]
            df = pd.read_csv(io.StringIO('\n'.join(kept)), names=cols,
                             dtype=str, on_bad_lines='skip')

            out = pd.DataFrame()
            out['settlementdate'] = pd.to_datetime(df['SETTLEMENTDATE'],
                                                   format='%Y/%m/%d %H:%M:%S')
            if table == 'prices5':
                if 'INTERVENTION' in df.columns:
                    df = df[df['INTERVENTION'].astype(str).str.strip() == '0']
                    out = out.loc[df.index]
                out['regionid'] = df['REGIONID'].str.strip()
                out['rrp'] = pd.to_numeric(df['RRP'], errors='coerce')
                out = out[out['regionid'].isin(MAIN_REGIONS)]
            else:
                out['duid'] = df['DUID'].str.strip()
                out['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
                out = out[out['scadavalue'].notna()]
            out = out.drop_duplicates()
            if self._save({table: out}, set(want), f"mms_{y}{mo:02d}"):
                got = set(out['settlementdate'].unique())
                done.extend(t for t in want if pd.Timestamp(t) in got)
        return done

    # ---------------------------------------------------------------- driver

    def stage_all(self, gaps, now=None):
        """Route every missing interval to the right source and stage it.

        Derived 30-minute tables are not staged: apply() rebuilds them from the
        5-minute tables. Tables with no feed are survey-only.
        """
        now = now or nem_now()
        work = defaultdict(lambda: defaultdict(set))   # feed -> frame name -> stamps
        mmsdm = defaultdict(set)           # table -> stamps (older than ARCHIVE)
        for table, stamps in gaps.items():
            if table == SNSG:
                work['snsg']['demand_less_snsg'] |= set(stamps)
                continue
            spec = TABLES.get(table)
            if not spec or not spec.feed or not stamps:
                continue
            work[spec.feed][table] |= set(stamps)
            if table == 'demand30':        # new demand rows need their SNSG value too
                work['snsg']['demand_less_snsg'] |= set(stamps)
            if table in MMSDM_TABLE:
                mmsdm[table] |= set(stamps)

        for feed, wanted in sorted(work.items()):
            self._wanted = {k: set(v) for k, v in wanted.items()}
            stamps = set().union(*wanted.values())
            cfg = FEEDS[feed]
            cut = now - timedelta(days=cfg['cur_days'])
            cur = sorted(t for t in stamps if t >= cut)
            done = set()
            if cur:
                self.log(f"  {feed}/current: {len(cur)} intervals")
                done |= set(self.stage_current(feed, cur))
            arc = sorted(t for t in stamps if t not in done)
            if arc and cfg['archive']:
                earliest = self._archive_earliest(feed)
                arc = [t for t in arc if earliest and t.date() >= earliest]
                if arc:
                    self.log(f"  {feed}/archive: {len(arc)} intervals")
                    done |= set(self.stage_archive(feed, arc))
            for table in [t for t in mmsdm if TABLES[t].feed == feed]:
                old = sorted(t for t in mmsdm[table] if t not in done)
                earliest = self._archive_earliest(feed)
                old = [t for t in old if earliest and t.date() < earliest]
                if old:
                    self.log(f"  mmsdm/{table}: {len(old)} intervals")
                    self.stage_mmsdm(table, old)

        self._wanted = None

    def _archive_earliest(self, feed='dispatchis'):
        cfg = FEEDS[feed]
        if not cfg['arc']:
            return None
        try:
            listing = self._listing(ARCHIVE_ROOT + cfg['arc'])
            st = sorted(set(re.findall(rf'{re.escape(cfg["prefix"])}(\d{{8}})', listing)))
            if not st and feed in ('dispatchis', 'scada'):
                st = sorted(set(re.findall(r'_(\d{8})', listing)))
            return datetime.strptime(st[0], '%Y%m%d').date() if st else None
        except Exception:
            return None

    # ---------------------------------------------------------------- apply

    def apply(self, con=None, gaps=None):
        """Insert staged rows and rebuild derived 30-minute labels.

        Needs the write lock: pass a connection from acquire_primary(), or None
        to open PRIMARY directly (tests, or a collector that is stopped).
        `gaps` (the survey result) lets absent derived labels be rebuilt even
        when no 5-minute rows were inserted in this run.

        Every insert is an anti-join on the table's key. Returns
        {table: rows inserted}, plus 'demand_less_snsg': NULLs filled.
        """
        own = con is None
        con = con or duckdb.connect(PRIMARY)
        inserted = {}
        new_pairs = {}
        try:
            for table in STAGED_TABLES:
                inserted[table] = 0
                files = list(STAGE.glob(f'{table}_*.parquet'))
                if not files:
                    continue
                spec = TABLES[table]
                if not con.execute("SELECT count(*) FROM information_schema.tables "
                                   f"WHERE table_name = '{table}'").fetchone()[0]:
                    self.log(f'  {table} absent from the database, staged rows ignored')
                    continue
                g = str(STAGE / f'{table}_*.parquet')
                cols = ', '.join(spec.cols)
                on = ' AND '.join(f't.{k} = s.{k}' for k in spec.keys)
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE new_rows AS
                    SELECT {cols} FROM (SELECT DISTINCT {cols} FROM read_parquet('{g}', union_by_name=true)) s
                    WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {on})
                """)
                n = con.execute("SELECT count(*) FROM new_rows").fetchone()[0]
                if n:
                    con.execute(f"INSERT INTO {table} ({cols}) SELECT {cols} FROM new_rows")
                    new_pairs[table] = con.execute(
                        f"SELECT DISTINCT settlementdate, {spec.keys[1]} AS k "
                        "FROM new_rows").fetchdf()
                inserted[table] = n

            inserted['demand_less_snsg'] = 0
            sfiles = list(STAGE.glob('demand_less_snsg_*.parquet'))
            if sfiles:
                g = str(STAGE / 'demand_less_snsg_*.parquet')
                before = con.execute(
                    "SELECT count(*) FROM demand30 WHERE demand_less_snsg IS NULL"
                ).fetchone()[0]
                con.execute(f"""
                    UPDATE demand30 SET demand_less_snsg = s.demand_less_snsg
                    FROM (SELECT DISTINCT settlementdate, regionid, demand_less_snsg
                          FROM read_parquet('{g}', union_by_name=true)) s
                    WHERE demand30.settlementdate = s.settlementdate
                      AND demand30.regionid = s.regionid
                      AND demand30.demand_less_snsg IS NULL
                """)
                inserted['demand_less_snsg'] = before - con.execute(
                    "SELECT count(*) FROM demand30 WHERE demand_less_snsg IS NULL"
                ).fetchone()[0]

            self._rebuild_derived(con, new_pairs, gaps or {}, inserted)
        finally:
            if own:
                con.close()
        return inserted

    def _rebuild_derived(self, con, new_pairs, gaps, inserted):
        """Rebuild 30-minute rows from the 5-minute table, only where all six
        intervals of the (label - 30 min, label] bucket exist.

        A (label, key) row is replaced only when a 5-minute row for that key was
        newly inserted into its bucket: it was built from an incomplete bucket.
        Labels the survey found absent are filled by anti-join. Every other 30-minute
        row is left as it is.
        """
        for tgt, src, key, aggs, cols_out, extra in DERIVED:
            pairs = new_pairs.get(src)
            rep = pd.DataFrame(columns=['label', 'k'])
            if pairs is not None and len(pairs):
                rep = pd.DataFrame({
                    'label': [label_of(pd.Timestamp(t)) for t in pairs['settlementdate']],
                    'k': pairs['k'].values}).drop_duplicates()
            fill = {pd.Timestamp(t) for t in gaps.get(tgt, [])}
            inserted[tgt] = 0
            if rep.empty and not fill:
                continue
            if not con.execute("SELECT count(*) FROM information_schema.tables "
                               f"WHERE table_name = '{tgt}'").fetchone()[0]:
                self.log(f'  {tgt} absent, skipping derived rebuild')
                continue
            labels = pd.DataFrame({'label': pd.to_datetime(
                sorted(set(rep['label']) | fill))})
            con.register('labels_df', labels)
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE rebuilt AS
                SELECT a.label AS settlementdate, s.{key}, {aggs}
                FROM labels_df a JOIN {src} s
                  ON s.settlementdate > a.label - INTERVAL '30 minutes'
                 AND s.settlementdate <= a.label
                WHERE 1=1 {extra}
                GROUP BY a.label, s.{key}
                HAVING count(DISTINCT s.settlementdate) = 6
            """)
            con.unregister('labels_df')
            if not rep.empty:
                rep['label'] = pd.to_datetime(rep['label'])
                con.register('rep_df', rep)
                con.execute(f"""
                    DELETE FROM {tgt} WHERE EXISTS (
                      SELECT 1 FROM rep_df p, rebuilt r
                      WHERE p.label = {tgt}.settlementdate AND p.k = {tgt}.{key}
                        AND r.settlementdate = p.label AND r.{key} = p.k)
                """)
                con.unregister('rep_df')
            before = con.execute(f"SELECT count(*) FROM {tgt}").fetchone()[0]
            con.execute(f"""
                INSERT INTO {tgt} (settlementdate, {key}, {cols_out})
                SELECT settlementdate, {key}, {cols_out} FROM rebuilt r
                WHERE NOT EXISTS (SELECT 1 FROM {tgt} t
                                  WHERE t.settlementdate = r.settlementdate
                                    AND t.{key} = r.{key})
            """)
            inserted[tgt] = con.execute(f"SELECT count(*) FROM {tgt}").fetchone()[0] - before

    @staticmethod
    def clear_stage():
        for p in STAGE.glob('*.parquet'):
            p.unlink()
