#!/usr/bin/env python3
"""Gap survey and repair for the 5-minute AEMO source tables.

Written 13-Aug-2026 after a 3-hour nemweb publishing outage exposed that the
weekly check found holes it could not fix: it only ever called the CURRENT-based
scripts, so anything older than ~2 days was logged as "manual ARCHIVE" and left.
1,687 intervals had accumulated that way, the oldest from July 2024.

Three sources, chosen by age:

  CURRENT  (< ~2 days)   individual 5-minute zips
  ARCHIVE  (< ~12 months) one daily zip per date, holding ~288 nested zips
  MMSDM    (older)        one monthly zip per table

Two boundary rules that cost real intervals when missed:

  * A daily ARCHIVE zip for date D spans 00:05 on D through 00:00 on D+1, so the
    00:00 interval of day D lives in day D-1's file. Same at month boundaries in
    MMSDM. Ignoring this leaves exactly one interval short per affected date.
  * A file can age out of CURRENT (~2 days) before ARCHIVE publishes it (ARCHIVE
    lags 1-2 days). Intervals in that seam are briefly unreachable and simply
    need retrying on a later run -- they are not permanent losses.

Staging is separated from applying so downloads run while the collector is live;
only the insert needs the write lock.
"""
import io
import os
import re
import sys
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

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

# Which nemweb feed serves which table.
FEED = {
    'prices5': 'dispatchis',
    'transmission5': 'dispatchis',
    'scada5': 'scada',
}
FEED_DIR = {'dispatchis': 'DispatchIS_Reports', 'scada': 'Dispatch_SCADA'}
FEED_PREFIX = {'dispatchis': 'PUBLIC_DISPATCHIS_', 'scada': 'PUBLIC_DISPATCHSCADA_'}
MMSDM_TABLE = {'prices5': 'DISPATCHPRICE', 'scada5': 'DISPATCH_UNIT_SCADA'}

KEYS = {
    'prices5': ['settlementdate', 'regionid'],
    'scada5': ['settlementdate', 'duid'],
    'transmission5': ['settlementdate', 'interconnectorid'],
}
COLS = {
    'prices5': ['settlementdate', 'regionid', 'rrp'],
    'scada5': ['settlementdate', 'duid', 'scadavalue'],
    'transmission5': ['settlementdate', 'interconnectorid', 'meteredmwflow',
                      'mwflow', 'exportlimit', 'importlimit', 'mwlosses'],
}


class GapRepairer:
    def __init__(self, log=print):
        self.log = log
        self.collector = UnifiedAEMOCollector()
        self._listing_cache = {}
        STAGE.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------- survey

    def survey(self, tables=('prices5', 'scada5', 'transmission5'), db=REPLICA,
               trailing_lag_minutes=20):
        """Return {table: [missing timestamps]} by absolute enumeration.

        Enumerates every expected interval rather than diffing adjacent rows, so
        a single missing interval is visible. The LAG-based detectors elsewhere
        in this tree test `> 600 seconds`, and one absent 5-minute interval is a
        gap of exactly 600 -- which is why isolated holes survived every pass.

        The upper bound is `now - trailing_lag_minutes`, not MAX(settlementdate).
        Bounding at MAX makes a hole at the newest end invisible, because the
        hole moves the bound with it -- the defect that let demand30 sit empty
        for 14 hours through a check that reported OK. The lag allowance keeps
        normal publication delay from reading as a gap.
        """
        con = duckdb.connect(db, read_only=True)
        upper_bound = datetime.now() - timedelta(minutes=trailing_lag_minutes)
        upper_bound -= timedelta(minutes=upper_bound.minute % 5,
                                 seconds=upper_bound.second,
                                 microseconds=upper_bound.microsecond)
        out = {}
        try:
            for t in tables:
                mn, mx = con.execute(
                    f'SELECT MIN(settlementdate), MAX(settlementdate) FROM "{t}"'
                ).fetchone()
                if mn is None:
                    out[t] = []
                    continue
                end = max(mx, upper_bound)
                rows = con.execute(f"""
                    WITH g AS (SELECT generate_series AS d FROM generate_series(
                                 TIMESTAMP '{mn}', TIMESTAMP '{end}', INTERVAL 5 MINUTE)),
                         i AS (SELECT DISTINCT settlementdate AS d FROM "{t}")
                    SELECT g.d FROM g LEFT JOIN i USING (d)
                    WHERE i.d IS NULL ORDER BY 1
                """).fetchall()
                out[t] = [r[0] for r in rows]
        finally:
            con.close()
        return out

    # ---------------------------------------------------------------- parsing

    def _parse(self, content, feed):
        out = {}
        c = self.collector
        if feed == 'dispatchis':
            df = c.parse_mms_csv(content, 'PRICE')
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                d = pd.DataFrame()
                d['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'), format='%Y/%m/%d %H:%M:%S')
                d['regionid'] = df['REGIONID'].str.strip()
                d['rrp'] = pd.to_numeric(df['RRP'], errors='coerce')
                out['prices5'] = d[d['regionid'].isin(MAIN_REGIONS)]
            df = c.parse_mms_csv(content, 'INTERCONNECTORRES')
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                d = pd.DataFrame()
                d['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'), format='%Y/%m/%d %H:%M:%S')
                d['interconnectorid'] = df['INTERCONNECTORID'].str.strip()
                for src, tgt in (('METEREDMWFLOW', 'meteredmwflow'), ('MWFLOW', 'mwflow'),
                                 ('EXPORTLIMIT', 'exportlimit'),
                                 ('IMPORTLIMIT', 'importlimit'), ('MWLOSSES', 'mwlosses')):
                    d[tgt] = (pd.to_numeric(df[src], errors='coerce')
                              if src in df.columns else pd.NA)
                out['transmission5'] = d
        else:
            df = c.parse_mms_csv(content, 'UNIT_SCADA')
            if not df.empty and 'SETTLEMENTDATE' in df.columns:
                d = pd.DataFrame()
                d['settlementdate'] = pd.to_datetime(
                    df['SETTLEMENTDATE'].str.strip('"'), format='%Y/%m/%d %H:%M:%S')
                d['duid'] = df['DUID'].str.strip()
                d['scadavalue'] = pd.to_numeric(df['SCADAVALUE'], errors='coerce')
                out['scada5'] = d[d['scadavalue'].notna()]
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

    def _save(self, frames, wanted, tag):
        n = 0
        for name, df in frames.items():
            if df is None or df.empty:
                continue
            df = df[df['settlementdate'].isin(wanted)]
            if not df.empty:
                df.to_parquet(STAGE / f"{name}_{tag}.parquet", index=False)
                n += df['settlementdate'].nunique()
        return n

    # ---------------------------------------------------------------- staging

    def stage_current(self, feed, stamps):
        url = CURRENT_ROOT + FEED_DIR[feed] + '/'
        files = self.collector.get_latest_files(url, FEED_PREFIX[feed])
        by_stamp = defaultdict(list)
        for f in files:
            m = re.search(r'_(\d{12})_', f)
            if m:
                by_stamp[m.group(1)].append(f)
        done = []
        for ts in stamps:
            key = f"{ts:%Y%m%d%H%M}"
            if key not in by_stamp:
                continue
            try:
                blob = self.collector._get_with_retry(
                    url + sorted(by_stamp[key])[-1], timeout=120).content
                content = self._inner_csv(blob)
                if content and self._save(self._parse(content, feed), {ts}, f"cur_{key}"):
                    done.append(ts)
            except Exception as e:
                self.log(f"    CURRENT {ts} failed: {e}")
            time.sleep(0.5)
        return done

    def stage_archive(self, feed, stamps):
        """Group by the date whose daily zip holds each interval.

        Midnight belongs to the previous day's file -- see the module docstring.
        """
        url = ARCHIVE_ROOT + FEED_DIR[feed] + '/'
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
                self.log(f"    ARCHIVE {fd} failed: {e}")
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
            if self._save(merged, want, f"arc_{fd:%Y%m%d}"):
                got = set()
                for v in merged.values():
                    got |= set(v['settlementdate'].unique())
                done.extend(t for t in want if pd.Timestamp(t) in got)
            time.sleep(1.0)
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

    def stage_all(self, gaps):
        """Route every missing interval to the right source and stage it."""
        now = datetime.now()
        current_cut = now - timedelta(days=2)
        archive_cut = self._archive_earliest()

        by_feed = defaultdict(lambda: {'current': [], 'archive': []})
        mmsdm = defaultdict(list)
        for table, stamps in gaps.items():
            feed = FEED[table]
            for ts in stamps:
                if ts >= current_cut:
                    by_feed[feed]['current'].append(ts)
                elif archive_cut and ts.date() >= archive_cut:
                    by_feed[feed]['archive'].append(ts)
                elif table in MMSDM_TABLE:
                    mmsdm[table].append(ts)

        for feed, buckets in by_feed.items():
            for kind in ('current', 'archive'):
                stamps = sorted(set(buckets[kind]))
                if not stamps:
                    continue
                self.log(f"  {feed}/{kind}: {len(stamps)} intervals")
                fn = self.stage_current if kind == 'current' else self.stage_archive
                fn(feed, stamps)
        for table, stamps in mmsdm.items():
            stamps = sorted(set(stamps))
            self.log(f"  mmsdm/{table}: {len(stamps)} intervals")
            self.stage_mmsdm(table, stamps)

    def _archive_earliest(self):
        try:
            listing = self._listing(ARCHIVE_ROOT + 'DispatchIS_Reports/')
            st = sorted(set(re.findall(r'_(\d{8})', listing)))
            return datetime.strptime(st[0], '%Y%m%d').date() if st else None
        except Exception:
            return None

    def apply(self):
        """Insert staged rows and rebuild affected 30-min labels. Needs the lock."""
        con = duckdb.connect(PRIMARY)
        inserted = {}
        try:
            for table in ('prices5', 'scada5', 'transmission5'):
                if not list(STAGE.glob(f'{table}_*.parquet')):
                    inserted[table] = 0
                    continue
                g = str(STAGE / f'{table}_*.parquet')
                cols = ', '.join(COLS[table])
                on = ' AND '.join(f't.{k} = s.{k}' for k in KEYS[table])
                n = con.execute(f"""
                    SELECT count(*) FROM (SELECT DISTINCT {cols}
                      FROM read_parquet('{g}')) s
                    WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {on})
                """).fetchone()[0]
                if n:
                    con.execute(f"""
                        INSERT INTO {table} ({cols})
                        SELECT {cols} FROM (SELECT DISTINCT {cols}
                          FROM read_parquet('{g}')) s
                        WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {on})
                    """)
                inserted[table] = n

            for tgt, src, key, aggs, cols_out, extra in (
                ('prices30', 'prices5', 'regionid', 'AVG(s.rrp) AS rrp', 'rrp',
                 f"AND s.regionid IN ({','.join(repr(r) for r in sorted(MAIN_REGIONS))})"),
                ('transmission30', 'transmission5', 'interconnectorid',
                 'AVG(s.meteredmwflow) AS meteredmwflow, AVG(s.mwflow) AS mwflow, '
                 'AVG(s.mwlosses) AS mwlosses, AVG(s.exportlimit) AS exportlimit, '
                 'AVG(s.importlimit) AS importlimit',
                 'meteredmwflow, mwflow, mwlosses, exportlimit, importlimit', ''),
            ):
                if not inserted.get(src):
                    continue
                exists = con.execute(
                    "SELECT count(*) FROM information_schema.tables "
                    f"WHERE table_name = '{tgt}'").fetchone()[0]
                if not exists:
                    self.log(f'  {tgt} absent, skipping derived rebuild')
                    continue
                g = str(STAGE / f'{src}_*.parquet')
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE affected AS
                    SELECT DISTINCT
                      CASE WHEN (extract(minute FROM settlementdate)::int % 30) = 0
                           THEN settlementdate
                           ELSE date_trunc('hour', settlementdate) + INTERVAL '30 minutes'
                                * (floor(extract(minute FROM settlementdate)/30.0)+1)
                      END AS label FROM read_parquet('{g}')
                """)
                con.execute(f"""
                    CREATE OR REPLACE TEMP TABLE rebuilt AS
                    SELECT a.label AS settlementdate, s.{key}, {aggs}
                    FROM affected a JOIN {src} s
                      ON s.settlementdate > a.label - INTERVAL '30 minutes'
                     AND s.settlementdate <= a.label
                    WHERE 1=1 {extra}
                    GROUP BY a.label, s.{key}
                    HAVING count(DISTINCT s.settlementdate) = 6
                """)
                con.execute(f"DELETE FROM {tgt} WHERE settlementdate IN "
                            f"(SELECT DISTINCT settlementdate FROM rebuilt)")
                con.execute(f"INSERT INTO {tgt} (settlementdate, {key}, {cols_out}) "
                            f"SELECT settlementdate, {key}, {cols_out} FROM rebuilt")
        finally:
            con.close()
        return inserted

    @staticmethod
    def clear_stage():
        for p in STAGE.glob('*.parquet'):
            p.unlink()
