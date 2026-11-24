#!/usr/bin/env python3
"""DuckDB storage for AEMO network-constraint data (separate ``constraints.duckdb``).

Same shape as bids_store: its own database file so the hot per-cycle
``aemo_test.duckdb`` copy does not grow. The schema, the loaders for AEMO-format CSV
files and the two views live here so the history backfill and a later live collector
share one code path.

Rules:
  * Every interval and definition insert is an anti-join on the table key. Existing rows are
    never changed or deleted; re-running a load is a no-op.
  * Three snapshot tables are the exception, because each file restates current state:
    ``dudetailsummary`` is replaced whole by a newer snapshot (stale END_DATE values would
    otherwise overlap the new rows and duplicate the date-ranged view join), and the two
    network-outage tables are upserted per OUTAGEID (revised end times and statuses are
    picked up; outages absent from the incoming file are kept).
  * Column names are lowercase. Source files are read as text and cast by the schema
    here, inserting by name: a column missing from an old file is NULL, an extra
    column in a new file is ignored.
  * ``_loaded`` records (tbl, ym, part) once a table-month has loaded in full, so a
    backfill can skip it without downloading.
  * Nothing derived is stored. ``unit_binding`` and ``local_price_computed`` are views.
"""
from __future__ import annotations

import os
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

import duckdb

TS_FORMAT = "%Y/%m/%d %H:%M:%S"


def default_db_path() -> Path:
    env = os.getenv("AEMO_CONSTRAINTS_DUCKDB_PATH")
    return Path(env) if env else Path.home() / "aemo_production/data/constraints.duckdb"


# column type codes: ts TIMESTAMP, int BIGINT, dbl DOUBLE, str VARCHAR
_SQL_TYPE = {"ts": "TIMESTAMP", "int": "BIGINT", "dbl": "DOUBLE", "str": "VARCHAR"}


class Spec:
    def __init__(self, cols: Dict[str, str], key: Sequence[str]):
        self.cols, self.key = cols, list(key)


TABLES: Dict[str, Spec] = {
    "constraint_binding5": Spec(dict(
        settlementdate="ts", constraintid="str", intervention="int", rhs="dbl", marginalvalue="dbl",
        violationdegree="dbl", lhs="dbl", genconid_effectivedate="ts", genconid_versionno="int"),
        ["settlementdate", "constraintid", "intervention"]),
    "constraint_intervals": Spec(dict(settlementdate="ts", n_listed="int", n_binding="int"),
                                 ["settlementdate"]),
    "local_price5": Spec(dict(settlementdate="ts", duid="str", local_price_adjustment="dbl",
                              locally_constrained="int"), ["settlementdate", "duid"]),
    "gencondata": Spec(dict(
        genconid="str", effectivedate="ts", versionno="int", constrainttype="str", constraintvalue="dbl",
        description="str", genericconstraintweight="dbl", dispatch="int", limittype="str", impact="str",
        source="str", reason="str", lastchanged="ts"), ["genconid", "effectivedate", "versionno"]),
    "spd_connection_point": Spec(dict(
        genconid="str", effectivedate="ts", versionno="int", connectionpointid="str", bidtype="str",
        factor="dbl"), ["genconid", "effectivedate", "versionno", "connectionpointid", "bidtype"]),
    "spd_interconnector": Spec(dict(
        genconid="str", effectivedate="ts", versionno="int", interconnectorid="str", factor="dbl"),
        ["genconid", "effectivedate", "versionno", "interconnectorid"]),
    "spd_region": Spec(dict(
        genconid="str", effectivedate="ts", versionno="int", regionid="str", bidtype="str", factor="dbl"),
        ["genconid", "effectivedate", "versionno", "regionid", "bidtype"]),
    "genconset": Spec(dict(
        genconsetid="str", effectivedate="ts", versionno="int", genconid="str", genconeffdate="ts",
        genconversionno="int"), ["genconsetid", "effectivedate", "versionno", "genconid"]),
    "genconsetinvoke": Spec(dict(
        invocation_id="int", genconsetid="str", startdate="ts", startperiod="int", enddate="ts",
        endperiod="int", intervention="str", asconstrainttype="str", startintervaldatetime="ts",
        endintervaldatetime="ts", systemnormal="int", lastchanged="ts"), ["invocation_id"]),
    "network_outagedetail": Spec(dict(
        outageid="int", substationid="str", equipmenttype="str", equipmentid="str", starttime="ts",
        endtime="ts", submitteddate="ts", outagestatuscode="str", resubmitreason="str",
        resubmitoutageid="int", recalltimeday="int", recalltimenight="int", lastchanged="str",
        reason="str", issecondary="int", actual_starttime="ts", actual_endtime="ts",
        companyrefcode="str", elementid="int"),
        ["outageid", "substationid", "equipmenttype", "equipmentid", "starttime"]),
    "network_outageconstraintset": Spec(dict(
        outageid="int", genconsetid="str", startinterval="ts", endinterval="ts"),
        ["outageid", "genconsetid", "startinterval"]),
    # Right-hand-side terms of the dispatch-scope constraint equation. FILTERED at load (see
    # RHS_FILTER_NOTE): only scope 'DS' and only constraints that appear in constraint_binding5.
    "generic_constraint_rhs": Spec(dict(
        genconid="str", effectivedate="ts", versionno="int", scope="str", termid="int", groupid="int",
        spd_id="str", spd_type="str", factor="dbl", operation="str", defaultvalue="dbl",
        parameterterm1="str", parameterterm2="str", parameterterm3="str", lastchanged="ts"),
        ["genconid", "effectivedate", "versionno", "scope", "termid"]),
    # English description of every term id (snapshot, replaced whole like dudetailsummary)
    "emsmaster": Spec(dict(spd_id="str", spd_type="str", description="str", grouping_id="str",
                           lastchanged="ts"), ["spd_id"]),
    "dudetailsummary": Spec(dict(
        duid="str", start_date="ts", end_date="ts", dispatchtype="str", connectionpointid="str",
        regionid="str", stationid="str"), ["duid", "start_date"]),
}

# MMSDM table name -> target table(s)
SOURCE_TABLES: Dict[str, str] = {
    "DISPATCHCONSTRAINT": "constraint_binding5",
    "DISPATCH_LOCAL_PRICE": "local_price5",
    "GENCONDATA": "gencondata",
    "SPDCONNECTIONPOINTCONSTRAINT": "spd_connection_point",
    "SPDINTERCONNECTORCONSTRAINT": "spd_interconnector",
    "SPDREGIONCONSTRAINT": "spd_region",
    "GENCONSET": "genconset",
    "GENCONSETINVOKE": "genconsetinvoke",
    "NETWORK_OUTAGEDETAIL": "network_outagedetail",
    "NETWORK_OUTAGECONSTRAINTSET": "network_outageconstraintset",
    "DUDETAILSUMMARY": "dudetailsummary",
    "GENERICCONSTRAINTRHS": "generic_constraint_rhs",
    "EMSMASTER": "emsmaster",
}

RHS_FILTER_NOTE = (
    "GENERICCONSTRAINTRHS is about 400,000 rows a month and the database is copied whole every "
    "collector cycle, so it is loaded FILTERED: only scope 'DS' rows whose genconid appears in "
    "constraint_binding5 (any time) are kept. Because that set grows, backfill_constraints.py "
    "--fill-rhs finds binding versions without rhs rows and fetches their effective months; it "
    "does not use _loaded for this table.")

# the key of genconsetinvoke when a file predates invocation_id
INVOKE_FALLBACK_KEY = ["genconsetid", "startdate", "startperiod"]

UNIT_BINDING_VIEW = """
CREATE OR REPLACE VIEW unit_binding AS
SELECT b.settlementdate, b.constraintid, b.marginalvalue, b.violationdegree, b.rhs,
       c.bidtype, c.factor, d.duid, d.regionid, d.dispatchtype
FROM constraint_binding5 b
JOIN spd_connection_point c
  ON c.genconid = b.constraintid
 AND c.effectivedate = b.genconid_effectivedate
 AND c.versionno = b.genconid_versionno
JOIN dudetailsummary d
  ON d.connectionpointid = c.connectionpointid
 AND b.settlementdate > d.start_date
 AND (d.end_date IS NULL OR b.settlementdate <= d.end_date)
WHERE b.intervention = 0 AND b.marginalvalue <> 0
"""

LOCAL_PRICE_COMPUTED_VIEW = """
CREATE OR REPLACE VIEW local_price_computed AS
SELECT settlementdate, duid, SUM(marginalvalue * factor) AS local_price_computed
FROM unit_binding
WHERE bidtype = 'ENERGY'
GROUP BY settlementdate, duid
"""


def ensure_tables(con) -> None:
    for name, spec in TABLES.items():
        cols = ", ".join(f"{c} {_SQL_TYPE[t]}" for c, t in spec.cols.items())
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} ({cols})")
    con.execute("CREATE TABLE IF NOT EXISTS _loaded (tbl VARCHAR, ym VARCHAR, part INTEGER, "
                "n_rows BIGINT, loaded_at TIMESTAMP)")
    con.execute(UNIT_BINDING_VIEW)
    con.execute(LOCAL_PRICE_COMPUTED_VIEW)


def is_loaded(con, tbl: str, ym: str) -> bool:
    return con.execute("SELECT 1 FROM _loaded WHERE tbl = ? AND ym = ? LIMIT 1", [tbl, ym]).fetchone() is not None


# ---------------------------------------------------------------- reading AEMO CSV

def _read_csv_sql(path: Path) -> str:
    # '#' in AEMO file names breaks the sniffer, so callers pass plain names (data.csv).
    # Everything is read as text; types are applied from the schema below.
    return (f"read_csv('{path}', skip=1, header=true, null_padding=true, ignore_errors=true, "
            f"all_varchar=true, delim=',', quote='\"', escape='\"', comment='', strict_mode=false)")


def read_raw(con, parts: Sequence[Tuple[int, Path]]) -> List[str]:
    """Load D rows of the given part files into temp table _raw (lowercase text columns plus
    _part). Returns the lowercase source column names."""
    pieces = []
    for part, path in parts:
        desc = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {_read_csv_sql(Path(path))}").fetchall()]
        keep = ", ".join(f'"{c}" AS "{c.lower()}"' for c in desc[4:])
        con.execute(f"CREATE OR REPLACE TEMP TABLE _part{part} AS SELECT {keep}, {int(part)} AS _part "
                    f"FROM {_read_csv_sql(Path(path))} WHERE \"{desc[0]}\" = 'D'")
        pieces.append(f"SELECT * FROM _part{part}")
    con.execute("CREATE OR REPLACE TEMP TABLE _raw AS " + " UNION ALL BY NAME ".join(pieces))
    for part, _ in parts:
        con.execute(f"DROP TABLE IF EXISTS _part{part}")
    return [r[0] for r in con.execute("DESCRIBE _raw").fetchall()]


def _cast(col: str, t: str) -> str:
    c = f'"{col}"'
    if t == "ts":
        return f"COALESCE(try_strptime({c}, '{TS_FORMAT}'), TRY_CAST({c} AS TIMESTAMP))"
    if t == "int":
        return f"TRY_CAST({c} AS BIGINT)"
    if t == "dbl":
        return f"TRY_CAST({c} AS DOUBLE)"
    return c


def _stage(con, target: str, src_cols: Sequence[str], src="_raw", stg="_stg") -> List[str]:
    spec = TABLES[target]
    have = [c for c in spec.cols if c in src_cols]
    sel = ", ".join(f"{_cast(c, spec.cols[c])} AS {c}" for c in have)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {stg} AS SELECT {sel} FROM {src}")
    return have


# ---------------------------------------------------------------- anti-join insert

def _dedup(con, target: str, stg: str, key: Sequence[str]) -> Tuple[int, int]:
    """Create temp table _dedup from staged ``stg`` (one row per key, all target columns, missing
    ones NULL) and _new (the rows of _dedup whose key is absent from ``target``).
    Returns (n_new, n_already_present)."""
    spec = TABLES[target]
    have = {r[0] for r in con.execute(f"DESCRIBE {stg}").fetchall()}
    missing_key = [k for k in key if k not in have]
    if missing_key:
        raise ValueError(f"{target}: key column(s) {missing_key} absent from source")
    cols = list(spec.cols)
    sel = ", ".join(f"{c}" if c in have else f"NULL::{_SQL_TYPE[spec.cols[c]]} AS {c}" for c in cols)
    part = ", ".join(key)
    present = ", ".join(c for c in cols if c in have)
    con.execute(f"CREATE OR REPLACE TEMP TABLE _dedup AS SELECT {sel} FROM "
                f"(SELECT {present}, "
                f"row_number() OVER (PARTITION BY {part}) AS _rn FROM {stg}) WHERE _rn = 1")
    on = " AND ".join(f"t.{k} IS NOT DISTINCT FROM s.{k}" for k in key)
    con.execute(f"CREATE OR REPLACE TEMP TABLE _new AS SELECT s.* FROM _dedup s "
                f"WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE {on})")
    n_src = con.execute("SELECT count(*) FROM _dedup").fetchone()[0]
    n_new = con.execute("SELECT count(*) FROM _new").fetchone()[0]
    return n_new, n_src - n_new


def insert_new(con, target: str, stg: str = "_stg", key: Optional[Sequence[str]] = None) -> Tuple[int, int]:
    """Insert rows of staged table ``stg`` not already in ``target`` by key. Columns missing in
    the stage are NULL. Returns (inserted, already_present)."""
    key = list(key or TABLES[target].key)
    n_new, n_present = _dedup(con, target, stg, key)
    if n_new:
        cl = ", ".join(TABLES[target].cols)
        con.execute(f"INSERT INTO {target} ({cl}) SELECT {cl} FROM _new")
    con.execute("DROP TABLE IF EXISTS _dedup")
    con.execute("DROP TABLE IF EXISTS _new")
    return n_new, n_present


# Snapshot tables: None = the incoming file replaces the whole table; otherwise the column
# whose values present in the incoming data scope which stored rows are replaced.
SNAPSHOT_SCOPE: Dict[str, Optional[str]] = {
    "dudetailsummary": None,
    "emsmaster": None,
    "network_outagedetail": "outageid",
    "network_outageconstraintset": "outageid",
}


def replace_staged(con, target: str, stg: str = "_stg") -> Tuple[int, int]:
    """Replace-on-load for a snapshot table (the caller holds the transaction). Returns
    (rows whose key was not stored before, rows whose key was). An empty incoming snapshot
    raises rather than wiping the table."""
    scope = SNAPSHOT_SCOPE[target]
    n_new, n_present = _dedup(con, target, stg, TABLES[target].key)
    if n_new + n_present == 0:
        raise ValueError(f"{target}: incoming snapshot is empty, not replacing")
    if scope is None:
        con.execute(f"DELETE FROM {target}")
    else:
        con.execute(f"DELETE FROM {target} WHERE {scope} IN "
                    f"(SELECT {scope} FROM _dedup WHERE {scope} IS NOT NULL)")
    cl = ", ".join(TABLES[target].cols)
    con.execute(f"INSERT INTO {target} ({cl}) SELECT {cl} FROM _dedup")
    con.execute("DROP TABLE IF EXISTS _dedup")
    con.execute("DROP TABLE IF EXISTS _new")
    return n_new, n_present


def _stage_rows(con, target: str, rows: Sequence[dict], stg: str = "_stg") -> None:
    """Stage dict rows (lowercase column names, text or typed values) through the same casts
    as the CSV path."""
    rows = [{k.lower(): v for k, v in r.items()} for r in rows]
    cols = sorted({k for r in rows for k in r})
    con.execute("CREATE OR REPLACE TEMP TABLE _rawrows (" + ", ".join(f'"{c}" VARCHAR' for c in cols) + ")")
    con.executemany(f"INSERT INTO _rawrows VALUES ({', '.join('?' for _ in cols)})",
                    [[None if r.get(c) is None else str(r.get(c)) for c in cols] for r in rows])
    _stage(con, target, cols, src="_rawrows", stg=stg)
    con.execute("DROP TABLE IF EXISTS _rawrows")


def upsert_outages(con, detail_rows: Sequence[dict], set_rows: Sequence[dict]) -> Dict[str, Tuple[int, int]]:
    """Apply an outage snapshot (e.g. a PUBLIC_NETWORK file) in one transaction. For each
    OUTAGEID present in a table's incoming rows, that outage's stored rows in that table are
    deleted and the incoming ones inserted; outages absent from the incoming data are untouched.
    Rows are dicts keyed by lowercase column name. Returns {table: (new, already_present)}."""
    out: Dict[str, Tuple[int, int]] = {}
    con.execute("BEGIN TRANSACTION")
    try:
        for target, rows in (("network_outagedetail", detail_rows), ("network_outageconstraintset", set_rows)):
            if not rows:
                out[target] = (0, 0)
                continue
            _stage_rows(con, target, rows)
            out[target] = replace_staged(con, target)
            con.execute("DROP TABLE IF EXISTS _stg")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return out


def connect_with_retry(path, attempts: int = 5, base_delay: float = 2.0, sleep=time.sleep, log=None,
                       read_only: bool = False):
    """duckdb.connect with exponential backoff on lock conflicts (several jobs write this file)."""
    for i in range(attempts):
        try:
            return duckdb.connect(str(path), read_only=True) if read_only else duckdb.connect(str(path))
        except duckdb.IOException as e:
            if i == attempts - 1:
                raise
            wait = base_delay * (2 ** i)
            if log:
                log(f"constraints DB locked (attempt {i + 1}/{attempts}), retrying in {wait:.0f}s: {e}")
            sleep(wait)


def refresh_readonly(db_path) -> Path:
    """Copy ``db_path`` to constraints_readonly.duckdb beside it, atomically (copy, then rename)."""
    import shutil
    db_path = Path(db_path)
    dst, tmp = db_path.parent / "constraints_readonly.duckdb", db_path.parent / "constraints_readonly.duckdb.tmp"
    try:
        shutil.copy2(str(db_path), str(tmp))
        os.rename(str(tmp), str(dst))
    finally:
        if tmp.exists():
            tmp.unlink()
    return dst


# ---------------------------------------------------------------- live DispatchIS files

def _section(csv_bytes: bytes, table: str) -> Optional[bytes]:
    """Re-assemble one table's section of a multi-table DispatchIS CSV as a single-table AEMO
    file (C line, I line, D rows) so it goes through the same reader as an MMSDM file.
    Returns None when the section has no I line."""
    i_pre = b"I,DISPATCH," + table.encode() + b","
    d_pre = b"D,DISPATCH," + table.encode() + b","
    i_line, d_lines = None, []
    for line in csv_bytes.splitlines():
        if line.startswith(i_pre):
            i_line = line
        elif line.startswith(d_pre):
            d_lines.append(line)
    if i_line is None:
        return None
    return b"\n".join([b"C,DISPATCHIS", i_line, *d_lines]) + b"\n"


def _has_rows(section: Optional[bytes]) -> bool:
    return section is not None and section.count(b"\n") > 2


def store_dispatchis(con, csv_bytes: bytes) -> dict:
    """Store one DispatchIS CSV in a single transaction: binding CONSTRAINT rows into
    constraint_binding5, every LOCAL_PRICE row into local_price5 and, last, the interval's row
    into constraint_intervals. A file with no CONSTRAINT rows raises and nothing is written.
    Typing goes through the same staging as the MMSDM loader, so rows are identical in value
    and type. Returns {binding, local_price, ledger: (inserted, already_present), intervals}."""
    cons, lp = _section(csv_bytes, "CONSTRAINT"), _section(csv_bytes, "LOCAL_PRICE")
    if not _has_rows(cons):
        raise ValueError("DispatchIS file has no CONSTRAINT rows")
    res: dict = {}
    with tempfile.TemporaryDirectory(prefix="dis_") as td:
        cpath, lpath = Path(td) / "data1.csv", Path(td) / "data2.csv"
        cpath.write_bytes(cons)
        if _has_rows(lp):
            lpath.write_bytes(lp)
        con.execute("BEGIN TRANSACTION")
        try:
            src_cols = read_raw(con, [(1, cpath)])
            have = _stage(con, "constraint_binding5", src_cols)
            if "intervention" in have:
                con.execute("UPDATE _stg SET intervention = 0 WHERE intervention IS NULL")
            else:
                con.execute("ALTER TABLE _stg ADD COLUMN intervention BIGINT DEFAULT 0")
            con.execute("CREATE OR REPLACE TEMP TABLE _led AS SELECT settlementdate, count(*) AS n_listed, "
                        "count(*) FILTER (WHERE coalesce(marginalvalue, 0) <> 0 OR coalesce(violationdegree, 0) <> 0) "
                        "AS n_binding FROM _stg WHERE intervention = 0 GROUP BY 1")
            con.execute("CREATE OR REPLACE TEMP TABLE _bind AS SELECT * FROM _stg WHERE "
                        "coalesce(marginalvalue, 0) <> 0 OR coalesce(violationdegree, 0) <> 0")
            res["binding"] = insert_new(con, "constraint_binding5", "_bind")
            if lpath.exists():
                src_cols = read_raw(con, [(2, lpath)])
                _stage(con, "local_price5", src_cols)
                res["local_price"] = insert_new(con, "local_price5", "_stg")
            else:
                res["local_price"] = (0, 0)
            res["intervals"] = [tuple(r) for r in con.execute(
                "SELECT settlementdate, n_listed, n_binding FROM _led ORDER BY 1").fetchall()]
            res["ledger"] = insert_new(con, "constraint_intervals", "_led")     # last
            for t in ("_raw", "_stg", "_led", "_bind"):
                con.execute(f"DROP TABLE IF EXISTS {t}")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    return res


# ---------------------------------------------------------------- month loader

def load_month_table(con, source_table: str, ym: str,
                     parts: Sequence[Tuple[int, Path]], record: bool = True) -> List[Tuple[str, int, int]]:
    """Load every part file (extracted AEMO CSVs) of one MMSDM table-month in one transaction,
    record it in _loaded, and return [(target_table, inserted, already_present), ...].
    DISPATCHCONSTRAINT also fills constraint_intervals from all intervention=0 rows."""
    target = SOURCE_TABLES[source_table]
    results: List[Tuple[str, int, int]] = []
    con.execute("BEGIN TRANSACTION")
    try:
        src_cols = read_raw(con, parts)
        counted = dict(con.execute("SELECT _part, count(*) FROM _raw GROUP BY 1").fetchall())
        part_counts = [(p, counted.get(p, 0)) for p, _ in parts]
        if source_table == "DISPATCHCONSTRAINT":
            have = _stage(con, "constraint_binding5", src_cols)
            if "intervention" in have:
                con.execute("UPDATE _stg SET intervention = 0 WHERE intervention IS NULL")
            else:
                con.execute("ALTER TABLE _stg ADD COLUMN intervention BIGINT DEFAULT 0")
            con.execute("CREATE OR REPLACE TEMP TABLE _led AS SELECT settlementdate, count(*) AS n_listed, "
                        "count(*) FILTER (WHERE coalesce(marginalvalue, 0) <> 0 OR coalesce(violationdegree, 0) <> 0) "
                        "AS n_binding FROM _stg WHERE intervention = 0 GROUP BY 1")
            con.execute("CREATE OR REPLACE TEMP TABLE _bind AS SELECT * FROM _stg WHERE "
                        "coalesce(marginalvalue, 0) <> 0 OR coalesce(violationdegree, 0) <> 0")
            i, p = insert_new(con, "constraint_binding5", "_bind")
            results.append(("constraint_binding5", i, p))
            i, p = insert_new(con, "constraint_intervals", "_led")
            results.append(("constraint_intervals", i, p))
            con.execute("DROP TABLE IF EXISTS _led")
            con.execute("DROP TABLE IF EXISTS _bind")
        else:
            _stage(con, target, src_cols)
            if target == "generic_constraint_rhs":
                # see RHS_FILTER_NOTE
                con.execute("DELETE FROM _stg WHERE scope IS DISTINCT FROM 'DS' OR genconid IS NULL OR genconid NOT IN "
                            "(SELECT DISTINCT constraintid FROM constraint_binding5)")
            key = None
            if target == "genconsetinvoke" and "invocation_id" not in src_cols:
                key = INVOKE_FALLBACK_KEY
            if target in SNAPSHOT_SCOPE:
                i, p = replace_staged(con, target, "_stg")
            else:
                i, p = insert_new(con, target, "_stg", key)
            results.append((target, i, p))
        for part, n in (part_counts if record else []):
            con.execute("INSERT INTO _loaded VALUES (?, ?, ?, ?, now())", [source_table, ym, int(part), int(n)])
        con.execute("DROP TABLE IF EXISTS _raw")
        con.execute("DROP TABLE IF EXISTS _stg")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return results
