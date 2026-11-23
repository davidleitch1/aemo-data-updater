#!/usr/bin/env python3
"""DuckDB storage for AEMO network-constraint data (separate ``constraints.duckdb``).

Same shape as bids_store: its own database file so the hot per-cycle
``aemo_test.duckdb`` copy does not grow. The schema, the loaders for AEMO-format CSV
files and the two views live here so the history backfill and a later live collector
share one code path.

Rules:
  * Every insert is an anti-join on the table key. Existing rows are never changed or
    deleted; re-running a load is a no-op.
  * Column names are lowercase. Source files are read as text and cast by the schema
    here, inserting by name: a column missing from an old file is NULL, an extra
    column in a new file is ignored.
  * ``_loaded`` records (tbl, ym, part) once a table-month has loaded in full, so a
    backfill can skip it without downloading.
  * Nothing derived is stored. ``unit_binding`` and ``local_price_computed`` are views.
"""
from __future__ import annotations

import os
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
}

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

def insert_new(con, target: str, stg: str = "_stg", key: Optional[Sequence[str]] = None) -> Tuple[int, int]:
    """Insert rows of staged table ``stg`` not already in ``target`` by key. Columns missing in
    the stage are NULL. Returns (inserted, already_present)."""
    spec = TABLES[target]
    key = list(key or spec.key)
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
    if n_new:
        cl = ", ".join(cols)
        con.execute(f"INSERT INTO {target} ({cl}) SELECT {cl} FROM _new")
    con.execute("DROP TABLE IF EXISTS _dedup")
    con.execute("DROP TABLE IF EXISTS _new")
    return n_new, n_src - n_new


# ---------------------------------------------------------------- month loader

def load_month_table(con, source_table: str, ym: str,
                     parts: Sequence[Tuple[int, Path]]) -> List[Tuple[str, int, int]]:
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
            key = None
            if target == "genconsetinvoke" and "invocation_id" not in src_cols:
                key = INVOKE_FALLBACK_KEY
            i, p = insert_new(con, target, "_stg", key)
            results.append((target, i, p))
        for part, n in part_counts:
            con.execute("INSERT INTO _loaded VALUES (?, ?, ?, ?, now())", [source_table, ym, int(part), int(n)])
        con.execute("DROP TABLE IF EXISTS _raw")
        con.execute("DROP TABLE IF EXISTS _stg")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise
    return results
