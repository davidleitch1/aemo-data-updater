#!/usr/bin/env python3
"""Pre-aggregated network-constraint summary tables for the dashboard's Network page.

Pure functions over one DuckDB connection with the two inputs attached as schemas:

  cn   constraints_readonly.duckdb   (constraint_binding5, constraint_intervals, local_price5,
                                      gencondata, spd_connection_point, genconset,
                                      network_outagedetail, network_outageconstraintset,
                                      dudetailsummary)
  am   aemo_readonly.duckdb          (duid_mapping, curtailment_duid5, prices5)

``build(con)`` leaves the output tables as TEMP tables in the connection's main schema;
``write_output(con, path)`` writes them to ``path + '.tmp'`` and renames it over ``path``
(atomic). ``run(con, path)`` does both. A full rebuild every time; nothing incremental.

Definitions (see AEMO_spot/plans/congestion_data_plan.md sections 4, 11, 13):
  * time: settlementdate is period-ending, so day/hour come from settlementdate - 5 minutes.
  * rows: intervention = 0 and marginalvalue <> 0; only class 'network' is summarised.
  * definition version for a (constraint, effectivedate, versionno) key: exact match in
    gencondata, else the latest earlier version of the same constraint, else the earliest
    later one, else none.
  * direction: factor * (-1 if constrainttype = '>=' else 1) > 0 is 'discharge'.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

from .network_equation import build_equation_tables

STABILITY_TYPES = {"System Strength", "Islanding - Unit"}
OTHER_CAP_TYPES = {"Discretionary", "Unit Zero", "Non-Conformance"}
UNIT_TYPES = STABILITY_TYPES | OTHER_CAP_TYPES
PLANT_CAUSES = ("Plant stability limit", "Commissioning", "Other plant cap")
JACCARD_MIN = 0.5          # area membership
CLUSTER_REL_MIN = 0.10     # unit-set threshold for clustering (relative weight within the constraint)
BEHIND_REL_MIN = 0.25      # "units behind" threshold (max relative weight across the area)
MIN_NAME_LETTERS = 6
NAME_MAX = 70
LAST_DAYS = 365            # ranking window for clustering

CLASS_SQL = ("case when {c} like 'F\\_%' escape '\\' or {c} like 'F-%' then 'fcas' "
             "when {c} like '#%' then 'quick' when {c} like '$%' then 'unit' else 'network' end")

OUTPUT_TABLES: Dict[str, str] = {
    # table -> sort order for the written copy
    "meta": "",
    "intervals_daily": "day",
    "constraint_meta": "constraintid",
    "area": "area_id",
    "constraint_hour": "constraintid, hour",
    "constraint_equation": "constraintid, side, ord",
    "constraint_equation_meta": "constraintid",
    "area_rank": "\"window\", rank_energy, area_id",
    "area_note": "area_id",
    "area_units": "area_id, direction, duid",
    "constraint_daily": "day, constraintid",
    "area_daily": "area_id, day",
    "cause_daily": "day, region, cause, limittype",
    "unit_daily": "duid, day",
    "unit_constraint_daily": "duid, day, constraintid, direction",
    "unit_hour_month": "duid, month, hour, direction",
    "outage_area": "outageid, genconsetid, area_id",
    "definition_rule_monthly": "month, definition_rule",
}


# ------------------------------------------------------------------ pure python helpers

REGION_PREFIXES = [("NSA_S", "SA"), ("C_V_", "VIC"), ("C_N_", "NSW"), ("C_Q_", "QLD"), ("C_S_", "SA"),
                   ("C_T_", "TAS"), ("SVML", "SA"), ("VS_", "VIC"), ("V_S_", "VIC")]


def region_of(cid: str) -> str:
    """Region from the constraint ID: specific prefixes first, then the first letter."""
    cid = cid or ""
    for p, r in REGION_PREFIXES:
        if cid.startswith(p):
            return r
    return {"N": "NSW", "L": "NSW", "Q": "QLD", "V": "VIC", "S": "SA", "T": "TAS"}.get(cid[:1], "Other")


def classify_cause(descr: Optional[str], limittype: Optional[str], cid: str) -> str:
    d = descr or ""
    if (cid or "").startswith("C_") or re.search(r"commissioning", d, re.I):
        return "Commissioning"
    if limittype in STABILITY_TYPES:
        return "Plant stability limit"
    if limittype in OTHER_CAP_TYPES:
        return "Other plant cap"
    if re.search(r"out\s*=\s*nil", d, re.I) or re.match(r"\s*nil\b", d, re.I) or "NIL" in cid:
        return "System normal"
    return "Outage"


def _tidy(name: str) -> str:
    """Remove a leading 'on ' / 'avoid ' and capitalise the first letter."""
    n = re.sub(r"^(?:avoid\s+|on\s+)", "", name.strip(), flags=re.I).strip()
    return n[:1].upper() + n[1:]


def _fallback_name(descr: Optional[str], cid: str) -> Tuple[str, str]:
    d = re.sub(r"\s+", " ", descr or "").strip()
    d = re.sub(r"^out\s*=\s*nil\s*,?\s*", "", d, flags=re.I).strip()
    d = re.sub(r"^(?:avoid\s+|on\s+)", "", d, flags=re.I).strip()
    if not d:
        return cid, "id"
    if len(d) > NAME_MAX:
        cut = d[:NAME_MAX + 1]
        sp = cut.rfind(" ", 0, NAME_MAX + 1)
        d = (cut[:sp] if sp > 0 else d[:NAME_MAX]).rstrip(" ,;-")
    return d[:1].upper() + d[1:], "description"


def element_name_src(descr: Optional[str], cid: str) -> Tuple[str, str]:
    """(name, source) where source is 'element', 'description' or 'id'. The protected element is
    parsed from AEMO's description with the phase 0 regex chain; a parse with fewer than 6 letters,
    or none, falls back to the description (leading 'Out= Nil,' removed, at most 70 characters at a
    word boundary), then to the ID. Leading 'on '/'avoid ' is dropped and the first letter capitalised."""
    d = re.sub(r"\s+", " ", descr or "")
    m = re.search(r"avoid(?:ing)?\s+(?:O/L|overload(?:ing)?)\s*(?:of\s+)?(?:either\s+)?(.+?)"
                  r"(?:\s+on\s+(?:the\s+)?trip|\s+using|\s+for\s+loss|,|\.|$)", d, re.I)
    if not m:
        m = re.search(r"limit\s+(.+?)\s+(?:flow|to\b|upper|generation)", d, re.I)
    if not m:
        m = re.search(r"(?:upper limit on|loading .* on either)\s+(.+?)\s+(?:generation|less)", d, re.I)
    if m:
        e = re.sub(r"\b(330|275|220|132|110|66)\s*kV\b|\bline\b|#", "", m.group(1), flags=re.I)
        e = re.sub(r"\s+", " ", e).strip(" ,")
        if sum(ch.isalpha() for ch in e) >= MIN_NAME_LETTERS:
            return _tidy(e), "element"
    return _fallback_name(descr, cid)


def element_name(descr: Optional[str], cid: str) -> str:
    return element_name_src(descr, cid)[0]


_STATION_PATTERNS = [
    r"\s*[-\u2013]\s*(?:phase|stage)\s*\d+\s*$",
    r"\s+(?:phase|stage)\s*\d+\s*$",
    r"\s*[-\u2013]\s*(?:solar|wind|bess|battery|wind farm|solar farm|energy storage)\s*$",
    r"\s+unit\s*\d+\s*$",
    r"\s*[-\u2013]\s*$",
    r"\s+\d+\s*$",
]


def station_name(site: str) -> str:
    """Strip trailing station qualifiers (phase/stage, technology, unit, suffix numbers)."""
    n = (site or "").strip()
    for _ in range(20):
        for p in _STATION_PATTERNS:
            m = re.sub(p, "", n, flags=re.I)
            if m != n:
                n = m
                break
        else:
            break
    return n or (site or "").strip()


LIMIT_LABEL = {"islanding - unit": "islanding risk", "discretionary": "discretionary cap"}


def limit_label(limittype: Optional[str]) -> str:
    lt = (limittype or "").lower()
    return LIMIT_LABEL.get(lt, lt)


def load_area_names(path) -> pd.DataFrame:
    """Overrides CSV (area_id, name, region). A missing file means no overrides."""
    cols = ["area_id", "name", "region"]
    if not path or not Path(str(path)).exists():
        return pd.DataFrame({c: pd.Series(dtype="object") for c in cols})
    df = pd.read_csv(path, encoding="utf-8", dtype=str).fillna("")
    for c in cols:
        if c not in df:
            df[c] = ""
    df["area_id"] = df.area_id.str.strip()
    return df[cols].drop_duplicates("area_id", keep="last")


def load_registry(path) -> Optional[dict]:
    """The area registry: {"version", "constraints": {cid: area_id}, "areas": {area_id: {seed, created}}}."""
    if not path or not Path(str(path)).exists():
        return None
    reg = json.loads(Path(str(path)).read_text(encoding="utf-8"))
    reg.setdefault("constraints", {})
    reg.setdefault("areas", {})
    return reg


def save_registry(path, reg: dict) -> None:
    """Atomic write; the previous version is kept as <path>.bak."""
    path = str(path)
    tmp = path + ".tmp"
    Path(tmp).write_text(json.dumps(reg, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
    try:
        if os.path.exists(path):
            shutil.copy2(path, path + ".bak")
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def cluster_with_registry(order: Sequence[str], unit_sets: Dict[str, frozenset], registry: Optional[dict],
                          today: str) -> Tuple[Dict[str, str], dict]:
    """Stable areas. Without a registry (or an empty one) cluster as cluster_areas does and record the
    result. With one, a registered constraint keeps its area; an unregistered one joins the existing area
    whose seed's unit set has the best Jaccard similarity (>= 0.5) with its own, else founds a new area
    with itself as seed (constraints with no unit terms always found their own). Returns (map, registry)."""
    if not registry or not registry.get("constraints"):
        out = cluster_areas(order, unit_sets)
        reg = {"version": 1, "constraints": dict(out),
               "areas": {c: {"seed": c, "created": today} for c in order if out[c] == c}}
        return out, reg
    reg = {"version": 1, "constraints": dict(registry["constraints"]),
           "areas": {a: dict(v) for a, v in registry["areas"].items()}}
    for cid in order:
        if cid in reg["constraints"]:
            continue
        s = unit_sets.get(cid)
        best, bj = None, 0.0
        if s:
            for aid, info in reg["areas"].items():
                ss = unit_sets.get(info["seed"])
                if not ss:
                    continue
                j = len(s & ss) / len(s | ss)
                if j >= JACCARD_MIN and j > bj:
                    best, bj = aid, j
        if best is None:
            best = cid
            reg["areas"][cid] = {"seed": cid, "created": today}
        reg["constraints"][cid] = best
    return {c: reg["constraints"][c] for c in order}, reg


def cluster_areas(order: Sequence[str], unit_sets: Dict[str, frozenset]) -> Dict[str, str]:
    """Greedy clustering. ``order`` is the processing order (most binding first). A constraint
    joins the first existing area whose SEED's unit set has Jaccard >= 0.5 with its own, else
    seeds a new area (area_id = seed id). No unit terms: its own area."""
    seeds: List[Tuple[str, frozenset]] = []
    out: Dict[str, str] = {}
    for cid in order:
        s = unit_sets.get(cid)
        if not s:
            out[cid] = cid
            continue
        for sid, ss in seeds:
            if len(s & ss) / len(s | ss) >= JACCARD_MIN:
                out[cid] = sid
                break
        else:
            seeds.append((cid, s))
            out[cid] = cid
    return out


# ------------------------------------------------------------------ SQL steps

def _x(con, sql: str, *a):
    return con.execute(sql, *a)


def _step_binding(con) -> None:
    cls = CLASS_SQL.format(c="constraintid")
    _x(con, f"""create or replace temp table b as
        select settlementdate t, cast(settlementdate - interval 5 minute as date) as day,
               hour(settlementdate - interval 5 minute) as hour, constraintid cid, marginalvalue mv,
               genconid_effectivedate eff, genconid_versionno ver, rhs, lhs
        from cn.constraint_binding5
        where intervention = 0 and marginalvalue <> 0 and {cls} = 'network'""")
    _x(con, """create or replace temp table data_bounds as
        select (select min(settlementdate) from cn.constraint_intervals) f,
               coalesce((select max(settlementdate) from cn.constraint_intervals), (select max(t) from b)) th,
               (select max(t) from b) bt, (select min(t) from b) bf""")
    _x(con, """create or replace temp table data_bounds2 as
        select *, cast(th - interval 5 minute as date) d0 from data_bounds""")
    _x(con, "create or replace temp table data_bounds as select * from data_bounds2")
    _x(con, "drop table data_bounds2")
    for name, ddl in (("generic_constraint_rhs", "genconid varchar, effectivedate timestamp, versionno bigint, "
                       "scope varchar, termid bigint, groupid bigint, spd_id varchar, spd_type varchar, factor double, "
                       "operation varchar, defaultvalue double, parameterterm1 varchar, parameterterm2 varchar, "
                       "parameterterm3 varchar, lastchanged timestamp"),
                      ("emsmaster", "spd_id varchar, spd_type varchar, description varchar, grouping_id varchar, "
                       "lastchanged timestamp")):
        have = con.execute("select count(*) from information_schema.tables where (table_catalog = 'cn' "
                           "or table_schema = 'cn') and table_name = ?", [name]).fetchone()[0]
        tgt = {"generic_constraint_rhs": "rhs_src", "emsmaster": "ems_src"}[name]
        if have:
            _x(con, f"create or replace temp table {tgt} as select * from cn.{name}"
               + (" where scope = 'DS'" if name == "generic_constraint_rhs" else ""))
        else:
            _x(con, f"create or replace temp table {tgt} ({ddl})")


def _step_version_map(con) -> None:
    _x(con, """create or replace temp table vkeys as
        select cid, eff, ver, max(t) last_t from b group by 1, 2, 3""")
    _x(con, """create or replace temp table gdef as
        select distinct genconid, effectivedate, versionno from cn.gencondata""")
    _x(con, """create or replace temp table vmap as
        with cand as (
          select k.cid, k.eff, k.ver, g.effectivedate d_eff, g.versionno d_ver,
            case when g.effectivedate = k.eff and g.versionno = k.ver then 'exact'
                 when g.effectivedate < k.eff or (g.effectivedate = k.eff and g.versionno < k.ver) then 'earlier'
                 when g.effectivedate > k.eff or (g.effectivedate = k.eff and g.versionno > k.ver) then 'later' end as rule
          from vkeys k join gdef g on g.genconid = k.cid),
        ranked as (
          select *, row_number() over (partition by cid, eff, ver order by
              case rule when 'exact' then 0 when 'earlier' then 1 else 2 end,
              case when rule = 'earlier' then d_eff end desc nulls last,
              case when rule = 'earlier' then d_ver end desc nulls last,
              d_eff asc, d_ver asc) rn
          from cand where rule is not null)
        select k.cid, k.eff, k.ver, k.last_t, r.d_eff, r.d_ver, coalesce(r.rule, 'none') as rule
        from vkeys k left join ranked r on r.cid = k.cid and r.eff is not distinct from k.eff
             and r.ver is not distinct from k.ver and r.rn = 1""")
    # one row per mapped definition version: connection-point terms with the constraint's type
    _x(con, """create or replace temp table dterms as
        select s.genconid cid, s.effectivedate d_eff, s.versionno d_ver, s.connectionpointid cp, s.factor,
               g.ct constrainttype
        from (select distinct d_eff, d_ver, cid from vmap where d_eff is not null) m
        join cn.spd_connection_point s on s.genconid = m.cid and s.effectivedate = m.d_eff
             and s.versionno = m.d_ver and s.bidtype = 'ENERGY' and s.factor <> 0
        left join (select genconid, effectivedate, versionno, any_value(constrainttype) ct
                   from cn.gencondata group by 1, 2, 3) g
          on g.genconid = s.genconid and g.effectivedate = s.effectivedate and g.versionno = s.versionno""")
    _x(con, """create or replace temp table du as
        select distinct duid, connectionpointid as cp, start_date, end_date from cn.dudetailsummary""")


DIR_SQL = "case when {f} * (case when {ct} = '>=' then -1 else 1 end) > 0 then 'discharge' else 'charge' end"


def _step_unit_binding(con) -> None:
    d = DIR_SQL.format(f="dt.factor", ct="dt.constrainttype")
    _x(con, f"""create or replace temp table ub as
        select b.t, b.day, b.hour, b.cid, du.duid, {d} direction,
               max(abs(dt.factor)) af, max(abs(dt.factor * b.mv)) w
        from b
        join vmap m on m.cid = b.cid and m.eff is not distinct from b.eff and m.ver is not distinct from b.ver
                   and m.d_eff is not null
        join dterms dt on dt.cid = m.cid and dt.d_eff = m.d_eff and dt.d_ver = m.d_ver
        join du on du.cp = dt.cp and b.t > du.start_date and (du.end_date is null or b.t <= du.end_date)
        group by 1, 2, 3, 4, 5, 6""")


def _step_constraint_meta(con, registry=None, today: Optional[str] = None) -> None:
    """constraint_meta + area assignment (clustering in Python)."""
    _x(con, """create or replace temp table lgd as
        select genconid, effectivedate l_eff, versionno l_ver, description, limittype, impact, constrainttype
        from (select *, row_number() over (partition by genconid order by effectivedate desc, versionno desc) rn
              from cn.gencondata) where rn = 1""")
    _x(con, """create or replace temp table cbase as
        with n as (
          select cid, count(*) n_all,
                 count(*) filter (where t > (select th from data_bounds) - interval 365 day) n365,
                 max(t) last_t from b group by 1),
        lr as (select cid, arg_max(rule, last_t) definition_rule from vmap group by 1)
        select n.cid, n.n_all, n.n365, n.last_t, g.l_eff, g.l_ver, g.description, g.limittype, g.impact,
               g.constrainttype, lr.definition_rule
        from n left join lgd g on g.genconid = n.cid left join lr on lr.cid = n.cid""")
    base = con.execute("select * from cbase").df()
    base["description"] = base["description"].fillna("").astype(str)
    base["limittype"] = base["limittype"].fillna("Not described").astype(str)
    base["impact"] = base["impact"].fillna("").astype(str)
    base["cause"] = [classify_cause(d, l, c) for d, l, c in zip(base.description, base.limittype, base.cid)]
    es = [element_name_src(d, c) for d, c in zip(base.description, base.cid)]
    base["element"] = [e[0] for e in es]
    base["element_src"] = [e[1] for e in es]
    base["region"] = base.cid.map(region_of)
    base = base.sort_values(["n365", "cid"], ascending=[False, True]).reset_index(drop=True)
    con.register("cbase_py", base)
    _x(con, "create or replace temp table cbase2 as select * from cbase_py")
    con.unregister("cbase_py")
    # unit terms of each constraint's latest definition version, units as mapped at its last binding
    d = DIR_SQL.format(f="s.factor", ct="c.constrainttype")
    _x(con, f"""create or replace temp table lterms as
        select cid, duid, factor, direction, abs(factor) / max(abs(factor)) over (partition by cid) rel
        from (
        select c.cid, du.duid, s.factor, {d} direction
        from cbase2 c
        join cn.spd_connection_point s on s.genconid = c.cid and s.effectivedate = c.l_eff
             and s.versionno = c.l_ver and s.bidtype = 'ENERGY' and s.factor <> 0
        join du on du.cp = s.connectionpointid and c.last_t > du.start_date
             and (du.end_date is null or c.last_t <= du.end_date)) x""")
    sets: Dict[str, set] = {}
    for cid, duid in con.execute(
            f"select distinct cid, duid from lterms where rel >= {CLUSTER_REL_MIN}").fetchall():
        sets.setdefault(cid, set()).add(duid)
    usets = {k: frozenset(v) for k, v in sets.items()}
    amap, reg_out = cluster_with_registry(list(base.cid), usets, registry, today or datetime.now().strftime("%Y-%m-%d"))
    _x(con, "create or replace temp table registry_out as select ?::varchar as j", [json.dumps(reg_out, ensure_ascii=False)])
    created = {a: v.get("created") for a, v in reg_out["areas"].items()}
    base["area_id"] = base.cid.map(amap)
    con.register("cbase_py", base)
    _x(con, "create or replace temp table cbase3 as select * from cbase_py")
    con.unregister("cbase_py")
    _x(con, """create or replace temp table constraint_meta as
        select cid as constraintid, 'network' as class, region, description, limittype, impact, constrainttype,
               cause, element, area_id, definition_rule from cbase3""")
    # areas: name = element of the most frequently binding member (the seed, by processing order)
    ar = base.sort_values(["n365", "n_all", "cid"], ascending=[False, False, True]).groupby("area_id", sort=False)
    area_py = pd.DataFrame({
        "area_id": list(ar.groups.keys()),
        "name": [g.element.iat[0] for _, g in ar],
        "top_constraintid": [g.cid.iat[0] for _, g in ar],
        "name_source": [g.element_src.iat[0] for _, g in ar],
        "created": [created.get(a) for a in ar.groups.keys()],
        "description": [g.description.iat[0] for _, g in ar],
        "region": [g.region.iat[0] for _, g in ar],
        "limittype": [g.limittype.iat[0] for _, g in ar],
        "n_constraints": [len(g) for _, g in ar]})
    con.register("area_py", area_py)
    _x(con, "create or replace temp table area_base as select * from area_py")
    con.unregister("area_py")


def _step_areas(con) -> None:
    _x(con, """create or replace temp table dm as
        select duid, arg_max("site name", capacity_mw) site, arg_max(fuel, capacity_mw) fuel,
               arg_max(region, capacity_mw) region, max(capacity_mw) capacity_mw
        from am.duid_mapping group by 1""")
    _x(con, """create or replace temp table area_units as
        select c.area_id, l.duid, m.site, m.fuel, m.region, m.capacity_mw,
               max(abs(l.factor)) max_factor, max(l.rel) max_rel_weight, l.direction
        from lterms l join cbase3 c on c.cid = l.cid left join dm m on m.duid = l.duid
        group by 1, 2, 3, 4, 5, 6, 9""")
    _x(con, f"""create or replace temp table area as
        select a.area_id, a.name, a.region, a.limittype, a.n_constraints,
               coalesce(u.n_units, 0) n_units, coalesce(u.units_mw, 0) units_mw,
               coalesce(u.weighted_mw, 0) weighted_mw, coalesce(u.missing, 0) missing_capacity_units,
               a.top_constraintid, a.description, a.name_source, coalesce(ic.ics, '') interconnectors, a.created
        from area_base a left join (select c.area_id, string_agg(distinct i.interconnectorid, ',' order by i.interconnectorid) ics
                                    from cbase3 c join cn.spd_interconnector i on i.genconid = c.cid
                                         and i.effectivedate = c.l_eff and i.versionno = c.l_ver and i.factor <> 0
                                    group by 1) ic on ic.area_id = a.area_id
        left join (
          select area_id,
                 count(*) filter (where max_rel_weight >= {BEHIND_REL_MIN}) n_units,
                 sum(coalesce(capacity_mw, 0)) filter (where max_rel_weight >= {BEHIND_REL_MIN}) units_mw,
                 sum(coalesce(capacity_mw, 0) * max_rel_weight) weighted_mw,
                 count(*) filter (where capacity_mw is null) missing
          from area_units where direction = 'discharge' group by 1) u on u.area_id = a.area_id""")


def _step_regions(con, overrides: pd.DataFrame) -> None:
    """Region: override file, else modal region of the units behind, else the ID-prefix rule."""
    _x(con, "create or replace temp table ovr (area_id varchar, name varchar, region varchar)")
    rows = [[str(r.area_id), str(r.name), str(r.region)] for r in overrides.itertuples()]
    if rows:
        con.executemany("insert into ovr values (?, ?, ?)", rows)
    _x(con, "create or replace temp table override_stale as select area_id from ovr "
            "where area_id not in (select area_id from area) order by 1")
    _x(con, f"""create or replace temp table area_reg as
        select area_id, region from (
          select area_id, region, row_number() over (partition by area_id order by n desc, region) rn from (
            select area_id, regexp_replace(region, '[0-9]+$', '') region, count(*) n from area_units
            where direction = 'discharge' and max_rel_weight >= {BEHIND_REL_MIN} and region is not null
            group by 1, 2)) where rn = 1""")
    _x(con, """create or replace temp table area_region_src as
        select a.area_id, coalesce(nullif(o.region, ''), r.region) region
        from area a left join area_reg r on r.area_id = a.area_id left join ovr o on o.area_id = a.area_id
        where coalesce(nullif(o.region, ''), r.region) is not null""")
    _x(con, "update area set region = r.region from area_region_src r where area.area_id = r.area_id")
    _x(con, "update cbase3 set region = r.region from area_region_src r where cbase3.area_id = r.area_id")
    _x(con, "update constraint_meta set region = r.region from area_region_src r "
            "where constraint_meta.area_id = r.area_id")


def stale_overrides(con) -> List[str]:
    return [r[0] for r in con.execute("select area_id from override_stale").fetchall()]


def _step_area_names(con) -> None:
    """Area name: units' site names for a small unit-specific area, else the top constraint's element."""
    df = con.execute(f"""
        select a.area_id, a.name, a.name_source, a.n_units, coalesce(d.us, 0) us, coalesce(d.n, 0) n,
               coalesce(d.nc, 0) ncomm
        from area a left join (select area_id, sum(n_unit_specific) us, sum(n_intervals) n,
                                      sum(n_commissioning) nc
                               from area_daily group by 1) d on d.area_id = a.area_id""").df()
    units = con.execute(f"""select area_id, duid, coalesce(site, duid) site from area_units
        where direction = 'discharge' and max_rel_weight >= {BEHIND_REL_MIN}
        order by area_id, max_rel_weight desc, duid""").df()
    sites = units.groupby("area_id").site.apply(list).to_dict()
    dom = dict(con.execute("""select area_id, arg_max(limittype, (n_all, cid)) from cbase3
        where cause in ('Plant stability limit', 'Other plant cap') group by 1""").fetchall())
    ov = {r.area_id: r for r in con.execute("select * from ovr").df().itertuples() if r.name}
    names = []
    for r in df.itertuples():
        nm, src = r.name, r.name_source
        comm = r.ncomm * 2 > r.n
        if (1 <= r.n_units <= 3 and r.n > 0 and r.us * 2 > r.n and r.area_id in sites
                and (comm or r.area_id in dom)):
            seen: Dict[str, str] = {}
            for st in sites[r.area_id]:
                k = station_name(st)
                seen.setdefault(k.lower(), k)
            nm = " + ".join(seen.values()) + " \u2014 " + ("commissioning" if comm else limit_label(dom[r.area_id]))
            src = "site"
        if r.area_id in ov:
            nm, src = ov[r.area_id].name, "override"
        names.append((r.area_id, nm, src))
    con.register("nm_py", pd.DataFrame(names, columns=["area_id", "nm", "src"]))
    _x(con, "update area set name = n.nm, name_source = n.src from nm_py n where area.area_id = n.area_id")
    con.unregister("nm_py")


def _step_daily(con) -> None:
    _x(con, """create or replace temp table intervals_daily as
        select cast(settlementdate - interval 5 minute as date) as day, count(*) n_intervals
        from cn.constraint_intervals group by 1""")
    _x(con, """create or replace temp table bc as
        select b.*, c.area_id, c.cause, c.region, c.limittype
        from b join cbase3 c on c.cid = b.cid""")
    _x(con, """create or replace temp table cd_base as
        select day, cid as constraintid, count(*) n_intervals, median(abs(mv)) median_abs_mv
        from b group by 1, 2""")
    _x(con, """create or replace temp table constraint_hour as
        select cid as constraintid, hour, count(*) n_intervals from b
        where day > (select d0 from data_bounds) - 365 group by 1, 2""")
    _x(con, """create or replace temp table cause_daily_ as
        select day, region, cause, limittype, count(*) n_constraint_intervals from bc group by 1, 2, 3, 4""")
    _x(con, "create or replace temp table cause_daily as select * from cause_daily_")
    _x(con, """create or replace temp table area_bind as
        select day, area_id, count(distinct t) n_intervals,
               count(distinct t) filter (where cause = 'System normal') n_sysnormal,
               count(distinct t) filter (where cause = 'Outage') n_outage,
               count(distinct t) filter (where cause = 'Plant stability limit') n_plant_stability,
               count(distinct t) filter (where cause = 'Commissioning') n_commissioning,
               count(distinct t) filter (where cause = 'Other plant cap') n_other_cap,
               count(distinct t) filter (where cause in ('Plant stability limit', 'Commissioning', 'Other plant cap'))
                 n_unit_specific
        from bc group by 1, 2""")
    _x(con, """create or replace temp table definition_rule_monthly as
        select date_trunc('month', b.day) as month, coalesce(m.rule, 'none') definition_rule, count(*) n_rows
        from b left join vmap m on m.cid = b.cid and m.eff is not distinct from b.eff
             and m.ver is not distinct from b.ver
        group by 1, 2""")


def _step_units(con) -> None:
    # distinct unit-interval-direction
    _x(con, """create or replace temp table ud as
        select t, day, hour, duid, direction from ub group by 1, 2, 3, 4, 5""")
    # dominant constraint for each discharge-limited unit-interval
    _x(con, """create or replace temp table dom as
        select t, duid, arg_min(cid, struct_pack(a := -w, b := cid)) cid
        from ub where direction = 'discharge' group by 1, 2""")
    _x(con, """create or replace temp table px as
        select settlementdate t, regionid, max(rrp) rrp from am.prices5 group by 1, 2""")
    _x(con, """create or replace temp table cur as
        select c.settlementdate t, cast(c.settlementdate - interval 5 minute as date) as day, c.duid,
               sum(c.curtailment) mw
        from am.curtailment_duid5 c where c.curtailment > 0 group by 1, 2, 3""")
    _x(con, """create or replace temp table curj as
        select cur.t, cur.day, cur.duid, cur.mw / 12.0 mwh, d.cid dom_cid, k.area_id,
               cur.mw / 12.0 * greatest(coalesce(p.rrp, 0), 0) as val
        from cur
        left join dom d on d.t = cur.t and d.duid = cur.duid
        left join cbase3 k on k.cid = d.cid
        left join dm on dm.duid = cur.duid
        left join px p on p.t = cur.t and p.regionid = dm.region""")
    _x(con, """create or replace temp table area_cur as
        select day, area_id, sum(mwh) constrained_mwh, sum(val) constrained_value
        from curj where dom_cid is not null group by 1, 2""")
    _x(con, """create or replace temp table con_cur as
        select day, dom_cid cid, sum(mwh) constrained_mwh, sum(val) constrained_value
        from curj where dom_cid is not null group by 1, 2""")
    _x(con, """create or replace temp table constraint_daily as
        select c.day, c.constraintid, c.n_intervals, c.median_abs_mv,
               coalesce(e.constrained_mwh, 0) constrained_mwh, coalesce(e.constrained_value, 0) constrained_value
        from cd_base c left join con_cur e on e.day = c.day and e.cid = c.constraintid""")
    _x(con, """create or replace temp table area_daily as
        select a.day, a.area_id, a.n_intervals, a.n_sysnormal, a.n_outage, a.n_plant_stability, a.n_commissioning,
               a.n_other_cap, a.n_unit_specific,
               coalesce(c.constrained_mwh, 0) constrained_mwh, coalesce(c.constrained_value, 0) constrained_value
        from area_bind a left join area_cur c on c.day = a.day and c.area_id = a.area_id""")
    _x(con, """create or replace temp table lp as
        select cast(settlementdate - interval 5 minute as date) as day, duid, count(*) n_local_price,
               median(local_price_adjustment) median_local_price_adj
        from cn.local_price5 where local_price_adjustment < 0 group by 1, 2""")
    _x(con, """create or replace temp table ud_agg as
        select day, duid, count(*) filter (where direction = 'discharge') n_discharge_limited,
               count(*) filter (where direction = 'charge') n_charge_limited
        from ud group by 1, 2""")
    _x(con, """create or replace temp table cur_agg as
        select day, duid,
               sum(mwh) filter (where dom_cid is not null) constrained_mwh,
               sum(mwh) filter (where dom_cid is null) other_curtailed_mwh,
               sum(val) filter (where dom_cid is not null) constrained_value
        from curj group by 1, 2""")
    _x(con, """create or replace temp table unit_daily as
        with k as (select day, duid from ud_agg union select day, duid from cur_agg union select day, duid from lp)
        select k.day, k.duid, coalesce(u.n_discharge_limited, 0) n_discharge_limited,
               coalesce(u.n_charge_limited, 0) n_charge_limited,
               coalesce(c.constrained_mwh, 0) constrained_mwh, coalesce(c.other_curtailed_mwh, 0) other_curtailed_mwh,
               coalesce(c.constrained_value, 0) constrained_value,
               coalesce(l.n_local_price, 0) n_local_price, l.median_local_price_adj
        from k left join ud_agg u on u.day = k.day and u.duid = k.duid
               left join cur_agg c on c.day = k.day and c.duid = k.duid
               left join lp l on l.day = k.day and l.duid = k.duid""")
    _x(con, """create or replace temp table unit_constraint_daily as
        select day, duid, cid constraintid, direction, count(*) n_intervals from ub group by 1, 2, 3, 4""")
    _x(con, """create or replace temp table unit_hour_month as
        select date_trunc('month', day) as month, duid, hour, direction, count(*) n_intervals
        from ud group by 1, 2, 3, 4""")


def _step_outages(con) -> None:
    _x(con, """create or replace temp table oset as
        select outageid, genconsetid, startinterval, endinterval from cn.network_outageconstraintset""")
    # set membership version: latest effective at/before the window start, else the earliest
    _x(con, """create or replace temp table oset_c as
        with cand as (
          select o.outageid, o.genconsetid, o.startinterval, g.effectivedate, g.versionno,
                 row_number() over (partition by o.outageid, o.genconsetid, o.startinterval order by
                   case when g.effectivedate <= o.startinterval then 0 else 1 end,
                   case when g.effectivedate <= o.startinterval then g.effectivedate end desc nulls last,
                   case when g.effectivedate <= o.startinterval then g.versionno end desc nulls last,
                   g.effectivedate asc, g.versionno asc) rn
          from (select distinct * from oset) o
          join (select distinct genconsetid, effectivedate, versionno from cn.genconset) g
               on g.genconsetid = o.genconsetid),
        pick as (select * from cand where rn = 1)
        select o.outageid, o.genconsetid, o.startinterval, o.endinterval, s.genconid cid
        from (select distinct * from oset) o
        join pick p on p.outageid = o.outageid and p.genconsetid = o.genconsetid and p.startinterval = o.startinterval
        join cn.genconset s on s.genconsetid = p.genconsetid and s.effectivedate = p.effectivedate
             and s.versionno = p.versionno""")
    _x(con, """create or replace temp table oband as
        select o.outageid, o.genconsetid, c.area_id, count(distinct b.t) n_binding_intervals
        from oset_c o
        join b on b.cid = o.cid and b.t >= o.startinterval and b.t <= coalesce(o.endinterval, timestamp '9999-01-01')
        join cbase3 c on c.cid = b.cid
        group by 1, 2, 3""")
    _x(con, """create or replace temp table outage_area as
        select x.outageid, x.genconsetid, x.area_id, d.substationid, d.equipmenttype, d.equipmentid,
               d.starttime, d.endtime, d.outagestatuscode, d.reason, x.n_binding_intervals
        from oband x left join (select distinct outageid, substationid, equipmenttype, equipmentid, starttime,
               endtime, outagestatuscode, reason from cn.network_outagedetail) d on d.outageid = x.outageid
        where x.n_binding_intervals > 0""")


def _step_rank_notes(con, notes) -> None:
    """area.lead_constraintid, area_rank (12m, 90d, 12m_prev) and area_note."""
    _x(con, """create or replace temp table lead as
        select area_id, arg_min(cid, struct_pack(a := -mwh, b := -n365, c := cid)) lead_constraintid from (
          select c.area_id, c.cid, coalesce(e.mwh, 0) mwh, c.n365 from cbase3 c
          left join (select constraintid cid, sum(constrained_mwh) mwh from constraint_daily
                     where day > (select d0 from data_bounds) - 365 group by 1) e on e.cid = c.cid)
        group by 1""")
    _x(con, "alter table area add column lead_constraintid varchar")
    _x(con, "update area set lead_constraintid = l.lead_constraintid from lead l where area.area_id = l.area_id")
    _x(con, """create or replace temp table win as
        select * from (values ('12m', 365, 0), ('90d', 90, 0), ('12m_prev', 90 + 365, 90)) v(win_name, lo, hi)""")
    _x(con, """create or replace temp table area_rank as
        with a as (
          select w.win_name, d.area_id, sum(d.constrained_mwh) / 1000.0 as gwh, sum(d.n_intervals) / 12.0 as hours
          from area_daily d, win w, data_bounds x
          where d.day > x.d0 - w.lo and d.day <= x.d0 - w.hi group by 1, 2)
        select win_name as "window", area_id, gwh, hours,
               dense_rank() over (partition by win_name order by gwh desc) rank_energy,
               dense_rank() over (partition by win_name order by hours desc) rank_hours from a""")
    _x(con, """create or replace temp table area_note_in (area_id varchar, constraintid varchar, note varchar,
        written_effectivedate timestamp, written_fingerprint varchar)""")
    if notes and Path(str(notes)).exists():
        data = json.loads(Path(str(notes)).read_text(encoding="utf-8"))
        rows = [[a, v.get("constraintid"), v.get("note"), v.get("written_effectivedate"),
                 v.get("written_fingerprint")] for a, v in data.items()]
        if rows:
            con.executemany("insert into area_note_in values (?, ?, ?, ?::timestamp, ?)", rows)
    _x(con, """create or replace temp table area_note as
        select n.area_id, n.constraintid, n.note, n.written_effectivedate, n.written_fingerprint,
               m.fingerprint current_fingerprint,
               (m.fingerprint is distinct from n.written_fingerprint
                or a.lead_constraintid is distinct from n.constraintid) is_stale
        from area_note_in n left join area a on a.area_id = n.area_id
             left join constraint_equation_meta m on m.constraintid = n.constraintid""")


def build(con, built_at: Optional[datetime] = None, area_names=None, registry=None,
          notes=None) -> Dict[str, int]:
    """Build every output table as a temp table in ``con``. Returns row counts."""
    t0 = time.time()
    _step_binding(con)
    _step_version_map(con)
    _step_unit_binding(con)
    built = built_at or datetime.now()
    _step_constraint_meta(con, load_registry(registry) if registry else None, built.strftime("%Y-%m-%d"))
    _step_areas(con)
    _step_regions(con, load_area_names(area_names))
    _step_daily(con)
    _step_units(con)
    _step_area_names(con)
    build_equation_tables(con)
    _step_rank_notes(con, notes)
    _step_outages(con)
    n_all = con.execute(
        "select count(*) from cn.constraint_binding5 where intervention = 0 and marginalvalue <> 0").fetchone()[0]
    secs = time.time() - t0
    _x(con, """create or replace temp table meta as
        select ? ::timestamp built_at, f data_from, th data_through, ? ::bigint n_binding_rows,
               ? ::double build_seconds from data_bounds""",
       [built_at or datetime.now(), n_all, secs])
    return {t: con.execute(f"select count(*) from {t}").fetchone()[0] for t in OUTPUT_TABLES}


# ------------------------------------------------------------------ writing

def write_output(con, out_path) -> None:
    """Write the output tables to <out>.tmp and os.rename it over <out>. A failure removes the
    tmp file and leaves any existing <out> untouched."""
    out_path = str(out_path)
    tmp = out_path + ".tmp"
    for p in (tmp, tmp + ".wal"):
        if os.path.exists(p):
            os.remove(p)
    attached = False
    try:
        con.execute(f"ATTACH '{tmp}' AS o")
        attached = True
        for t, order in OUTPUT_TABLES.items():
            o = f" order by {order}" if order else ""
            con.execute(f"create table o.{t} as select * from {t}{o}")
        con.execute("DETACH o")
        attached = False
        os.rename(tmp, out_path)
    except BaseException:
        if attached:
            try:
                con.execute("DETACH o")
            except Exception:
                pass
        for p in (tmp, tmp + ".wal"):
            if os.path.exists(p):
                os.remove(p)
        raise


def run(con, out_path, area_names=None, registry=None, notes=None) -> Dict[str, int]:
    counts = build(con, area_names=area_names, registry=registry, notes=notes)
    if registry:
        j = con.execute("select j from registry_out").fetchone()[0]
        save_registry(registry, json.loads(j))
    write_output(con, out_path)
    return counts
