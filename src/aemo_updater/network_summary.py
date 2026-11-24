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

import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

UNIT_TYPES = {"System Strength", "Discretionary", "Unit Zero", "Non-Conformance", "Islanding - Unit"}
JACCARD_MIN = 0.5          # area membership
CLUSTER_FACTOR_MIN = 0.07  # unit-set threshold for clustering
BEHIND_FACTOR_MIN = 0.2    # "units behind" threshold
LAST_DAYS = 365            # ranking window for clustering

CLASS_SQL = ("case when {c} like 'F\\_%' escape '\\' or {c} like 'F-%' then 'fcas' "
             "when {c} like '#%' then 'quick' when {c} like '$%' then 'unit' else 'network' end")

OUTPUT_TABLES: Dict[str, str] = {
    # table -> sort order for the written copy
    "meta": "",
    "intervals_daily": "day",
    "constraint_meta": "constraintid",
    "area": "area_id",
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

def region_of(cid: str) -> str:
    return {"N": "NSW", "L": "NSW", "Q": "QLD", "V": "VIC", "S": "SA", "T": "TAS"}.get((cid or "")[:1], "Other")


def classify_cause(descr: Optional[str], limittype: Optional[str], cid: str) -> str:
    d = descr or ""
    if limittype in UNIT_TYPES:
        return "Unit-specific limit"
    if re.search(r"out\s*=\s*nil", d, re.I) or re.match(r"\s*nil\b", d, re.I) or "NIL" in cid:
        return "System normal"
    return "Outage"


def element_name(descr: Optional[str], cid: str) -> str:
    """Protected element named in AEMO's constraint description (regex chain from the phase 0 prototype)."""
    d = re.sub(r"\s+", " ", descr or "")
    m = re.search(r"avoid(?:ing)?\s+(?:O/L|overload(?:ing)?)\s*(?:of\s+)?(?:either\s+)?(.+?)"
                  r"(?:\s+on\s+(?:the\s+)?trip|\s+using|\s+for\s+loss|,|\.|$)", d, re.I)
    if not m:
        m = re.search(r"limit\s+(.+?)\s+(?:flow|to\b|upper|generation)", d, re.I)
    if not m:
        m = re.search(r"(?:upper limit on|loading .* on either)\s+(.+?)\s+(?:generation|less)", d, re.I)
    if not m:
        return cid
    e = re.sub(r"\b(330|275|220|132|110|66)\s*kV\b|\bline\b|#", "", m.group(1), flags=re.I)
    return re.sub(r"\s+", " ", e).strip(" ,")


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
               genconid_effectivedate eff, genconid_versionno ver
        from cn.constraint_binding5
        where intervention = 0 and marginalvalue <> 0 and {cls} = 'network'""")
    _x(con, """create or replace temp table data_bounds as
        select (select min(settlementdate) from cn.constraint_intervals) f,
               coalesce((select max(settlementdate) from cn.constraint_intervals), (select max(t) from b)) th,
               (select max(t) from b) bt, (select min(t) from b) bf""")


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


def _step_constraint_meta(con) -> None:
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
    base["element"] = [element_name(d, c) for d, c in zip(base.description, base.cid)]
    base["region"] = base.cid.map(region_of)
    base = base.sort_values(["n365", "cid"], ascending=[False, True]).reset_index(drop=True)
    con.register("cbase_py", base)
    _x(con, "create or replace temp table cbase2 as select * from cbase_py")
    con.unregister("cbase_py")
    # unit terms of each constraint's latest definition version, units as mapped at its last binding
    d = DIR_SQL.format(f="s.factor", ct="c.constrainttype")
    _x(con, f"""create or replace temp table lterms as
        select c.cid, du.duid, s.factor, {d} direction
        from cbase2 c
        join cn.spd_connection_point s on s.genconid = c.cid and s.effectivedate = c.l_eff
             and s.versionno = c.l_ver and s.bidtype = 'ENERGY' and s.factor <> 0
        join du on du.cp = s.connectionpointid and c.last_t > du.start_date
             and (du.end_date is null or c.last_t <= du.end_date)""")
    sets: Dict[str, set] = {}
    for cid, duid in con.execute(
            f"select distinct cid, duid from lterms where abs(factor) >= {CLUSTER_FACTOR_MIN}").fetchall():
        sets.setdefault(cid, set()).add(duid)
    usets = {k: frozenset(v) for k, v in sets.items()}
    amap = cluster_areas(list(base.cid), usets)
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
               max(abs(l.factor)) max_factor, l.direction
        from lterms l join cbase3 c on c.cid = l.cid left join dm m on m.duid = l.duid
        group by 1, 2, 3, 4, 5, 6, 8""")
    _x(con, f"""create or replace temp table area as
        select a.area_id, a.name, a.region, a.limittype, a.n_constraints,
               coalesce(u.n_units, 0) n_units, coalesce(u.weighted_mw, 0) weighted_mw,
               coalesce(u.missing, 0) missing_capacity_units
        from area_base a left join (
          select area_id,
                 count(*) filter (where max_factor >= {BEHIND_FACTOR_MIN}) n_units,
                 sum(coalesce(capacity_mw, 0) * max_factor) weighted_mw,
                 count(*) filter (where capacity_mw is null) missing
          from area_units where direction = 'discharge' group by 1) u on u.area_id = a.area_id""")


def _step_daily(con) -> None:
    _x(con, """create or replace temp table intervals_daily as
        select cast(settlementdate - interval 5 minute as date) as day, count(*) n_intervals
        from cn.constraint_intervals group by 1""")
    _x(con, """create or replace temp table bc as
        select b.*, c.area_id, c.cause, c.region, c.limittype
        from b join cbase3 c on c.cid = b.cid""")
    _x(con, """create or replace temp table constraint_daily as
        select day, cid constraintid, count(*) n_intervals, median(abs(mv)) median_abs_mv
        from b group by 1, 2""")
    _x(con, """create or replace temp table cause_daily_ as
        select day, region, cause, limittype, count(*) n_constraint_intervals from bc group by 1, 2, 3, 4""")
    _x(con, "create or replace temp table cause_daily as select * from cause_daily_")
    _x(con, """create or replace temp table area_bind as
        select day, area_id, count(distinct t) n_intervals,
               count(distinct t) filter (where cause = 'System normal') n_sysnormal,
               count(distinct t) filter (where cause = 'Outage') n_outage,
               count(distinct t) filter (where cause = 'Unit-specific limit') n_unit_specific
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
    _x(con, """create or replace temp table area_daily as
        select a.day, a.area_id, a.n_intervals, a.n_sysnormal, a.n_outage, a.n_unit_specific,
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


def build(con, built_at: Optional[datetime] = None) -> Dict[str, int]:
    """Build every output table as a temp table in ``con``. Returns row counts."""
    t0 = time.time()
    _step_binding(con)
    _step_version_map(con)
    _step_unit_binding(con)
    _step_constraint_meta(con)
    _step_areas(con)
    _step_daily(con)
    _step_units(con)
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


def run(con, out_path) -> Dict[str, int]:
    counts = build(con)
    write_output(con, out_path)
    return counts
