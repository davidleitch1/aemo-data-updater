#!/usr/bin/env python3
"""Constraint-equation tables for the dashboard Network page.

``build_equation_tables(con)`` expects the temp tables made by network_summary (``b``, ``vkeys``,
``du``, ``dm``, ``data_bounds``) and the attached ``cn`` schema. It creates two temp tables:

  constraint_equation       one row per term, left or right side, for every network constraint that
                            bound in the last 365 days, at its latest binding version (the version
                            used is the exact one if generic_constraint_rhs holds it, else the latest
                            earlier one that it holds, else none)
  constraint_equation_meta  one row per such constraint

The wording of the right-side sentences (``rhs_reading``) is ported from the Phase E1 prototype
(analysis_code/equations/render_equations.py, ``read_rhs``). GENERICEQUATIONDESC is not used.
"""
from __future__ import annotations

import hashlib
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd

FEEDBACK_TOL = 1e-6
EQ_COLS = ["constraintid", "side", "ord", "kind", "spd_id", "spd_type", "factor", "operation", "defaultvalue",
           "duid", "site", "fuel", "capacity_mw", "aemo_description", "reading", "is_feedback"]


def num(x, d=3):
    """Thousands separators, up to d decimals, trailing decimal zeros dropped (never integer zeros)."""
    t = f"{x:,.{d}f}"
    return t.rstrip("0").rstrip(".") if "." in t else t


def _nn(v):
    return None if v is None or (isinstance(v, float) and v != v) or v is pd.NA else v


def rhs_reading(factor, spd_id, spd_type, description=None, operation=None, defaultvalue=None) -> str:
    """Plain-English line for one right-side term."""
    f, name, ty, d, op = float(factor), spd_id or "", spd_type, _nn(description), _nn(operation)
    sign = "minus" if f < 0 else "plus"
    a = abs(f)
    w = "" if abs(a - 1) < 1e-9 else f"{num(a, 4)} × "
    if ty == "E":
        kind = ("15-minute rating" if "RAT15" in name else "sustained emergency rating" if "RATSE" in name
                else "emergency rating" if "RATEM" in name else "continuous rating" if "RATCN" in name
                else "rating or calculated value")
        dv = _nn(defaultvalue)
        fallback = f"AEMO uses {num(float(dv), 0)}" if dv is not None else "AEMO uses a default value"
        return (f"The {kind} of the protected equipment, in MVA, from AEMO's rating equation {name}. "
                f"If the live value is unavailable {fallback}.")
    if ty == "A":
        return f"{sign.capitalize()} {w}a live measurement: {d or name}."
    if ty == "S":
        return f"A switch that is 1 or 0: {d or name}." + (f" When on, it adds {num(f, 2)}." if abs(f) != 1 else "")
    if ty == "I":
        return f"{sign.capitalize()} {w}the current flow on an interconnector: {d or name}."
    if ty == "C":
        if "margin" in name.lower():
            return f"An operating margin of {num(a, 1)} held back as a safety allowance."
        if "confidence" in name.lower():
            return f"A statistical confidence allowance of {num(a, 1)}, {'subtracted' if f < 0 else 'added'}."
        return f"A fixed number set by AEMO: {num(f, 1)}."
    if ty == "U":
        if name.lower().startswith("scale"):
            return (f"Everything above is multiplied by {num(f, 3)}. This converts room on the equipment into "
                    f"room for the weighted sum on the left side.")
        if op == "EXLEZ":
            return ("A calculation step that chooses between the two ratings above according to the switch. "
                    "AEMO's description of this constraint says the 15-minute rating is the one used in normal operation.")
        return f"A calculation step named “{name}”" + (f" (operation {op})." if isinstance(op, str) else ".")
    if ty == "G":
        return f"A label for an intermediate result, “{name}”" + (f"; weight {num(f, 0)}." if abs(f) != 1 else ".")
    if ty == "T":      # a unit's current output (normally the feedback copy of a left-side term)
        return f"{sign.capitalize()} {w}the current output of {d or name}."
    return f"Term “{name}” of a type not covered by these explanations (AEMO type {ty})."


def lhs_reading(kind: str, factor: float, constrainttype: Optional[str], description: Optional[str] = None,
                bidtype: Optional[str] = None, name: Optional[str] = None) -> str:
    ge = constrainttype == ">="
    x = num(abs(factor), 2)
    who = ""
    if kind == "interconnector":
        who = f" of flow ({description or name})"
    elif kind == "region":
        who = f" of {bidtype or ''} in {name}".replace("  ", " ")
    if factor > 0:
        return f"Each extra MW{who} counts {x} towards the minimum" if ge else f"Each extra MW{who} adds {x} to the left side"
    return f"Each extra MW{who} takes {x} off the minimum" if ge else f"Each extra MW{who} takes {x} off the left side"


def term_kind(spd_type: Optional[str], spd_id: Optional[str], operation: Optional[str]) -> str:
    """Kind of a right-side term, evaluated in this order."""
    ty = spd_type
    if ty == "E":
        return "rating"
    if ty == "A":
        return "measurement"
    if ty == "S":
        return "status"
    if ty == "C":
        return "constant"
    if ty == "U":
        return "scale" if (spd_id or "").lower().startswith("scale") else "step"
    if _nn(operation):
        return "step"
    if ty == "G":
        return "label"
    if ty == "I":
        return "interconnector"
    if ty == "T":
        return "unit"
    return "other"


def fingerprint(terms: Sequence[Tuple[str, str, Optional[str]]]) -> str:
    """First 12 hex of the sha1 of the ordered (spd_id, spd_type, operation) list: structure only."""
    s = "\n".join(f"{i}|{t}|{o or ''}" for i, t, o in terms)
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:12]


def is_feedback(spd_type, spd_id, factor, lhs_cp: Dict[str, float], lhs_ic: Dict[str, float]) -> bool:
    """A T or I term that repeats a left-side term with the same factor (T matches on the part of the
    id after the dot = connectionpointid)."""
    if spd_type == "T":
        f = lhs_cp.get((spd_id or "").split(".")[-1])
    elif spd_type == "I":
        f = lhs_ic.get(spd_id)
    else:
        return False
    return f is not None and abs(float(factor) - f) < FEEDBACK_TOL


def _q(con, sql, *a) -> pd.DataFrame:
    return con.execute(sql, *a).df()


def build_equation_tables(con) -> None:
    con.execute("""create or replace temp table eq_con as
        with lat as (select cid, arg_max(eff, last_t) eff, arg_max(ver, last_t) ver, max(last_t) last_t
                     from vkeys group by 1),
        act as (select cid, count(*) n_binding_intervals,
                       quantile_cont(rhs, 0.1) rhs_p10, quantile_cont(rhs, 0.5) rhs_med,
                       quantile_cont(rhs, 0.9) rhs_p90, quantile_cont(lhs, 0.5) lhs_med
                from b where day > (select d0 from data_bounds) - 365 group by 1),
        rv as (select distinct genconid, effectivedate, versionno from rhs_src),
        cand as (select l.cid, r.effectivedate d_eff, r.versionno d_ver,
                   case when r.effectivedate = l.eff and r.versionno = l.ver then 'exact'
                        when r.effectivedate < l.eff or (r.effectivedate = l.eff and r.versionno < l.ver)
                        then 'earlier' end as rule
                 from lat l join rv r on r.genconid = l.cid),
        ranked as (select *, row_number() over (partition by cid order by
                     case rule when 'exact' then 0 else 1 end, d_eff desc, d_ver desc) rn
                   from cand where rule is not null)
        select a.cid, l.last_t, k.d_eff, k.d_ver, coalesce(k.rule, 'none') as version_rule,
               a.n_binding_intervals, a.rhs_p10, a.rhs_med, a.rhs_p90, a.lhs_med
        from act a join lat l on l.cid = a.cid left join ranked k on k.cid = a.cid and k.rn = 1""")
    units = _q(con, """select e.cid, s.connectionpointid cp, s.factor, du.duid, dm.site, dm.fuel, dm.capacity_mw
        from eq_con e join cn.spd_connection_point s on s.genconid = e.cid and s.effectivedate = e.d_eff
             and s.versionno = e.d_ver and s.bidtype = 'ENERGY' and s.factor <> 0
        left join du on du.cp = s.connectionpointid and e.last_t > du.start_date
             and (du.end_date is null or e.last_t <= du.end_date)
        left join dm on dm.duid = du.duid""")
    ics = _q(con, """select e.cid, s.interconnectorid iid, s.factor, m.description
        from eq_con e join cn.spd_interconnector s on s.genconid = e.cid and s.effectivedate = e.d_eff
             and s.versionno = e.d_ver and s.factor <> 0
        left join ems_src m on m.spd_id = s.interconnectorid""")
    regs = _q(con, """select e.cid, s.regionid, s.bidtype, s.factor
        from eq_con e join cn.spd_region s on s.genconid = e.cid and s.effectivedate = e.d_eff
             and s.versionno = e.d_ver and s.factor <> 0""")
    rhs = _q(con, """select e.cid, r.termid, r.spd_id, r.spd_type, r.factor, r.operation, r.defaultvalue,
               m.description, du.duid, dm.site, dm.fuel, dm.capacity_mw
        from eq_con e join rhs_src r on r.genconid = e.cid and r.effectivedate = e.d_eff
             and r.versionno = e.d_ver and r.scope = 'DS'
        left join ems_src m on m.spd_id = r.spd_id
        left join du on r.spd_type = 'T' and du.cp = regexp_extract(r.spd_id, '([^.]*)$', 1)
             and e.last_t > du.start_date and (du.end_date is null or e.last_t <= du.end_date)
        left join dm on dm.duid = du.duid
        order by e.cid, r.termid""")
    gt = dict(con.execute("""select e.cid, g.constrainttype from eq_con e left join
        (select genconid, effectivedate, versionno, any_value(constrainttype) constrainttype
         from cn.gencondata group by 1, 2, 3) g
        on g.genconid = e.cid and g.effectivedate = e.d_eff and g.versionno = e.d_ver""").fetchall())
    lgt = dict(con.execute("select genconid, constrainttype from lgd").fetchall())
    units_g = {k: g for k, g in units.groupby("cid")}
    ics_g = {k: g for k, g in ics.groupby("cid")}
    regs_g = {k: g for k, g in regs.groupby("cid")}
    rhs_g = {k: g for k, g in rhs.groupby("cid")}
    base = con.execute("select * from eq_con").df()
    rows: List[list] = []
    meta: List[list] = []
    for e in base.itertuples():
        cid = e.cid
        if e.version_rule == "none":
            meta.append([cid, None, None, "none", gt.get(cid) or lgt.get(cid), 1.0, 0, 0, 0, False, None,
                         _nn(e.rhs_p10), _nn(e.rhs_med), _nn(e.rhs_p90), _nn(e.lhs_med),
                         int(e.n_binding_intervals)])
            continue
        ct = gt.get(cid) or lgt.get(cid)
        ord_ = 0
        lhs_cp: Dict[str, float] = {}
        lhs_ic: Dict[str, float] = {}
        n_u = n_i = 0
        ug = units_g.get(cid)
        if ug is not None:
            for r in ug.assign(a=ug.factor.abs()).sort_values(["a", "cp", "duid"], ascending=[False, True, True],
                                                             na_position="last").itertuples():
                lhs_cp[r.cp] = float(r.factor)
                n_u += 1
                ord_ += 1
                rows.append([cid, "lhs", ord_, "unit", r.cp, "T", float(r.factor), None, None, _nn(r.duid), _nn(r.site),
                             _nn(r.fuel), _nn(r.capacity_mw), None, lhs_reading("unit", r.factor, ct), False])
        ig = ics_g.get(cid)
        if ig is not None:
            for r in ig.assign(a=ig.factor.abs()).sort_values(["a", "iid"], ascending=[False, True]).itertuples():
                lhs_ic[r.iid] = float(r.factor)
                n_i += 1
                ord_ += 1
                rows.append([cid, "lhs", ord_, "interconnector", r.iid, "I", float(r.factor), None, None, None, None,
                             None, None, _nn(r.description),
                             lhs_reading("interconnector", r.factor, ct, _nn(r.description), None, r.iid), False])
        rg = regs_g.get(cid)
        if rg is not None:
            for r in rg.assign(a=rg.factor.abs()).sort_values(["a", "regionid", "bidtype"],
                                                             ascending=[False, True, True]).itertuples():
                ord_ += 1
                rows.append([cid, "lhs", ord_, "region", r.regionid, "R", float(r.factor), None, None, None, None, None,
                             None, None, lhs_reading("region", r.factor, ct, None, r.bidtype, r.regionid), False])
        terms = []
        n_fb = 0
        scale = 1.0
        has_ops = False
        gr = rhs_g.get(cid)
        if gr is not None:
            for r in gr.itertuples():
                fb = is_feedback(r.spd_type, r.spd_id, r.factor, lhs_cp, lhs_ic)
                op = _nn(r.operation)
                kind = term_kind(r.spd_type, r.spd_id, op)
                if fb:
                    n_fb += 1
                else:
                    terms.append((r.spd_id, r.spd_type, op))
                    if op:
                        has_ops = True
                    if kind == "scale" and scale == 1.0:
                        scale = float(r.factor)
                rows.append([cid, "rhs", int(r.termid), kind, r.spd_id, r.spd_type, float(r.factor), op,
                             _nn(r.defaultvalue), _nn(r.duid), _nn(r.site), _nn(r.fuel), _nn(r.capacity_mw),
                             _nn(r.description),
                             rhs_reading(r.factor, r.spd_id, r.spd_type, r.description, op, r.defaultvalue), bool(fb)])
        meta.append([cid, e.d_eff, int(e.d_ver), e.version_rule, ct, scale, n_u, n_i, n_fb, has_ops,
                     fingerprint(terms), _nn(e.rhs_p10), _nn(e.rhs_med), _nn(e.rhs_p90), _nn(e.lhs_med),
                     int(e.n_binding_intervals)])
    con.execute("""create or replace temp table constraint_equation (constraintid varchar, side varchar, ord bigint,
        kind varchar, spd_id varchar, spd_type varchar, factor double, operation varchar, defaultvalue double,
        duid varchar, site varchar, fuel varchar, capacity_mw double, aemo_description varchar, reading varchar,
        is_feedback boolean)""")
    if rows:
        con.executemany("insert into constraint_equation values (" + ",".join("?" * 16) + ")", rows)
    con.execute("""create or replace temp table constraint_equation_meta (constraintid varchar, effectivedate timestamp,
        versionno bigint, version_rule varchar, constrainttype varchar, scale double, n_lhs_units bigint,
        n_lhs_interconnectors bigint, n_feedback bigint, has_operations boolean, fingerprint varchar,
        rhs_p10 double, rhs_med double, rhs_p90 double, lhs_med double, n_binding_intervals bigint)""")
    if meta:
        con.executemany("insert into constraint_equation_meta values (" + ",".join("?" * 16) + ")",
                        [[(m[1].to_pydatetime() if hasattr(m[1], "to_pydatetime") else m[1])
                          if i == 1 else v for i, v in enumerate(m)] for m in meta])
