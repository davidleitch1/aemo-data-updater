#!/usr/bin/env python3
"""Monthly change report for the Network page: compares the current network_summary.duckdb with the
snapshot saved by the previous run, prints/emails what has moved and which hand-kept files need an
edit, then saves a new snapshot.

Sections (each omitted when empty):
  1. areas entering or leaving the 12-month top ten, by energy and by hours
  2. rising: in the 90-day top five by energy but not the 12-month top five
  3. areas whose lead constraint changed since the last report
  4. notes now stale
  5. top-30 areas (12-month hours) with no line mapping in network_geo.json (area_lines or area_status)
  6. units of 30 MW or more in area_units but not in the geo file's units
  7. binding constraints with definition_rule 'none'
  8. areas created in the registry since the last report

The first run (no state) sends a baseline: the current top ten plus every section that does not need
a previous snapshot. Nothing is sent when every section is empty.

    .venv/bin/python scripts/network_change_report.py [--summary PATH] [--state PATH] [--geo PATH] [--no-email]

Suggested cron (monthly, after the 10th-of-month archive load):  15 7 11 * *  cd ~/aemo_production/aemo-data-updater && .venv/bin/python scripts/network_change_report.py >> ~/aemo_production/logs/network_change_report.log 2>&1
"""
from __future__ import annotations

import argparse
import html
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

DATA = Path.home() / "aemo_production/data"
DEFAULT_GEO = Path.home() / "aemo-redesign/src/aemo_dashboard/web/network_geo.json"
GEO_FILE = "aemo-redesign/src/aemo_dashboard/web/network_geo.json (rebuild with analysis_code/network_map/export_network_geo.py)"
NAMES_FILE = "config/network_area_names.csv"
NOTES_FILE = "config/network_area_notes.json"
MIN_UNIT_MW = 30.0
TOP = 10


def _label(names: Dict[str, str], aid: str) -> str:
    return f"{names.get(aid, aid)} [{aid}]"


def _rank_sets(con) -> Tuple[List[str], List[str], List[str], List[str]]:
    q = lambda w, col, n: [r[0] for r in con.execute(  # noqa: E731
        f'select area_id from area_rank where "window" = ? and {col} <= {n} order by {col}, area_id', [w]).fetchall()]
    return q("12m", "rank_energy", TOP), q("12m", "rank_hours", TOP), q("90d", "rank_energy", 5), q("12m", "rank_energy", 5)


def collect(summary: Path, state: Optional[dict], geo: Optional[dict]) -> Tuple[List[Tuple[str, List[str], str]], dict]:
    """Returns ([(title, lines, file_to_edit)], new_state)."""
    con = duckdb.connect(str(summary), read_only=True)
    try:
        names = dict(con.execute("select area_id, name from area").fetchall())
        leads = dict(con.execute("select area_id, lead_constraintid from area").fetchall())
        created = dict(con.execute("select area_id, created from area").fetchall())
        top_e, top_h, r90_5, r12_5 = _rank_sets(con)
        meta = con.execute("select built_at, data_through from meta").fetchone()
        sections: List[Tuple[str, List[str], str]] = []
        lab = lambda a: _label(names, a)  # noqa: E731

        if state is None:
            sections.append(("Baseline: top ten areas by energy held back, last 12 months",
                             [f"{i}. {lab(a)}" for i, a in enumerate(top_e, 1)], "(none: this is the starting snapshot)"))
        else:
            for title, now, prev in (("Areas entering or leaving the top ten by energy held back (12 months)", top_e,
                                      state.get("top10_energy", [])),
                                     ("Areas entering or leaving the top ten by binding hours (12 months)", top_h,
                                      state.get("top10_hours", []))):
                ent = [a for a in now if a not in prev]
                lef = [a for a in prev if a not in now]
                if ent or lef:
                    lines = [f"entered: {lab(a)}" for a in ent] + [f"left: {lab(a)}" for a in lef]
                    sections.append((title, lines, f"{NAMES_FILE} (names), {NOTES_FILE} (notes for new entrants)"))
        rising = [a for a in r90_5 if a not in r12_5]
        if rising:
            sections.append(("Rising: in the 90-day top five by energy but not the 12-month top five",
                             [lab(a) for a in rising], f"{NAMES_FILE}, {NOTES_FILE}"))
        if state is not None:
            prev_leads = state.get("leads", {})
            ch = [a for a in leads if a in prev_leads and leads[a] != prev_leads[a]]
            if ch:
                sections.append(("Areas whose lead constraint changed",
                                 [f"{lab(a)}: {prev_leads[a]} -> {leads[a]}" for a in ch], NOTES_FILE))
        stale = con.execute("""select n.area_id, n.constraintid, n.written_effectivedate, m.effectivedate
            from area_note n left join constraint_equation_meta m on m.constraintid = n.constraintid
            where n.is_stale order by n.area_id""").fetchall()
        if stale:
            def v(x):
                return str(x)[:10] if x is not None else "unknown"
            sections.append(("Notes now stale",
                             [f"{lab(a)}: note for {c}, written for the version effective {v(w)}; current version "
                              f"{v(cur)}, lead constraint now {leads.get(a)}" for a, c, w, cur in stale], NOTES_FILE))
        if geo is not None:
            mapped = set(geo.get("area_lines", {})) | set(geo.get("area_status", {}))
            top30 = [r[0] for r in con.execute(
                """select area_id from area_rank where "window" = '12m' order by hours desc, area_id limit 30""").fetchall()]
            um = [a for a in top30 if a not in mapped]
            if um:
                sections.append(("Top-30 areas by hours with no line mapping", [lab(a) for a in um], GEO_FILE))
            known = set(geo.get("units", {}))
            miss = con.execute(f"""select duid, any_value(site), max(capacity_mw) from area_units
                where capacity_mw >= {MIN_UNIT_MW} group by 1 order by max(capacity_mw) desc, 1""").fetchall()
            miss = [(d, s, c) for d, s, c in miss if d not in known]
            if miss:
                sections.append((f"Units of {MIN_UNIT_MW:.0f} MW or more with no map location",
                                 [f"{d} ({s or 'no site'}, {c:,.0f} MW)" for d, s, c in miss], GEO_FILE))
        none = con.execute("""select m.constraintid, coalesce(sum(d.n_intervals), 0) n from constraint_meta m
            left join constraint_daily d on d.constraintid = m.constraintid
              and d.day > cast((select data_through from meta) as date) - 365
            where m.definition_rule = 'none' group by 1 having coalesce(sum(d.n_intervals), 0) > 0
            order by n desc, 1""").fetchall()
        if none:
            top = [f"{c}: {n / 12:,.0f} h in the last 12 months" for c, n in none[:15]]
            if len(none) > 15:
                top.append(f"... and {len(none) - 15} more ({len(none)} in total)")
            sections.append(("Binding constraints with no definition version (definition_rule 'none')", top,
                             "nothing to edit: AEMO's definition tables arrive 4-6 weeks late "
                             "(scripts/backfill_constraints.py --auto loads them)"))
        if state is not None:
            seen = set(state.get("areas", []))
            new = [a for a in names if a not in seen]
            if new:
                sections.append(("New areas created in the registry since the last report",
                                 [f"{lab(a)} (created {created.get(a)})" for a in new], NAMES_FILE))
    finally:
        con.close()
    new_state = {"saved": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "built_at": str(meta[0]),
                 "data_through": str(meta[1]), "top10_energy": top_e, "top10_hours": top_h, "leads": leads,
                 "areas": sorted(names)}
    return sections, new_state


def render(sections, header: str) -> str:
    out = [header, ""]
    for title, lines, edit in sections:
        out.append(title)
        out.append("-" * len(title))
        out += [f"  {l}" for l in lines]
        out.append(f"  Edit: {edit}")
        out.append("")
    return "\n".join(out).rstrip() + "\n"


def save_state(path: Path, state: dict) -> None:
    tmp = Path(str(path) + ".tmp")
    tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
    os.replace(tmp, path)


def main(argv=None, send=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--summary", default=str(DATA / "network_summary.duckdb"))
    ap.add_argument("--state", default=str(DATA / "network_change_report_state.json"))
    ap.add_argument("--geo", default=str(DEFAULT_GEO))
    ap.add_argument("--no-email", action="store_true")
    a = ap.parse_args(argv)
    sp = Path(a.state)
    state = json.loads(sp.read_text(encoding="utf-8")) if sp.exists() else None
    gp = Path(a.geo)
    geo = json.loads(gp.read_text(encoding="utf-8")) if gp.exists() else None
    if geo is None:
        print(f"geo file not found ({gp}): line-mapping and unit-location sections skipped")
    sections, new_state = collect(Path(a.summary), state, geo)
    kind = "baseline" if state is None else "change report"
    header = (f"Network {kind}: summary built {new_state['built_at'][:16]}, data to {new_state['data_through'][:16]}"
              + ("" if state is None else f"; previous report {state.get('saved', '?')[:16]}"))
    if sections:
        body = render(sections, header)
        if a.no_email:
            print(body)
        else:
            if send is None:
                from weekly_integrity import send_email as send
            ok = send(f"Network {kind}: {len(sections)} section(s)",
                      "<pre style=\"font-family:Menlo,Consolas,monospace;font-size:13px\">"
                      + html.escape(body) + "</pre>")
            print(f"emailed: {bool(ok)}")
            if not ok:
                print(body)
                return 1
    else:
        print(header + "\nNothing changed; no email sent.")
    save_state(sp, new_state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
