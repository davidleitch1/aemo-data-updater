#!/usr/bin/env python3
"""Weekly refresh of duid_mapping from AEMO registration data.

Sources: NEM Registration and Exemption List, MMSDM DUDETAILSUMMARY/DUDETAIL,
and 14-day SCADA activity from scada30. Matching rules live in
aemo_updater.duid_registry. Inserts and blank-field fills are applied
(logged, in one transaction, after a snapshot of the table); conflicts on
filled rows are only reported.

Usage:
  refresh_duid_mapping.py              apply and email
  refresh_duid_mapping.py --dry-run    download, compute, print; no DB write, no email
  refresh_duid_mapping.py --dry-run --email   same, but send the report
"""
import argparse
import datetime as dt
import html
import io
import os
import smtplib
import sys
import tempfile
import traceback
import zipfile
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import duckdb
import pandas as pd
import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / 'src'))
from aemo_updater import duid_registry as dr  # noqa: E402

DATA_DIR = Path('/Users/davidleitch/aemo_production/data')
WRITE_DB = DATA_DIR / 'aemo_test.duckdb'
READ_DB = DATA_DIR / 'aemo_readonly.duckdb'
HISTORY_DIR = DATA_DIR / 'duid_mapping_history'
ACTIVE_DAYS = 14
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/124.0 Safari/537.36'}


# ------------------------------------------------------------------ downloads

def download(url, dest, timeout=180):
    """Fetch url to dest. Returns True on success, False on 404/failure."""
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
    except requests.RequestException as e:
        print(f'  download failed: {url}: {e}')
        return False
    if r.status_code != 200:
        print(f'  HTTP {r.status_code}: {url}')
        return False
    Path(dest).write_bytes(r.content)
    return True


def fetch_registration(tmp):
    path = Path(tmp) / 'reglist.xlsx'
    if not download(dr.REGISTRATION_URL, path):
        return None
    raw = pd.read_excel(path, sheet_name='PU and Scheduled Loads', engine='openpyxl')
    return dr.parse_registration(raw)


def _csv_from_zip(path):
    with zipfile.ZipFile(path) as z:
        name = next(n for n in z.namelist() if n.lower().endswith('.csv'))
        return z.read(name).decode('utf-8', errors='replace')


def fetch_mms(tmp, today):
    """Newest month (current back 4) where both tables exist. Returns (summary, detail, label)."""
    for y, m in dr.candidate_months(today, 4):
        paths = {}
        for table in ('DUDETAILSUMMARY', 'DUDETAIL'):
            p = Path(tmp) / f'{table}_{y}{m:02d}.zip'
            if not download(dr.mmsdm_url(y, m, table), p):
                break
            paths[table] = p
        else:
            summary = dr.latest_dudetailsummary(dr.parse_mms_csv(_csv_from_zip(paths['DUDETAILSUMMARY'])))
            detail = dr.latest_dudetail(dr.parse_mms_csv(_csv_from_zip(paths['DUDETAIL'])))
            return summary, detail, f'{y}-{m:02d}'
    return None, None, None


# ------------------------------------------------------------------ DB reads

def _read_only(path, tries=5):
    import time
    for i in range(tries):
        try:
            return duckdb.connect(str(path), read_only=True)
        except duckdb.IOException:
            if i == tries - 1:
                raise
            time.sleep(3)


def read_active():
    con = _read_only(READ_DB)
    try:
        rows = con.execute(
            f"SELECT duid FROM scada30 WHERE settlementdate >= "
            f"(SELECT max(settlementdate) FROM scada30) - INTERVAL {ACTIVE_DAYS} DAY "
            f"GROUP BY duid HAVING bool_or(scadavalue <> 0)").fetchall()
    finally:
        con.close()
    return {r[0] for r in rows if r[0]}


def read_mapping_readonly():
    con = _read_only(READ_DB)
    try:
        return con.execute('SELECT * FROM duid_mapping').df()
    finally:
        con.close()


# ------------------------------------------------------------------ report

def _fmt(v):
    if v is None or (isinstance(v, float) and v != v):
        return ''
    if isinstance(v, float):
        return f'{v:g}'
    return str(v)


def summary_counts(plan):
    return len(plan.inserts), len(plan.fills), len(plan.conflicts)


def subject_for(plan, partial=False):
    i, f, c = summary_counts(plan)
    s = f'duid_mapping refresh: {i} inserted, {f} filled, {c} conflicts'
    if plan.unfound or plan.unclassified:
        s += f', {len(plan.unfound) + len(plan.unclassified)} unmatched'
    return s + (' (partial sources)' if partial else '')


def text_report(plan, notes, applied):
    verb = 'applied' if applied else 'would apply (dry run)'
    L = list(notes)
    i, f, c = summary_counts(plan)
    L.append(f'Inserts {verb}: {i}; rows filled: {f}; conflicts (report only): {c}; '
             f'active DUIDs in no source: {len(plan.unfound)}; unclassified: {len(plan.unclassified)}')
    if plan.inserts:
        L.append('\nINSERTS')
        for r in plan.inserts:
            L.append('  ' + ' | '.join(_fmt(r[k]) for k in dr.MAPPING_COLS))
    if plan.edits:
        fills = [e for e in plan.edits if e['action'] == 'fill']
        if fills:
            L.append('\nFILLS (duid, field, old -> new)')
            for e in fills:
                L.append(f"  {e['duid']}: {e['field']}: {_fmt(e['old'])!r} -> {_fmt(e['new'])!r}")
    if plan.conflicts:
        L.append('\nCONFLICTS (duid, kind, mapping vs source)')
        for x in plan.conflicts:
            L.append(f"  {x['duid']}: {x['kind']}: mapping {_fmt(x['mapping'])} vs source "
                     f"{_fmt(x['source'])} ({x['detail']})")
    if plan.unfound:
        L.append('\nACTIVE, IN NO SOURCE: ' + ', '.join(u['duid'] for u in plan.unfound))
    if plan.unclassified:
        L.append('\nACTIVE, MMS ONLY, FUEL UNKNOWN (not inserted)')
        for u in plan.unclassified:
            L.append(f"  {u['duid']}: region {u['region']}, dispatch type {u['dispatchtype']}")
    return '\n'.join(L)


def _table(rows, headers):
    th = ''.join(f'<th align="left" style="padding:4px 10px;border-bottom:1px solid #ccc">{h}</th>'
                 for h in headers)
    tr = ''.join('<tr>' + ''.join(
        f'<td style="padding:4px 10px;border-bottom:1px solid #eee">{html.escape(_fmt(c))}</td>'
        for c in r) + '</tr>' for r in rows)
    return (f'<table style="border-collapse:collapse;font-family:-apple-system,Segoe UI,sans-serif;'
            f'font-size:14px"><tr>{th}</tr>{tr}</table>')


def html_report(plan, notes, applied):
    i, f, c = summary_counts(plan)
    p = ['<div style="font-family:-apple-system,Segoe UI,sans-serif;font-size:14px">']
    for n in notes:
        p.append(f'<p><b>{html.escape(n)}</b></p>')
    if not plan.edits and not plan.conflicts and not plan.unfound and not plan.unclassified:
        p.append('<p>No changes. duid_mapping matches the registration data for all DUIDs active '
                 'in the last 14 days.</p></div>')
        return '\n'.join(p)
    verb = 'Applied' if applied else 'Would apply'
    p.append(f'<p>{verb} {i} insert(s) and {f} fill(s). {c} conflict(s) reported and left unchanged.</p>')
    if plan.inserts:
        p.append('<h4>Inserted</h4>' + _table(
            [[r[k] for k in dr.MAPPING_COLS] for r in plan.inserts], dr.MAPPING_COLS))
    fills = [e for e in plan.edits if e['action'] == 'fill']
    if fills:
        p.append('<h4>Filled (blank fields only)</h4>' + _table(
            [[e['duid'], e['field'], e['old'], e['new']] for e in fills],
            ['duid', 'field', 'old', 'new']))
    if plan.conflicts:
        p.append('<h4>Conflicts (not changed)</h4>' + _table(
            [[x['duid'], x['kind'], x['mapping'], x['source'], x['detail']] for x in plan.conflicts],
            ['duid', 'kind', 'mapping', 'source', 'detail']))
    if plan.unfound:
        p.append('<h4>Active but in no source</h4><p>' +
                 html.escape(', '.join(u['duid'] for u in plan.unfound)) + '</p>')
    if plan.unclassified:
        p.append('<h4>Active, MMS only, fuel unknown (not inserted)</h4>' + _table(
            [[u['duid'], u['region'], u['dispatchtype']] for u in plan.unclassified],
            ['duid', 'region', 'dispatch type']))
    p.append('</div>')
    return '\n'.join(p)


# ------------------------------------------------------------------ email

def load_env():
    for p in (REPO_ROOT.parent / 'aemo-energy-dashboard2' / '.env', REPO_ROOT / '.env'):
        if Path(p).exists():
            load_dotenv(p, override=False)


def send_email(subject, body_html):
    load_env()
    server = os.getenv('EMAIL_SMTP_SERVER') or os.getenv('SMTP_SERVER', 'smtp.mail.me.com')
    port = int(os.getenv('EMAIL_SMTP_PORT') or os.getenv('SMTP_PORT', '587'))
    from_addr = (os.getenv('REPORT_FROM_EMAIL') or os.getenv('EMAIL_ADDRESS')
                 or os.getenv('ALERT_EMAIL'))
    login = os.getenv('EMAIL_LOGIN') or from_addr
    password = os.getenv('EMAIL_PASSWORD')
    to_addr = os.getenv('RECIPIENT_EMAIL') or from_addr
    if not all([from_addr, password, to_addr]):
        print('  email credentials not configured, not sent')
        return False
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'ITK DUID Registry <{from_addr}>'
    msg['To'] = to_addr
    msg.attach(MIMEText(body_html, 'html'))
    try:
        with smtplib.SMTP(server, port) as s:
            s.starttls()
            s.login(login, password)
            s.sendmail(from_addr, to_addr, msg.as_string())
        print(f'  emailed {to_addr}: {subject}')
        return True
    except Exception as e:
        print(f'  EMAIL FAILED: {e}')
        return False


# ------------------------------------------------------------------ main

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--dry-run', action='store_true', help='no DB write, no email unless --email')
    ap.add_argument('--email', action='store_true', help='with --dry-run, still send the email')
    args = ap.parse_args(argv)
    today = dt.date.today()
    notes = []

    with tempfile.TemporaryDirectory(prefix='duid_registry_') as tmp:
        print('Downloading registration list ...')
        reg = fetch_registration(tmp)
        print('Downloading MMSDM DUDETAILSUMMARY / DUDETAIL ...')
        summary, detail, month = fetch_mms(tmp, today)

    if reg is None and summary is None:
        msg = 'duid_mapping refresh FAILED: both sources failed to download'
        print(msg)
        if not args.dry_run:
            send_email(msg, f'<p>{msg}. No changes made.</p>')
        return 2
    if reg is None:
        notes.append('Registration list failed to download; used MMS only '
                     f'(MMSDM {month}). Fuel for new DUIDs is limited to bidirectional=battery.')
    elif summary is None:
        notes.append('MMSDM archive failed to download for the last 5 months; used the registration '
                     'list only (region from the registration list, which has known errors).')
    else:
        print(f'Sources: registration list ({len(reg)} DUIDs), MMSDM {month} '
              f'({len(summary)} summary, {len(detail)} detail)')
    partial = bool(notes)

    ref = dr.build_reference(reg, summary, detail)
    active = read_active()
    print(f'Active DUIDs (nonzero scada30 in last {ACTIVE_DAYS} days): {len(active)}')
    plan = dr.build_plan(read_mapping_readonly(), ref, active, today=today)

    applied = False
    if plan.changed and not args.dry_run:
        con = dr.connect_with_retry(WRITE_DB, interval=5, timeout=300)
        try:
            current = con.execute('SELECT * FROM duid_mapping').df()
            # Re-plan against the table as it is now, under the lock.
            plan = dr.build_plan(current, ref, active, today=today)
            if plan.changed:
                snap = dr.write_snapshot(current, HISTORY_DIR, today)
                edits = dr.write_edits(plan.edits, HISTORY_DIR, today)
                dr.apply_plan(con, plan)
                applied = True
                print(f'Snapshot: {snap}\nEdits log: {edits}')
        finally:
            con.close()

    print()
    print(text_report(plan, notes, applied))

    if not args.dry_run or args.email:
        send_email(subject_for(plan, partial), html_report(plan, notes, applied))
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        print(tb)
        if '--dry-run' not in sys.argv:
            send_email('duid_mapping refresh FAILED', f'<pre>{html.escape(tb)}</pre>')
        sys.exit(1)
