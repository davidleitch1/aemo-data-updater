#!/usr/bin/env python3
"""Weekly DB integrity check, repair, and email report for the AEMO DuckDB.

Rewritten 13-Aug-2026. The previous version surveyed correctly but could only
repair gaps still inside the ~2-day CURRENT window; everything older was logged
as "manual ARCHIVE" and left. It also wrote to a log file and notified nobody,
so 1,687 missing intervals accumulated unnoticed, the oldest from July 2024.

Now:
  1. Survey the 5-minute source tables by absolute interval enumeration.
  2. Repair from CURRENT, ARCHIVE or MMSDM according to each interval's age,
     then rebuild the derived 30-minute labels that depend on them.
  3. Re-survey and email a report of found / fixed / remaining.
  4. If repair left intervals that should have been reachable, send a second
     email flagging it, so a silent partial failure cannot pass as success.

Repair needs the DuckDB write lock, so the collector is stopped for the insert
only -- downloads happen while it is still running -- and restarted in a finally
block whichever way the run ends.

Usage:
  weekly_integrity.py                 survey, repair, email
  weekly_integrity.py --check-only    survey and email, no writes
  weekly_integrity.py --no-email      survey and repair, print only
"""
import argparse
import os
import smtplib
import subprocess
import sys
import time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from gap_repair import GapRepairer, STAGE, PRIMARY

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = os.environ.get(
    'INTEGRITY_LOG_PATH',
    '/Users/davidleitch/aemo_production/logs/db_integrity.log')
TABLES = ('prices5', 'scada5', 'transmission5')
COLLECTOR_CMD = (f'cd {REPO_ROOT} && '
                 '.venv/bin/python -m aemo_updater.collectors.unified_collector_duckdb')
TMUX_WINDOW = 'services:0'

# An interval younger than this may legitimately be unreachable: it can age out
# of CURRENT before ARCHIVE publishes it. Not a failure -- it clears next week.
SEAM_DAYS = 4

_lines = []


def log(msg):
    print(msg, flush=True)
    _lines.append(msg)


def write_log():
    Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, 'a') as f:
        f.write('\n'.join(_lines) + '\n')


# ---------------------------------------------------------------- collector

def collector_running():
    return subprocess.run(['pgrep', '-f', 'unified_collector_duckdb'],
                          capture_output=True).returncode == 0


def stop_collector(timeout=150):
    subprocess.run(['tmux', 'send-keys', '-t', TMUX_WINDOW, 'C-c'], check=False)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not collector_running():
            return True
        time.sleep(5)
    return False


def start_collector():
    subprocess.run(['tmux', 'send-keys', '-t', TMUX_WINDOW, COLLECTOR_CMD, 'C-m'],
                   check=False)


# ---------------------------------------------------------------- email

def load_env():
    for p in (REPO_ROOT.parent / 'aemo-energy-dashboard2' / '.env',
              REPO_ROOT / '.env'):
        if Path(p).exists():
            load_dotenv(p, override=False)


def send_email(subject, html):
    load_env()
    server = os.getenv('EMAIL_SMTP_SERVER') or os.getenv('SMTP_SERVER', 'smtp.mail.me.com')
    port = int(os.getenv('EMAIL_SMTP_PORT') or os.getenv('SMTP_PORT', '587'))
    from_addr = (os.getenv('REPORT_FROM_EMAIL') or os.getenv('EMAIL_ADDRESS')
                 or os.getenv('ALERT_EMAIL'))
    login = os.getenv('EMAIL_LOGIN') or from_addr
    password = os.getenv('EMAIL_PASSWORD')
    to_addr = os.getenv('RECIPIENT_EMAIL') or from_addr
    if not all([from_addr, password, to_addr]):
        log('  email credentials not configured, not sent')
        return False
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = f'ITK Data Integrity <{from_addr}>'
    msg['To'] = to_addr
    msg.attach(MIMEText(html, 'html'))
    try:
        with smtplib.SMTP(server, port) as s:
            s.starttls()
            s.login(login, password)
            s.sendmail(from_addr, to_addr, msg.as_string())
        log(f'  emailed {to_addr}: {subject}')
        return True
    except Exception as e:
        log(f'  EMAIL FAILED: {e}')
        return False


def fmt_table(rows, headers):
    th = ''.join(f'<th align="left" style="padding:4px 10px;border-bottom:1px solid #ccc">'
                 f'{h}</th>' for h in headers)
    tr = ''.join(
        '<tr>' + ''.join(
            f'<td style="padding:4px 10px;border-bottom:1px solid #eee">{c}</td>'
            for c in r) + '</tr>' for r in rows)
    return (f'<table style="border-collapse:collapse;font-family:-apple-system,'
            f'Segoe UI,sans-serif;font-size:14px"><tr>{th}</tr>{tr}</table>')


def report_html(before, after, inserted, remaining_detail, repaired, checked_only):
    rows = []
    for t in TABLES:
        b, a = len(before.get(t, [])), len(after.get(t, []))
        rows.append([t, f'{b:,}', f'{b - a:,}', f'{a:,}'])
    body = [
        f'<p style="font-family:-apple-system,Segoe UI,sans-serif">'
        f'Weekly integrity check, {datetime.now():%a %d %b %Y %H:%M}.</p>',
        fmt_table(rows, ['table', 'missing before', 'recovered', 'still missing']),
    ]
    if checked_only:
        body.append('<p style="font-family:sans-serif"><b>Check-only run.</b> '
                    'No repair attempted.</p>')
    elif repaired:
        ins = ', '.join(f'{k} {v:,}' for k, v in inserted.items() if v)
        body.append(f'<p style="font-family:sans-serif">Rows inserted: {ins or "none"}. '
                    f'Derived 30-minute labels rebuilt where all six intervals were '
                    f'present.</p>')
    if remaining_detail:
        body.append('<p style="font-family:sans-serif"><b>Still missing</b> '
                    '(within the CURRENT/ARCHIVE seam these clear on a later run):</p>')
        body.append(fmt_table(remaining_detail, ['table', 'interval', 'age']))
    else:
        body.append('<p style="font-family:sans-serif"><b>No gaps remain.</b></p>')
    return ''.join(body)


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--check-only', action='store_true')
    ap.add_argument('--no-email', action='store_true')
    args = ap.parse_args()

    now = datetime.now()
    log(f'\n=== {now:%Y-%m-%d %H:%M} weekly integrity check ===')

    r = GapRepairer(log=log)
    before = r.survey(TABLES)
    total_before = sum(len(v) for v in before.values())
    for t in TABLES:
        log(f'  {t}: {len(before[t])} missing')

    inserted, repaired = {}, False
    after = None
    if total_before and not args.check_only:
        log('Staging from CURRENT / ARCHIVE / MMSDM…')
        r.clear_stage()
        try:
            r.stage_all(before)
        except Exception as e:
            log(f'  STAGING ERROR: {e}')
        staged = list(Path(STAGE).glob('*.parquet'))
        if not staged:
            log('  nothing staged — not touching the collector or the database')
        else:
            log(f'Applying {len(staged)} staged files '
                f'(collector stopped for the insert)…')
            was_running = collector_running()
            if was_running and not stop_collector():
                log('  collector would not stop — skipping apply')
            else:
                try:
                    inserted = r.apply()
                    repaired = True
                    log(f'  inserted: {inserted}')
                    # Re-survey the primary while the lock is still held. The
                    # read-only replica the survey normally reads is refreshed
                    # by the collector every few minutes, so surveying it here
                    # reported every repaired interval as still missing and
                    # sent a false follow-up (21-Sep-2026).
                    after = r.survey(TABLES, db=PRIMARY)
                except Exception as e:
                    log(f'  APPLY ERROR: {e}')
                finally:
                    if was_running:
                        start_collector()
                        log('  collector restarted')
        r.clear_stage()

    if after is None:
        after = before
    total_after = sum(len(v) for v in after.values())

    seam_cut = now - timedelta(days=SEAM_DAYS)
    detail, unexpected = [], 0
    for t in TABLES:
        for ts in after[t][:40]:
            age = (now - ts).days
            detail.append([t, f'{ts:%Y-%m-%d %H:%M}', f'{age} d'])
            if ts < seam_cut:
                unexpected += 1

    log(f'[SUMMARY] {total_before} missing before, {total_before - total_after} '
        f'recovered, {total_after} remaining ({unexpected} outside the '
        f'{SEAM_DAYS}-day CURRENT/ARCHIVE seam)')
    write_log()

    if args.no_email:
        return 0

    if total_before == 0:
        send_email('NEM data integrity: all complete',
                   report_html(before, after, inserted, [], repaired, args.check_only))
    else:
        send_email(
            f'NEM data integrity: {total_before - total_after} recovered, '
            f'{total_after} remaining',
            report_html(before, after, inserted, detail, repaired, args.check_only))

    # Follow-up only when repair left something it should have reached.
    if unexpected and not args.check_only:
        rows = [d for d in detail
                if datetime.strptime(d[1], '%Y-%m-%d %H:%M') < seam_cut]
        send_email(
            f'NEM data integrity FOLLOW-UP: {unexpected} intervals not repaired',
            '<p style="font-family:sans-serif">The weekly repair ran but these '
            f'intervals are older than {SEAM_DAYS} days, so they were not in the '
            'CURRENT/ARCHIVE seam and should have been recoverable. They were '
            'not. Likely causes: the source file is absent from nemweb ARCHIVE, '
            'AEMO never published that interval, or the download failed '
            'repeatedly.</p>'
            + fmt_table(rows, ['table', 'interval', 'age'])
            + f'<p style="font-family:sans-serif">Log: {LOG_PATH}</p>')
    return 0


if __name__ == '__main__':
    sys.exit(main())
