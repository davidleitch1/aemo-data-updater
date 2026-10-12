#!/usr/bin/env python3
"""Daily DB integrity check, repair, and email report for the AEMO DuckDB.

Rewritten 13-Aug-2026. The previous version surveyed correctly but could only
repair gaps still inside the ~2-day CURRENT window; everything older was logged
as "manual ARCHIVE" and left. It also wrote to a log file and notified nobody,
so 1,687 missing intervals accumulated unnoticed, the oldest from July 2024.

Extended 13-Oct-2026: surveys and repairs every collector-written base table
(five-minute: prices5, scada5, transmission5, bdu5, curtailment_regional5,
curtailment5, curtailment_duid5; thirty-minute: prices30, scada30,
transmission30, demand30, rooftop30; plus the demand30.demand_less_snsg column),
runs daily, and takes the DuckDB lock by waiting for the collector's gap between
cycles instead of stopping the collector.

  1. Survey every table by absolute interval enumeration.
  2. Repair from CURRENT, ARCHIVE or MMSDM according to each interval's age and
     feed, then rebuild the derived 30-minute labels that depend on them.
  3. Re-survey on the primary and email a report of found / fixed / remaining.
  4. If repair left intervals that should have been reachable, send a second
     email flagging it, so a silent partial failure cannot pass as success.

Email policy: Sundays always send the full report; other days send only when
something was missing before repair or remains after it.

Known-unreachable ledger: intervals older than SEAM_DAYS that survive a repair
(e.g. rooftop30 holes from 2024, before nemweb ARCHIVE starts) are written to a
ledger, are excluded from the daily counts so they do not alarm every morning,
and are retried in full every Sunday.

Locking: the collector closes its DuckDB connection for the ~4.5-minute sleep
between cycles and retries a lock conflict for ~30 s when it opens. The insert
takes seconds, so the script waits for the gap and takes the lock itself
(gap_repair.acquire_primary). It does not stop the collector. The previous
approach (Ctrl-C into tmux, then type the start command) failed twice on
13-Oct-2026: pgrep matched the ssh shell running the script, and a 150 s timeout
is shorter than one cycle, after which the start command was typed into the
still-running collector's terminal and queued. `--stop-collector` keeps that
route, fixed, for a collector that will not release the lock.

Usage:
  weekly_integrity.py                 survey, repair, email per policy
  weekly_integrity.py --check-only    survey and email, no writes
  weekly_integrity.py --no-email      survey and repair, print only
  weekly_integrity.py --full          retry the known-unreachable ledger too
"""
import argparse
import json
import os
import shlex
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
from gap_repair import (GapRepairer, STAGE, PRIMARY, ALL_TABLES, SNSG,  # noqa: E402
                        acquire_primary, LockTimeout, nem_now)

REPO_ROOT = Path(__file__).resolve().parents[1]
LOG_PATH = os.environ.get(
    'INTEGRITY_LOG_PATH',
    '/Users/davidleitch/aemo_production/logs/db_integrity.log')
LEDGER_PATH = os.environ.get(
    'INTEGRITY_LEDGER_PATH',
    '/Users/davidleitch/aemo_production/logs/integrity_known_gaps.json')
TABLES = ALL_TABLES + (SNSG,)
COLLECTOR_MODULE = 'aemo_updater.collectors.unified_collector_duckdb'
COLLECTOR_CMD = (f'cd {REPO_ROOT} && .venv/bin/python -m {COLLECTOR_MODULE}')
TMUX_WINDOW = 'services:0'

# An interval younger than this may legitimately be unreachable: it can age out
# of CURRENT before ARCHIVE publishes it. Not a failure -- it clears next run.
SEAM_DAYS = 4
SUNDAY = 6
DETAIL_PER_TABLE = 10

_lines = []


def log(msg):
    print(msg, flush=True)
    _lines.append(msg)


def write_log():
    Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_PATH, 'a') as f:
        f.write('\n'.join(_lines) + '\n')


# ---------------------------------------------------------------- collector

def collector_pids(ps_text, exclude=()):
    """PIDs of the collector in `ps -axo pid=,command=` output.

    Matches the process itself -- a python executable run as
    `-m aemo_updater.collectors.unified_collector_duckdb` -- not any command
    line that merely contains the module name. `pgrep -f` matched the ssh and
    shell processes that were running this script, so it reported "running"
    indefinitely.
    """
    found = []
    for line in ps_text.splitlines():
        line = line.strip()
        if not line:
            continue
        pid_s, _, cmd = line.partition(' ')
        try:
            pid = int(pid_s)
            argv = shlex.split(cmd)
        except ValueError:
            continue
        if pid in exclude or not argv:
            continue
        exe = os.path.basename(argv[0]).lower()
        if not exe.startswith('python'):
            continue
        if '-m' in argv and argv[argv.index('-m') + 1:argv.index('-m') + 2] == [COLLECTOR_MODULE]:
            found.append(pid)
    return found


def _ancestors():
    pids, pid = {os.getpid()}, os.getppid()
    while pid > 1 and pid not in pids:
        pids.add(pid)
        out = subprocess.run(['ps', '-o', 'ppid=', '-p', str(pid)],
                             capture_output=True, text=True).stdout.strip()
        pid = int(out) if out.isdigit() else 0
    return pids


def collector_running():
    ps = subprocess.run(['ps', '-axo', 'pid=,command='],
                        capture_output=True, text=True).stdout
    return bool(collector_pids(ps, exclude=_ancestors()))


def stop_collector(timeout=600, poll=5):
    """Ctrl-C the collector's tmux window and wait for the process to exit.

    One cycle can take ~4 minutes and Ctrl-C lands only at an interruptible
    point, so the default wait outlasts a full cycle.
    """
    subprocess.run(['tmux', 'send-keys', '-t', TMUX_WINDOW, 'C-c'], check=False)
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not collector_running():
            return True
        time.sleep(poll)
    return False


def start_collector():
    subprocess.run(['tmux', 'send-keys', '-t', TMUX_WINDOW, COLLECTOR_CMD, 'C-m'],
                   check=False)


class CollectorWontStop(RuntimeError):
    pass


def run_with_collector_stopped(fn, stop=None, running=None, start=None):
    """Run fn() with the collector stopped, restarting it only if it was running.

    If the stop times out, the start command is NOT sent: typing it into the
    terminal of a collector that is still mid-cycle queues it, and it runs
    later. The Ctrl-C may still land after the timeout, so the final check
    restarts the collector only if it has by then exited.
    """
    stop = stop or stop_collector
    running = running or collector_running
    start = start or start_collector
    was_running = running()
    if was_running and not stop():
        if not running():          # the interrupt landed just after the timeout
            start()
        raise CollectorWontStop('collector still running after the stop timeout')
    try:
        return fn()
    finally:
        if was_running:
            start()


# ---------------------------------------------------------------- ledger

def load_ledger(path=None):
    try:
        raw = json.loads(Path(path or LEDGER_PATH).read_text())
    except (OSError, ValueError):
        return {}
    return {t: {datetime.fromisoformat(s) for s in v} for t, v in raw.items()}


def save_ledger(ledger, path=None):
    p = Path(path or LEDGER_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps({t: sorted(s.isoformat() for s in v)
                             for t, v in ledger.items() if v}, indent=1))


def exclude_known(gaps, ledger):
    return {t: [ts for ts in v if ts not in ledger.get(t, ())] for t, v in gaps.items()}


def unreachable(gaps, now, seam_days=SEAM_DAYS):
    """Gaps older than the CURRENT/ARCHIVE seam: should have been repairable."""
    cut = now - timedelta(days=seam_days)
    return {t: {ts for ts in v if ts < cut} for t, v in gaps.items()}


# ---------------------------------------------------------------- email

def email_decision(now, total_before, total_after, policy='auto'):
    """Whether to send the report. Sundays: always. Other days: only if anything
    was missing before repair or remains after it."""
    if policy == 'always':
        return True
    if policy == 'never':
        return False
    if now.weekday() == SUNDAY:
        return True
    return bool(total_before or total_after)


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


def report_html(before, after, inserted, remaining_detail, repaired, checked_only,
                known=None, now=None):
    now = now or datetime.now()
    rows = []
    for t in TABLES:
        b, a = len(before.get(t, [])), len(after.get(t, []))
        rows.append([t, f'{b:,}', f'{b - a:,}', f'{a:,}'])
    body = [
        f'<p style="font-family:-apple-system,Segoe UI,sans-serif">'
        f'Data integrity check, {now:%a %d %b %Y %H:%M}.</p>',
        fmt_table(rows, ['table', 'missing before', 'recovered', 'still missing']),
    ]
    if checked_only:
        body.append('<p style="font-family:sans-serif"><b>Check-only run.</b> '
                    'No repair attempted.</p>')
    elif repaired:
        ins = ', '.join(f'{k} {v:,}' for k, v in inserted.items() if v)
        body.append(f'<p style="font-family:sans-serif">Rows written: {ins or "none"}. '
                    f'Derived 30-minute labels rebuilt where all six intervals were '
                    f'present.</p>')
    if remaining_detail:
        body.append('<p style="font-family:sans-serif"><b>Still missing</b> '
                    f'(first {DETAIL_PER_TABLE} per table; within the CURRENT/ARCHIVE '
                    'seam these clear on a later run):</p>')
        body.append(fmt_table(remaining_detail, ['table', 'interval', 'age']))
    else:
        body.append('<p style="font-family:sans-serif"><b>No gaps remain.</b></p>')
    if known:
        k = ', '.join(f'{t} {len(v):,}' for t, v in known.items() if v)
        if k:
            body.append('<p style="font-family:sans-serif">Known-unreachable historic '
                        f'intervals (not in the daily counts, retried each Sunday): {k}.</p>')
    return ''.join(body)


# ---------------------------------------------------------------- main

def main(argv=None, now=None, nem=None):
    ap = argparse.ArgumentParser()
    ap.add_argument('--check-only', action='store_true')
    ap.add_argument('--no-email', action='store_true')
    ap.add_argument('--full', action='store_true',
                    help='retry the known-unreachable ledger (default on Sundays)')
    ap.add_argument('--email-policy', choices=('auto', 'always', 'never'), default='auto')
    ap.add_argument('--stop-collector', action='store_true',
                    help='stop the collector for the insert instead of waiting for its '
                         'gap between cycles')
    ap.add_argument('--lock-timeout', type=int, default=900)
    args = ap.parse_args(argv)

    # `now` is the host clock (cron weekday, report header). Table timestamps are
    # NEM time, so ages are measured against `nem`.
    nem = nem or (now if now else nem_now())
    now = now or datetime.now()
    full = args.full or now.weekday() == SUNDAY
    log(f'\n=== {now:%Y-%m-%d %H:%M} integrity check ({"full" if full else "daily"}) ===')

    r = GapRepairer(log=log)
    ledger = load_ledger()
    raw_before = r.survey()
    before = raw_before if full else exclude_known(raw_before, ledger)
    total_before = sum(len(v) for v in before.values())
    for t in TABLES:
        log(f'  {t}: {len(before.get(t, []))} missing')

    inserted, repaired = {}, False
    attempted = False        # repair genuinely tried: gaps may be called unreachable
    after = None
    if total_before and not args.check_only:
        log('Staging from CURRENT / ARCHIVE / MMSDM…')
        r.clear_stage()
        stage_ok = True
        try:
            r.stage_all(before)
        except Exception as e:
            stage_ok = False
            log(f'  STAGING ERROR: {e}')
        staged = list(Path(STAGE).glob('*.parquet'))
        if not staged:
            log('  nothing staged — not touching the database')
            attempted = stage_ok
        else:
            log(f'Applying {len(staged)} staged files…')

            def do_apply(con=None):
                nonlocal inserted, repaired, attempted, after
                inserted = r.apply(con, gaps=before)
                repaired = attempted = True
                log(f'  inserted: {inserted}')
                # Re-survey the primary under the same lock. The read-only replica
                # the survey normally reads is refreshed by the collector every
                # few minutes, so surveying it here reported every repaired
                # interval as still missing and sent a false follow-up
                # (21-Sep-2026).
                after = r.survey(con=con)

            try:
                if args.stop_collector:
                    run_with_collector_stopped(lambda: do_apply(None))
                else:
                    con = acquire_primary(timeout=args.lock_timeout, log=log)
                    try:
                        do_apply(con)
                    finally:
                        con.close()
            except (LockTimeout, CollectorWontStop) as e:
                log(f'  not applied: {e}')
            except Exception as e:
                log(f'  APPLY ERROR: {e}')
        r.clear_stage()

    if after is None:
        after = before
    elif not full:
        after = exclude_known(after, ledger)
    total_after = sum(len(v) for v in after.values())

    old = unreachable(after, nem)
    new_unreachable = {t: {ts for ts in v if ts not in ledger.get(t, ())}
                       for t, v in old.items()}
    n_new = sum(len(v) for v in new_unreachable.values())

    detail = []
    for t in TABLES:
        for ts in after.get(t, [])[:DETAIL_PER_TABLE]:
            detail.append([t, f'{ts:%Y-%m-%d %H:%M}', f'{(nem - ts).days} d'])

    # Ledger: only a run that actually attempted repair may promote a gap to
    # "known unreachable"; a check-only, failed-staging or failed-apply run
    # proves nothing.
    if attempted:
        if full:
            ledger = {t: set(v) for t, v in old.items() if v}
        else:
            for t, v in old.items():
                ledger.setdefault(t, set()).update(v)
        save_ledger(ledger)

    log(f'[SUMMARY] {total_before} missing before, {total_before - total_after} '
        f'recovered, {total_after} remaining ({n_new} newly unreachable, older than '
        f'{SEAM_DAYS} days; {sum(len(v) for v in ledger.values())} in the ledger)')
    write_log()

    if args.no_email:
        return 0

    if email_decision(now, total_before, total_after, args.email_policy):
        subject = ('NEM data integrity: all complete' if total_before == 0
                   else f'NEM data integrity: {total_before - total_after} recovered, '
                        f'{total_after} remaining')
        send_email(subject, report_html(before, after, inserted, detail, repaired,
                                        args.check_only, known=ledger, now=now))

    # Follow-up only when repair left something it should have reached.
    if n_new and attempted and not args.check_only:
        rows = [[t, f'{ts:%Y-%m-%d %H:%M}', f'{(nem - ts).days} d']
                for t, v in new_unreachable.items() for ts in sorted(v)[:DETAIL_PER_TABLE]]
        send_email(
            f'NEM data integrity FOLLOW-UP: {n_new} intervals not repaired',
            '<p style="font-family:sans-serif">The repair ran but these intervals are '
            f'older than {SEAM_DAYS} days, so they were not in the CURRENT/ARCHIVE seam '
            'and should have been recoverable. They were not. Likely causes: the source '
            'file is absent from nemweb ARCHIVE, AEMO never published that interval, or '
            'the download failed repeatedly. They are now in the known-unreachable '
            f'ledger and will not be counted again until the Sunday retry (first '
            f'{DETAIL_PER_TABLE} per table shown).</p>'
            + fmt_table(rows, ['table', 'interval', 'age'])
            + f'<p style="font-family:sans-serif">Log: {LOG_PATH}</p>')
    return 0


if __name__ == '__main__':
    sys.exit(main())
