"""Single source of truth for which alert IDs go to which sinks.

Generated from the catalogue in
aemo-energy-dashboard2/docs/alerts.md. When a new alert ID is added
there, add a row here too — keep them in sync.

Empty at step 2; populated as plugins land in subsequent steps. Any
alert ID not in this table falls through to `DEFAULT_SINKS`.
"""
from __future__ import annotations


ALERT_ROUTING: dict[str, list[str]] = {
    # ── Steps 4-5 + Phase B (PriceBreachPlugin) ───────────────────────
    # Step 5 flipped from ['log'] to ['sms', 'log'] (alongside deletion
    # of the legacy collectors/twilio_price_alerts.py). Phase B added
    # 'apns' so iOS testers get a lock-screen banner + sound + badge
    # on every breach.
    'spot-price-high-breach':   ['sms', 'apns', 'log'],
    'spot-price-extreme-spike': ['sms', 'apns', 'log'],
    'spot-price-recovery':      ['sms', 'apns', 'log'],

    # ── Step 6 + Phase B (NewDuidPlugin) ──────────────────────────────
    # Step 6: email-only (admin-facing). Phase B adds 'apns' which
    # delivers as a SILENT push — bumps the iOS app icon badge by 1
    # without a banner / sound. Tester sees the badge tick up; opens
    # the app to find what's new. SMS skipped — new DUIDs appear
    # weekly-ish, would be excessive.
    'new-duid-detected':        ['email', 'apns', 'log'],

    # ── Step 7 (DataFreshnessPlugin) ──────────────────────────────────
    # Stale → admin email (operational). Missing → email + sms because
    # missing data usually means the pipeline has failed and the
    # operator needs to act quickly.
    'data-file-stale':          ['email', 'log'],
    'data-file-missing':        ['email', 'sms', 'log'],

    # ── Step 8 (BatteryRecordsPlugin + BatteryLowSocPlugin) ──────────
    # SHADOW MODE: log-only at this step. The standalone
    # /Users/davidleitch/aemo_production/battery_monitor.py daemon
    # keeps firing SMS as the source of truth; plugins observe in
    # parallel for ~24h. Cutover commit (later) flips routing to
    # ['sms', 'log'] and stops the daemon.
    **{f'battery-soc-record-{r}':       ['log']
       for r in ('nem', 'nsw1', 'qld1', 'vic1', 'sa1')},
    **{f'battery-discharge-record-{r}': ['log']
       for r in ('nem', 'nsw1', 'qld1', 'vic1', 'sa1')},
    **{f'battery-charge-record-{r}':    ['log']
       for r in ('nem', 'nsw1', 'qld1', 'vic1', 'sa1')},
    **{f'battery-low-soc-{r}':          ['log']
       for r in ('nsw1', 'qld1', 'vic1', 'sa1')},
    **{f'battery-low-soc-recovery-{r}': ['log']
       for r in ('nsw1', 'qld1', 'vic1', 'sa1')},

    # ── Step 9 (RenewableRecordsPlugin) ──────────────────────────────
    # SHADOW MODE: log-only at this step. Standalone gauge daemon
    # (tmux services:2) keeps firing SMS as source of truth. Cutover
    # commit later flips to ['sms', 'log'] and stops the daemon.
    'renewable-record-percentage': ['log'],
    'wind-record-mw':              ['log'],
    'solar-record-mw':             ['log'],
    'hydro-record-mw':             ['log'],
    'rooftop-solar-record-mw':     ['log'],

    # Populated by future steps:
    #   step 8  →  battery-{soc,discharge,charge}-record-{nem,nsw1,...},
    #              battery-low-soc-{...}
    #   step 9  →  renewable-record-percentage, {wind,solar,hydro,rooftop}-record-mw
    #   step 10 →  outage-{new-detected,extended,cancelled,capacity-changed}
}


DEFAULT_SINKS: list[str] = ['log']
"""Sinks that fire for any alert ID not explicitly listed in
ALERT_ROUTING. Keeps observability for typo'd IDs / new plugins
that haven't been added to the table yet."""
