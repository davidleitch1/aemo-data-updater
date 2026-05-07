"""Single source of truth for which alert IDs go to which sinks.

Generated from the catalogue in
aemo-energy-dashboard2/docs/alerts.md. When a new alert ID is added
there, add a row here too — keep them in sync.

Empty at step 2; populated as plugins land in subsequent steps. Any
alert ID not in this table falls through to `DEFAULT_SINKS`.
"""
from __future__ import annotations


ALERT_ROUTING: dict[str, list[str]] = {
    # ── Steps 4-5 (PriceBreachPlugin) ─────────────────────────────────
    # Flipped to ['sms', 'log'] at step 5, alongside deletion of the
    # legacy collectors/twilio_price_alerts.py module so SMS isn't
    # duplicated. iOS push will be added at Phase B as a third entry
    # for the high-breach + extreme-spike + recovery IDs.
    'spot-price-high-breach':   ['sms', 'log'],
    'spot-price-extreme-spike': ['sms', 'log'],
    'spot-price-recovery':      ['sms', 'log'],

    # ── Step 6 (NewDuidPlugin) ────────────────────────────────────────
    # Email-only (admin-facing). New DUIDs appear roughly weekly so
    # SMS would be excessive. Catalogue says iOS could surface this
    # inline (Phase B); not pushed.
    'new-duid-detected':        ['email', 'log'],

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
