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

    # Populated by future steps:
    #   step 6  →  new-duid-detected
    #   step 7  →  data-file-stale, data-file-missing
    #   step 8  →  battery-{soc,discharge,charge}-record-{nem,nsw1,...},
    #              battery-low-soc-{...}
    #   step 9  →  renewable-record-percentage, {wind,solar,hydro,rooftop}-record-mw
    #   step 10 →  outage-{new-detected,extended,cancelled,capacity-changed}
}


DEFAULT_SINKS: list[str] = ['log']
"""Sinks that fire for any alert ID not explicitly listed in
ALERT_ROUTING. Keeps observability for typo'd IDs / new plugins
that haven't been added to the table yet."""
