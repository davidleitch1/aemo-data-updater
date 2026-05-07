"""Single source of truth for which alert IDs go to which sinks.

Generated from the catalogue in
aemo-energy-dashboard2/docs/alerts.md. When a new alert ID is added
there, add a row here too — keep them in sync.

Empty at step 2; populated as plugins land in subsequent steps. Any
alert ID not in this table falls through to `DEFAULT_SINKS`.
"""
from __future__ import annotations


ALERT_ROUTING: dict[str, list[str]] = {
    # ── Step 4 (PriceBreachPlugin) ────────────────────────────────────
    # Routed to LOG ONLY at this step. The legacy
    # collectors/twilio_price_alerts.py is still firing SMS for these
    # events from a separate path; flipping these to ['sms', 'log']
    # while the legacy path runs would deliver duplicate SMS. Step 5
    # flips the routing AND deletes the legacy path in the same PR.
    'spot-price-high-breach':   ['log'],
    'spot-price-extreme-spike': ['log'],
    'spot-price-recovery':      ['log'],

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
