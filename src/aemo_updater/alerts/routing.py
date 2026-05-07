"""Single source of truth for which alert IDs go to which sinks.

Generated from the catalogue in
aemo-energy-dashboard2/docs/alerts.md. When a new alert ID is added
there, add a row here too — keep them in sync.

Empty at step 2; populated as plugins land in subsequent steps. Any
alert ID not in this table falls through to `DEFAULT_SINKS`.
"""
from __future__ import annotations


ALERT_ROUTING: dict[str, list[str]] = {
    # Populated incrementally as plugins land:
    #   step 4  →  spot-price-high-breach, spot-price-extreme-spike
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
