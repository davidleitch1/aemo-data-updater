"""Sinks deliver alerts via individual channels.

Each sink has:
  * a `name` (matches the sink-key used in the routing table)
  * an `enabled` flag (set at construction; usually based on whether
    the sink's credentials are configured)
  * an `emit(alert)` method that delivers the alert via its channel
"""
