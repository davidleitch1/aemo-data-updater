"""AlertContext — everything a plugin needs to evaluate.

Constructed once per dispatcher cycle and shared across plugins.
Plugins are expected to open their own short-lived DuckDB connection
from `db_path` (per-request open is ~1ms; matches the API server).
Each plugin reads/writes its own state file under `data_dir`.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass
class AlertContext:
    """Carrier object passed to every plugin's `evaluate(ctx)`."""

    db_path: str
    """Filesystem path to the DuckDB. Plugin opens its own connection."""

    data_dir: Path
    """Directory plugins use for their own JSON state files."""

    now: datetime
    """UTC. Same value across all plugins in a single cycle."""

    last_run_at: datetime
    """When the dispatcher last ran. Plugins can use this for
    incremental queries ('rows newer than last_run_at')."""

    nem_now: datetime
    """NEM-naive datetime (UTC+10/+11), useful when querying DuckDB
    tables that store naive NEM-time timestamps."""
