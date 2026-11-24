"""Refresh a read-only DuckDB replica beside its primary.

The replica is made by APFS clone (macOS ``clonefile``) into ``<dst>.tmp`` and then renamed over
``dst``. A clone shares disk blocks with the primary until either file changes, so it costs
almost no space or write traffic, and Time Machine's hourly local snapshots pin only the blocks
the primary changed rather than a full copy per hour. The clone is also taken at one instant,
where a byte copy reads over about a second.

Readers see no difference: same path, same bytes, and the rename is atomic, so a reader holding
the old replica keeps reading it. If cloning is unavailable (another OS, another filesystem, or
an error) the function falls back to ``shutil.copy2``, the previous method.
"""
from __future__ import annotations

import ctypes
import os
import shutil
import sys
from pathlib import Path

_libc = None


def _clone(src: str, dst: str) -> None:
    """clonefile(2); raises OSError if it fails or the platform has none."""
    global _libc
    if sys.platform != "darwin":
        raise OSError("clonefile is only available on macOS")
    if _libc is None:
        _libc = ctypes.CDLL("libc.dylib", use_errno=True)
    if _libc.clonefile(os.fsencode(src), os.fsencode(dst), 0) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err), dst)


def refresh_replica(src, dst) -> str:
    """Replace ``dst`` with a point-in-time copy of ``src``; return "clone" or "copy".

    The caller must have closed its write connection to ``src`` first, as before.
    On failure the old ``dst`` is left untouched, no ``.tmp`` remains, and the error is raised.
    """
    src, dst = Path(src), Path(dst)
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        if tmp.exists():
            tmp.unlink()
        try:
            _clone(str(src), str(tmp))
            method = "clone"
        except OSError:
            if tmp.exists():
                tmp.unlink()
            shutil.copy2(str(src), str(tmp))
            method = "copy"
        os.rename(str(tmp), str(dst))
        return method
    finally:
        if tmp.exists():
            tmp.unlink()
