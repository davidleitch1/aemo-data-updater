#!/usr/bin/env python3
"""Build network_summary.duckdb (pre-aggregated tables for the dashboard Network page).

Reads constraints_readonly.duckdb and aemo_readonly.duckdb (each opened once, read-only) and
writes the output atomically (<out>.tmp then rename). Full rebuild every run.
"""
import argparse
import shutil
import sys
import tempfile
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from aemo_updater import network_summary as ns  # noqa: E402

DATA = Path.home() / "aemo_production/data"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--constraints", default=str(DATA / "constraints_readonly.duckdb"))
    ap.add_argument("--aemo", default=str(DATA / "aemo_readonly.duckdb"))
    ap.add_argument("--out", default=str(DATA / "network_summary.duckdb"))
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--memory-limit", default="12GB")
    a = ap.parse_args(argv)
    t0 = time.time()
    spill = tempfile.mkdtemp(prefix="network_summary_spill_", dir=str(Path(a.out).parent))
    con = duckdb.connect()
    try:
        con.execute(f"SET threads = {int(a.threads)}")
        con.execute(f"SET memory_limit = '{a.memory_limit}'")
        con.execute(f"SET temp_directory = '{spill}'")
        con.execute(f"ATTACH '{a.constraints}' AS cn (READ_ONLY)")
        con.execute(f"ATTACH '{a.aemo}' AS am (READ_ONLY)")
        counts = ns.run(con, a.out)
    finally:
        con.close()
        shutil.rmtree(spill, ignore_errors=True)
    for t, n in counts.items():
        print(f"{t:26s} {n:>12,d}")
    size = Path(a.out).stat().st_size
    print(f"wrote {a.out} ({size / 1e6:,.1f} MB) in {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
