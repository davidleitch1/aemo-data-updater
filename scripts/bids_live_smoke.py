#!/usr/bin/env python3
"""Live smoke test for bid collection.

Downloads the single latest CURRENT/Bidmove_Complete file from NEMWEB, parses it,
stores it into a throwaway bids.duckdb, and prints stats. Requires network; NOT
part of the offline pytest suite. Run on the collector host:

    .venv/bin/python scripts/bids_live_smoke.py
"""
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import duckdb  # noqa: E402

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector  # noqa: E402
from aemo_updater.collectors.bids_store import merge_bids  # noqa: E402


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="bids_smoke_")
    collector = UnifiedAEMOCollector(config={"data_path": tmp, "max_files_per_cycle": 1})

    print("Downloading + parsing latest Bidmove_Complete (1 file)...", flush=True)
    vol, price = collector.collect_bids()
    print(f"parsed: volume rows={len(vol)}, price rows={len(price)}")
    assert not vol.empty, "no BIDPEROFFER_D volume rows parsed"
    assert not price.empty, "no BIDDAYOFFER_D price rows parsed"

    conn = duckdb.connect(os.path.join(tmp, "bids.duckdb"))
    print("merge counts:", merge_bids(conn, vol, price))

    for t in ("bid_volume5", "bid_price_bands"):
        n, mn, mx = conn.execute(
            f"SELECT COUNT(*), MIN(settlementdate), MAX(settlementdate) FROM {t}"
        ).fetchone()
        nd = conn.execute(f"SELECT COUNT(DISTINCT duid) FROM {t}").fetchone()[0]
        dirs = sorted(r[0] for r in conn.execute(f"SELECT DISTINCT direction FROM {t}").fetchall())
        print(f"{t}: rows={n} duids={nd} span={mn}..{mx} directions={dirs}")

    sample = conn.execute(
        "SELECT duid, direction, maxavail, bandavail1, bandavail10 "
        "FROM bid_volume5 ORDER BY settlementdate DESC LIMIT 1"
    ).fetchone()
    print("sample vol row (duid, dir, maxavail, band1, band10):", sample)
    conn.close()
    print("LIVE SMOKE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
