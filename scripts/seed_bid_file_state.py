#!/usr/bin/env python3
"""Build bid_file_state.json from what bids.duckdb already holds.

Lists NEMweb CURRENT Bidmove_Complete and Next_Day_Dispatch, and marks as merged every file
whose trading day already has rows in the bids DB (bid_volume5 / bid_dispatch). Run once before
the first collector restart that uses the state file, or if the file is lost. Reads the readonly
replica, so it never takes the collector's lock. Dry run unless --write.

    python scripts/seed_bid_file_state.py            # report only
    python scripts/seed_bid_file_state.py --write    # write the state file (refuses to replace one)
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from aemo_updater import bid_file_state as bfs  # noqa: E402
from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector  # noqa: E402

DATA = Path.home() / "aemo_production" / "data"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=str(DATA / "bids_readonly.duckdb"))
    ap.add_argument("--state", default=str(DATA / "bid_file_state.json"))
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--force", action="store_true", help="replace an existing state file")
    a = ap.parse_args()

    c = UnifiedAEMOCollector(config={"data_path": str(DATA)})
    listings = {
        "bids": c.get_latest_files(c.current_urls["bids"], "PUBLIC_BIDMOVE_COMPLETE_"),
        "bid_dispatch": c.get_latest_files(c.current_urls["curtailment5"], "PUBLIC_NEXT_DAY_DISPATCH_"),
    }
    if not all(listings.values()):
        sys.exit(f"empty NEMweb listing, not writing: { {k: len(v) for k, v in listings.items()} }")
    seeded = bfs.seed_from_db(a.db, listings)
    for k in bfs.KEYS:
        missing = sorted(set(listings[k]) - seeded[k])
        print(f"{k}: {len(listings[k])} on CURRENT, {len(seeded[k])} already in DB, "
              f"{len(missing)} not in DB {missing[:5]}")
    if not a.write:
        print("dry run; pass --write to save")
        return
    if Path(a.state).exists() and not a.force:
        sys.exit(f"{a.state} exists; pass --force to replace it")
    bfs.save(a.state, seeded)
    print(f"wrote {a.state}")


if __name__ == "__main__":
    main()
