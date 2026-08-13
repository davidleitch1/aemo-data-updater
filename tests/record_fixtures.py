#!/usr/bin/env python3
"""Record real nemweb files so the collector cache tests run offline and
deterministically.

Every feed a cycle touches is captured, not just DispatchIS, because the point
of the regression suite is to prove the cache changes nothing anywhere.
"""
import json
import sys
import zipfile
import io
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector

OUT = Path(__file__).resolve().parent / 'fixtures' / 'nemweb'

# feed key -> how many of the newest files to keep
FEEDS = {
    'prices5': 3,        # DispatchIS — the 4x-duplicated one
    'scada5': 3,         # Dispatch_SCADA
    'rooftop': 2,
    'demand': 2,
    'demand_less_snsg': 2,
}
PATTERNS = {
    'prices5': 'PUBLIC_DISPATCHIS_',
    'scada5': 'PUBLIC_DISPATCHSCADA_',
    'rooftop': 'PUBLIC_ROOFTOP_PV_ACTUAL_',
    'demand': 'PUBLIC_ACTUAL_OPERATIONAL_DEMAND_HH_',
    'demand_less_snsg': 'PUBLIC_ACTUAL_OPERATIONAL_DEM_LESS_SNSG_HH_',
}


def main():
    c = UnifiedAEMOCollector()
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = {}

    for feed, keep in FEEDS.items():
        url = c.current_urls.get(feed)
        if not url:
            print(f"  {feed}: no url configured, skipped")
            continue
        try:
            files = c.get_latest_files(url, PATTERNS[feed])
        except Exception as e:
            print(f"  {feed}: listing failed {e}")
            continue
        if not files:
            print(f"  {feed}: no files matched {PATTERNS[feed]}")
            continue
        chosen = files[-keep:]
        saved = []
        for fn in chosen:
            try:
                blob = c._get_with_retry(url + fn, timeout=120).content
            except Exception as e:
                print(f"    {fn}: {e}")
                continue
            (OUT / fn).write_bytes(blob)
            # sanity: it really is a zip holding a CSV
            with zipfile.ZipFile(io.BytesIO(blob)) as z:
                csvs = [n for n in z.namelist() if n.lower().endswith('.csv')]
            saved.append(fn)
            print(f"    {fn}  {len(blob):,} bytes, {len(csvs)} csv")
        manifest[feed] = {'url': url, 'pattern': PATTERNS[feed], 'files': saved}
        print(f"  {feed}: {len(saved)} recorded")

    (OUT / 'manifest.json').write_text(json.dumps(manifest, indent=2))
    print(f"\nmanifest: {OUT / 'manifest.json'}")
    print(f"total {sum(len(v['files']) for v in manifest.values())} files, "
          f"{sum(p.stat().st_size for p in OUT.glob('*.zip')):,} bytes")


if __name__ == '__main__':
    main()
