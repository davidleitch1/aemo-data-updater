#!/usr/bin/env python3
"""Offline test of UnifiedAEMOCollector.collect_bids (network monkeypatched).

Verifies the collector's file-diffing, parse, cross-file dedup, and last_files
bookkeeping without touching NEMWEB. The full end-to-end run against a live
Bidmove_Complete download is exercised as a Phase-1 verification step on .71.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector  # noqa: E402

FIXTURE_ZIP_CSV = (Path(__file__).parent / "fixtures" / "bids_sample.csv").read_bytes()
SAMPLE_FILE = "PUBLIC_BIDMOVE_COMPLETE_20260629_0000000000000001.zip"


@pytest.fixture
def collector(tmp_path):
    return UnifiedAEMOCollector(config={"data_path": str(tmp_path)})


def test_collect_bids_parses_and_tracks(collector, monkeypatch):
    monkeypatch.setattr(collector, "get_latest_files", lambda url, pattern: [SAMPLE_FILE])
    monkeypatch.setattr(
        collector, "_download_zip_csv_bytes", lambda url, fn: FIXTURE_ZIP_CSV
    )

    vol, price = collector.collect_bids()

    assert len(vol) == 4
    assert len(price) == 3
    assert set(price["duid"]) == {"AAA1", "BBB1", "CCC1"}
    assert SAMPLE_FILE in collector.last_files["bids"]


def test_collect_bids_skips_already_seen(collector, monkeypatch):
    monkeypatch.setattr(collector, "get_latest_files", lambda url, pattern: [SAMPLE_FILE])
    monkeypatch.setattr(
        collector, "_download_zip_csv_bytes", lambda url, fn: FIXTURE_ZIP_CSV
    )
    collector.collect_bids()  # consume the file

    vol, price = collector.collect_bids()  # nothing new
    assert vol.empty and price.empty
