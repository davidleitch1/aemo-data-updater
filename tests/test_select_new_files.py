"""Behavioural test for UnifiedAEMOCollector._select_new_files.

Reproduces the 30-Jun-2026 loss (40-file catch-up, 35 intervals dropped) against
the old semantics and asserts the new helper drains the backlog instead.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))

from aemo_updater.collectors.unified_collector import UnifiedAEMOCollector


class Shim:
    """Minimal stand-in: the helper only touches last_files + max_files_per_cycle."""
    def __init__(self, limit=20):
        self.max_files_per_cycle = limit
        self.last_files = {'prices5': set()}

    _select_new_files = UnifiedAEMOCollector._select_new_files


def names(a, b):
    return [f'PUBLIC_DISPATCHIS_{i:012d}.zip' for i in range(a, b)]


failures = []


def check(label, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {label}")
    if not ok:
        print(f"        got  {got}\n        want {want}")
        failures.append(label)


# 1. Cold start: listing is history the DB already holds -> take newest 20, mark all seen.
s = Shim()
listing = names(0, 300)
batch = s._select_new_files('prices5', listing)
check('cold start processes newest 20', len(batch), 20)
check('cold start marks the whole listing seen', len(s.last_files['prices5']), 300)
check('cold start took the newest, not the oldest', batch[-1], listing[-1])

# 2. Steady state: one new file per cycle.
listing = names(0, 301)
check('steady state returns the single new file',
      s._select_new_files('prices5', listing), [names(300, 301)[0]])

# 3. The 30-Jun scenario: 40-file backlog after an outage, primed set.
#    Old code returned 5 and marked all 40 -> 35 lost for ever.
s2 = Shim(limit=20)
s2.last_files['prices5'] = set(names(0, 100))
backlog = names(0, 140)
first = s2._select_new_files('prices5', backlog)
check('burst: first cycle takes newest 20', len(first), 20)
check('burst: leaves the remainder unseen', len(s2.last_files['prices5']), 120)
second = s2._select_new_files('prices5', backlog)
check('burst: second cycle drains the other 20', len(second), 20)
third = s2._select_new_files('prices5', backlog)
check('burst: fully drained, nothing left', third, [])
recovered = set(first) | set(second)
check('burst: every backlog file processed exactly once, none dropped',
      recovered, set(names(100, 140)))

# 4. No new files.
check('no new files returns empty', s2._select_new_files('prices5', backlog), [])

# 5. Explicit limit override (trading path uses 20).
s3 = Shim(limit=5)
s3.last_files['prices5'] = {'seed'}
check('limit override respected', len(s3._select_new_files('prices5', names(0, 50), limit=20)), 20)

print()
if failures:
    print(f"{len(failures)} FAILED: {failures}")
    sys.exit(1)
print("all checks passed")
