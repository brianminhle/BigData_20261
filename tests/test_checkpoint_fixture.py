"""Keep the tracked checkpoint fixture usable by the offline data commands."""

import json
from pathlib import Path
import tempfile
import unittest

from scripts.profile_sample import profile_manifest
from scripts.replay_sample import run_replay


MANIFEST = Path(__file__).resolve().parents[1] / "data/fixtures/checkpoint_manifest.json"


class CheckpointFixtureTests(unittest.TestCase):
    def test_replay_exercises_quality_and_rejection_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "replay.jsonl"
            summary = run_replay(
                MANIFEST, max_rows=10, events_per_second=1_000_000,
                run_id="checkpoint-test", stale_seconds=30, output=output,
            )
            records = [json.loads(line) for line in output.read_text().splitlines()]

        self.assertEqual(summary["counts"], {"observation": 8, "rejected": 2})
        self.assertEqual(len(records), 10)
        self.assertEqual([record["observed_at"] for record in records[:4]], [1656302410] * 4)

        repeated = [
            record for record in records
            if record.get("observation_id") == "opensky:state:abcdef:1656302410"
        ]
        self.assertEqual(len(repeated), 3)
        self.assertEqual(len({record["source_payload_sha256"] for record in repeated}), 2)
        self.assertTrue(any("missing_position" in record.get("quality_flags", []) for record in records))
        self.assertTrue(any("stale_position" in record.get("quality_flags", []) for record in records))
        rejections = {
            tuple(record["reasons"])
            for record in records if record["record_type"] == "rejected"
        }
        self.assertEqual(rejections, {("invalid:heading",), ("invalid:time",)})

    def test_profile_covers_every_fixture_row(self):
        report = profile_manifest(MANIFEST, max_rows=10, stale_seconds=30)
        self.assertEqual(report["scope"]["total_source_rows"], 10)
        self.assertFalse(report["scope"]["truncated"])
        self.assertEqual(report["quality_counts"]["rows_with_missing_position"], 2)
        self.assertEqual(report["quality_counts"]["duplicate_keys"], 2)
        self.assertEqual(report["quality_counts"]["duplicates_conflicting_with_first_payload"], 1)
        self.assertEqual(report["freshness"]["fields"]["lastposupdate"]["older_than_threshold"], 1)


if __name__ == "__main__":
    unittest.main()
