"""A live checkpoint passes only with observed data and matching isolated archive counts."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.demo_runtime import DemoError
from scripts.live_config import LIVE_TOPIC, LiveConfig
from scripts.live_demo import run_live_demo


SESSION = {"run_id": "test", "status": "completed", "uncertain_deliveries": 0,
           "acknowledged": {"observation": 3, "rejected": 1}}
ARCHIVE = {"run_id": "test", "rows": 4, "unique_kafka_offsets": 4,
           "status_counts": {"observation": 3, "source_rejected": 1},
           "mode_counts": {"live": 4}, "topic_counts": {LIVE_TOPIC: 4}}


class LiveDemoTests(unittest.TestCase):
    def exercise(self, session, archive, succeeds):
        results = [subprocess.CompletedProcess([], 0, '{}', ''),
                   subprocess.CompletedProcess([], 0, json.dumps(session), '')]
        with tempfile.TemporaryDirectory() as folder, \
                patch("scripts.live_demo.checked_compose", side_effect=results), \
                patch("scripts.live_demo.wait_for_archive", return_value=archive):
            output = Path(folder) / "demo"
            if succeeds:
                self.assertEqual(run_live_demo(LiveConfig("test"), output)["status"], "passed")
            else:
                with self.assertRaises(DemoError):
                    run_live_demo(LiveConfig("test"), output)
            self.assertEqual((output / "summary.json").exists(), succeeds)

    def test_success_matches_known_acknowledgments_and_isolated_mode(self):
        self.exercise(SESSION, ARCHIVE, True)

    def test_empty_or_uncertain_session_cannot_pass(self):
        for changes in ({"acknowledged": {"observation": 0, "rejected": 0}},
                        {"uncertain_deliveries": 1}, {"status": "failed"}):
            with self.subTest(changes=changes):
                self.exercise({**SESSION, **changes}, ARCHIVE, False)

    def test_wrong_counts_mode_topic_or_duplicate_offsets_cannot_pass(self):
        for changes in ({"rows": 3}, {"mode_counts": {"replay": 4}},
                        {"topic_counts": {"airtraffic.replay.v1": 4}}, {"unique_kafka_offsets": 3}):
            with self.subTest(changes=changes):
                self.exercise(SESSION, {**ARCHIVE, **changes}, False)


if __name__ == "__main__":
    unittest.main()
