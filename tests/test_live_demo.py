"""A live demo needs source evidence, matching delivery counts, and saved live results."""

from copy import deepcopy
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from airtraffic.demo_runtime import DemoError
from airtraffic.live_config import LIVE_ARCHIVE, LIVE_TOPIC, LiveConfig
from scripts.live_demo import run_live_demo


SESSION = {"run_id": "test", "status": "completed", "uncertain_deliveries": 0,
           "mode": "live", "topic": LIVE_TOPIC, "source_url": LiveConfig("test").url,
           "acknowledged": {"observation": 3, "rejected": 1},
           "attempts": [{"number": 1, "http_status": 200, "outcome": "ok", "snapshot_time": 1000,
                         "received_at": 1010, "normalized_records": 4,
                         "freshness": {"fresh_positions_at_receipt": 2, "stale_positions_at_receipt": 1}}]}
ARCHIVE = {"run_id": "test", "rows": 4, "unique_kafka_offsets": 4,
           "status_counts": {"observation": 3, "source_rejected": 1},
           "mode_counts": {"live": 4}, "topic_counts": {LIVE_TOPIC: 4}}
OUTPUT_PATH = "hdfs://namenode:9000/airtraffic/batch/live-v1/test/v1"
BATCH = {"run_id": "test", "source_path": LIVE_ARCHIVE, "output_path": OUTPUT_PATH,
         "archived_rows": 4, "eligible_positions": 2, "max_position_age_seconds": 30,
         "datasets": {"traffic": {"row_count": 1}, "trajectory_segments": {"row_count": 1}}}
SAVED = {"status": "verified", "run_id": "test", "output_path": OUTPUT_PATH, "datasets": {
    "traffic": {"row_count": 1, "rows": [{"minute_start": 960, "latitude_cell": 21,
        "longitude_cell": 105, "aircraft_count": 1, "mean_ground_speed_mps": 200.0}]},
    "trajectory_segments": {"row_count": 1, "rows": [{"icao24": "abcdef", "segment_number": 1,
        "position_updates": 2, "observed_distance_km": 1.2, "unmeasured_edges": 0}]},
}}


class LiveDemoTests(unittest.TestCase):
    def test_source_url_is_identical_after_command_line_float_parsing(self):
        self.assertEqual(LiveConfig("test").url,
                         LiveConfig("test", bounds=tuple(float(value) for value in LiveConfig("test").bounds)).url)

    def exercise(self, session, archive, succeeds, *, batch=None, saved=None):
        batch = BATCH if batch is None else batch
        saved = SAVED if saved is None else saved
        results = [value if isinstance(value, Exception) else subprocess.CompletedProcess([], 0, json.dumps(value), '')
                   for value in ({}, session, batch, saved)]
        with tempfile.TemporaryDirectory() as folder, \
                patch("scripts.live_demo.checked_compose", side_effect=results) as command, \
                patch("scripts.live_demo.wait_for_archive", return_value=archive), \
                redirect_stdout(io.StringIO()):
            output = Path(folder) / "demo"
            if succeeds:
                summary = run_live_demo(LiveConfig("test"), output)
                self.assertEqual(summary["status"], "passed")
                self.assertEqual(summary["batch_output"], OUTPUT_PATH)
                self.assertEqual(summary["eligible_positions"], batch["eligible_positions"])
                self.assertEqual(summary["traffic_groups"], batch["datasets"]["traffic"]["row_count"])
                arguments = command.call_args_list[2].args[0]
                self.assertEqual(arguments[arguments.index("--path") + 1], LIVE_ARCHIVE)
                self.assertEqual(arguments[arguments.index("--run-id") + 1], "test")
                self.assertEqual(json.loads((output / "saved-results.json").read_text()), saved)
                report = (output / "report.md").read_text()
                self.assertIn(session["source_url"], report)
                self.assertIn("Spark batch", report)
                if batch["eligible_positions"] == 0:
                    self.assertIn("No traffic rows", report)
                else:
                    self.assertIn("abcdef", report)
            else:
                with self.assertRaises(DemoError):
                    run_live_demo(LiveConfig("test"), output)
            self.assertEqual((output / "summary.json").exists(), succeeds)

    def test_success_uses_live_archive_and_reports_saved_results(self):
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

    def test_missing_http_evidence_or_wrong_source_cannot_pass(self):
        for changes in ({"attempts": []}, {"source_url": "fixture.json"}, {"mode": "replay"},
                        {"attempts": [{"number": 1, "http_status": 429, "outcome": "rate_limit"}]}):
            with self.subTest(changes=changes):
                self.exercise({**SESSION, **changes}, ARCHIVE, False)

    def test_wrong_batch_source_run_or_saved_counts_cannot_pass(self):
        for changes in ({"source_path": "hdfs://namenode:9000/airtraffic/bronze/replay-v1"},
                        {"run_id": "previous-run"}, {"archived_rows": 3}, {"datasets": {}}):
            with self.subTest(changes=changes):
                self.exercise(SESSION, ARCHIVE, False, batch={**BATCH, **changes})
        saved = deepcopy(SAVED)
        saved["datasets"]["traffic"]["row_count"] = 2
        self.exercise(SESSION, ARCHIVE, False, saved=saved)

    def test_batch_or_readback_failure_never_marks_complete(self):
        self.exercise(SESSION, ARCHIVE, False, batch=DemoError("write failed"))
        self.exercise(SESSION, ARCHIVE, False, saved=DemoError("incomplete version"))

    def test_real_ingestion_with_no_eligible_positions_reports_empty_tables(self):
        batch, saved = deepcopy(BATCH), deepcopy(SAVED)
        batch["eligible_positions"] = 0
        for name in batch["datasets"]:
            batch["datasets"][name]["row_count"] = 0
            saved["datasets"][name] = {"row_count": 0, "rows": []}
        self.exercise(SESSION, ARCHIVE, True, batch=batch, saved=saved)


if __name__ == "__main__":
    unittest.main()
