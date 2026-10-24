"""Checkpoint evidence must only declare success after all stages verify."""

import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from scripts.checkpoint_demo import run_demo
from scripts.demo_runtime import DemoError, wait_for_archive


def completed(value, returncode=0):
    return subprocess.CompletedProcess([], returncode, json.dumps(value), "")


ARCHIVE = {
    "run_id": "test-run", "rows": 10, "unique_kafka_offsets": 10, "unique_observations": 6,
    "status_counts": {"observation": 8, "source_rejected": 2},
}
ANALYTICS = {
    "run_id": "test-run", "archived_rows": 10, "eligible_positions": 3,
    "traffic_group_count": 2, "trajectory_segment_count": 2,
}
BATCH = {
    "run_id": "test-run", "archived_rows": 10, "eligible_positions": 3,
    "output_path": "hdfs://namenode:9000/airtraffic/batch/replay-v1/test-run/v1",
    "datasets": {"traffic": {"row_count": 2}, "trajectory_segments": {"row_count": 2}},
}


class DemoTests(unittest.TestCase):
    def test_success_retains_evidence_and_failed_publication_never_marks_success(self):
        for final_result in (completed(BATCH), DemoError("batch write interrupted")):
            with self.subTest(final_result=type(final_result).__name__), tempfile.TemporaryDirectory() as folder:
                output = Path(folder) / "evidence"
                results = [completed({}), completed({
                    "run_id": "test-run", "acknowledged": {"observation": 8, "rejected": 2},
                }), completed(ANALYTICS), final_result]
                with patch("scripts.checkpoint_demo.checked_compose", side_effect=results), \
                        patch("scripts.checkpoint_demo.wait_for_archive", return_value=ARCHIVE):
                    if isinstance(final_result, DemoError):
                        with self.assertRaises(DemoError):
                            run_demo("test-run", output, build=False, timeout_seconds=20)
                        self.assertFalse((output / "summary.json").exists())
                    else:
                        summary = run_demo("test-run", output, build=False, timeout_seconds=20)
                        self.assertEqual(summary["status"], "passed")
                        self.assertEqual(json.loads((output / "batch.json").read_text()), BATCH)
                self.assertEqual(json.loads((output / "archive.json").read_text()), ARCHIVE)

    def test_bad_counts_stop_before_batch_publication(self):
        with tempfile.TemporaryDirectory() as folder, \
                patch("scripts.checkpoint_demo.checked_compose", side_effect=[
                    completed({}), completed({"run_id": "test-run", "acknowledged": {"observation": 7, "rejected": 2}}),
                ]) as command:
            output = Path(folder) / "evidence"
            with self.assertRaisesRegex(DemoError, "Producer"):
                run_demo("test-run", output, build=False, timeout_seconds=20)
            self.assertEqual(command.call_count, 2)
            self.assertFalse((output / "summary.json").exists())

    def test_existing_evidence_and_unsafe_run_id_fail_before_docker(self):
        with tempfile.TemporaryDirectory() as folder, patch("scripts.checkpoint_demo.checked_compose") as command:
            with self.assertRaises(FileExistsError):
                run_demo("test-run", Path(folder), build=False, timeout_seconds=20)
            with self.assertRaises(ValueError):
                run_demo("../unsafe", Path(folder) / "new", build=False, timeout_seconds=20)
            command.assert_not_called()

    def test_archive_wait_retries_incomplete_reads(self):
        with patch("scripts.demo_runtime.run_compose", side_effect=[
            completed({"rows": 0}, returncode=1), completed(ARCHIVE),
        ]) as command, patch("scripts.demo_runtime.time.sleep"):
            self.assertEqual(wait_for_archive("test-run", Path("unused.log"), 20), ARCHIVE)
            self.assertEqual(command.call_count, 2)
            self.assertTrue(all(call.args[0][2] == "inspect" for call in command.call_args_list))

    def test_archive_wait_refuses_duplicate_run_instead_of_retrying(self):
        with patch("scripts.demo_runtime.run_compose", return_value=completed({"rows": 20}, returncode=1)) as command:
            with self.assertRaisesRegex(DemoError, "new run ID"):
                wait_for_archive("test-run", Path("unused.log"), 20)
            command.assert_called_once()


if __name__ == "__main__":
    unittest.main()
