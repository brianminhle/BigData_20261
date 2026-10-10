"""Offline replay ordering, pacing, integrity, and command-line checks."""

from contextlib import redirect_stderr, redirect_stdout
import csv
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.historical import normalize_historical
from scripts.observations import SourceReference
from scripts.replay import ordered_prefix, replay_records
from scripts.replay_sample import main, run_replay
from scripts.sample_io import FIELDS, SampleError, sha256_file, write_json


def source_row(index, timestamp="1656302410", identifier="00abcd"):
    return "sample.csv", index, {
        **dict.fromkeys(FIELDS, ""), "time": timestamp, "icao24": identifier,
    }


class Clock:
    def __init__(self):
        self.elapsed = 0
        self.sleeps = []

    def monotonic(self):
        return self.elapsed

    def wall(self):
        return 1800000000 + self.elapsed

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.elapsed += seconds


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.source = SourceReference("OpenSky", "test", "https://example.org", "a" * 64, "", 0)
        self.clock = Clock()

    def replay(self, rows, **options):
        return replay_records(
            rows, self.source, **{"run_id": "test", "events_per_second": 2, **options},
            wall_clock=self.clock.wall, monotonic=self.clock.monotonic, sleep=self.clock.sleep,
        )

    def test_sorting_is_bounded_deterministic_and_preserves_duplicate_deliveries(self):
        rows = [source_row(1, "1656302420"), source_row(2), source_row(3), source_row(4, "bad")]
        def input_rows():
            yield from rows
            self.fail("Read beyond selected prefix")
        selected = ordered_prefix(input_rows(), 4)
        self.assertEqual([row[1] for row in selected], [2, 3, 1, 4])
        records = list(self.replay(selected))
        self.assertEqual(len(records), 4)
        self.assertEqual(records[0].observation_id, records[1].observation_id)
        self.assertEqual(records[-1].record_type, "rejected")
        self.assertEqual([record.provenance.record_index for record in records], [2, 3, 1, 4])

    def test_receive_clock_is_sampled_at_release_before_normalization(self):
        with patch("scripts.replay.normalize_historical", wraps=normalize_historical) as normalize:
            stream = self.replay([source_row(1), source_row(2), source_row(3)])
            first = next(stream)
            normalize.assert_called_once()
            self.assertEqual(first.received_at, 1800000000)
            remaining = list(stream)
            self.assertEqual(
                [call.kwargs["received_at"] for call in normalize.call_args_list],
                [1800000000, 1800000000.5, 1800000001],
            )
        self.assertEqual([record.received_at for record in remaining], [1800000000.5, 1800000001])
        self.assertEqual(self.clock.sleeps, [0.5, 0.5])
        self.assertEqual(remaining[0].observed_at, 1656302410)

    def test_slow_consumer_does_not_trigger_catchup_burst(self):
        stream = self.replay([source_row(1), source_row(2), source_row(3)])
        next(stream)
        self.clock.elapsed += 10
        second = next(stream)
        third = next(stream)
        self.assertEqual(second.received_at, 1800000010)
        self.assertEqual(third.received_at, 1800000010.5)
        self.assertEqual(self.clock.sleeps, [0.5])

    def test_invalid_settings_and_empty_sources_fail(self):
        for rate in (0, -1, float("inf"), float("nan")):
            with self.subTest(rate=rate), self.assertRaises(ValueError):
                list(self.replay([source_row(1)], events_per_second=rate))
        with self.assertRaisesRegex(ValueError, "positive"):
            ordered_prefix([], 0)
        with self.assertRaisesRegex(ValueError, "No observations"):
            ordered_prefix([], 1)


class ReplayCommandTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.archive = self.root / "sample.csv"
        with self.archive.open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=FIELDS)
            writer.writeheader()
            writer.writerows([source_row(1, "1656302420")[2], source_row(2)[2], source_row(3, "bad")[2]])
        self.manifest = self.root / "manifest.json"
        write_json(self.manifest, {
            "manifest_version": 1, "provider": "The OpenSky Network", "dataset_id": "fixture",
            "local_path": "sample.csv", "bytes": self.archive.stat().st_size,
            "sha256": sha256_file(self.archive), "source_url": "https://example.org/sample.csv",
        })
        self.output = self.root / "events.jsonl"

    def run_replay(self, **options):
        return run_replay(self.manifest, **{
            "max_rows": 3, "events_per_second": 1000000, "run_id": "test", "stale_seconds": 30,
            "output": self.output, **options,
        })

    def test_verified_manifest_to_jsonl_includes_accepted_and_rejected_records(self):
        summary = self.run_replay()
        records = [json.loads(line) for line in self.output.read_text().splitlines()]
        self.assertEqual(summary["counts"], {"observation": 2, "rejected": 1})
        self.assertEqual([record.get("observed_at") for record in records], [1656302410, 1656302420, None])
        self.assertEqual(records[0]["provenance"]["record_index"], 2)
        self.assertEqual(records[0]["provenance"]["archive_sha256"], sha256_file(self.archive))
        self.assertEqual(records[2]["raw_fields"]["time"], "bad")

    def test_stdout_is_only_jsonl_and_summary_is_on_stderr(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = main(["--manifest", str(self.manifest), "--max-rows", "1", "--run-id", "cli-test"])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout.getvalue())["run_id"], "cli-test")
        self.assertEqual(json.loads(stderr.getvalue())["selected_rows"], 1)

    def test_source_corruption_prevents_any_output(self):
        self.archive.write_bytes(self.archive.read_bytes().replace(b"00abcd", b"00abce"))
        with self.assertRaisesRegex(SampleError, "SHA-256"):
            self.run_replay()
        self.assertFalse(self.output.exists())

    def test_existing_output_including_source_and_manifest_is_never_overwritten(self):
        self.output.write_text("keep me")
        for path in (self.output, self.archive, self.manifest):
            original = path.read_bytes()
            with self.subTest(path=path), self.assertRaisesRegex(SampleError, "already exists"):
                self.run_replay(output=path)
            self.assertEqual(path.read_bytes(), original)

    def test_invalid_configuration_fails_before_reading_the_source(self):
        with patch("scripts.replay_sample.load_replay_source") as verify:
            for options in (
                {"max_rows": 0}, {"max_rows": 100001}, {"events_per_second": float("nan")},
                {"stale_seconds": -1}, {"run_id": " "},
            ):
                with self.subTest(options=options), self.assertRaises(SampleError):
                    self.run_replay(**options)
            verify.assert_not_called()


if __name__ == "__main__":
    unittest.main()
