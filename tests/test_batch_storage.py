"""Verify persisted batch outputs and failure handling with real Spark Parquet."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

HAS_SPARK = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(HAS_SPARK, "Run in the Spark image to execute batch storage checks")
class BatchStorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("batch-storage-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_verified_outputs_include_manifest_and_refuse_overwrite(self):
        from py4j.protocol import Py4JJavaError
        from jobs.batch_storage import _write_text, persist_datasets
        records = self.spark.createDataFrame(
            [("a", 1, 1.25), ("b", 2, None)], "name STRING, count LONG, mean DOUBLE"
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "v1"
            result = persist_datasets(self.spark, str(output), {"traffic": records}, {"run_id": "test"})
            self.assertEqual(result["datasets"]["traffic"]["row_count"], 2)
            self.assertEqual(json.loads((output / "_manifest.json").read_text()), result)
            self.assertTrue((output / "_SUCCESS").is_file())
            self.assertTrue((output / "_RESERVED").is_file())
            with self.assertRaises(Py4JJavaError):
                _write_text(self.spark, str(output / "_RESERVED"), "late writer")
            with self.assertRaises(FileExistsError):
                persist_datasets(self.spark, str(output), {"traffic": records.limit(0)}, {})
            self.assertEqual(self.spark.read.parquet(str(output / "traffic")).count(), 2)

    def test_empty_outputs_keep_schema_and_complete(self):
        from jobs.batch_storage import persist_datasets
        empty = self.spark.createDataFrame([], "run_id STRING, aircraft_count LONG")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "v1"
            result = persist_datasets(self.spark, str(output), {"traffic": empty}, {})
            self.assertEqual(result["datasets"]["traffic"]["row_count"], 0)
            self.assertTrue((output / "_SUCCESS").is_file())

    def test_failed_verification_never_marks_partial_output_complete(self):
        from jobs.batch_storage import persist_datasets
        records = self.spark.createDataFrame([("a",)], "name STRING")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "v1"
            with patch("jobs.batch_storage._verify_dataset", side_effect=[1, ValueError("read-back failed")]):
                with self.assertRaisesRegex(ValueError, "read-back failed"):
                    persist_datasets(self.spark, str(output), {"traffic": records, "segments": records}, {})
            self.assertTrue((output / "traffic").is_dir())
            self.assertFalse((output / "_SUCCESS").exists())
            self.assertFalse((output / "_manifest.json").exists())
            self.assertTrue((output / "_RESERVED").is_file())

    def test_readback_rejects_changed_values_with_same_count_and_schema(self):
        from jobs.batch_storage import _verify_dataset
        expected = self.spark.createDataFrame([("a", 1)], "name STRING, count LONG")
        changed = self.spark.createDataFrame([("a", 2)], "name STRING, count LONG")
        with self.assertRaisesRegex(ValueError, "content differs"):
            _verify_dataset(expected, changed, "/batch/test")

    def test_output_cannot_be_inside_source_archive(self):
        from jobs.batch_storage import validate_output_location
        for output in ("/tmp/bronze", "/tmp/bronze/batch", "/tmp/bronze/../bronze/batch"):
            with self.assertRaisesRegex(ValueError, "outside the source"):
                validate_output_location(self.spark, "/tmp/bronze", output)
        validate_output_location(self.spark, "/tmp/bronze", "/tmp/bronze-other/batch")

    def test_comparison_keeps_nulls_and_duplicate_multiplicity(self):
        from jobs.batch_storage import _verify_dataset
        expected = self.spark.createDataFrame([(None, 1), (None, 1), ("b", 2)], "name STRING, count LONG")
        changed = self.spark.createDataFrame([(None, 1), ("b", 2), ("b", 2)], expected.schema)
        self.assertEqual(_verify_dataset(expected, expected, "/test"), 3)
        with self.assertRaisesRegex(ValueError, "content differs"):
            _verify_dataset(expected, changed, "/test")

    def test_fixture_materialization_and_saved_reader(self):
        from dataclasses import asdict
        from datetime import datetime
        from jobs.archive_schema import archive_rows
        from jobs.inspect_batch import inspect_batch
        from jobs.materialize_analytics import materialize
        from airtraffic.historical import normalize_historical
        from airtraffic.observations import SourceReference
        from airtraffic.sample_io import ROOT
        import csv
        payloads = []
        with (ROOT / "data/fixtures/checkpoint_states.csv").open() as source:
            for index, row in enumerate(csv.DictReader(source)):
                reference = SourceReference("synthetic", "fixture", "fixture://test", "a" * 64, "fixture.csv", index + 1)
                record = normalize_historical(row, reference, run_id="storage-test", received_at=1800000000)
                payloads.append(("test", 0, index, datetime(2026, 10, 25), b"key", json.dumps(asdict(record)).encode()))
        kafka = self.spark.createDataFrame(payloads, "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY")
        with tempfile.TemporaryDirectory() as directory:
            source, output = str(Path(directory) / "bronze"), str(Path(directory) / "v1")
            archive_rows(kafka).write.parquet(source)
            manifest = materialize(self.spark, source, "storage-test", output)
            self.assertEqual((manifest["archived_rows"], manifest["eligible_positions"]), (10, 3))
            saved = inspect_batch(self.spark, output)
            self.assertEqual(saved["datasets"]["traffic"]["row_count"], 2)
            self.assertEqual(saved["datasets"]["trajectory_segments"]["row_count"], 2)
            (Path(output) / "_SUCCESS").unlink()
            with self.assertRaisesRegex(ValueError, "incomplete"):
                inspect_batch(self.spark, output)


if __name__ == "__main__":
    unittest.main()
