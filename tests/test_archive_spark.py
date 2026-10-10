"""Actual Spark transformations; run in the provided Spark container."""

from datetime import datetime
import importlib.util
import json
import unittest

HAS_SPARK = importlib.util.find_spec("pyspark") is not None


@unittest.skipUnless(HAS_SPARK, "Run in the Spark image to execute these integration checks")
class ArchiveSparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("archive-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def archive(self, payloads):
        from jobs.archive_schema import archive_rows
        rows = [("test", 0, index, datetime(2026, 10, 10), b"key", value) for index, value in enumerate(payloads)]
        frame = self.spark.createDataFrame(rows, "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY")
        return archive_rows(frame).orderBy("offset").collect()

    def test_bad_json_rejections_and_unknown_versions_are_all_archived(self):
        payloads = [b'{"schema_version":1,"record_type":"rejected","raw_fields":{"time":"bad"}}',
                    b'{invalid', b'{"schema_version":99}', b'null']
        records = self.archive(payloads)
        self.assertEqual([row.envelope_status for row in records],
                         ["source_rejected", "malformed_json", "unsupported_schema", "malformed_json"])
        self.assertEqual([bytes(row.payload) for row in records], payloads)
        self.assertEqual([row.offset for row in records], [0, 1, 2, 3])

    def test_duplicates_nulls_and_source_times_survive_the_raw_archive(self):
        payload = json.dumps({
            "schema_version": 1, "record_type": "observation", "observed_at": 1656302410,
            "position_updated_at": 1656302379.5, "latitude": None, "ground_speed_mps": 0,
            "on_ground": False, "squawk": "0042", "quality_flags": ["missing_position"],
        }).encode()
        rows = self.archive([payload, payload])
        self.assertEqual(len(rows), 2)
        first = rows[0].envelope
        self.assertEqual(first.observed_at, 1656302410)
        self.assertEqual(first.position_updated_at, 1656302379.5)
        self.assertIsNone(first.latitude)
        self.assertEqual(first.ground_speed_mps, 0)
        self.assertIs(first.on_ground, False)
        self.assertEqual(first.squawk, "0042")
        self.assertEqual(rows[0].envelope_status, "observation")


if __name__ == "__main__":
    unittest.main()
