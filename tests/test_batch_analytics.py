"""Spark batch analytics rules on a small independent position sequence."""

from datetime import datetime, timezone
import importlib.util
import json
import unittest

HAS_SPARK = importlib.util.find_spec("pyspark") is not None
BASE = 1656302400


@unittest.skipUnless(HAS_SPARK, "Run in the Spark image to execute batch analytics checks")
class BatchAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (SparkSession.builder.master("local[1]").appName("batch-analytics-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "1").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def archive(self, records):
        from jobs.archive_schema import archive_rows
        rows = [
            ("test", 0, index, datetime(2026, 10, 23, tzinfo=timezone.utc), b"key", json.dumps(record).encode())
            for index, record in enumerate(records)
        ]
        frame = self.spark.createDataFrame(
            rows, "topic STRING, partition INT, offset LONG, timestamp TIMESTAMP, key BINARY, value BINARY"
        )
        return archive_rows(frame)

    def observation(self, aircraft, seconds, latitude, longitude, *, position_seconds=None,
                    speed=None, fingerprint="a"):
        return {
            "schema_version": 1, "record_type": "observation", "run_id": "checkpoint",
            "observation_id": f"opensky:state:{aircraft}:{seconds}", "icao24": aircraft,
            "observed_at": seconds, "position_updated_at": seconds if position_seconds is None else position_seconds,
            "latitude": latitude, "longitude": longitude, "ground_speed_mps": speed,
            "source_payload_sha256": fingerprint,
        }

    def test_duplicate_delivery_freshness_and_traffic_representatives(self):
        from jobs.batch_analytics import eligible_positions, minute_traffic
        first = self.observation("a00001", BASE + 10, 10.1, 20.1, speed=100)
        records = [
            first, first,
            self.observation("b00002", BASE + 20, 10.2, 20.2),
            self.observation("a00001", BASE + 40, 10.3, 20.3, speed=120),
            self.observation("a00001", BASE + 70, 10.4, 20.4),
            self.observation("a00001", BASE + 90, 10.4, 20.4, position_seconds=BASE + 70),
            self.observation("c00003", BASE + 15, 10.4, 20.4, position_seconds=BASE - 100),
            {"schema_version": 1, "record_type": "rejected", "run_id": "checkpoint"},
        ]
        positions = eligible_positions(self.archive(records), "checkpoint")
        self.assertEqual(positions.count(), 5)
        traffic = {row.minute_start: row for row in minute_traffic(positions).collect()}
        self.assertEqual(set(traffic), {BASE, BASE + 60})
        self.assertEqual(traffic[BASE].aircraft_count, 2)
        self.assertEqual(traffic[BASE].speed_sample_count, 1)
        self.assertEqual(traffic[BASE].mean_ground_speed_mps, 120)
        self.assertEqual(traffic[BASE + 60].aircraft_count, 1)

    def test_trajectory_uses_distinct_updates_and_does_not_bridge_long_gaps(self):
        from jobs.batch_analytics import eligible_positions, trajectory_segments
        records = [
            self.observation("a00001", BASE + 10, 10.0, 20.0),
            self.observation("a00001", BASE + 40, 10.0, 20.1),
            self.observation("a00001", BASE + 70, 10.0, 20.2),
            self.observation("a00001", BASE + 90, 10.0, 20.2, position_seconds=BASE + 70),
            self.observation("a00001", BASE + 800, 10.0, 21.0),
        ]
        positions = eligible_positions(self.archive(records), "checkpoint")
        segments = trajectory_segments(positions).orderBy("segment_number").collect()
        self.assertEqual([row.position_updates for row in segments], [3, 1])
        self.assertEqual([row.segment_number for row in segments], [1, 2])
        self.assertAlmostEqual(segments[0].observed_distance_km, 21.9, delta=0.5)
        self.assertEqual(segments[1].observed_distance_km, 0)
        self.assertEqual([row.unmeasured_edges for row in segments], [0, 0])

    def test_conflicting_payload_tie_breaks_by_fingerprint(self):
        from jobs.batch_analytics import eligible_positions
        later_delivery = self.observation("a00001", BASE + 10, 11.0, 20.0, fingerprint="a")
        earlier_delivery = self.observation("a00001", BASE + 10, 10.0, 20.0, fingerprint="z")
        row = eligible_positions(self.archive([earlier_delivery, later_delivery]), "checkpoint").first()
        self.assertEqual(row.latitude, 11.0)

        missing_winner = self.observation("a00001", BASE + 10, None, None, fingerprint="0")
        self.assertEqual(eligible_positions(
            self.archive([earlier_delivery, missing_winner]), "checkpoint"
        ).count(), 0)


if __name__ == "__main__":
    unittest.main()
