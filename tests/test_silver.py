"""Small fixtures for the production decoder and Spark preprocessing policy."""

from dataclasses import asdict
from datetime import datetime
import json
from pathlib import Path
import tempfile
import unittest

from airtraffic.preprocessing.decode import decode, position
from airtraffic.ingestion.source import reception

EVEN = "8D40621D58C382D690C8AC2863A7"
ODD = "8D40621D58C386435CC412692AD6"


class DecoderTests(unittest.TestCase):
    def test_known_coordinates_and_unresolved_reasons(self):
        lat, lon, reason = position(EVEN, ODD, 100, 99)
        self.assertEqual(reason, "decoded")
        self.assertAlmostEqual(lat, 52.257202, places=4)
        self.assertAlmostEqual(lon, 3.919373, places=4)
        self.assertEqual(position(EVEN, ODD, 111, 100)[2], "expired_pair")
        self.assertEqual(position(EVEN, None, 100, None)[2], "no_prior_opposite")
        # Altered CPR latitude deliberately crosses a latitude-zone boundary.
        self.assertEqual(
            position("8D40621D58C3821D54C8AC2863A7", ODD, 100, 99)[2], "zone_mismatch"
        )
        self.assertEqual(position(EVEN, EVEN, 100, 99)[2], "decode_error")

    def test_validation_and_nullable_metadata(self):
        for message, timestamp, metadata, expected in [
            ("bad", 100, "{}", "invalid_frame_format"),
            (EVEN, None, "{}", "invalid_timestamp"),
            (EVEN, float("nan"), "{}", "invalid_timestamp"),
            (EVEN, 100, "null", "invalid_metadata"),
            (EVEN, 100, '{"sensorLatitude":91}', "invalid_receiver_location"),
            (EVEN[:-1] + "0", 100, "{}", "crc_failure"),
            (
                EVEN[:-6] + "000000",
                100,
                '{"sensorType":"SBS-3"}',
                "receiver_zero_parity",
            ),
        ]:
            with self.subTest(expected=expected):
                self.assertEqual(
                    decode(message, timestamp, metadata)["reason"], expected
                )
        self.assertEqual(
            decode(EVEN, 100, '{"sensorLatitude":null}')["status"], "valid"
        )


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def position_frames(self):
        return self.spark.createDataFrame(
            [
                ("s", "40621d", 99.0, "a", ODD, 1, 38000.0, False, "position"),
                ("s", "40621d", 100.0, "b", EVEN, 0, 38000.0, False, "position"),
                ("s", "40621d", 111.0, "c", EVEN, 0, 38000.0, False, "position"),
            ],
            "source_sha256 string, icao24 string, event_time double, transmission_id string, raw_message string, parity int, altitude_ft double, ambiguous boolean, category string",
        )

    def test_delivery_deduplication_and_provenance(self):
        from airtraffic.ingestion.bronze import archive_rows
        from airtraffic.preprocessing.receptions import receptions

        record = dict(
            rawMessage=EVEN,
            timeAtServer=100.0,
            sensorSerialNumber=1,
            sensorLatitude=None,
        )
        rows = []
        for offset in range(2):
            envelope = asdict(reception(record, "source", 0, "fixture", 200.0 + offset))
            rows.append(
                (
                    "topic",
                    0,
                    offset,
                    datetime(2020, 1, 1),
                    b"key",
                    json.dumps(envelope).encode(),
                )
            )
        bronze = archive_rows(
            self.spark.createDataFrame(
                rows,
                "topic string, partition int, offset long, timestamp timestamp, key binary, value binary",
            )
        )
        deliveries, unique = receptions(bronze)
        self.assertEqual(deliveries.count(), 2)
        self.assertEqual(unique.count(), 1)
        restored = unique.first()
        self.assertEqual(restored.raw_message, EVEN)
        self.assertEqual(restored.event_time, 100.0)
        self.assertEqual(json.loads(restored.source_record_json), record)
        self.assertEqual(restored.status, "valid")

    def test_temporal_matching_and_equal_time_ties(self):
        from airtraffic.preprocessing.observations import reconstruct
        from pyspark.sql import functions as F

        frames = self.position_frames()
        positions, quality = reconstruct(frames)
        self.assertEqual(positions.count(), 1)
        self.assertEqual(
            {r.transmission_id: r.outcome for r in quality.collect()},
            {"a": "no_prior_opposite", "b": "decoded", "c": "expired_pair"},
        )
        repeated, _ = reconstruct(frames.repartition(2))
        self.assertEqual(positions.collect(), repeated.collect())
        tied = frames.where("transmission_id != 'c'").withColumn(
            "event_time", F.lit(100.0)
        )
        tied_positions, _ = reconstruct(tied)
        tied_again, _ = reconstruct(
            tied.orderBy(F.desc("transmission_id")).repartition(2)
        )
        self.assertEqual(tied_positions.collect(), tied_again.collect())
        tied_row = tied_positions.first()
        self.assertEqual(tied_row.transmission_id, "b")
        self.assertEqual(tied_row.partner_id, "a")
        self.assertAlmostEqual(tied_row.latitude, 52.265780, places=4)

    def test_conservative_receiver_grouping(self):
        from airtraffic.preprocessing.receptions import transmissions

        raw = self.spark.createDataFrame(
            [
                ("s", "a", EVEN, 1, 100.001),
                ("s", "b", EVEN, 2, 100.002),
                ("s", "c", EVEN, 1, 101.001),
                ("s", "d", EVEN, 1, 101.002),
                ("s", "e", EVEN, 1, 102.009),
                ("s", "f", EVEN, 2, 102.011),
            ],
            "source_sha256 string, reception_id string, raw_message string, receiver_id int, event_time double",
        )
        tx, lineage = transmissions(raw)
        self.assertEqual(tx.count(), 5)
        self.assertEqual(lineage.count(), 6)
        self.assertEqual(lineage.where("ambiguous").count(), 2)

    def test_attribute_freshness_and_missing_values(self):
        from airtraffic.preprocessing.observations import reconstruct, states
        from pyspark.sql import functions as F

        frames = self.position_frames()
        positions, _ = reconstruct(frames)
        attrs = frames.withColumn(
            "callsign", F.when(F.col("transmission_id") == "a", "FIXTURE")
        ).withColumn(
            "speed_knots", F.when(F.col("transmission_id") == "a", F.lit(250.0))
        )
        attrs = attrs.withColumn("track_deg", F.lit(None).cast("double")).withColumn(
            "vertical_rate_fpm", F.lit(None).cast("double")
        )
        state = states(attrs, positions).first()
        self.assertEqual(state.callsign, "FIXTURE")
        self.assertEqual(state.callsign_updated_at, 99.0)
        self.assertIsNone(state.track_deg)
        future = positions.withColumn("event_time", F.lit(500.0))
        stale = states(attrs, future).first()
        self.assertIsNone(stale.callsign)
        self.assertEqual(stale.callsign_status, "stale")
        self.assertEqual(stale.callsign_updated_at, 99.0)

    def test_publication_requires_complete_readback(self):
        from airtraffic.preprocessing.storage import persist_datasets
        from airtraffic.preprocessing.observations import reconstruct

        positions, _ = reconstruct(self.position_frames())
        with tempfile.TemporaryDirectory() as directory:
            target = directory + "/version"
            persist_datasets(self.spark, target, {"positions": positions}, {})
            self.assertTrue(Path(target, "_SUCCESS").exists())
            with self.assertRaises(FileExistsError):
                persist_datasets(self.spark, target, {"positions": positions}, {})
            with self.assertRaises(AttributeError):
                persist_datasets(
                    self.spark, directory + "/failed", {"positions": None}, {}
                )
            self.assertFalse(Path(directory, "failed", "_SUCCESS").exists())


if __name__ == "__main__":
    unittest.main()
