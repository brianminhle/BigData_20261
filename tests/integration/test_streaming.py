"""Service integration tests: requires the existing Kafka and HDFS Compose stack."""

from dataclasses import asdict
from datetime import datetime
import json
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4
from pyspark.sql import SparkSession, functions as F
from kafka import KafkaConsumer, TopicPartition
from airtraffic.ingestion.source import reception
from airtraffic.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC, STREAMING_ROOT
from airtraffic.ingestion.kafka import create_producer
from airtraffic.ingestion.bronze import archive_rows
from airtraffic.pipelines.streaming import StreamingEpochs, read_manifest
from airtraffic.storage.silver import _filesystem, _verify_dataset
from airtraffic.pipelines.batch import preprocess

EVEN = "8D40621D58C382D690C8AC2863A7"
ODD = "8D40621D58C386435CC412692AD6"


class StreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.hadoop.parquet.block.size", "33554432")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def envelope(self, index, timestamp, message, receiver=1, run="fixture"):
        return asdict(
            reception(
                dict(
                    rawMessage=message,
                    timeAtServer=timestamp,
                    sensorSerialNumber=receiver,
                    sensorLatitude=None,
                ),
                "fixture-source",
                index,
                run,
                datetime.now().timestamp(),
            )
        )

    def static_rows(self, envelopes):
        rows = [
            (RAW_TOPIC, 0, i, datetime.now(), b"key", json.dumps(e).encode())
            for i, e in enumerate(envelopes)
        ]
        return archive_rows(
            self.spark.createDataFrame(
                rows,
                "topic string, partition int, offset long, timestamp timestamp, key binary, value binary",
            )
        )

    def test_boundary_summaries_precede_pending_updates(self):
        from airtraffic.pipelines.streaming import retained_receptions

        raw = self.static_rows(
            [
                self.envelope(0, 80.0, ODD),
                self.envelope(1, 100.0, EVEN),
                self.envelope(2, 120.0, ODD),
            ]
        )
        datasets = preprocess(raw)
        retained = retained_receptions(datasets, 100.0)
        self.assertEqual(retained.count(), 3)

    def test_retained_ambiguous_group_keeps_identity(self):
        from airtraffic.pipelines.streaming import retained_receptions

        raw = self.static_rows(
            [
                self.envelope(0, 80.001, ODD),
                self.envelope(1, 80.002, ODD),
                self.envelope(2, 120.0, EVEN),
            ]
        )
        original = preprocess(raw)
        ids = retained_receptions(original, 100.0)
        retained = raw.join(
            ids, raw.envelope.reception_id == ids.reception_id, "left_semi"
        )
        recovered = preprocess(retained)
        _verify_dataset(
            original["transmissions"], recovered["transmissions"], "retained candidates"
        )

    def test_partial_write_and_committed_retry(self):
        import airtraffic.pipelines.streaming as module

        envelopes = [self.envelope(0, 99.0, ODD), self.envelope(1, 100.0, EVEN)]
        rows = self.static_rows(envelopes)
        with tempfile.TemporaryDirectory() as root:
            sink = StreamingEpochs(self.spark, root + "/epochs", "fixture", expected=2)

            def interrupted(spark, path, datasets, metadata):
                datasets["positions"].write.parquet(path + "/positions")
                raise RuntimeError("injected interruption after one table")

            with patch.object(module, "persist_datasets", interrupted):
                with self.assertRaises(RuntimeError):
                    sink(rows, 0)
            sink = StreamingEpochs(self.spark, root + "/epochs", "fixture", expected=2)
            sink(rows, 0)
            sink(rows, 0)
            self.assertEqual(
                self.spark.read.parquet(
                    root + "/epochs/00000000000000000000/positions"
                ).count(),
                1,
            )

    def test_kafka_restart_reordering_lateness_and_isolation(self):
        run = "s7-test-" + uuid4().hex
        root = STREAMING_ROOT.rstrip("/") + "/" + run
        consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        partitions = [
            TopicPartition(RAW_TOPIC, p)
            for p in consumer.partitions_for_topic(RAW_TOPIC)
        ]
        start = {
            RAW_TOPIC: {
                str(p.partition): offset
                for p, offset in consumer.end_offsets(partitions).items()
            }
        }
        consumer.close()
        producer = create_producer(KAFKA_BOOTSTRAP_SERVERS)
        rows = (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", RAW_TOPIC)
            .option("startingOffsets", json.dumps(start))
            .load()
        )
        incoming = archive_rows(rows).where(F.col("envelope.run_id") == run)

        def launch():
            return (
                incoming.writeStream.option("checkpointLocation", root + "/checkpoint")
                .foreachBatch(
                    StreamingEpochs(self.spark, root + "/epochs", run, expected=8)
                )
                .start()
            )

        accepted = []

        def send(envelope, include=True):
            producer.send(
                RAW_TOPIC, key=run.encode(), value=json.dumps(envelope).encode()
            ).get(timeout=35)
            if include:
                accepted.append(envelope)

        query = launch()
        try:
            send(self.envelope(0, 99.0, ODD, run=run))
            send(self.envelope(99, 999999.0, EVEN, run=run + "-unrelated"), False)
            query.processAllAvailable()
            first = self.spark.read.parquet(root + "/epochs/*/positions")
            self.assertEqual(first.count(), 0)
            fs, directory = _filesystem(self.spark, root + "/epochs")
            paths = sorted(e.getPath().toString() for e in fs.listStatus(directory))
            self.assertEqual(read_manifest(self.spark, paths[-1])["max_event"], 99.0)
            self.assertEqual(read_manifest(self.spark, paths[-1])["state_rows"], 1)
            query.stop()
            query = launch()
            send(self.envelope(1, 100.0, EVEN, run=run))
            query.processAllAvailable()
            send(self.envelope(0, 99.0, ODD, run=run))
            send(self.envelope(2, 100.001, EVEN, receiver=2, run=run))
            send(self.envelope(3, 99.5, ODD, run=run))
            query.processAllAvailable()
            self.assertEqual(
                self.spark.read.parquet(root + "/epochs/*/positions").count(), 0
            )
            send(self.envelope(4, 140.0, EVEN, run=run))
            query.processAllAvailable()
            self.assertEqual(
                self.spark.read.parquet(root + "/epochs/*/positions").count(), 1
            )
            send(self.envelope(5, 100.5, ODD, run=run), False)
            query.processAllAvailable()
            self.assertEqual(
                self.spark.read.parquet(root + "/epochs/*/late_deliveries").count(), 1
            )
            send(self.envelope(6, 151.0, EVEN, run=run))
            query.processAllAvailable()
            reference = preprocess(self.static_rows(accepted))
            for name in ("positions", "states", "reconstruction_quality"):
                actual = self.spark.read.parquet(root + "/epochs/*/" + name)
                _verify_dataset(reference[name], actual, name)
            self.assertEqual(
                self.spark.read.parquet(root + "/epochs/*/positions")
                .first()
                .source_reception_ids.__len__(),
                3,
            )
            paths = sorted(e.getPath().toString() for e in fs.listStatus(directory))
            final = read_manifest(self.spark, paths[-1])
            self.assertTrue(final["complete"])
            self.assertEqual(final["state_rows"], 0)
            print(
                json.dumps(
                    {
                        "integration_root": root,
                        "positions": 1,
                        "late": 1,
                        "restart_pending_state": True,
                        "batch_equivalent": True,
                    }
                ),
                flush=True,
            )
        finally:
            query.stop()
            producer.close()


if __name__ == "__main__":
    unittest.main()
