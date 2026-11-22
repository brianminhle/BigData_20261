"""Consume one isolated replay run and publish finalized streaming Silver epochs."""

import argparse
import math
from pyspark.sql import SparkSession, functions as F
from airtraffic.config import KAFKA_BOOTSTRAP_SERVERS, RAW_TOPIC, STREAMING_ROOT
from airtraffic.ingestion.bronze import archive_rows
from airtraffic.pipelines.streaming import StreamingEpochs


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--output-root", default=STREAMING_ROOT)
    parser.add_argument("--watermark-seconds", type=float, default=30)
    parser.add_argument("--expected-deliveries", type=int)
    parser.add_argument("--max-offsets", type=int, default=10000)
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--starting-offsets", default="earliest")
    args = parser.parse_args(argv)
    if not args.version.replace("-", "").replace("_", "").isalnum():
        parser.error(
            "Version must contain only letters, numbers, hyphens or underscores"
        )
    if (
        not math.isfinite(args.watermark_seconds)
        or args.watermark_seconds < 0
        or args.max_offsets < 1
    ):
        parser.error("Invalid watermark or offset limit")
    if args.expected_deliveries is not None and args.expected_deliveries < 1:
        parser.error("Completion count must be positive")
    return args


def main():
    args = parse_args()
    spark = (
        SparkSession.builder.appName("raw-adsb-stateful")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.parquet.block.size", "33554432")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    root = args.output_root.rstrip("/") + "/" + args.version
    incoming = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", args.max_offsets)
        .load()
    )
    rows = archive_rows(incoming).where(F.col("envelope.run_id") == args.run_id)
    sink = StreamingEpochs(
        spark,
        root + "/epochs",
        args.run_id,
        args.watermark_seconds,
        args.expected_deliveries,
    )
    writer = rows.writeStream.option(
        "checkpointLocation", root + "/checkpoint"
    ).foreachBatch(sink)
    writer = (
        writer.trigger(availableNow=True)
        if args.available_now
        else writer.trigger(processingTime="5 seconds")
    )
    try:
        query = writer.start()
        while not query.awaitTermination(2):
            progress = query.lastProgress
            if (
                sink.completed
                and progress
                and progress["batchId"] >= sink.completed_epoch
            ):
                query.stop()
                break
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
