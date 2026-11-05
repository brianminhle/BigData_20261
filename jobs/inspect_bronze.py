"""Read committed Parquet and verify a bounded replay run using Spark batch SQL."""

import argparse
import json

from pyspark.sql import SparkSession, functions as F
from airtraffic.ingestion.source import RAW_ARCHIVE


def summarize(rows):
    return rows.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct("topic", "partition", "offset")).alias("unique_kafka_offsets"),
        F.countDistinct("envelope.reception_id").alias("unique_receptions"),
        F.countDistinct("envelope.delivery_id").alias("unique_deliveries"),
        F.min("envelope.event_time").alias("first_event_time"),
        F.max("envelope.event_time").alias("last_event_time"),
    ).first().asDict()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default=RAW_ARCHIVE)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--expect-rows", type=int)
    args = parser.parse_args()
    spark = (SparkSession.builder.appName("airtraffic-archive-check")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        # Read the root so Spark honors the file sink's committed-file metadata.
        rows = spark.read.parquet(args.path).where(F.col("envelope.run_id") == args.run_id).cache()
        summary = {"run_id": args.run_id, **summarize(rows)}
        print(json.dumps(summary, sort_keys=True))
        if args.expect_rows is not None and summary["rows"] != args.expect_rows:
            raise ValueError(f"Expected {args.expect_rows} rows, found {summary['rows']}")
        if summary["rows"] != summary["unique_deliveries"]:
            raise ValueError("Archive contains repeated delivery IDs")
        if summary["rows"] != summary["unique_kafka_offsets"]:
            raise ValueError("Archive contains repeated Kafka offsets")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
