"""Read committed Parquet and verify a bounded replay run using Spark batch SQL."""

import argparse
import json

from pyspark.sql import SparkSession, functions as F


def summarize(rows):
    summary = rows.agg(
        F.count("*").alias("rows"),
        F.countDistinct(F.struct("topic", "partition", "offset")).alias("unique_kafka_offsets"),
        F.countDistinct("envelope.observation_id").alias("unique_observations"),
        F.min("envelope.observed_at").alias("first_source_time"),
        F.max("envelope.observed_at").alias("last_source_time"),
        F.percentile_approx(F.col("processed_at").cast("double") - F.col("envelope.received_at"), 0.95)
        .alias("receipt_to_batch_start_p95_seconds"),
    ).first().asDict()
    summary["status_counts"] = {row.envelope_status: row['count'] for row in rows.groupBy("envelope_status").count().collect()}
    summary["quality_flags"] = {
        row.flag: row['count'] for row in rows.select(F.explode("envelope.quality_flags").alias("flag"))
        .groupBy("flag").count().collect()
    }
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default="hdfs://namenode:9000/airtraffic/bronze/replay-v1")
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
        if summary["rows"] != summary["unique_kafka_offsets"]:
            raise ValueError("Archive contains repeated Kafka offsets")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
