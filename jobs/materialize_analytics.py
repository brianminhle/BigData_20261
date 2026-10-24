"""Save verified traffic and trajectory Parquet datasets for one replay run."""

import argparse
import json
import math

from pyspark.sql import SparkSession, functions as F

from jobs.batch_analytics import eligible_positions, minute_traffic, trajectory_segments
from jobs.batch_storage import persist_datasets, validate_output_location


def materialize(spark, source_path, run_id, output_path, max_position_age_seconds=30):
    validate_output_location(spark, source_path, output_path)
    archive = spark.read.parquet(source_path)
    run_rows = archive.where(F.col("envelope.run_id") == run_id).cache()
    cached_frames = [run_rows]
    try:
        archived_count = run_rows.count()
        if archived_count == 0:
            raise ValueError(f"No archived records for run {run_id!r}")
        positions = eligible_positions(run_rows, run_id, max_position_age_seconds).cache()
        cached_frames.append(positions)
        traffic = minute_traffic(positions).cache()
        segments = trajectory_segments(positions).cache()
        cached_frames.extend([traffic, segments])
        return persist_datasets(
            spark, output_path, {"traffic": traffic, "trajectory_segments": segments},
            {
                "analytics_version": 1,
                "run_id": run_id,
                "source_path": source_path,
                "archived_rows": archived_count,
                "eligible_positions": positions.count(),
                "max_position_age_seconds": max_position_age_seconds,
            },
        )
    finally:
        for frame in cached_frames:
            frame.unpersist()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default="hdfs://namenode:9000/airtraffic/bronze/replay-v1")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", required=True, help="New output directory for this run/version")
    parser.add_argument("--max-position-age-seconds", type=float, default=30)
    args = parser.parse_args()
    if not args.run_id.strip():
        parser.error("--run-id must not be blank")
    if not args.output.strip() or not args.output.rstrip("/"):
        parser.error("--output must be a nonempty directory path")
    if not math.isfinite(args.max_position_age_seconds) or args.max_position_age_seconds < 0:
        parser.error("--max-position-age-seconds must be finite and nonnegative")
    return args


def main():
    args = parse_args()
    spark = (SparkSession.builder.appName("airtraffic-batch-materialize")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        manifest = materialize(spark, args.path, args.run_id, args.output, args.max_position_age_seconds)
        print(json.dumps(manifest, sort_keys=True, allow_nan=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
