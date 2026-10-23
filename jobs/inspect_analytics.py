"""Show bounded batch traffic and trajectory summaries for one replay run."""

import argparse
import json
import math

from pyspark.sql import SparkSession, functions as F

from batch_analytics import eligible_positions, minute_traffic, trajectory_segments


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", default="hdfs://namenode:9000/airtraffic/bronze/replay-v1")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--max-position-age-seconds", type=float, default=30)
    parser.add_argument("--limit", type=int, default=20, help="Maximum rows per result list")
    args = parser.parse_args()
    if not args.run_id.strip():
        parser.error("--run-id must not be blank")
    if not math.isfinite(args.max_position_age_seconds) or args.max_position_age_seconds < 0:
        parser.error("--max-position-age-seconds must be finite and nonnegative")
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")

    spark = (SparkSession.builder.appName("airtraffic-batch-analytics")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    positions = None
    try:
        archive = spark.read.parquet(args.path)
        run_rows = archive.where(F.col("envelope.run_id") == args.run_id)
        if not run_rows.take(1):
            raise ValueError(f"No archived records for run {args.run_id!r}")
        positions = eligible_positions(run_rows, args.run_id, args.max_position_age_seconds).cache()
        traffic = minute_traffic(positions)
        segments = trajectory_segments(positions)
        result = {
            "run_id": args.run_id,
            "archived_rows": run_rows.count(),
            "eligible_positions": positions.count(),
            "max_position_age_seconds": args.max_position_age_seconds,
            "traffic_group_count": traffic.count(),
            "traffic": [row.asDict() for row in traffic.orderBy(
                "minute_start", "latitude_cell", "longitude_cell"
            ).limit(args.limit).collect()],
            "trajectory_segment_count": segments.count(),
            "trajectory_segments": [row.asDict() for row in segments.orderBy(
                "icao24", "segment_number"
            ).limit(args.limit).collect()],
            "result_limit": args.limit,
        }
        print(json.dumps(result, sort_keys=True, allow_nan=False))
    finally:
        if positions is not None:
            positions.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
