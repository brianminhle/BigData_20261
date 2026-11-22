"""Publish a validated immutable Silver version from one Bronze replay run."""

import argparse
import json
from pyspark.sql import SparkSession
from airtraffic.config import RAW_ARCHIVE
from airtraffic.pipelines.batch import run_batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source", default=RAW_ARCHIVE)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("raw-adsb-silver")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.parquet.block.size", "33554432")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        manifest = run_batch(spark, args.source, args.output, args.run_id)
        print(
            json.dumps(
                {
                    name: item["row_count"]
                    for name, item in manifest["datasets"].items()
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
