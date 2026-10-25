"""Read a completed batch version, validate its manifest, and preview saved rows."""

import argparse
import json

from pyspark.sql import SparkSession

from jobs.batch_storage import _filesystem


def inspect_batch(spark, output_path, limit=20):
    output_path = output_path.rstrip("/")
    filesystem, marker = _filesystem(spark, output_path + "/_SUCCESS")
    if not filesystem.exists(marker):
        raise ValueError("Batch version is incomplete: missing root _SUCCESS")
    _, location = _filesystem(spark, output_path + "/_manifest.json")
    stream = filesystem.open(location)
    try:
        manifest = json.loads(spark._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
    finally:
        stream.close()
    if manifest["output_path"] != output_path:
        raise ValueError("Manifest points to a different output directory")
    result = {"status": "verified", "run_id": manifest["run_id"], "output_path": output_path, "datasets": {}}
    for name, table in manifest["datasets"].items():
        if table["path"] != f"{output_path}/{name}":
            raise ValueError(f"Manifest table path differs: {name}")
        frame = spark.read.parquet(table["path"])
        count = frame.count()
        if count != table["row_count"] or frame.schema.jsonValue() != table["schema"]:
            raise ValueError(f"Saved table differs from its manifest: {name}")
        result["datasets"][name] = {
            "row_count": count, "rows": [row.asDict() for row in frame.orderBy(*frame.columns).limit(limit).collect()],
        }
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()
    if not 1 <= args.limit <= 1000:
        parser.error("--limit must be between 1 and 1000")
    spark = SparkSession.builder.appName("airtraffic-saved-batch-check").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        print(json.dumps(inspect_batch(spark, args.output, args.limit), sort_keys=True, allow_nan=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
