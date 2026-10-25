"""Write immutable batch datasets and mark completion after reading them back."""

import json
from datetime import datetime, timezone
import logging

LOGGER = logging.getLogger(__name__)


def _filesystem(spark, path):
    location = spark._jvm.org.apache.hadoop.fs.Path(path)
    filesystem = location.getFileSystem(spark._jsc.hadoopConfiguration())
    return filesystem, location


def _write_text(spark, path, content):
    filesystem, location = _filesystem(spark, path)
    stream = filesystem.create(location, False)
    try:
        stream.write(bytearray(content.encode("utf-8")))
    finally:
        stream.close()


def validate_output_location(spark, source_path, output_path):
    source_fs, source = _filesystem(spark, source_path)
    output_fs, output = _filesystem(spark, output_path)
    source_uri = source_fs.makeQualified(source).toUri().normalize()
    output_uri = output_fs.makeQualified(output).toUri().normalize()
    if (source_uri.getScheme(), source_uri.getAuthority()) != (
        output_uri.getScheme(), output_uri.getAuthority()
    ):
        return
    source_directory = source_uri.getPath().rstrip("/")
    output_directory = output_uri.getPath().rstrip("/")
    if output_directory == source_directory or output_directory.startswith(source_directory + "/"):
        raise ValueError("Batch output must be outside the source archive directory")


def _reserve_output(spark, output_path):
    filesystem, location = _filesystem(spark, output_path)
    if filesystem.exists(location):
        raise FileExistsError(f"Output already exists; choose a new version: {output_path}")
    if not filesystem.mkdirs(location):
        raise OSError(f"Could not create output directory: {output_path}")
    # Keep the exclusive claim so a delayed concurrent writer cannot acquire it.
    _write_text(spark, f"{output_path}/_RESERVED", "")


def _verify_dataset(expected, restored, path):
    expected_count = expected.count()
    if expected.dtypes != restored.dtypes or restored.count() != expected_count:
        raise ValueError(f"Read-back schema or row count differs: {path}")
    from pyspark.sql import functions as F

    def frequencies(frame):
        return frame.groupBy(F.struct(*[frame[name] for name in frame.columns]).alias("row")) \
            .agg(F.count("*").alias("copies"))

    # A null-safe frequency join preserves duplicates without Spark 3.5's stalled EXCEPT ALL union.
    before, after = frequencies(expected).alias("before"), frequencies(restored).alias("after")
    differences = before.join(after, before.row.eqNullSafe(after.row), "full_outer") \
        .where(~before.copies.eqNullSafe(after.copies))
    if differences.take(1):
        raise ValueError(f"Read-back content differs: {path}")
    return expected_count


def persist_datasets(spark, output_path, datasets, metadata):
    """Leave failed writes in place without a root completion marker."""
    output_path = output_path.rstrip("/")
    _reserve_output(spark, output_path)
    dataset_results = {}
    for name, frame in datasets.items():
        path = f"{output_path}/{name}"
        LOGGER.warning("Writing batch table: %s", path)
        frame.write.mode("errorifexists").parquet(path)
        restored = spark.read.parquet(path)
        LOGGER.warning("Verifying batch table: %s", path)
        count = _verify_dataset(frame, restored, path)
        LOGGER.warning("Verified %s: %s rows", name, count)
        dataset_results[name] = {
            "path": path, "row_count": count, "schema": restored.schema.jsonValue(),
        }

    manifest = {
        **metadata,
        "schema_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "output_path": output_path,
        "datasets": dataset_results,
    }
    _write_text(spark, f"{output_path}/_manifest.json", json.dumps(manifest, sort_keys=True, allow_nan=False) + "\n")
    _write_text(spark, f"{output_path}/_SUCCESS", "")
    return manifest
