"""Read file-sink outputs and only batch runs with a root completion marker."""

import re

from pyspark.sql import functions as F
from airtraffic.pipelines.state_stream import OBSERVATION_SCHEMA

from airtraffic.serving.aggregate import build_view, bounded_rows


def latest_completed_run(spark, prefix):
    path = spark._jvm.org.apache.hadoop.fs.Path(prefix)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(path):
        return None
    candidates = []
    for item in filesystem.listStatus(path):
        if not item.isDirectory():
            continue
        marker = spark._jvm.org.apache.hadoop.fs.Path(item.getPath(), '_SUCCESS')
        if filesystem.exists(marker):
            candidates.append((filesystem.getFileStatus(marker).getModificationTime(), item.getPath().toString()))
    return max(candidates)[1] if candidates else None


GOLD_SCHEMA = ('scope_id string, window_start long, window_end long, '
               'cell_lat_min long, cell_lon_min long, distinct_aircraft int')


def committed_table(spark, path, schema):
    metadata = spark._jvm.org.apache.hadoop.fs.Path(path.rstrip('/') + '/_spark_metadata')
    filesystem = metadata.getFileSystem(spark._jsc.hadoopConfiguration())
    if not filesystem.exists(metadata) or not any(
            re.fullmatch(r'\d+(\.compact)?', item.getPath().getName())
            for item in filesystem.listStatus(metadata)):
        raise OSError('Streaming output has not committed a batch yet')
    return spark.read.schema(schema).parquet(path)


def serving_view(spark, root, analytics_prefix, hours):
    view = build_view(committed_table(spark, root.rstrip('/') + '/silver', OBSERVATION_SCHEMA + ', event_timestamp timestamp'),
                      committed_table(spark, root.rstrip('/') + '/gold', GOLD_SCHEMA), hours)
    run = latest_completed_run(spark, analytics_prefix)
    view['analytics_run'] = run
    view['movement'] = []
    if run:
        view['movement'] = bounded_rows(spark.read.parquet(run + '/movement')
                                       .where(F.col('snapshot_time') >= (view['latest_snapshot'] or 0) - hours * 3600)
                                       .drop('event_date').orderBy('scope_id', 'icao24', 'time_position'))
    return view
