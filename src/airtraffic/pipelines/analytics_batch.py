"""Publish a reproducible analytics run; root success marker is written last."""

import json
from pyspark import StorageLevel
from pyspark.sql import functions as F
from airtraffic.analytics.movement import movement_segments
from airtraffic.analytics.density import density_statistics
from airtraffic.analytics.quality import quality_views


def write_text(spark, uri, content):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    stream = filesystem.create(path, False)
    try:
        stream.write(bytearray(content.encode('utf-8')))
    finally:
        stream.close()


def run_analytics(spark, args):
    root, output = args.root.rstrip('/'), args.output.rstrip('/')
    path = spark._jvm.org.apache.hadoop.fs.Path(output)
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(path):
        raise ValueError('Output already exists; choose a new run directory')
    filesystem.mkdirs(path)
    write_text(spark, output + '/run-config.json', json.dumps(vars(args), sort_keys=True))
    silver = (spark.read.parquet(root + '/silver')
              .where((F.col('snapshot_time') >= args.start) & (F.col('snapshot_time') < args.end))
              .persist(StorageLevel.MEMORY_AND_DISK))
    segments = None
    try:
        source_count = silver.count()
        if not source_count:
            raise ValueError('No Silver observations in the selected interval')
        segments = movement_segments(silver, args.gap_seconds, args.max_speed_mps).persist(StorageLevel.MEMORY_AND_DISK)
        gold = (spark.read.parquet(root + '/gold')
                .where((F.col('window_start') >= args.start - args.baseline_seconds)
                       & (F.col('window_start') < args.end)))
        density = density_statistics(gold, args.baseline_seconds).where(F.col('window_start') >= args.start)
        enriched, wide, long = quality_views(silver)
        outputs = {'movement': (segments, 'snapshot_time'),
                   'gaps': (segments.where('observation_gap'), 'snapshot_time'),
                   'density': (density, 'window_start'),
                   'quality': (enriched, 'window_start'),
                   'quality_wide': (wide, 'window_start'),
                   'quality_long': (long, 'window_start')}
        counts = {}
        for name, (frame, timestamp) in outputs.items():
            partitioned = frame.withColumn('event_date', F.to_date(F.timestamp_seconds(timestamp)))
            partitioned.write.mode('errorifexists').partitionBy('event_date').parquet(output + '/' + name)
            counts[name] = frame.count()
            write_text(spark, output + '/' + name + '-plan.txt',
                       frame._jdf.queryExecution().executedPlan().toString())
        report = {'source_observations': source_count, 'counts': counts,
                  'time_basis': 'snapshot interval; movement uses time_position',
                  'limitations': ['positions at run boundaries have no outside-interval context',
                                  'missing density windows are not zero-filled',
                                  'observed segments are not complete flights']}
        write_text(spark, output + '/report.json', json.dumps(report, indent=2))
        write_text(spark, output + '/_SUCCESS', 'complete\n')
        return report
    finally:
        silver.unpersist()
        if segments is not None:
            segments.unpersist()
