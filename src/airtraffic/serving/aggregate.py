"""Prepare bounded dashboard views using committed Spark file-sink outputs."""

import time
from pyspark.sql import functions as F


def bounded_rows(frame, limit=20000):
    rows = frame.limit(limit + 1).collect()
    if len(rows) > limit:
        raise ValueError('Dashboard export exceeds row limit; reduce --hours')
    return [row.asDict() for row in rows]


def build_view(silver, gold, hours=24):
    latest = silver.agg(F.max('snapshot_time')).first()[0]
    if latest is None:
        return dict(generated_at=int(time.time()), latest_snapshot=None, hours=hours, windows=[], cells=[])
    cutoff = (latest - hours * 3600) // 300 * 300
    observations = silver.where(F.col('snapshot_time') >= cutoff)
    valid = ~F.array_contains('quality_flags', 'invalid_icao24')
    windows = (observations.groupBy('scope_id', (F.floor(F.col('snapshot_time') / 300) * 300).cast('long').alias('window_start'))
               .agg(F.count('*').alias('observations'),
                    F.countDistinct(F.when(valid, F.col('icao24'))).alias('distinct_aircraft'),
                    F.sum(F.col('has_position').cast('long')).alias('with_position'),
                    F.sum(F.array_contains('quality_flags', 'stale_position').cast('long')).alias('stale_positions'),
                    F.max('snapshot_time').alias('last_snapshot'))
               .orderBy('scope_id', 'window_start'))
    return dict(generated_at=int(time.time()), latest_snapshot=latest, hours=hours,
                windows=bounded_rows(windows),
                cells=bounded_rows(gold.where(F.col('window_start') >= cutoff).orderBy('scope_id', 'window_start', 'cell_lat_min', 'cell_lon_min')))
