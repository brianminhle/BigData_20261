"""Long and wide quality summaries plus broadcast enrichment of status labels."""

from pyspark.sql import functions as F


def quality_views(silver):
    base = silver.withColumn('window_start', (F.floor(F.col('snapshot_time') / 300) * 300).cast('long'))
    status = (F.when(F.array_contains('quality_flags', 'invalid_icao24'), 'invalid_identity')
              .when(~F.col('has_position'), 'missing_position')
              .when(F.array_contains('quality_flags', 'stale_position'), 'stale_position')
              .otherwise('usable_position'))
    counts = (base.withColumn('quality_status', status)
              .groupBy('scope_id', 'window_start', 'quality_status').count())
    labels = silver.sparkSession.createDataFrame([
        ('invalid_identity', 'Invalid aircraft identity', 3),
        ('missing_position', 'Missing or invalid coordinates', 2),
        ('stale_position', 'Position older than 15 seconds', 1),
        ('usable_position', 'Position available and recent', 0),
    ], 'quality_status string, description string, severity int')
    enriched = counts.join(F.broadcast(labels), 'quality_status', 'left')
    statuses = ['invalid_identity', 'missing_position', 'stale_position', 'usable_position']
    wide = (counts.groupBy('scope_id', 'window_start').pivot('quality_status', statuses)
            .sum('count').fillna(0, subset=statuses))
    long = wide.unpivot(['scope_id', 'window_start'], statuses, 'quality_status', 'observations')
    return enriched, wide, long
