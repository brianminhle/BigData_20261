"""Position-to-position metrics; these are observed segments, not inferred flights."""

from pyspark.sql import Window, functions as F


def movement_segments(silver, gap_seconds=600, max_speed_mps=400):
    valid = silver.where(F.col('has_position') & ~F.array_contains('quality_flags', 'invalid_icao24')
                        & F.col('time_position').isNotNull()
                        & (F.col('time_position') <= F.col('snapshot_time')))
    # Repeated/stale positions in successive snapshots are one position measurement.
    identity = Window.partitionBy('scope_id', 'icao24', 'time_position').orderBy(
        F.col('snapshot_time').desc(), F.col('observation_id').desc())
    positions = valid.withColumn('position_rank', F.row_number().over(identity)).where('position_rank = 1')
    track = Window.partitionBy('scope_id', 'icao24').orderBy('time_position')
    result = positions
    for field in ('latitude', 'longitude', 'time_position', 'baro_altitude'):
        result = result.withColumn('previous_' + field, F.lag(field).over(track))
    result = result.withColumn('next_position_time', F.lead('time_position').over(track))
    lat1, lat2 = F.radians('previous_latitude'), F.radians('latitude')
    dlat = lat2 - lat1
    dlon = F.radians(F.col('longitude') - F.col('previous_longitude'))
    haversine = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    result = (result.withColumn('elapsed_seconds', F.col('time_position') - F.col('previous_time_position'))
              .withColumn('distance_m', F.when(F.col('previous_time_position').isNotNull(),
                          6371000 * 2 * F.asin(F.sqrt(F.least(F.lit(1.0), F.greatest(F.lit(0.0), haversine))))))
              .withColumn('observed_speed_mps', F.when(F.col('elapsed_seconds') > 0,
                          F.col('distance_m') / F.col('elapsed_seconds')))
              .withColumn('altitude_change_m', F.col('baro_altitude') - F.col('previous_baro_altitude'))
              .withColumn('observation_gap', F.coalesce(F.col('elapsed_seconds') > gap_seconds, F.lit(False)))
              .withColumn('implausible_speed', F.coalesce(F.col('observed_speed_mps') > max_speed_mps, F.lit(False))))
    return result.select('scope_id', 'icao24', 'observation_id', 'snapshot_time', 'time_position',
                         'latitude', 'longitude', 'previous_time_position', 'next_position_time',
                         'elapsed_seconds', 'distance_m', 'observed_speed_mps', 'altitude_change_m',
                         'observation_gap', 'implausible_speed')
