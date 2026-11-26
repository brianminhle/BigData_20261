"""Verify cross-cell distinct counts against actual Spark aggregation."""
from pyspark.sql import SparkSession
from airtraffic.serving.aggregate import build_view

spark = SparkSession.builder.master('local[1]').appName('dashboard-aggregation-test').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    silver = spark.createDataFrame([
        ('a', 310, 'abc123', True, []),
        ('a', 320, 'abc123', True, ['stale_position']),
        ('a', 340, 'bad', False, ['invalid_icao24']),
        ('b', 320, 'def456', False, [])
    ], 'scope_id string, snapshot_time long, icao24 string, has_position boolean, quality_flags array<string>')
    gold = spark.createDataFrame([
        ('a', 300, 600, 20, 105, 1), ('a', 300, 600, 21, 105, 1)
    ], 'scope_id string, window_start long, window_end long, cell_lat_min long, cell_lon_min long, distinct_aircraft int')
    result = build_view(silver, gold)
    row = next(row for row in result['windows'] if row['scope_id'] == 'a')
    assert row['distinct_aircraft'] == 1, row
    assert row['observations'] == 3 and row['with_position'] == 2 and row['stale_positions'] == 1, row
    assert len(result['cells']) == 2
    assert build_view(silver.where('false'), gold)['latest_snapshot'] is None
    print('DASHBOARD_AGGREGATION_PASS')
finally:
    spark.stop()
