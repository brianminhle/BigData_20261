"""Semantic tests executed with actual Spark, not mocked DataFrame calls."""

from argparse import Namespace
from tempfile import TemporaryDirectory
from pathlib import Path
from pyspark.sql import SparkSession
from airtraffic.analytics.movement import movement_segments
from airtraffic.analytics.density import density_statistics
from airtraffic.analytics.quality import quality_views
from airtraffic.pipelines.analytics_batch import run_analytics

spark = SparkSession.builder.master('local[2]').appName('analytics-test').config('spark.sql.shuffle.partitions','2').getOrCreate()
spark.sparkContext.setLogLevel('ERROR')
try:
    schema = ('scope_id string, icao24 string, observation_id string, snapshot_time long, time_position double, '
              'latitude double, longitude double, baro_altitude double, has_position boolean, quality_flags array<string>')
    silver = spark.createDataFrame([
        ('a','abc123','one',120,100.,20.,105.,1000.,True,[]),
        ('a','abc123','repeat',240,100.,20.,105.,1000.,True,['stale_position']),
        ('a','abc123','two',300,280.,20.1,105.,1100.,True,[]),
        ('a','abc123','three',1200,1180.,21.,105.,1200.,True,[]),
        ('b','abc123','other',300,280.,20.,105.,1000.,True,[]),
        ('a','invalid','bad',300,280.,20.,105.,1000.,True,['invalid_icao24']),
    ], schema)
    rows = movement_segments(silver).where("scope_id = 'a'").orderBy('time_position').collect()
    assert len(rows) == 3
    assert rows[0].observation_id == 'repeat' and rows[0].distance_m is None
    assert rows[0].next_position_time == 280
    assert 11000 < rows[1].distance_m < 11200
    assert rows[1].altitude_change_m == 100 and not rows[1].observation_gap
    assert rows[2].observation_gap and rows[2].elapsed_seconds == 900
    enriched, wide, long = quality_views(silver)
    assert sum(row['count'] for row in enriched.collect()) == 6
    assert sum(row.observations for row in long.collect()) == 6
    assert 'BroadcastHashJoin' in enriched._jdf.queryExecution().executedPlan().toString()
    gold = spark.createDataFrame([('a',i*300,20,105,v) for i,v in enumerate([1,2,3,10])],
                                'scope_id string, window_start long, cell_lat_min long, cell_lon_min long, distinct_aircraft long')
    stats = density_statistics(gold).orderBy('window_start').collect()
    assert stats[0].baseline_mean is None
    assert stats[-1].baseline_mean == 2 and stats[-1].density_zscore == 8
    with TemporaryDirectory() as directory:
        root = directory + '/lake'
        silver.write.parquet(root+'/silver'); gold.write.parquet(root+'/gold')
        args = Namespace(root=root,output=directory+'/run',start=0,end=1500,gap_seconds=600,
                         max_speed_mps=400.,baseline_seconds=3600,shuffle_partitions=2,insecure=True)
        report = run_analytics(spark,args)
        assert report['source_observations'] == 6 and report['counts']['gaps'] == 1
        assert Path(args.output+'/_SUCCESS').exists()
        try:
            run_analytics(spark,args)
            raise AssertionError('Existing run was overwritten')
        except ValueError:
            pass
        loaded = spark.read.parquet(args.output+'/movement').where("event_date = '1970-01-01'")
        assert 'PartitionFilters' in loaded._jdf.queryExecution().executedPlan().toString()
    print('ANALYTICS_INTEGRATION_PASS')
finally:
    spark.stop()
