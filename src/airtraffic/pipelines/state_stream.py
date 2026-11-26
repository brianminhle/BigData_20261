"""Independent checkpointed stages for state-vector Structured Streaming."""

from pyspark.sql import functions as F

from airtraffic.processing.state_envelope import parse_envelope
from airtraffic.storage.stream_contract import verify_contract

OBSERVATION_SCHEMA = '''observation_id string, snapshot_id string, snapshot_time long,
 event_time long, event_time_basis string, collected_at double, schema_version int,
 source_object struct<bucket:string,key:string,sha256:string>, scope_id string,
 icao24 string, callsign string, latitude double, longitude double,
 baro_altitude double, geo_altitude double, velocity double, true_track double,
 vertical_rate double, time_position double, last_contact double,
 position_age_seconds double, snapshot_age_at_collection_seconds double,
 has_position boolean, on_ground boolean, quality_flags array<string>'''
BRONZE_SCHEMA = '''topic string, partition int, offset long, timestamp timestamp,
 payload string, parsed struct<observation:string,error:string>'''


def archive_frame(kafka, box):
    parser = F.udf(lambda payload: parse_envelope(payload, box),
                   'observation string, error string')
    return kafka.select('topic', 'partition', 'offset', 'timestamp',
                        F.col('value').cast('string').alias('payload')).withColumn('parsed', parser('payload'))


def normalized_frame(bronze, watermark):
    observations = (bronze.where('parsed.error IS NULL')
                    .select(F.from_json('parsed.observation', OBSERVATION_SCHEMA).alias('observation'))
                    .select('observation.*')
                    .withColumn('event_timestamp', F.timestamp_seconds('event_time')))
    return observations.withWatermark('event_timestamp', watermark).dropDuplicatesWithinWatermark(['observation_id'])


def aircraft_windows(silver, box, watermark):
    valid = silver.where(
        F.col('has_position') & ~F.array_contains('quality_flags', 'invalid_icao24')
        & F.col('latitude').between(box.lamin, box.lamax)
        & F.col('longitude').between(box.lomin, box.lomax))
    return (valid.withWatermark('event_timestamp', watermark)
            .groupBy(F.window('event_timestamp', '5 minutes'), 'scope_id',
                     F.floor('latitude').alias('cell_lat_min'),
                     F.floor('longitude').alias('cell_lon_min'))
            .agg(F.size(F.collect_set('icao24')).alias('distinct_aircraft'))
            .select('scope_id', F.col('window.start').cast('long').alias('window_start'),
                    F.col('window.end').cast('long').alias('window_end'),
                    'cell_lat_min', 'cell_lon_min', 'distinct_aircraft'))


def start_stage(spark, args):
    verify_contract(spark, args)
    root = args.output.rstrip('/')
    for name in ('bronze', 'silver', 'gold'):
        path = spark._jvm.org.apache.hadoop.fs.Path(f'{root}/{name}')
        path.getFileSystem(spark._jsc.hadoopConfiguration()).mkdirs(path)
    if args.stage == 'archive':
        kafka = (spark.readStream.format('kafka')
                 .option('kafka.bootstrap.servers', args.bootstrap_servers)
                 .option('subscribe', args.topic).option('startingOffsets', 'earliest')
                 .option('failOnDataLoss', 'true').load())
        frame, destination = archive_frame(kafka, args.box), 'bronze'
    elif args.stage == 'dedup':
        bronze = spark.readStream.schema(BRONZE_SCHEMA).parquet(f'{root}/bronze')
        frame, destination = normalized_frame(bronze, args.watermark), 'silver'
    else:
        schema = OBSERVATION_SCHEMA + ', event_timestamp timestamp'
        silver = spark.readStream.schema(schema).parquet(f'{root}/silver')
        frame, destination = aircraft_windows(silver, args.box, args.watermark), 'gold'
    writer = (frame.writeStream.format('parquet').outputMode('append')
              .option('path', f'{root}/{destination}')
              .option('checkpointLocation', f'{root}/checkpoints/{args.stage}'))
    writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime='30 seconds')
    return writer.start()
