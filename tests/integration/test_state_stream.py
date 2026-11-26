"""Run in the stream image on the Compose network; never calls OpenSky."""

from argparse import Namespace
import json
import os
import uuid

from pyspark.sql import SparkSession
from kafka.admin import KafkaAdminClient, NewTopic

from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.ingestion.kafka import create_producer
from airtraffic.ingestion.state_kafka import publish_states
from airtraffic.storage.state_connection import connect_store
from airtraffic.pipelines.state_stream import start_stage, aircraft_windows
from airtraffic.pipelines.state_batch import process_snapshots
from tempfile import TemporaryDirectory
from pathlib import Path


def main():
    store = connect_store(insecure=True)
    topic = 'states.verify.' + uuid.uuid4().hex
    admin = KafkaAdminClient(bootstrap_servers='kafka:9092')
    admin.create_topics([NewTopic(topic, 1, 1)])
    args = Namespace(stage='archive', output=f's3a://{store.bucket}/verification/{topic}',
                     bootstrap_servers='kafka:9092', topic=topic, box=BoundingBox(),
                     watermark='10 minutes', available_now=True)
    spark = (SparkSession.builder.master('local[2]').appName('state-stream-verification')
             .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.shuffle.partitions', '2')
             .config('spark.hadoop.fs.s3a.endpoint', 'minio:9000')
             .config('spark.hadoop.fs.s3a.access.key', os.environ['AIRTRAFFIC_MINIO_ACCESS_KEY'])
             .config('spark.hadoop.fs.s3a.secret.key', os.environ['AIRTRAFFIC_MINIO_SECRET_KEY'])
             .config('spark.hadoop.fs.s3a.path.style.access', 'true')
             .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    producer = create_producer('kafka:9092')
    try:
        events = []
        for item in store.client.list_objects(store.bucket, prefix='snapshots/opensky/', recursive=True):
            document = store.load_verified(item.object_name)
            for event in state_events(document['response'], BoundingBox(**document['bounding_box']), document['collected_at']):
                event['source_object'] = dict(bucket=store.bucket, key=item.object_name, sha256=document['source_sha256'])
                events.append(event)
        assert events, 'No archived real snapshots available'
        publish_states(events + events, producer, topic)
        producer.send(topic, value=b'{broken json').get(timeout=30)
        def run(stage):
            args.stage = stage
            query = start_stage(spark, args)
            if not query.awaitTermination(120):
                query.stop()
                raise AssertionError(f'{stage} timeout')
        run('archive')
        run('dedup')
        bronze = spark.read.parquet(args.output + '/bronze')
        assert bronze.count() == len(events) * 2 + 1
        assert bronze.where('parsed.error IS NOT NULL').count() == 1
        silver = spark.read.parquet(args.output + '/silver')
        expected_ids = {e['observation_id'] for e in events}
        assert {r.observation_id for r in silver.collect()} == expected_ids
        assert silver.count() == len(expected_ids)
        with TemporaryDirectory() as directory:
            output = Path(directory) / 'batch'
            process_snapshots(store, output)
            expected = [json.loads(line) for line in (output / 'aircraft_windows.jsonl').read_text().splitlines()]
            actual = [row.asDict() for row in aircraft_windows(silver, args.box, args.watermark).collect()]
            sort_key = lambda row: (row['scope_id'], row['window_start'], row['cell_lat_min'], row['cell_lon_min'])
            assert sorted(actual, key=sort_key) == sorted(expected, key=sort_key), (actual, expected)
        # Restart with duplicate deliveries: state and Kafka offsets must survive.
        publish_states(events, producer, topic)
        run('archive')
        run('dedup')
        assert spark.read.parquet(args.output + '/silver').count() == len(expected_ids)
        run('windows')
        # Advance event time in controlled fixtures to finalize real windows.
        future = max(e['event_time'] for e in events) + 1800
        synthetic, = state_events({'time': future, 'states': [events[0]['source_row']]}, args.box, future + 1)
        synthetic['source_object'] = events[0]['source_object']
        publish_states([synthetic], producer, topic)
        run('archive')
        run('dedup')
        run('windows')
        gold = [r.asDict() for r in spark.read.parquet(args.output + '/gold').collect()]
        assert sorted(gold, key=sort_key) == sorted(expected, key=sort_key), gold
        # Out-of-order but inside the watermark is still admitted.
        delayed, = state_events({'time': future - 60, 'states': [events[0]['source_row']]}, args.box, future + 1)
        delayed['source_object'] = events[0]['source_object']
        publish_states([delayed], producer, topic)
        run('archive')
        run('dedup')
        assert spark.read.parquet(args.output + '/silver').where(
            f"observation_id = '{delayed['observation_id']}'").count() == 1
        # An unseen observation older than the watermark remains in Bronze only.
        old = dict(events[0])
        row = list(old['source_row']); row[1] = 'LATE'
        late, = state_events({'time': old['snapshot_time'], 'states': [row]}, args.box, future + 2)
        late['source_object'] = old['source_object']
        publish_states([late], producer, topic)
        run('archive')
        run('dedup')
        assert spark.read.parquet(args.output + '/silver').where(f"observation_id = '{late['observation_id']}'").count() == 0
        print(json.dumps({'result': 'PASS', 'real_observations': len(expected_ids),
                          'final_windows': len(gold), 'duplicate_restart': True,
                          'invalid_quarantined': True, 'out_of_order_accepted': True,
                          'late_dropped_from_silver': True,
                          'output': args.output}))
    finally:
        producer.close()
        admin.delete_topics([topic])
        admin.close()
        spark.stop()


if __name__ == '__main__':
    main()
