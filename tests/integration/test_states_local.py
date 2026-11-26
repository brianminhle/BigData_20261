"""Read-only verification of archived real snapshots against local Kafka delivery."""

import json
import os
import time
from kafka import KafkaConsumer, TopicPartition

from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.storage.state_connection import connect_store


def main():
    print('Checking MinIO snapshots', flush=True)
    store = connect_store(insecure=True)
    topic = os.environ['AIRTRAFFIC_STATES_TOPIC']
    expected = {}
    snapshot_count = 0
    for item in store.client.list_objects(store.bucket, prefix='snapshots/opensky/', recursive=True):
        document = store.load_verified(item.object_name)
        snapshot_count += 1
        receipt = store.read(store.receipt_key(item.object_name, topic))
        events = list(state_events(document['response'], BoundingBox(**document['bounding_box']), document['collected_at']))
        assert receipt['acknowledged'] == len(events)
        for event in events:
            expected[event['observation_id']] = (event, item.object_name)
    assert snapshot_count > 0, 'Collect at least one snapshot first'
    print(f'Checking Kafka for {len(expected)} observations', flush=True)
    consumer = KafkaConsumer(bootstrap_servers=os.environ['AIRTRAFFIC_KAFKA_BOOTSTRAP_SERVERS'],
                             enable_auto_commit=False, group_id=None, request_timeout_ms=10000,
                             api_version_auto_timeout_ms=10000)
    deliveries = 0
    seen = set()
    try:
        partitions = consumer.partitions_for_topic(topic)
        assert partitions is not None, 'Topic missing'
        assigned = [TopicPartition(topic, partition) for partition in partitions]
        consumer.assign(assigned)
        # Resolve offsets before position(); avoid deferred reset in kafka-python.
        for partition, offset in consumer.beginning_offsets(assigned).items():
            consumer.seek(partition, offset)
        end_offsets = consumer.end_offsets(assigned)
        deadline = time.monotonic() + 30
        while any(consumer.position(partition) < end_offsets[partition] for partition in assigned):
            assert time.monotonic() < deadline, 'Timed out reading Kafka'
            for records in consumer.poll(timeout_ms=1000).values():
                for record in records:
                    if record.offset >= end_offsets[TopicPartition(record.topic, record.partition)]:
                        continue
                    event = json.loads(record.value)
                    original, key = expected[event['observation_id']]
                    assert record.key.decode() == event['observation_id']
                    assert event['source_row'] == original['source_row']
                    assert event['source_object']['key'] == key
                    assert event['event_time'] == original['snapshot_time']
                    assert event['collected_at'] == original['collected_at']
                    seen.add(event['observation_id'])
                    deliveries += 1
        assert seen == set(expected), 'Missing or unexpected observations'
    finally:
        consumer.close()
    print(json.dumps({'status': 'passed', 'minio_snapshots': snapshot_count,
                      'unique_observations': len(expected), 'kafka_deliveries': deliveries,
                      'duplicate_deliveries': deliveries - len(seen),
                      'provenance_and_source_values': 'verified'}, indent=2))


if __name__ == '__main__':
    import faulthandler
    faulthandler.dump_traceback_later(40, exit=True)
    main()
    faulthandler.cancel_dump_traceback_later()
