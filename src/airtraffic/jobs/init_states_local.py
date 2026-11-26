"""Create the bucket and Kafka topic for the isolated local Compose stack."""

import os
import time
from airtraffic.storage.state_connection import connect_store


def main():
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    store = connect_store(insecure=True)
    for attempt in range(30):
        try:
            if not store.client.bucket_exists(store.bucket):
                store.client.make_bucket(store.bucket)
            break
        except Exception:
            if attempt == 29:
                raise
            time.sleep(2)
    admin = KafkaAdminClient(bootstrap_servers=os.environ['AIRTRAFFIC_KAFKA_BOOTSTRAP_SERVERS'])
    try:
        try:
            admin.create_topics([NewTopic(os.environ['AIRTRAFFIC_STATES_TOPIC'], 3, 1)])
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()
    print('Local bucket and topic ready')


if __name__ == '__main__':
    main()
