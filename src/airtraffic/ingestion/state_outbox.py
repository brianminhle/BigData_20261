"""Archive before publication and recover unacknowledged snapshots."""

import logging
import time

from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.ingestion.state_kafka import publish_states


LOGGER = logging.getLogger(__name__)


def publish_pending(store, producer, topic):
    total = 0
    for key in store.pending(topic):
        document = store.load_verified(key)
        events = list(state_events(document["response"], BoundingBox(**document["bounding_box"]),
                                   document["collected_at"]))
        for event in events:
            event["source_object"] = {"bucket": store.bucket, "key": key,
                                      "sha256": document["source_sha256"]}
        count = publish_states(events, producer, topic)
        store.published(key, topic, count)
        total += count
        LOGGER.info("Published snapshot=%s acknowledged=%s", key, count)
    return total


def collect_once(client, producer, box, topic, store):
    # Drain first so a Kafka outage cannot consume additional API credits.
    recovered = publish_pending(store, producer, topic)
    snapshot = client.snapshot(box)
    store.save(snapshot, box, time.time())
    return recovered + publish_pending(store, producer, topic)
