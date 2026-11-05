"""Publish raw receptions with broker acknowledgment before counting success."""

from collections import deque
from dataclasses import asdict
import json


def create_producer(bootstrap_servers: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers, client_id="airtraffic-raw-ingestion",
        acks="all", enable_idempotence=True, max_in_flight_requests_per_connection=1,
        compression_type="gzip", linger_ms=10, max_block_ms=10000,
        request_timeout_ms=10000, delivery_timeout_ms=30000,
        allow_auto_create_topics=False,
    )


def publish_records(records, producer, topic: str, max_pending: int = 100) -> dict:
    if max_pending < 1:
        raise ValueError("max_pending must be positive")
    pending = deque()
    acknowledged = 0
    for record in records:
        value = json.dumps(asdict(record), separators=(",", ":"), allow_nan=False).encode()
        key = f"{record.run_id}:{record.reception_id}".encode()
        pending.append(producer.send(topic, key=key, value=value))
        if len(pending) >= max_pending:
            pending.popleft().get(timeout=35)
            acknowledged += 1
    while pending:
        pending.popleft().get(timeout=35)
        acknowledged += 1
    return {"raw_reception": acknowledged}
