"""Bounded asynchronous publication; success counts only broker-acknowledged records."""

from collections import Counter, deque
from dataclasses import asdict
import json


def record_key(record) -> bytes:
    identity = getattr(record, "icao24", None) or record.source_payload_sha256
    return json.dumps([record.mode, record.run_id, identity], separators=(",", ":")).encode()


def publish_records(records, producer, topic: str, max_pending: int = 100) -> dict:
    if max_pending < 1:
        raise ValueError("max_pending must be positive")
    pending = deque()
    counts = Counter({"observation": 0, "rejected": 0})

    def acknowledge():
        future, kind = pending.popleft()
        future.get(timeout=35)
        counts[kind] += 1

    for record in records:
        value = json.dumps(asdict(record), separators=(",", ":"), allow_nan=False).encode()
        future = producer.send(topic, key=record_key(record), value=value)
        pending.append((future, record.record_type))
        if len(pending) >= max_pending:
            acknowledge()
    while pending:
        acknowledge()
    return dict(counts)


def create_producer(bootstrap_servers: str):
    from kafka import KafkaProducer

    return KafkaProducer(
        bootstrap_servers=bootstrap_servers, client_id="airtraffic-replay",
        acks="all", enable_idempotence=True, max_in_flight_requests_per_connection=1,
        compression_type="gzip", linger_ms=10, max_block_ms=10000,
        request_timeout_ms=10000, delivery_timeout_ms=30000,
        allow_auto_create_topics=False,
    )
