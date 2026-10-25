"""Bounded asynchronous publication; success counts only broker-acknowledged records."""

from collections import Counter, deque
from dataclasses import asdict, dataclass, field
import json


def record_key(record) -> bytes:
    identity = getattr(record, "icao24", None) or record.source_payload_sha256
    return json.dumps([record.mode, record.run_id, identity], separators=(",", ":")).encode()


@dataclass
class DeliveryProgress:
    attempted: int = 0
    acknowledged: Counter = field(default_factory=lambda: Counter({"observation": 0, "rejected": 0}))

    @property
    def uncertain(self):
        return self.attempted - sum(self.acknowledged.values())


def publish_records(records, producer, topic: str, max_pending: int = 100, *, progress=None) -> dict:
    if max_pending < 1:
        raise ValueError("max_pending must be positive")
    pending = deque()
    progress = progress if progress is not None else DeliveryProgress()
    counts = Counter({"observation": 0, "rejected": 0})

    def acknowledge():
        future, kind = pending.popleft()
        future.get(timeout=35)
        counts[kind] += 1
        progress.acknowledged[kind] += 1

    for record in records:
        value = json.dumps(asdict(record), separators=(",", ":"), allow_nan=False).encode()
        progress.attempted += 1
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
        bootstrap_servers=bootstrap_servers, client_id="airtraffic-ingestion",
        acks="all", enable_idempotence=True, max_in_flight_requests_per_connection=1,
        compression_type="gzip", linger_ms=10, max_block_ms=10000,
        request_timeout_ms=10000, delivery_timeout_ms=30000,
        allow_auto_create_topics=False,
    )
