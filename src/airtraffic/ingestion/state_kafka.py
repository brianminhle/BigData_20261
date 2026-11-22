"""Publish OpenSky state vectors to their own Kafka topic."""

from collections import deque
import json


def publish_states(events, producer, topic: str, max_pending: int = 100) -> int:
    if max_pending < 1:
        raise ValueError("max_pending must be positive")
    pending = deque()
    acknowledged = 0
    for event in events:
        key = event["observation_id"].encode()
        payload = json.dumps(event, separators=(",", ":"), allow_nan=False).encode()
        pending.append(producer.send(topic, key=key, value=payload))
        if len(pending) >= max_pending:
            pending.popleft().get(timeout=35)
            acknowledged += 1
    while pending:
        pending.popleft().get(timeout=35)
        acknowledged += 1
    return acknowledged
