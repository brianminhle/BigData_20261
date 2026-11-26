"""Validate Kafka envelopes against their original state vector row."""

from dataclasses import asdict
import hashlib
import json
import math

from airtraffic.ingestion.opensky_api import state_events
from airtraffic.processing.state_vectors import normalize_observation


def parse_envelope(payload, box):
    try:
        event = json.loads(payload)
        timestamp = event['snapshot_time']
        collected = event['collected_at']
        if (type(timestamp) is not int or timestamp < 0
                or type(collected) not in (int, float) or not math.isfinite(collected)):
            raise ValueError('invalid timestamps')
        expected, = state_events({'time': timestamp, 'states': [event['source_row']]}, box, collected)
        if any(event.get(key) != value for key, value in expected.items()):
            raise ValueError('envelope mismatch or unexpected bounding box')
        source = event['source_object']
        if not all(isinstance(source.get(key), str) and source[key] for key in ('bucket', 'key', 'sha256')):
            raise ValueError('missing source provenance')
        observation = normalize_observation(event)
        observation['scope_id'] = hashlib.sha256(
            json.dumps(asdict(box), sort_keys=True).encode()).hexdigest()[:16]
        return json.dumps(observation, allow_nan=False), None
    except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
        return None, f'{type(error).__name__}: {error}'
