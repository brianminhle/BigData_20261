"""Bounded local batch reference over archived snapshots; no Spark required."""

from collections import Counter, defaultdict
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.processing.state_vectors import normalize_observation


def process_snapshots(store, output_dir, expected_interval=120):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=False)
    counts = Counter()
    flags = Counter()
    seen = set()
    aircraft = set()
    positions = set()
    scopes = defaultdict(set)
    windows = defaultdict(set)
    source_keys = []
    with (output_dir / 'observations.jsonl').open('w') as output:
        for item in store.client.list_objects(store.bucket, prefix='snapshots/opensky/', recursive=True):
            if not item.object_name.endswith('.json'):
                continue
            document = store.load_verified(item.object_name)
            source_keys.append(item.object_name)
            box = BoundingBox(**document['bounding_box'])
            scope = json.dumps(asdict(box), sort_keys=True)
            scope_id = hashlib.sha256(scope.encode()).hexdigest()[:16]
            scopes[scope].add(document['response']['time'])
            counts['snapshots'] += 1
            if not document['response']['states']:
                counts['empty_snapshots'] += 1
            for event in state_events(document['response'], box, document['collected_at']):
                counts['source_rows'] += 1
                if event['observation_id'] in seen:
                    counts['duplicate_rows'] += 1
                    continue
                seen.add(event['observation_id'])
                event['source_object'] = {'bucket': store.bucket, 'key': item.object_name,
                                          'sha256': document['source_sha256']}
                observation = normalize_observation(event)
                counts['observations'] += 1
                flags.update(observation['quality_flags'])
                if 'invalid_icao24' not in observation['quality_flags']:
                    aircraft.add(observation['icao24'])
                    if observation['has_position']:
                        lat, lon = observation['latitude'], observation['longitude']
                        if box.lamin <= lat <= box.lamax and box.lomin <= lon <= box.lomax:
                            cell = (int(lat // 1), int(lon // 1))
                            window = (scope_id, int(observation['snapshot_time'] // 300) * 300, *cell)
                            windows[window].add(observation['icao24'])
                        else:
                            counts['positions_outside_requested_bbox'] += 1
                if observation['has_position']:
                    counts['rows_with_position'] += 1
                    position_key = (scope, observation['icao24'], observation['time_position'],
                                    observation['latitude'], observation['longitude'])
                    if position_key in positions:
                        counts['repeated_position_rows'] += 1
                    positions.add(position_key)
                output.write(json.dumps(observation, allow_nan=False) + '\n')
    gaps = []
    for scope, timestamps in scopes.items():
        ordered = sorted(timestamps)
        for earlier, later in zip(ordered, ordered[1:]):
            if later - earlier > expected_interval * 1.5:
                gaps.append({'bbox': json.loads(scope), 'from': earlier, 'to': later,
                             'seconds': later - earlier})
    with (output_dir / 'aircraft_windows.jsonl').open('w') as output:
        for (scope, start, lat, lon), members in sorted(windows.items()):
            output.write(json.dumps({'scope_id': scope, 'window_start': start, 'window_end': start + 300,
                                     'cell_lat_min': lat, 'cell_lon_min': lon,
                                     'distinct_aircraft': len(members)}) + '\n')
    report = {'schema_version': 1, 'counts': dict(counts), 'distinct_aircraft': len(aircraft),
              'quality_flags': dict(flags), 'snapshot_gaps': gaps, 'sources': source_keys,
              'expected_interval_seconds': expected_interval, 'cell_degrees': 1,
              'window_seconds': 300, 'scope': 'archived observations, not all aircraft in airspace'}
    (output_dir / 'quality.json').write_text(json.dumps(report, indent=2))
    # Consumers only use a completed output directory.
    (output_dir / '_SUCCESS').write_text('complete\n')
    return report
