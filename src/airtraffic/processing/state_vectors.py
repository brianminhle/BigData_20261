"""Normalize observations without inventing missing source measurements."""

import math
import re


NUMERIC_FIELDS = {
    'latitude': (-90, 90), 'longitude': (-180, 180),
    'baro_altitude': (None, None), 'geo_altitude': (None, None),
    'velocity': (0, None), 'true_track': (0, 360),
    'vertical_rate': (None, None), 'time_position': (0, None),
    'last_contact': (0, None),
}


def normalize_observation(event):
    state = event['state']
    flags = []
    result = {key: event[key] for key in (
        'observation_id', 'snapshot_id', 'snapshot_time', 'event_time',
        'event_time_basis', 'collected_at', 'source_object')}
    result['schema_version'] = 1
    result['icao24'] = state['icao24'].lower()
    if not re.fullmatch('[0-9a-f]{6}', result['icao24']):
        flags.append('invalid_icao24')
    callsign = state.get('callsign')
    result['callsign'] = callsign.strip() or None if isinstance(callsign, str) else None
    for name, (lower, upper) in NUMERIC_FIELDS.items():
        value = state.get(name)
        valid = (isinstance(value, (int, float)) and not isinstance(value, bool)
                 and math.isfinite(value) and (lower is None or value >= lower)
                 and (upper is None or value <= upper))
        if name == 'true_track' and value == 360:
            valid = False
        if value is not None and not valid:
            flags.append(f'invalid_{name}')
        result[name] = float(value) if valid else None
    for name in ('latitude', 'longitude', 'velocity', 'baro_altitude', 'time_position', 'last_contact'):
        if result[name] is None:
            flags.append(f'missing_{name}')
    for name in ('time_position', 'last_contact'):
        if result[name] is not None and result[name] > result['snapshot_time']:
            flags.append(f'future_{name}')
    position_time = result['time_position']
    result['position_age_seconds'] = None if position_time is None else result['snapshot_time'] - position_time
    if result['position_age_seconds'] is not None and result['position_age_seconds'] > 15:
        flags.append('stale_position')
    result['snapshot_age_at_collection_seconds'] = result['collected_at'] - result['snapshot_time']
    if result['snapshot_age_at_collection_seconds'] < 0:
        flags.append('snapshot_after_collection')
    result['has_position'] = result['latitude'] is not None and result['longitude'] is not None
    result['on_ground'] = state.get('on_ground') if isinstance(state.get('on_ground'), bool) else None
    result['quality_flags'] = sorted(flags)
    return result
