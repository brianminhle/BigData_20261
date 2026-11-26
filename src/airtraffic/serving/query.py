"""Filter pre-aggregated views without summing distinct counts across cells."""


def query_view(view, params):
    scopes = view.get('scopes', sorted({row['scope_id'] for row in view['windows']}))
    scope = params.get('scope', [scopes[0] if scopes else ''])[0]
    if scopes and scope not in scopes:
        raise ValueError('Unknown scope')
    start, end = time_range(view['latest_snapshot'], params)
    def selected(rows):
        return [row for row in rows if row['scope_id'] == scope and start <= row['window_start'] < end]
    windows, cells = selected(view['windows']), selected(view['cells'])
    observations = sum(row['observations'] for row in windows)
    return dict(generated_at=view['generated_at'], latest_snapshot=view['latest_snapshot'],
                scopes=scopes, scope=scope, windows=windows, cells=cells,
                summary=dict(observations=observations,
                             with_position=sum(row['with_position'] for row in windows),
                             stale_positions=sum(row['stale_positions'] for row in windows),
                             peak_window_aircraft=max((row['distinct_aircraft'] for row in windows), default=None)))


def time_range(latest_snapshot, params):
    if 'hours' in params:
        hours = int(params['hours'][0])
        if not 1 <= hours <= 168 or 'start' in params or 'end' in params:
            raise ValueError('Use hours 1–168 or an explicit range')
        end = ((latest_snapshot or 0) // 300 + 1) * 300
        start = max(0, end - hours * 3600)
    else:
        start = int(params.get('start', ['0'])[0])
        end = int(params.get('end', [str(2**53)])[0])
    if start < 0 or end <= start or start % 300 or (end != 2**53 and end % 300):
        raise ValueError('Use a valid time range aligned to 5-minute boundaries')
    return start, end
