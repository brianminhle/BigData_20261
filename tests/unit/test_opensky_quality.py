"""Quality and batch reference use observations, never inferred aircraft counts."""

import json
import io
from contextlib import redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from test_opensky_outbox import MemoryObjects
from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.jobs.collect_states import parse_args
from airtraffic.pipelines.state_batch import process_snapshots
from airtraffic.processing.state_vectors import normalize_observation
from airtraffic.storage.snapshots import SnapshotStore


class StateQualityTests(unittest.TestCase):
    def setUp(self):
        self.row = ['abc123', ' ABC ', 'Vietnam', 100, 110, 105, 20, 10000,
                    False, 100, 90, 0, None, None, None, False, 0]
        self.store = SnapshotStore(MemoryObjects(), 'fixture')

    def normalize(self):
        event, = state_events({'time': 120, 'states': [self.row]}, BoundingBox(), 125)
        event['source_object'] = {'key': 'fixture'}
        return normalize_observation(event)

    def test_normalizes_units_and_flags_without_filling_nulls(self):
        self.row[5] = None
        self.row[9] = -20
        observation = self.normalize()
        self.assertEqual(observation['callsign'], 'ABC')
        self.assertIsNone(observation['longitude'])
        self.assertIsNone(observation['velocity'])
        self.assertFalse(observation['has_position'])
        self.assertIn('invalid_velocity', observation['quality_flags'])
        self.assertIn('stale_position', observation['quality_flags'])
        self.assertEqual(observation['position_age_seconds'], 20)

    def test_future_time_and_invalid_coordinates_are_not_valid_position(self):
        self.row[3] = 500
        self.row[6] = 91
        self.row[10] = 360
        observation = self.normalize()
        self.assertIn('future_time_position', observation['quality_flags'])
        self.assertIn('invalid_latitude', observation['quality_flags'])
        self.assertIn('invalid_true_track', observation['quality_flags'])
        self.assertFalse(observation['has_position'])

    def test_batch_counts_distinct_aircraft_not_rows_and_records_empty_gap(self):
        for timestamp in (120, 240):
            self.store.save({'time': timestamp, 'states': [self.row, self.row]}, BoundingBox(), timestamp + 1)
        self.store.save({'time': 600, 'states': None}, BoundingBox(), 601)
        with TemporaryDirectory() as directory:
            output = Path(directory) / 'batch'
            report = process_snapshots(self.store, output)
            self.assertEqual(report['counts']['source_rows'], 4)
            self.assertEqual(report['counts']['observations'], 2)
            self.assertEqual(report['counts']['duplicate_rows'], 2)
            self.assertEqual(report['counts']['repeated_position_rows'], 1)
            self.assertEqual(report['counts']['empty_snapshots'], 1)
            self.assertEqual(report['snapshot_gaps'][0]['seconds'], 360)
            windows = [json.loads(line) for line in (output / 'aircraft_windows.jsonl').read_text().splitlines()]
            self.assertEqual(len(windows), 1)
            self.assertEqual(windows[0]['distinct_aircraft'], 1)
            self.assertTrue((output / '_SUCCESS').exists())
            with self.assertRaises(FileExistsError):
                process_snapshots(self.store, output)

    def test_corruption_does_not_publish_success(self):
        key = self.store.save({'time': 120, 'states': [self.row]}, BoundingBox(), 121)
        self.store.client.objects[key] = b'{}'
        with TemporaryDirectory() as directory:
            output = Path(directory) / 'batch'
            with self.assertRaises(KeyError):
                process_snapshots(self.store, output)
            self.assertFalse((output / '_SUCCESS').exists())

    def test_anonymous_mode_is_explicit_one_shot(self):
        self.assertTrue(parse_args(['--anonymous', '--once']).anonymous)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(['--anonymous'])


if __name__ == '__main__':
    unittest.main()
