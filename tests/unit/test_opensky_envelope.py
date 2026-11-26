"""Envelope validation preserves source identity and quarantines malformed rows."""

import json
import unittest

from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.processing.state_envelope import parse_envelope


class EnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.box = BoundingBox()
        row = ['abc123', ' ABC ', 'Vietnam', 100, 110, 105, 20, 10000,
               False, 100, 90, 0, None, None, None, False, 0]
        self.event, = state_events({'time': 120, 'states': [row]}, self.box, 125)
        self.event['source_object'] = dict(bucket='fixture', key='snapshot', sha256='digest')

    def test_valid_and_missing_measurements(self):
        observation, error = parse_envelope(json.dumps(self.event), self.box)
        self.assertIsNone(error)
        self.assertEqual(json.loads(observation)['callsign'], 'ABC')

    def test_payload_mutation_or_wrong_scope_rejected(self):
        self.event['state']['latitude'] = 21
        self.assertIsNotNone(parse_envelope(json.dumps(self.event), self.box)[1])

    def test_invalid_envelopes_do_not_crash_stream(self):
        for payload in ('null', '[]', '{}', '{broken', '123'):
            with self.subTest(payload=payload):
                observation, error = parse_envelope(payload, self.box)
                self.assertIsNone(observation)
                self.assertIsNotNone(error)
