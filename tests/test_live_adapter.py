"""REST indexing, identity, nulls and rejected evidence must match the shared contract."""

from dataclasses import replace
import hashlib
import json
import unittest

from airtraffic.live_adapter import normalize_response
from airtraffic.live_config import LiveConfig
from airtraffic.observations import RejectedObservation
from airtraffic.sample_io import ROOT

FIXTURE = (ROOT / "data/fixtures/live_states.json").read_bytes()


class LiveAdapterTests(unittest.TestCase):
    def normalize(self, body=FIXTURE, **kwargs):
        return normalize_response(body, source_url="https://example.org/states", run_id="test", received_at=1800000010, **kwargs)

    def test_field_mapping_source_clocks_nulls_and_quality(self):
        snapshot, records = self.normalize()
        self.assertEqual(snapshot, 1800000000)
        first = records[0]
        self.assertEqual((first.mode, first.icao24, first.observation_id), ("live", "abcdef", "opensky:state:abcdef:1800000000"))
        self.assertEqual((first.latitude, first.longitude, first.ground_speed_mps), (21, 105.8, 200))
        self.assertEqual((first.position_updated_at, first.last_contact_at, first.received_at), (1799999995, 1799999999, 1800000010))
        self.assertEqual((first.callsign, first.squawk, first.spi, first.alert), ("TEST01", "0042", False, None))
        self.assertEqual(first.provenance.archive_sha256, hashlib.sha256(FIXTURE).hexdigest())
        self.assertIsNone(records[1].latitude)
        self.assertEqual(records[1].ground_speed_mps, 0)
        self.assertIn("missing_position", records[1].quality_flags)
        self.assertIn("stale_position", records[2].quality_flags)
        self.assertEqual([record.record_type for record in records], ["observation"] * 3 + ["rejected"] * 2)
        self.assertIn("invalid:heading", records[3].reasons)
        self.assertEqual(json.loads(records[4].raw_fields["state_vector"]), ["short"])

    def test_identity_does_not_depend_on_receipt_or_run(self):
        _, first = self.normalize()
        _, again = normalize_response(FIXTURE, source_url="https://example.org/states", run_id="other", received_at=1800000020)
        self.assertEqual(first[0].observation_id, again[0].observation_id)
        self.assertEqual(first[0].source_payload_sha256, again[0].source_payload_sha256)

    def test_bad_types_are_rejected_without_coercing_boolean_to_speed(self):
        payload = json.loads(FIXTURE)
        for index, value in ((9, True), (8, "false"), (0, 123456), (5, "105")):
            with self.subTest(index=index):
                vector = payload["states"][0].copy()
                vector[index] = value
                _, records = self.normalize(json.dumps({"time": payload["time"], "states": [vector]}).encode())
                self.assertIsInstance(records[0], RejectedObservation)

    def test_empty_and_invalid_responses_are_distinct(self):
        for states in (None, []):
            self.assertEqual(self.normalize(json.dumps({"time": 1800000000, "states": states}).encode())[1], [])
        for body in (b'{}', b'null', b'{"time":true,"states":[]}', b'{"time":1,"states":{}}', b'{"time":NaN,"states":[]}', b'<html>'):
            with self.subTest(body=body), self.assertRaises(ValueError):
                self.normalize(body)

    def test_config_rejects_unbounded_or_invalid_inputs_and_calculates_cost(self):
        config = LiveConfig("test")
        self.assertEqual(config.estimated_credits_per_attempt, 3)
        self.assertEqual(replace(config, bounds=(20, 104, 22, 107)).estimated_credits_per_attempt, 1)
        for changes in ({"max_attempts": 0}, {"interval_seconds": 1}, {"duration_seconds": float("inf")},
                        {"timeout_seconds": 0}, {"bounds": (24, 102, 8, 110)}, {"run_id": "../bad"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(config, **changes)


if __name__ == "__main__":
    unittest.main()
