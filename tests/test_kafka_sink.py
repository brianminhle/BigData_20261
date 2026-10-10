"""Publication correctness independent of a running broker."""

from dataclasses import replace
import json
import unittest
from unittest.mock import Mock

from scripts.historical import normalize_historical
from scripts.kafka_sink import publish_records, record_key
from scripts.observations import SourceReference
from scripts.sample_io import FIELDS


class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.row = {**dict.fromkeys(FIELDS, ""), "time": "1656302410", "icao24": "00abcd"}
        source = SourceReference("OpenSky", "test", "https://example.org", "a" * 64, "fixture.csv", 1)
        self.record = normalize_historical(self.row, source, run_id="test-run", received_at=1800000000)

    def test_key_keeps_aircraft_in_one_partition_and_isolates_runs(self):
        later = replace(self.record, observed_at=1656302420)
        other_run = replace(self.record, run_id="other")
        self.assertEqual(record_key(self.record), record_key(later))
        self.assertNotEqual(record_key(self.record), record_key(other_run))
        self.assertEqual(json.loads(record_key(self.record)), ["replay", "test-run", "00abcd"])

    def test_success_requires_acknowledgments_and_preserves_original_event(self):
        producer = Mock()
        counts = publish_records([self.record, self.record], producer, "test-topic", max_pending=1)
        self.assertEqual(counts, {"observation": 2, "rejected": 0})
        self.assertEqual(producer.send.return_value.get.call_count, 2)
        payload = json.loads(producer.send.call_args.kwargs["value"])
        self.assertEqual(payload["observed_at"], 1656302410)
        self.assertEqual(payload["received_at"], 1800000000)
        self.assertEqual(payload["observation_id"], self.record.observation_id)

    def test_delivery_failure_propagates_before_unbounded_input_is_consumed(self):
        producer = Mock()
        producer.send.return_value.get.side_effect = TimeoutError("broker unavailable")
        def records():
            yield self.record
            self.fail("Continued to consume source after a bounded delivery failure")
        with self.assertRaisesRegex(TimeoutError, "broker unavailable"):
            publish_records(records(), producer, "test-topic", max_pending=1)

    def test_rejected_rows_are_published_with_raw_evidence(self):
        rejected = normalize_historical(
            {**self.row, "icao24": "bad"}, self.record.provenance,
            run_id="test-run", received_at=1800000000,
        )
        producer = Mock()
        counts = publish_records([rejected], producer, "test-topic")
        self.assertEqual(counts["rejected"], 1)
        payload = json.loads(producer.send.call_args.kwargs["value"])
        self.assertEqual(payload["raw_fields"]["icao24"], "bad")
        self.assertEqual(json.loads(record_key(rejected))[-1], rejected.source_payload_sha256)


if __name__ == "__main__":
    unittest.main()
