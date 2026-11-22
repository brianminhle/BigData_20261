"""Exercise failure boundaries across object storage and Kafka acknowledgment."""

import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from minio.error import S3Error

from airtraffic.ingestion.opensky_api import BoundingBox, state_events
from airtraffic.ingestion.state_outbox import collect_once, publish_pending
from airtraffic.storage.snapshots import SnapshotStore


class MemoryObjects:
    def __init__(self):
        self.objects = {}
        self.fail_prefix = None

    def stat_object(self, bucket, key):
        if key not in self.objects:
            raise S3Error(None, "NoSuchKey", "missing", key, "request", "host")
        return SimpleNamespace(size=len(self.objects[key]))

    def put_object(self, bucket, key, stream, length, **kwargs):
        if self.fail_prefix and key.startswith(self.fail_prefix):
            raise OSError("storage failure")
        self.objects[key] = stream.read(length)

    def get_object(self, bucket, key):
        response = io.BytesIO(self.objects[key])
        response.release_conn = Mock()
        return response

    def list_objects(self, bucket, prefix, **kwargs):
        return [SimpleNamespace(object_name=key) for key in sorted(self.objects) if key.startswith(prefix)]


class OutboxTests(unittest.TestCase):
    def setUp(self):
        self.objects = MemoryObjects()
        self.store = SnapshotStore(self.objects, "states")
        self.box = BoundingBox()
        self.row = ["abc123", None, "Vietnam", 100, 110, 105, 20, 10000,
                    False, 100, 90, 0, None, None, None, False, 0]
        self.snapshot = {"time": 120, "states": [self.row]}
        self.api = Mock()
        self.api.snapshot.return_value = self.snapshot
        self.producer = Mock()
        self.topic = "fixture.states"

    def collect(self):
        return collect_once(self.api, self.producer, self.box, self.topic, self.store)

    def test_storage_failure_sends_nothing(self):
        self.objects.fail_prefix = "snapshots/"
        with self.assertRaises(OSError):
            self.collect()
        self.producer.send.assert_not_called()

    def test_restart_recovers_after_ack_failure_without_api(self):
        self.producer.send.return_value.get.side_effect = TimeoutError()
        with self.assertRaises(TimeoutError):
            self.collect()
        first_payload = self.producer.send.call_args.kwargs["value"]
        self.assertEqual(len(list(self.store.pending(self.topic))), 1)
        restarted = SnapshotStore(self.objects, "states")
        self.producer.send.return_value.get.side_effect = None
        self.assertEqual(publish_pending(restarted, self.producer, self.topic), 1)
        self.assertEqual(first_payload, self.producer.send.call_args.kwargs["value"])
        self.assertEqual(list(restarted.pending(self.topic)), [])
        self.api.snapshot.assert_called_once()

    def test_receipt_failure_causes_safe_repeat(self):
        self.objects.fail_prefix = "published/"
        with self.assertRaises(OSError):
            self.collect()
        first_key = self.producer.send.call_args.kwargs["key"]
        self.objects.fail_prefix = None
        publish_pending(self.store, self.producer, self.topic)
        self.assertEqual(first_key, self.producer.send.call_args.kwargs["key"])
        self.assertEqual(self.producer.send.call_count, 2)

    def test_same_response_does_not_republish_or_change_collected_at(self):
        self.collect()
        stored = dict(self.objects.objects)
        self.collect()
        self.assertEqual(self.producer.send.call_count, 1)
        self.assertEqual(stored, self.objects.objects)

    def test_equivalent_bbox_from_cli_keeps_snapshot_identity(self):
        first = self.store.save(self.snapshot, self.box, 130)
        second = self.store.save(self.snapshot, BoundingBox(8, 24, 102, 110), 140)
        self.assertEqual(first, second)

    def test_failure_draining_backlog_does_not_fetch_new_snapshot(self):
        self.store.save(self.snapshot, self.box, 130)
        self.producer.send.side_effect = OSError("broker down")
        with self.assertRaises(OSError):
            self.collect()
        self.api.snapshot.assert_not_called()

    def test_empty_snapshot_has_receipt_without_fake_aircraft(self):
        self.api.snapshot.return_value = {"time": 120, "states": None}
        self.assertEqual(self.collect(), 0)
        self.producer.send.assert_not_called()
        self.assertEqual(list(self.store.pending(self.topic)), [])
        self.assertEqual(len(self.objects.objects), 2)

    def test_invalid_row_is_preserved_but_not_published(self):
        self.api.snapshot.return_value = {"time": 120, "states": [self.row, ["broken"]]}
        with self.assertRaises(ValueError):
            self.collect()
        self.producer.send.assert_not_called()
        self.assertEqual(len(list(self.store.pending(self.topic))), 1)

    def test_new_topic_can_replay_existing_snapshots(self):
        self.collect()
        self.assertEqual(publish_pending(self.store, self.producer, "other.states"), 1)

    def test_source_reference_and_changed_row_identity(self):
        self.collect()
        event = json.loads(self.producer.send.call_args.kwargs["value"])
        self.assertIn(event["source_object"]["key"], self.objects.objects)
        first_id = event["observation_id"]
        self.row[9] = 101
        updated, = state_events(self.snapshot, self.box, 130)
        self.assertNotEqual(first_id, updated["observation_id"])

    def test_corrupt_object_never_reaches_kafka(self):
        key = self.store.save(self.snapshot, self.box, 130)
        payload = json.loads(self.objects.objects[key])
        payload["response"]["time"] = 999
        self.objects.objects[key] = json.dumps(payload).encode()
        with self.assertRaisesRegex(ValueError, "checksum"):
            publish_pending(self.store, self.producer, self.topic)
        self.producer.send.assert_not_called()


if __name__ == "__main__":
    unittest.main()
