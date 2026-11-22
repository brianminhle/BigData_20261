"""Raw imports publish only complete data and recover without duplicate uploads."""

from dataclasses import replace
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from fastavro import writer
from minio.error import S3Error

from airtraffic.ingestion.catalog import SourceFile
from airtraffic.ingestion.importer import import_once, poll
from airtraffic.ingestion.source import sha256_file
from airtraffic.storage.raw import RawObjectStore


RAW_SCHEMA = {
    "type": "record", "name": "RawReception",
    "fields": [{"name": "rawMessage", "type": "string"},
               {"name": "timeAtServer", "type": "double"},
               {"name": "sensorSerialNumber", "type": "int"}],
}


class MemoryS3:
    def __init__(self):
        self.objects = {}
        self.uploads = 0
        self.fail_manifest = False

    def stat_object(self, bucket, key):
        if key not in self.objects:
            raise S3Error(None, "NoSuchKey", "missing", key, "request", "host")
        content, metadata = self.objects[key]
        return SimpleNamespace(size=len(content), metadata=metadata)

    def fput_object(self, bucket, key, path, *, metadata, content_type):
        self.uploads += 1
        self.objects[key] = (Path(path).read_bytes(), {"x-amz-meta-sha256": metadata["sha256"]})

    def put_object(self, bucket, key, stream, length, *, content_type):
        if self.fail_manifest:
            raise OSError("Manifest upload interrupted")
        self.objects[key] = (stream.read(length), {})


class RawImportTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "raw.avro"
        with self.path.open("wb") as stream:
            writer(stream, RAW_SCHEMA, [
                {"rawMessage": "8D40621D58C382D690C8AC2863A7", "timeAtServer": timestamp,
                 "sensorSerialNumber": 12} for timestamp in [103.0, 100.0, float("nan")]
            ])
        self.source = SourceFile("fixture", self.path.as_uri(),
                                 expected_sha256=sha256_file(self.path))
        self.client = MemoryS3()
        self.store = RawObjectStore(self.client, "fixture-bucket")

    def run_import(self, files=None, **kwargs):
        return import_once(files if files is not None else [self.source], self.store,
                           max_bytes=kwargs.get("max_bytes", 100_000),
                           work_dir=self.directory.name)

    def test_commit_preserves_bytes_event_time_and_skips_on_restart(self):
        self.assertEqual(self.run_import()["imported"], 1)
        marker = self.store.manifest_key(self.source)
        manifest = json.loads(self.client.objects[marker][0])
        self.assertEqual(manifest["record_count"], 3)
        self.assertEqual(manifest["event_time_min"], 100)
        self.assertEqual(manifest["event_time_max"], 103)
        self.assertEqual(self.client.objects[manifest["object_key"]][0], self.path.read_bytes())
        restarted = RawObjectStore(self.client, "fixture-bucket")
        with patch("airtraffic.ingestion.importer.download_file") as download:
            result = import_once([self.source], restarted, max_bytes=100_000)
        download.assert_not_called()
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(self.client.uploads, 1)

    def test_manifest_failure_retries_and_reuses_raw_object(self):
        self.client.fail_manifest = True
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            self.assertEqual(self.run_import()["failed"], 1)
        self.assertFalse(self.store.is_committed(self.source))
        self.client.fail_manifest = False
        self.assertEqual(self.run_import()["imported"], 1)
        self.assertEqual(self.client.uploads, 1)

    def test_bad_checksum_does_not_commit_or_stop_other_files(self):
        broken = replace(self.source, expected_sha256="0" * 64)
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            result = self.run_import([broken, self.source])
        self.assertEqual((result["failed"], result["imported"]), (1, 1))
        self.assertFalse(self.store.is_committed(broken))
        self.assertEqual(list(Path(self.directory.name).iterdir()), [self.path])

    def test_processed_state_vectors_are_rejected(self):
        with self.path.open("wb") as stream:
            writer(stream, {"type": "record", "name": "State", "fields": [
                {"name": "lat", "type": "double"}]}, [{"lat": 42.0}])
        self.source = replace(self.source, expected_sha256=sha256_file(self.path))
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            self.assertEqual(self.run_import()["failed"], 1)
        self.assertEqual(self.client.objects, {})

    def test_truncated_avro_never_commits(self):
        self.path.write_bytes(self.path.read_bytes()[:-5])
        self.source = replace(self.source, expected_sha256=sha256_file(self.path))
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            self.assertEqual(self.run_import()["failed"], 1)
        self.assertEqual(self.client.objects, {})

    def test_source_revision_gets_manifest_but_same_bytes_share_raw_object(self):
        revision = replace(self.source, last_modified="2026-11-22T00:00:00Z")
        self.assertEqual(self.run_import([self.source, revision])["imported"], 2)
        self.assertEqual(self.client.uploads, 1)
        self.assertTrue(self.store.is_committed(revision))

    def test_size_limit_does_not_publish_and_cleans_temporary_file(self):
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            self.assertEqual(self.run_import(max_bytes=20)["failed"], 1)
        self.assertEqual(self.client.objects, {})
        self.assertEqual(list(Path(self.directory.name).iterdir()), [self.path])

    def test_inconsistent_existing_object_is_not_published(self):
        key = f"raw/fixture/sha256/{self.source.expected_sha256}.avro"
        self.client.objects[key] = (b"broken", {})
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            self.assertEqual(self.run_import()["failed"], 1)
        self.assertFalse(self.store.is_committed(self.source))

    def test_upload_error_never_creates_manifest(self):
        with patch.object(self.client, "fput_object", side_effect=OSError("upload failed")):
            with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
                self.assertEqual(self.run_import()["failed"], 1)
        self.assertFalse(self.store.is_committed(self.source))

    def test_poll_retries_listing_error_after_300_seconds(self):
        stop = Mock()
        stop.is_set.side_effect = [False, False, True]
        discover = Mock(side_effect=[OSError("unavailable"), [self.source]])
        with patch("airtraffic.ingestion.importer.time.monotonic", return_value=100):
            with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
                self.assertEqual(poll(discover, self.store, interval=300, stop=stop), 0)
        self.assertEqual(discover.call_count, 2)
        self.assertEqual(stop.wait.call_args_list[0].args, (300,))
        self.assertTrue(self.store.is_committed(self.source))

    def test_once_failure_is_nonzero(self):
        stop = Mock()
        stop.is_set.return_value = False
        with self.assertLogs("airtraffic.ingestion.importer", level="ERROR"):
            result = poll(Mock(side_effect=OSError("offline")), self.store,
                          interval=300, stop=stop, once=True)
        self.assertEqual(result, 1)
        stop.wait.assert_not_called()

    def test_access_denied_is_not_treated_as_missing_object(self):
        error = S3Error(None, "AccessDenied", "denied", "key", "request", "host")
        with patch.object(self.client, "stat_object", side_effect=error):
            with self.assertRaises(S3Error):
                self.store.is_committed(self.source)


if __name__ == "__main__":
    unittest.main()
