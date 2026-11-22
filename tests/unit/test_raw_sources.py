"""Source discovery, download consistency and importer CLI defaults."""

from contextlib import redirect_stderr
from datetime import datetime, timezone
import io
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from airtraffic.ingestion.catalog import SourceFile, s3_files
from airtraffic.ingestion.download import download_file
from airtraffic.jobs.import_raw import parse_args


class RawSourceTests(unittest.TestCase):
    def test_discovery_filters_placeholder_and_preserves_source_revision(self):
        modified = datetime(2026, 11, 22, tzinfo=timezone.utc)
        entries = [
            SimpleNamespace(object_name="raw/placeholder.txt", size=0),
            SimpleNamespace(object_name="raw/day one.avro", size=100,
                            etag="revision", last_modified=modified),
            SimpleNamespace(object_name="raw/empty.avro", size=0),
        ]
        with patch("minio.Minio") as client:
            client.return_value.list_objects.return_value = iter(entries)
            files = s3_files("https://source.example", "samples", "raw/", "fixture")
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0].url, "https://source.example/samples/raw/day%20one.avro")
        self.assertEqual(files[0].etag, "revision")
        self.assertEqual(files[0].last_modified, modified.isoformat())
        client.return_value.list_objects.assert_called_once_with("samples", prefix="raw/", recursive=True)

    def test_invalid_source_endpoint_is_rejected(self):
        for endpoint in ["file:///tmp", "https://example.org/path", "https://user:pass@example.org"]:
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                s3_files(endpoint, "bucket", "raw/", "fixture")

    def test_cli_defaults_to_300_seconds_and_allows_discovery_without_minio(self):
        arguments = parse_args(["--discover-only"])
        self.assertEqual(arguments.interval_seconds, 300)
        self.assertEqual(arguments.source, "opensky-raw")

    def test_invalid_poll_interval_and_dataset_are_rejected(self):
        for arguments in [["--interval-seconds", "nan"], ["--interval-seconds", "0"],
                          ["--dataset", "../escape"], ["--source", "s3"]]:
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse_args(["--discover-only", *arguments])

    def download_response(self, headers, body=b"raw", *, expected_size=None, etag=None):
        response = io.BytesIO(body)
        response.headers = headers
        source = SourceFile("fixture", "https://source.example/raw.avro", size=expected_size, etag=etag)
        with TemporaryDirectory() as directory:
            with patch("airtraffic.ingestion.download.urlopen", return_value=response) as open_url:
                checksum = download_file(source, Path(directory) / "raw.avro", 1000)
        return checksum, open_url.call_args.args[0]

    def test_download_uses_conditional_request(self):
        checksum, request = self.download_response({"ETag": '"version1"', "Content-Length": "3"},
                                                  expected_size=3, etag="version1")
        self.assertEqual(request.get_header("If-match"), '"version1"')
        self.assertEqual(len(checksum), 64)

    def test_changed_source_revision_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "ETag changed"):
            self.download_response({"ETag": '"new"'}, etag="old")

    def test_short_http_response_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.download_response({"Content-Length": "10"})

    def test_wrong_source_size_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "incomplete"):
            self.download_response({}, expected_size=10)


if __name__ == "__main__":
    unittest.main()
