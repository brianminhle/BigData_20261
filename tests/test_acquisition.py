"""Offline tests with synthetic observations; no downloaded telemetry is included."""

import csv
import gzip
import hashlib
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

from scripts.download_sample import download
from scripts.profile_sample import profile, profile_manifest
from scripts.sample_io import FIELDS, SampleError, inspect_header, iter_rows, write_json


def observation(**changes):
    row = dict(zip(FIELDS, (
        "1656302410", "abcdef", "21.0", "105.0", "200", "90", "-2", "TEST001 ",
        "False", "False", "False", "1200", "-30.48", "1000", "1656302379.5", "1656302409.5",
    )))
    row.update(changes)
    return row


def csv_bytes(rows):
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, FIELDS)
    writer.writeheader()
    writer.writerows(rows)
    return text.getvalue().encode()


class TemporaryFiles(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def csv(self, rows):
        path = self.root / "sample.csv"
        path.write_bytes(csv_bytes(rows))
        return path

    def archive(self, members):
        path = self.root / "sample.tar"
        with tarfile.open(path, "w") as archive:
            for name, content in members:
                info = tarfile.TarInfo(name)
                info.size = len(content)
                archive.addfile(info, io.BytesIO(content))
        return path


class ReaderTests(TemporaryFiles):
    def test_plain_and_compressed_members_are_read_without_extraction(self):
        content = csv_bytes([observation()])
        path = self.archive([("nested/a.csv", content), ("../b.csv.gz", gzip.compress(content))])
        rows = list(iter_rows(path))
        self.assertEqual([row[0] for row in rows], ["nested/a.csv", "../b.csv.gz"])
        self.assertEqual(rows[1][2]["callsign"], "TEST001 ")
        self.assertEqual(list(self.root.iterdir()), [path])

    def test_gzip_csv(self):
        path = self.root / "sample.csv.gz"
        path.write_bytes(gzip.compress(csv_bytes([observation()])))
        self.assertEqual(len(list(iter_rows(path))), 1)

    def test_archive_without_csv_and_symbolic_links_are_rejected(self):
        path = self.archive([("readme.txt", b"not data")])
        with self.assertRaisesRegex(SampleError, "no CSV"):
            list(iter_rows(path))
        with tarfile.open(path, "w") as archive:
            info = tarfile.TarInfo("link.csv")
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
            archive.addfile(info)
        with self.assertRaisesRegex(SampleError, "regular file"):
            list(iter_rows(path))

    def test_invalid_schema_and_empty_input(self):
        path = self.root / "sample.csv"
        for content, error in ((b"time,lat\n1,2\n", "missing OpenSky"),
                               ((",".join(FIELDS) + ",time\n").encode(), "duplicate CSV"),
                               (csv_bytes([]), "no observations")):
            with self.subTest(error=error):
                path.write_bytes(content)
                with self.assertRaisesRegex(SampleError, error):
                    inspect_header(path)


class ProfileTests(TemporaryFiles):
    def test_nulls_invalid_numbers_and_valid_negative_altitude(self):
        path = self.csv([
            observation(),
            observation(icao24="invalid", lat="", lon="\\N", velocity="NaN", onground="maybe", squawk="8888"),
            observation(lat="91", lon="-181", heading="360", velocity="-1", geoaltitude="inf", time="1.5"),
        ])
        report = profile(path)
        self.assertEqual(report["scope"]["total_source_rows"], 3)
        self.assertEqual(report["missing_by_field"]["lat"], 1)
        self.assertEqual(report["missing_by_field"]["lon"], 1)
        self.assertEqual(report["invalid_by_field"]["velocity"], 2)
        self.assertEqual(report["invalid_by_field"]["baroaltitude"], 0)
        self.assertEqual(report["invalid_by_field"]["squawk"], 1)
        self.assertEqual(report["quality_counts"]["usable_positions"], 1)
        self.assertEqual(report["quality_counts"]["rows_with_invalid_values"], 2)

    def test_fractional_freshness_and_future_updates_are_separate(self):
        report = profile(self.csv([observation(), observation(lastposupdate="", lastcontact="1656302410.5")]))
        positions = report["freshness"]["fields"]["lastposupdate"]
        contacts = report["freshness"]["fields"]["lastcontact"]
        self.assertEqual(positions["comparable_rows"], 1)
        self.assertEqual(positions["older_than_threshold"], 1)
        self.assertEqual(positions["max_age_seconds"], 30.5)
        self.assertEqual(contacts["after_observation_time"], 1)
        self.assertEqual(contacts["older_than_threshold"], 0)

    def test_duplicates_conflicts_and_ordering(self):
        report = profile(self.csv([
            observation(), observation(), observation(lat="22"),
            observation(time="1656302400"), observation(icao24="ABCDEF", time="1656302401"),
        ]))
        counts = report["quality_counts"]
        self.assertEqual(counts["duplicate_keys"], 2)
        self.assertEqual(counts["duplicates_matching_first_payload"], 1)
        self.assertEqual(counts["duplicates_conflicting_with_first_payload"], 1)
        self.assertEqual(counts["source_time_decreases"], 1)
        self.assertEqual(counts["per_aircraft_time_decreases"], 1)
        self.assertEqual(report["distinct_aircraft_in_profile"], 1)
        self.assertEqual(report["distinct_aircraft_time_keys_in_profile"], 3)

    def test_limit_distinguishes_prefix_from_complete_file(self):
        path = self.csv([observation(), observation()])
        partial = profile(path, max_rows=1)["scope"]
        complete = profile(path, max_rows=2)["scope"]
        self.assertTrue(partial["truncated"])
        self.assertIsNone(partial["total_source_rows"])
        self.assertEqual(partial["rows_profiled"], 1)
        self.assertFalse(complete["truncated"])
        self.assertEqual(complete["total_source_rows"], 2)

    def test_malformed_rows_do_not_count_as_usable_keys(self):
        path = self.csv([observation()])
        with path.open("ab") as stream:
            stream.write(b"1656302410,abcdef,21\n")
            stream.write(csv_bytes([observation()]).splitlines()[1] + b",extra\n")
        report = profile(path)
        self.assertEqual(report["quality_counts"]["malformed_rows"], 2)
        self.assertEqual(report["quality_counts"]["usable_identity_and_time"], 1)
        self.assertEqual(report["quality_counts"]["duplicate_keys"], 0)

    def test_invalid_limits(self):
        path = self.csv([observation()])
        for limit, stale in ((0, 30), (1, -1), (1, float("nan"))):
            with self.assertRaises(SampleError):
                profile(path, limit, stale)


class Response(io.BytesIO):
    status = 200

    def __init__(self, payload, length=None):
        super().__init__(payload)
        self.headers = {"Content-Length": str(len(payload) if length is None else length), "ETag": '"test"'}

    def geturl(self):
        return "https://example.org/sample.csv"


class DownloadTests(TemporaryFiles):
    def setUp(self):
        super().setUp()
        self.payload = csv_bytes([observation()])
        self.source = self.root / "source.json"
        self.data = self.root / "data"
        self.config = {
            "dataset_id": "test_sample", "filename": "sample.csv", "url": "https://example.org/sample.csv",
            "expected_bytes": len(self.payload), "expected_sha256": hashlib.sha256(self.payload).hexdigest(),
        }
        write_json(self.source, self.config)

    def run_download(self):
        return download(self.source, self.data, 1024 * 1024, 5)

    def test_download_manifest_cached_run_and_profile(self):
        with patch("scripts.download_sample.urlopen", return_value=Response(self.payload)) as request:
            path, cached = self.run_download()
            self.assertFalse(cached)
            self.assertEqual(self.run_download(), (path, True))
            request.assert_called_once()
        manifest = json.loads(path.read_text())
        self.assertEqual(manifest["local_path"], "../raw/sample.csv")
        self.assertEqual(manifest["sha256"], self.config["expected_sha256"])
        self.assertEqual((self.data / "raw/sample.csv").read_bytes(), self.payload)
        report = profile_manifest(path, 100, 30)
        self.assertEqual(report["scope"]["rows_profiled"], 1)
        self.assertNotIn("abcdef", json.dumps(report))

    def test_corruption_is_rejected_by_downloader_and_profiler(self):
        with patch("scripts.download_sample.urlopen", return_value=Response(self.payload)):
            manifest, _ = self.run_download()
        (self.data / "raw/sample.csv").write_bytes(self.payload.replace(b"abcdef", b"abcdee"))
        with self.assertRaisesRegex(SampleError, "SHA-256"):
            self.run_download()
        with self.assertRaisesRegex(SampleError, "SHA-256"):
            profile_manifest(manifest, 100, 30)

    def test_incomplete_or_changed_download_leaves_no_archive_or_manifest(self):
        for content, length, error in (
            (self.payload[:-10], len(self.payload), "Incomplete"),
            (self.payload, len(self.payload) + 1, "Content-Length"),
            (self.payload.replace(b"abcdef", b"abcdee"), len(self.payload), "SHA-256"),
        ):
            with self.subTest(error=error):
                with patch("scripts.download_sample.urlopen", return_value=Response(content, length)):
                    with self.assertRaisesRegex(SampleError, error):
                        self.run_download()
                self.assertEqual(list(self.data.rglob("*.*")), [])

    def test_wrong_schema_is_not_published(self):
        self.config["expected_sha256"] = None
        content = b"<html>Not a dataset</html>"
        self.config["expected_bytes"] = len(content)
        write_json(self.source, self.config)
        with patch("scripts.download_sample.urlopen", return_value=Response(content)):
            with self.assertRaisesRegex(SampleError, "missing OpenSky"):
                self.run_download()
        self.assertEqual(list(self.data.rglob("*.*")), [])

    def test_size_limit_and_unsafe_filename_fail_before_network(self):
        with patch("scripts.download_sample.urlopen") as request:
            with self.assertRaisesRegex(SampleError, "download limit"):
                download(self.source, self.data, 1, 5)
            self.config["filename"] = "../sample.csv"
            write_json(self.source, self.config)
            with self.assertRaisesRegex(SampleError, "simple filename"):
                self.run_download()
            request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
