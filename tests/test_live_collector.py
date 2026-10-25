"""Finite collection, source failures and uncertain Kafka delivery without network access."""

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock
from urllib.error import URLError

from scripts.live_collector import collect_session
from scripts.live_config import LiveConfig
from scripts.opensky_http import SnapshotResponse
from scripts.sample_io import ROOT

FIXTURE = (ROOT / "data/fixtures/live_states.json").read_bytes()


class FakeClock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class LiveCollectorTests(unittest.TestCase):
    def run_session(self, responses, *, config=None, producer=None, token=None):
        fetch = Mock(side_effect=responses)
        clock = FakeClock()
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / "session"
            report = collect_session(config or LiveConfig("test", max_attempts=2), producer or Mock(), output,
                                     fetch=fetch, token=token, monotonic=clock.monotonic, sleep=clock.sleep)
            self.assertEqual(json.loads((output / "session.json").read_text()), json.loads(json.dumps(report)))
            bodies = [path.read_bytes() for path in sorted(output.glob("*.body"))]
        return report, fetch, bodies

    def response(self, status=200, body=FIXTURE, headers=None):
        return SnapshotResponse(status, body, 1800000010, headers or {})

    def test_success_acknowledgments_freshness_and_evidence(self):
        report, fetch, bodies = self.run_session([self.response(), self.response(headers={"x-rate-limit-remaining": "394"})])
        self.assertEqual((report["status"], fetch.call_count, report["uncertain_deliveries"]), ("completed", 2, 0))
        self.assertEqual(report["acknowledged"], {"observation": 6, "rejected": 4})
        self.assertEqual(bodies, [FIXTURE, FIXTURE])
        freshness = report["attempts"][0]["freshness"]
        self.assertEqual((freshness["fresh_positions_at_receipt"], freshness["stale_positions_at_receipt"],
                          freshness["missing_positions_or_times"]), (1, 1, 1))
        self.assertEqual(report["attempts"][1]["rate_limit"]["x-rate-limit-remaining"], "394")

    def test_transport_and_server_retries_use_the_attempt_budget(self):
        for failures in ([URLError("timeout"), self.response()], [self.response(503), self.response(503)]):
            with self.subTest(failures=failures):
                report, fetch, _ = self.run_session(failures)
                self.assertEqual(fetch.call_count, 2)
                self.assertEqual(report["stop_reason"], "attempt_budget")
                self.assertEqual(report["status"], "completed" if isinstance(failures[0], URLError) else "failed")
                self.assertGreaterEqual(report["elapsed_seconds"], 10)

    def test_quota_and_auth_failures_stop_without_leaking_token(self):
        for status, reason in ((401, "http_error"), (403, "http_error"), (429, "rate_limit")):
            report, fetch, _ = self.run_session([self.response(status, b'error', {"x-rate-limit-retry-after-seconds": "3600"})], token="secret-token")
            self.assertEqual((report["status"], report["stop_reason"], fetch.call_count), ("failed", reason, 1))
            self.assertNotIn("secret-token", json.dumps(report))

    def test_invalid_body_is_retained_without_publication(self):
        producer = Mock()
        report, fetch, bodies = self.run_session([self.response(body=b'not JSON')], producer=producer)
        self.assertEqual(report["stop_reason"], "invalid_response")
        self.assertEqual(bodies, [b'not JSON'])
        producer.send.assert_not_called()
        self.assertEqual(fetch.call_count, 1)

    def test_partial_publication_preserves_known_acks_and_marks_uncertainty(self):
        producer = Mock()
        success, failure = Mock(), Mock()
        failure.get.side_effect = TimeoutError("broker timeout containing secret-token")
        producer.send.side_effect = [success, failure, Mock(), Mock(), Mock()]
        report, fetch, _ = self.run_session([self.response()], producer=producer)
        self.assertEqual(report["stop_reason"], "publication_error")
        self.assertEqual(report["acknowledged"], {"observation": 1, "rejected": 0})
        self.assertEqual((report["attempted_deliveries"], report["uncertain_deliveries"]), (5, 4))
        self.assertNotIn("secret-token", json.dumps(report))
        self.assertEqual(fetch.call_count, 1)

    def test_empty_response_is_successful_collection_with_no_fabricated_records(self):
        producer = Mock()
        report, _, _ = self.run_session([self.response(body=b'{"time":1800000000,"states":null}')],
                                        config=LiveConfig("test", max_attempts=1), producer=producer)
        self.assertEqual(report["status"], "completed")
        self.assertEqual(sum(report["acknowledged"].values()), 0)
        producer.send.assert_not_called()

    def test_duration_prevents_another_request_and_existing_evidence_is_refused(self):
        report, fetch, _ = self.run_session([self.response()], config=LiveConfig("test", duration_seconds=5))
        self.assertEqual((report["stop_reason"], fetch.call_count), ("duration", 1))
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(FileExistsError):
                collect_session(LiveConfig("test"), Mock(), Path(folder), fetch=Mock())

    def test_interruption_retains_session_report(self):
        report, fetch, _ = self.run_session([KeyboardInterrupt()])
        self.assertEqual((report["status"], report["stop_reason"]), ("interrupted", "interrupted"))


if __name__ == "__main__":
    unittest.main()
