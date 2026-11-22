"""State snapshots preserve source semantics and respect API limits."""

from contextlib import redirect_stderr
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from airtraffic.ingestion.opensky_api import BoundingBox, OpenSkyClient, RateLimited, state_events
from airtraffic.ingestion.state_kafka import publish_states
from airtraffic.jobs.collect_states import collect_loop, parse_args


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class OpenSkyStateTests(unittest.TestCase):
    def setUp(self):
        self.box = BoundingBox()
        self.row = ["abc123", "ABC123", "Vietnam", 100, 110, 105.1, 20.1,
                    11000.0, False, 180.0, 90.0, 0.0, None, None, None, False, 0]

    def test_120_second_default_fits_standard_quota(self):
        args = parse_args([])
        self.assertEqual(args.interval_seconds, 120)
        self.assertEqual(self.box.credit_cost * 86400 / args.interval_seconds, 2160)
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parse_args(["--interval-seconds", "10"])

    def test_event_keeps_snapshot_and_position_time_separate(self):
        event, = state_events({"time": 120, "states": [self.row]}, self.box, 125.5)
        self.assertEqual(event["event_time"], 120)
        self.assertEqual(event["state"]["time_position"], 100)
        self.assertEqual(event["state"]["last_contact"], 110)
        self.assertEqual(event["source_row"], self.row)
        self.assertEqual(event["record_type"], "opensky_state_vector")
        next_event, = state_events({"time": 120, "states": [self.row]}, self.box, 130.0)
        self.assertEqual(event["observation_id"], next_event["observation_id"])

    def test_kafka_publish_waits_for_ack_and_uses_stable_key(self):
        event, = state_events({"time": 120, "states": [self.row]}, self.box, 125.5)
        producer = Mock()
        producer.send.return_value.get.return_value = None
        self.assertEqual(publish_states([event], producer, "fixture.states"), 1)
        kwargs = producer.send.call_args.kwargs
        self.assertEqual(kwargs["key"], event["observation_id"].encode())
        self.assertEqual(json.loads(kwargs["value"])["source_row"], self.row)
        producer.send.return_value.get.assert_called_once_with(timeout=35)

    def test_oauth_token_is_cached_until_expiration(self):
        token = Response(b'{"access_token":"token","expires_in":1800}')
        snapshot = b'{"time":120,"states":[]}'
        with patch("airtraffic.ingestion.opensky_api.urlopen", side_effect=[token, Response(snapshot), Response(snapshot)]) as fetch:
            client = OpenSkyClient("id", "secret")
            self.assertEqual(client.snapshot(self.box)["time"], 120)
            self.assertEqual(client.snapshot(self.box)["time"], 120)
        self.assertEqual(fetch.call_count, 3)
        request = fetch.call_args_list[1].args[0]
        self.assertTrue(request.full_url.startswith("https://opensky-network.org/api/states/all?"))
        self.assertEqual(request.get_header("Authorization"), "Bearer token")

    def test_429_respects_retry_header(self):
        error = HTTPError("https://opensky-network.org", 429, "limited",
                          {"X-Rate-Limit-Retry-After-Seconds": "720"}, None)
        with patch("airtraffic.ingestion.opensky_api.urlopen", side_effect=[
            Response(b'{"access_token":"token"}'), error]):
            with self.assertRaises(RateLimited) as raised:
                OpenSkyClient("id", "secret").snapshot(self.box)
        self.assertEqual(raised.exception.retry_after, 720)

    def test_loop_waits_for_rate_limit_before_retry(self):
        client = Mock()
        client.snapshot.side_effect = [RateLimited(720), {"time": 120, "states": [self.row]}]
        producer = Mock()
        stop = Mock()
        stop.is_set.side_effect = [False, False, True]
        with patch("airtraffic.jobs.collect_states.time.monotonic", return_value=10):
            self.assertEqual(collect_loop(client, producer, self.box, "fixture.states", 120, stop, Mock(pending=Mock(return_value=[]))), 0)
        self.assertEqual(stop.wait.call_args_list[0].args, (720,))
        self.assertEqual(client.snapshot.call_count, 2)


if __name__ == "__main__":
    unittest.main()
