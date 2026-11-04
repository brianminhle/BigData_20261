"""Exercise the real HTTP transport against a local fixture server."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest
from unittest.mock import patch

from airtraffic.opensky_http import fetch_snapshot


class FixtureHandler(BaseHTTPRequestHandler):
    requests = []

    def do_GET(self):
        self.requests.append((self.path, self.headers.get("Authorization")))
        status = 302 if self.path == "/redirect" else 429 if self.path == "/quota" else 200
        self.send_response(status)
        self.send_header("X-Rate-Limit-Remaining", "12")
        self.send_header("X-Rate-Limit-Retry-After-Seconds", "120")
        self.send_header("Location", "/should-not-follow")
        self.end_headers()
        self.wfile.write(b'{"time":1800000000,"states":null}')

    def log_message(self, *_):
        pass


class OpenSkyHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_body_headers_and_optional_bearer(self):
        response = fetch_snapshot(self.base + "/ok", 2, "test-token")
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers["x-rate-limit-remaining"], "12")
        self.assertEqual(FixtureHandler.requests[-1], ("/ok", "Bearer test-token"))
        self.assertNotIn("test-token", str(response))

    def test_error_response_body_and_retry_header_are_retained(self):
        response = fetch_snapshot(self.base + "/quota", 2)
        self.assertEqual(response.status, 429)
        self.assertEqual(response.headers["x-rate-limit-retry-after-seconds"], "120")
        self.assertIn(b'"states":null', response.body)

    def test_redirect_is_not_followed_with_credentials(self):
        before = len(FixtureHandler.requests)
        response = fetch_snapshot(self.base + "/redirect", 2, "test-token")
        self.assertEqual(response.status, 302)
        self.assertEqual(len(FixtureHandler.requests), before + 1)

    def test_body_size_limit_is_explicit(self):
        with patch("airtraffic.opensky_http.MAX_RESPONSE_BYTES", 5):
            response = fetch_snapshot(self.base + "/ok", 2)
        self.assertTrue(response.truncated)
        self.assertEqual(len(response.body), 5)


if __name__ == "__main__":
    unittest.main()
