"""One bounded HTTP attempt; retry policy belongs to the session collector."""

from dataclasses import dataclass
import time
from urllib.error import HTTPError
from urllib.request import HTTPRedirectHandler, Request, build_opener

MAX_RESPONSE_BYTES = 2 * 1024 * 1024
RATE_HEADERS = ("x-rate-limit-remaining", "x-rate-limit-retry-after-seconds", "retry-after")


@dataclass(frozen=True)
class SnapshotResponse:
    status: int
    body: bytes
    received_at: float
    headers: dict[str, str]
    truncated: bool = False


class NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


def fetch_snapshot(url: str, timeout: float, token: str | None = None) -> SnapshotResponse:
    headers = {"Accept": "application/json", "User-Agent": "airtraffic-course-collector/1.0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = Request(url, headers=headers)
    try:
        response = build_opener(NoRedirects()).open(request, timeout=timeout)
    except HTTPError as exc:
        response = exc
    with response:
        body = response.read(MAX_RESPONSE_BYTES + 1)
        return SnapshotResponse(
            status=response.code, body=body[:MAX_RESPONSE_BYTES], received_at=time.time(),
            headers={name: response.headers[name] for name in RATE_HEADERS if name in response.headers},
            truncated=len(body) > MAX_RESPONSE_BYTES,
        )
