"""Fetch OpenSky state vector snapshots using OAuth client credentials."""

from dataclasses import dataclass
import json
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
STATES_URL = "https://opensky-network.org/api/states/all"
STATE_FIELDS = (
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
)


class RateLimited(Exception):
    def __init__(self, retry_after: int):
        super().__init__(f"OpenSky rate limit reached; retry after {retry_after}s")
        self.retry_after = retry_after


@dataclass(frozen=True)
class BoundingBox:
    lamin: float = 8.0
    lamax: float = 24.0
    lomin: float = 102.0
    lomax: float = 110.0

    def __post_init__(self):
        for field in ("lamin", "lamax", "lomin", "lomax"):
            object.__setattr__(self, field, float(getattr(self, field)))
        if not (-90 <= self.lamin < self.lamax <= 90 and
                -180 <= self.lomin < self.lomax <= 180):
            raise ValueError("Invalid latitude/longitude bounds")

    @property
    def credit_cost(self):
        area = (self.lamax - self.lamin) * (self.lomax - self.lomin)
        return 1 if area <= 25 else 2 if area <= 100 else 3 if area <= 400 else 4


class OpenSkyClient:
    def __init__(self, client_id: str, client_secret: str, *, anonymous=False):
        self.anonymous = anonymous
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.expires_at = 0.0

    def _access_token(self):
        if self.token and time.monotonic() < self.expires_at:
            return self.token
        payload = urlencode({"grant_type": "client_credentials", "client_id": self.client_id,
                             "client_secret": self.client_secret}).encode()
        request = Request(TOKEN_URL, data=payload,
                          headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urlopen(request, timeout=30) as response:
            body = json.load(response)
        self.token = body["access_token"]
        self.expires_at = time.monotonic() + max(0, int(body.get("expires_in", 1800)) - 60)
        return self.token

    def snapshot(self, box: BoundingBox) -> dict:
        query = urlencode({"lamin": box.lamin, "lamax": box.lamax,
                           "lomin": box.lomin, "lomax": box.lomax})
        for attempt in range(2):
            request = Request(f"{STATES_URL}?{query}",
                              headers={} if self.anonymous else {"Authorization": f"Bearer {self._access_token()}"})
            try:
                with urlopen(request, timeout=30) as response:
                    result = json.load(response)
                if (not isinstance(result, dict) or not isinstance(result.get("time"), int)
                        or "states" not in result
                        or not isinstance(result["states"], (list, type(None)))):
                    raise ValueError("Unexpected OpenSky snapshot format")
                return result
            except HTTPError as error:
                if error.code == 401 and attempt == 0 and not self.anonymous:
                    self.token = None
                    continue
                if error.code == 429:
                    try:
                        retry_after = int(error.headers.get("X-Rate-Limit-Retry-After-Seconds", "300"))
                    except ValueError:
                        retry_after = 300
                    raise RateLimited(max(1, retry_after)) from error
                raise
        raise RuntimeError("OpenSky authentication failed")


def state_events(snapshot: dict, box: BoundingBox, collected_at: float):
    import hashlib

    timestamp = snapshot["time"]
    for row in snapshot["states"] or []:
        if not isinstance(row, list) or len(row) < 17 or not isinstance(row[0], str):
            raise ValueError("Unexpected OpenSky state vector row")
        state = dict(zip(STATE_FIELDS, row))
        # Preserve changed content even when OpenSky returns the same snapshot timestamp.
        identity = hashlib.sha256(json.dumps([str(box), timestamp, row],
                                             separators=(",", ":"), allow_nan=False).encode()).hexdigest()
        yield {
            "schema_version": 2, "record_type": "opensky_state_vector",
            "source": "opensky_rest_states_all", "mode": "api_snapshot",
            "snapshot_id": f"opensky:{box}:{timestamp}", "observation_id": identity,
            "snapshot_time": timestamp, "event_time": timestamp,
            "event_time_basis": "snapshot_time", "collected_at": collected_at,
            "state": state, "source_row": row,
        }
