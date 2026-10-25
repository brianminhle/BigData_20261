"""Bounded live-session settings and the fixed public OpenSky endpoint."""

from dataclasses import dataclass
import math
import re
from urllib.parse import urlencode

LIVE_TOPIC = "airtraffic.live.v1"
LIVE_ARCHIVE = "hdfs://namenode:9000/airtraffic/bronze/live-v1"


@dataclass(frozen=True)
class LiveConfig:
    run_id: str
    bounds: tuple[float, float, float, float] = (8, 102, 24, 110)
    max_attempts: int = 3
    duration_seconds: float = 60
    interval_seconds: float = 10
    timeout_seconds: float = 10
    backoff_seconds: float = 5
    stale_seconds: float = 30

    def __post_init__(self):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", self.run_id):
            raise ValueError("run_id must be 1–80 safe letters, digits, dots, underscores or dashes")
        if len(self.bounds) != 4 or not all(math.isfinite(value) for value in self.bounds):
            raise ValueError("bounds must contain four finite coordinates")
        south, west, north, east = self.bounds
        if not (-90 <= south < north <= 90 and -180 <= west < east <= 180):
            raise ValueError("bounds must be SOUTH WEST NORTH EAST without crossing the dateline")
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 1000:
            raise ValueError("max_attempts must be between 1 and 1000, including retries")
        for name in ("duration_seconds", "interval_seconds", "timeout_seconds", "backoff_seconds", "stale_seconds"):
            if not math.isfinite(getattr(self, name)) or getattr(self, name) <= 0:
                raise ValueError(f"{name} must be finite and positive")
        if self.interval_seconds < 10:
            raise ValueError("interval_seconds must be at least 10 (anonymous source resolution)")
        if self.duration_seconds > 1800 or self.timeout_seconds > 30:
            raise ValueError("session duration is capped at 1800 seconds; HTTP timeout at 30 seconds")

    @property
    def url(self):
        return "https://opensky-network.org/api/states/all?" + urlencode(
            dict(zip(("lamin", "lomin", "lamax", "lomax"), self.bounds))
        )

    @property
    def estimated_credits_per_attempt(self):
        south, west, north, east = self.bounds
        area = (north - south) * (east - west)
        return 1 if area <= 25 else 2 if area <= 100 else 3 if area <= 400 else 4
