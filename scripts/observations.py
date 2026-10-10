"""Version-one observation contract. Times are UTC Unix seconds; measurements use SI."""

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class SourceReference:
    provider: str
    dataset_id: str
    source_url: str
    archive_sha256: str
    member: str
    record_index: int
    adapter_version: str = "opensky_csv_v1"


@dataclass(frozen=True, kw_only=True)
class Envelope:
    run_id: str
    mode: Literal["live", "replay"]
    received_at: float
    provenance: SourceReference
    source_payload_sha256: str
    schema_version: int = field(default=1, init=False)
    source: str = field(default="opensky", init=False)


@dataclass(frozen=True, kw_only=True)
class Observation(Envelope):
    observation_id: str
    icao24: str
    observed_at: int
    position_updated_at: float | None
    last_contact_at: float | None
    latitude: float | None
    longitude: float | None
    ground_speed_mps: float | None
    track_deg: float | None
    vertical_rate_mps: float | None
    barometric_altitude_m: float | None
    geometric_altitude_m: float | None
    callsign: str | None
    on_ground: bool | None
    alert: bool | None
    spi: bool | None
    squawk: str | None
    quality_flags: tuple[str, ...]
    record_type: str = field(default="observation", init=False)


@dataclass(frozen=True, kw_only=True)
class RejectedObservation(Envelope):
    reasons: tuple[str, ...]
    raw_fields: dict[str, str | None]
    extra_values: tuple[str, ...]
    record_type: str = field(default="rejected", init=False)
