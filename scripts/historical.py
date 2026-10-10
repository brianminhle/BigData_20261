"""Convert original OpenSky CSV cells without losing source times or rejected rows."""

import hashlib
import json
import math
import re

from .observations import Observation, RejectedObservation, SourceReference
from .sample_io import FIELDS
from .sample_values import BOOLEANS, NULLS, NUMERIC, numeric_value

MEASUREMENTS = {
    "lastposupdate": "position_updated_at", "lastcontact": "last_contact_at",
    "lat": "latitude", "lon": "longitude", "velocity": "ground_speed_mps",
    "heading": "track_deg", "vertrate": "vertical_rate_mps",
    "baroaltitude": "barometric_altitude_m", "geoaltitude": "geometric_altitude_m",
    "callsign": "callsign", "onground": "on_ground", "alert": "alert",
    "spi": "spi", "squawk": "squawk",
}


def parse_cell(field: str, raw: str) -> str | float | bool | None:
    text = raw.strip()
    if text.lower() in NULLS:
        return None
    if field in NUMERIC:
        value = numeric_value(field, text)
    elif field in BOOLEANS:
        value = {"true": True, "false": False, "1": True, "0": False}.get(text.lower())
    elif field == "icao24":
        value = text.lower() if re.fullmatch(r"[0-9a-fA-F]{6}", text) else None
    elif field == "squawk":
        value = text if re.fullmatch(r"[0-7]{1,4}", text) else None
    else:
        return text
    if value is None:
        raise ValueError(f"invalid:{field}")
    return value


def quality_flags(parsed: dict, stale_seconds: float) -> tuple[str, ...]:
    flags = []
    if parsed["lat"] is None or parsed["lon"] is None:
        flags.append("missing_position")
    for field, label in (("lastposupdate", "position"), ("lastcontact", "contact")):
        updated = parsed[field]
        if updated is None:
            flags.append(f"missing_{label}_time")
        elif updated > parsed["time"]:
            flags.append(f"{label}_time_after_snapshot")
        elif parsed["time"] - updated > stale_seconds:
            flags.append(f"stale_{label}")
    return tuple(flags)


def normalize_historical(
    row: dict, provenance: SourceReference, *, run_id: str, received_at: float,
    stale_seconds: float = 30,
) -> Observation | RejectedObservation:
    if not run_id.strip():
        raise ValueError("run_id must not be blank")
    if not math.isfinite(received_at) or received_at <= 0:
        raise ValueError("received_at must be a finite positive Unix timestamp")
    if not math.isfinite(stale_seconds) or stale_seconds < 0:
        raise ValueError("stale_seconds must be finite and nonnegative")

    raw_fields = {key: value for key, value in row.items() if key is not None}
    extra_values = tuple(row.get(None, ()))
    payload = json.dumps(
        {"fields": raw_fields, "extra_values": extra_values},
        sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    metadata = {
        "run_id": run_id, "mode": "replay", "received_at": received_at,
        "provenance": provenance, "source_payload_sha256": hashlib.sha256(payload).hexdigest(),
    }
    reasons = []
    if None in row or any(row.get(field) is None for field in FIELDS):
        reasons.append("malformed_row")
    parsed = {}
    for field in FIELDS:
        try:
            parsed[field] = parse_cell(field, row.get(field) or "")
        except ValueError as exc:
            reasons.append(str(exc))
    for required in ("icao24", "time"):
        if required in parsed and parsed[required] is None:
            reasons.append(f"missing:{required}")
    if reasons:
        return RejectedObservation(
            **metadata, reasons=tuple(reasons), raw_fields=raw_fields, extra_values=extra_values,
        )
    observed_at = int(parsed["time"])
    return Observation(
        **metadata, observation_id=f"opensky:state:{parsed['icao24']}:{observed_at}",
        icao24=parsed["icao24"], observed_at=observed_at,
        quality_flags=quality_flags(parsed, stale_seconds),
        **{target: parsed[source] for source, target in MEASUREMENTS.items()},
    )
