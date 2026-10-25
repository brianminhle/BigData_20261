"""Map OpenSky REST state vectors onto the shared observation contract."""

from dataclasses import replace
import hashlib
import json

from .historical import normalize_historical
from .observations import RejectedObservation, SourceReference
from .sample_io import FIELDS

STATE_FIELDS = {
    "icao24": 0, "callsign": 1, "lastposupdate": 3, "lastcontact": 4,
    "lon": 5, "lat": 6, "baroaltitude": 7, "onground": 8, "velocity": 9,
    "heading": 10, "vertrate": 11, "geoaltitude": 13, "squawk": 14, "spi": 15,
}
TEXT_FIELDS = {"icao24", "callsign", "squawk"}
BOOL_FIELDS = {"onground", "spi"}


def _invalid_constant(value):
    raise ValueError(f"Non-finite JSON number: {value}")


def _type_errors(vector):
    if not isinstance(vector, list) or len(vector) < 17:
        return ["malformed_state_vector"]
    errors = []
    for field, index in STATE_FIELDS.items():
        value = vector[index]
        if value is None:
            continue
        expected = (str,) if field in TEXT_FIELDS else (bool,) if field in BOOL_FIELDS else (int, float)
        if type(value) not in expected:
            errors.append(f"invalid_type:{field}")
    return errors


def _normalize_vector(vector, snapshot_time, provenance, run_id, received_at, stale_seconds):
    original = json.dumps(vector, separators=(",", ":"), sort_keys=True, allow_nan=False)
    fingerprint = hashlib.sha256(f"{snapshot_time}:{original}".encode()).hexdigest()
    metadata = {
        "run_id": run_id, "mode": "live", "received_at": received_at,
        "provenance": provenance, "source_payload_sha256": fingerprint,
    }
    errors = _type_errors(vector)
    if errors:
        return RejectedObservation(
            **metadata, reasons=tuple(errors),
            raw_fields={"snapshot_time": str(snapshot_time), "state_vector": original}, extra_values=(),
        )
    row = dict.fromkeys(FIELDS, "")
    row["time"] = str(snapshot_time)
    for field, index in STATE_FIELDS.items():
        row[field] = "" if vector[index] is None else str(vector[index])
    record = normalize_historical(
        row, provenance, run_id=run_id, received_at=received_at, stale_seconds=stale_seconds,
    )
    record = replace(record, mode="live", source_payload_sha256=fingerprint)
    if isinstance(record, RejectedObservation):
        record = replace(record, raw_fields={"snapshot_time": str(snapshot_time), "state_vector": original})
    return record


def normalize_response(body: bytes, *, source_url: str, run_id: str, received_at: float,
                       stale_seconds: float = 30):
    response = json.loads(body, parse_constant=_invalid_constant)
    if not isinstance(response, dict):
        raise ValueError("REST response must be an object")
    snapshot_time = response.get("time")
    if type(snapshot_time) is not int or not 0 < snapshot_time < 253402300800:
        raise ValueError("REST response needs a positive integer snapshot time")
    states = response.get("states")
    if "states" not in response or (states is not None and not isinstance(states, list)):
        raise ValueError("REST states must be an array or null")
    states = states or []
    if len(states) > 10000:
        raise ValueError("REST response exceeds the 10,000-vector session limit")
    response_hash = hashlib.sha256(body).hexdigest()
    records = []
    for index, vector in enumerate(states):
        provenance = SourceReference(
            provider="OpenSky Network", dataset_id="opensky-rest-states-all",
            source_url=source_url, archive_sha256=response_hash,
            member=f"states[{index}]", record_index=index + 1, adapter_version="opensky_rest_v1",
        )
        records.append(_normalize_vector(vector, snapshot_time, provenance, run_id, received_at, stale_seconds))
    return snapshot_time, records
