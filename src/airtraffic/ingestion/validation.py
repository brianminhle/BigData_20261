"""Validate the raw container; leave frame quality checks to processing."""

import math
from pathlib import Path

from fastavro import reader


def inspect_raw_avro(path: Path) -> dict:
    count = 0
    first_time = None
    last_time = None
    with path.open("rb") as stream:
        records = reader(stream)
        fields = {field["name"] for field in records.writer_schema.get("fields", [])}
        if not {"rawMessage", "timeAtServer", "sensorSerialNumber"} <= fields:
            raise ValueError("Avro must contain raw reception fields, not aircraft state vectors")
        for record in records:
            count += 1
            timestamp = record["timeAtServer"]
            if isinstance(timestamp, (int, float)) and math.isfinite(timestamp):
                first_time = timestamp if first_time is None else min(first_time, timestamp)
                last_time = timestamp if last_time is None else max(last_time, timestamp)
    if not count:
        raise ValueError("Raw Avro contains no receptions")
    return {"record_count": count, "event_time_min": first_time,
            "event_time_max": last_time, "event_time_basis": "timeAtServer"}
