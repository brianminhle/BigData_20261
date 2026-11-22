"""Loss-preserving replay of the pinned OpenSky raw Avro sample."""

from dataclasses import dataclass
import hashlib
from itertools import islice
import json
import math
from pathlib import Path
import time
from uuid import uuid4


SAMPLE_URL = 'https://raw.githubusercontent.com/openskynetwork/osky-sample/1b179c1c541b178944010a2eb6fa5189a4f9193d/avro/raw20150421_sample.avro'
SAMPLE_SHA256 = 'f8adc737ff6b2da6446bcaece62586eec91210339ece0147ca699d39555d96af'
@dataclass(frozen=True)
class RawReception:
    run_id: str
    reception_id: str
    delivery_id: str
    source_sha256: str
    source_url: str
    record_index: int
    raw_message: str
    receiver_id: int
    source_reception_time: float | None
    event_time: float | None
    replay_released_at: float
    source_record_json: str
    source_payload_sha256: str
    dataset_id: str = 'opensky_raw_20150421'
    event_time_basis: str = 'timeAtServer'
    schema_version: int = 1
    record_type: str = 'raw_reception'
    mode: str = 'replay'


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def reception(record, checksum, index, run_id, released_at):
    # Keep all source fields, including nulls and nonfinite Avro doubles, as text.
    source_json = json.dumps(record, separators=(',', ':'), sort_keys=True)
    source_time = record['timeAtServer']
    timestamp = source_time if math.isfinite(source_time) else None
    return RawReception(
        run_id=run_id, reception_id=hashlib.sha256(f'{checksum}:{index}'.encode()).hexdigest(),
        delivery_id=uuid4().hex, source_sha256=checksum, source_url=SAMPLE_URL,
        record_index=index, raw_message=record['rawMessage'], receiver_id=record['sensorSerialNumber'],
        source_reception_time=timestamp, event_time=timestamp, replay_released_at=released_at,
        source_record_json=source_json, source_payload_sha256=hashlib.sha256(source_json.encode()).hexdigest(),
    )


def replay_raw(path: Path, *, run_id: str, max_rows: int, start_record: int = 0,
               events_per_second: float = 100, expected_sha256: str = SAMPLE_SHA256):
    from fastavro import reader

    if not run_id.strip() or max_rows < 1 or start_record < 0:
        raise ValueError('Supply a run ID, positive row limit and nonnegative start record')
    if not math.isfinite(events_per_second) or events_per_second <= 0:
        raise ValueError('Replay rate must be finite and positive')
    checksum = sha256_file(path)
    if checksum != expected_sha256:
        raise ValueError('Raw Avro sample SHA-256 mismatch')
    with path.open('rb') as stream:
        records = reader(stream)
        fields = {field['name'] for field in records.writer_schema['fields']}
        required = {'rawMessage', 'timeAtServer', 'sensorSerialNumber'}
        if not required <= fields:
            raise ValueError('Avro schema lacks raw reception fields')
        count = 0
        deadline = time.monotonic()
        for index, record in enumerate(islice(records, start_record, start_record + max_rows), start_record):
            time.sleep(max(0, deadline - time.monotonic()))
            yield reception(record, checksum, index, run_id, time.time())
            deadline = time.monotonic() + 1 / events_per_second
            count += 1
        if count == 0:
            raise ValueError('No raw receptions in the selected interval')
