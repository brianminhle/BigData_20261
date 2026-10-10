"""Bounded, timestamp-ordered replay; the selected prefix fits in memory."""

from collections.abc import Callable, Iterable, Iterator
from dataclasses import replace
from itertools import islice
import math
import time

from .historical import normalize_historical
from .observations import Observation, RejectedObservation, SourceReference
from .sample_values import numeric_value

SourceRow = tuple[str, int, dict]


def replay_order(record: SourceRow) -> tuple:
    member, index, row = record
    timestamp = numeric_value("time", (row.get("time") or "").strip())
    identifier = (row.get("icao24") or "").strip().lower()
    return (timestamp if timestamp is not None else math.inf, identifier, member, index)


def ordered_prefix(rows: Iterable[SourceRow], max_rows: int) -> list[SourceRow]:
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    selected = list(islice(rows, max_rows))
    if not selected:
        raise ValueError("No observations found")
    return sorted(selected, key=replay_order)


def replay_records(
    rows: Iterable[SourceRow], source: SourceReference, *, run_id: str,
    events_per_second: float, stale_seconds: float = 30,
    wall_clock: Callable[[], float] = time.time,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[Observation | RejectedObservation]:
    if not math.isfinite(events_per_second) or events_per_second <= 0:
        raise ValueError("events_per_second must be finite and positive")
    if not run_id.strip():
        raise ValueError("run_id must not be blank")
    if not math.isfinite(stale_seconds) or stale_seconds < 0:
        raise ValueError("stale_seconds must be finite and nonnegative")
    deadline = monotonic()
    for member, index, row in rows:
        delay = deadline - monotonic()
        if delay > 0:
            sleep(delay)
        received_at = wall_clock()
        # Do not burst to catch up after a slow consumer.
        deadline = monotonic() + 1 / events_per_second
        yield normalize_historical(
            row, replace(source, member=member, record_index=index),
            run_id=run_id, received_at=received_at, stale_seconds=stale_seconds,
        )
