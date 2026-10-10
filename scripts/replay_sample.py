"""Replay a bounded historical prefix as JSON Lines, without Kafka or network access."""

import argparse
from collections import Counter
from dataclasses import asdict
import json
import math
from pathlib import Path
import sys
from uuid import uuid4

from .replay import replay_records
from .replay_source import load_replay_source
from .sample_io import DEFAULT_MANIFEST, SampleError


def run_replay(
    manifest_path: Path, *, max_rows: int, events_per_second: float,
    run_id: str, stale_seconds: float, output: Path | None,
) -> dict:
    if max_rows <= 0 or max_rows > 100000:
        raise SampleError("--max-rows must be between 1 and 100000 for this in-memory prototype")
    if not math.isfinite(events_per_second) or events_per_second <= 0:
        raise SampleError("--events-per-second must be finite and positive")
    if not math.isfinite(stale_seconds) or stale_seconds < 0:
        raise SampleError("--stale-seconds must be finite and nonnegative")
    if not run_id.strip():
        raise SampleError("--run-id must not be blank")
    if output is not None and (output.exists() or output.is_symlink()):
        raise SampleError(f"Output already exists; choose a new path: {output}")
    selected, source = load_replay_source(manifest_path, max_rows)
    records = replay_records(
        selected, source, run_id=run_id, events_per_second=events_per_second,
        stale_seconds=stale_seconds,
    )
    counts = Counter({"observation": 0, "rejected": 0})
    stream = sys.stdout
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        stream = output.open("x", encoding="utf-8")
    try:
        for record in records:
            stream.write(json.dumps(asdict(record), allow_nan=False, separators=(",", ":")) + "\n")
            stream.flush()
            counts[record.record_type] += 1
    finally:
        if output is not None:
            stream.close()
    return {
        "run_id": run_id, "mode": "replay", "selected_rows": len(selected),
        "selection": "first_rows_in_archive_order_then_sorted", "counts": dict(counts),
        "requested_events_per_second": events_per_second, "stale_seconds": stale_seconds,
        "output": str(output) if output is not None else "stdout",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--events-per-second", type=float, default=100)
    parser.add_argument("--run-id", default=f"replay-{uuid4().hex}")
    parser.add_argument("--stale-seconds", type=float, default=30)
    parser.add_argument("--output", type=Path, help="New JSONL file; otherwise emit JSONL to stdout")
    args = parser.parse_args(argv)
    try:
        summary = run_replay(
            args.manifest, max_rows=args.max_rows, events_per_second=args.events_per_second,
            run_id=args.run_id, stale_seconds=args.stale_seconds, output=args.output,
        )
    except KeyboardInterrupt:
        print("Replay interrupted; any output file contains only the rows emitted so far.", file=sys.stderr)
        return 130
    except (OSError, ValueError) as exc:
        print(f"Replay failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
