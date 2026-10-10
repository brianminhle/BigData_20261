"""Release normalized historical events directly into the replay Kafka topic."""

import argparse
import json
import math
from pathlib import Path
import sys
from uuid import uuid4

from .kafka_sink import create_producer, publish_records
from .replay import replay_records
from .replay_source import load_replay_source
from .sample_io import DEFAULT_MANIFEST


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--events-per-second", type=float, default=100)
    parser.add_argument("--stale-seconds", type=float, default=30)
    parser.add_argument("--run-id", default=f"replay-{uuid4().hex}")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="airtraffic.replay.v1")
    args = parser.parse_args(argv)
    try:
        if not math.isfinite(args.events_per_second) or args.events_per_second <= 0:
            raise ValueError("--events-per-second must be finite and positive")
        if not math.isfinite(args.stale_seconds) or args.stale_seconds < 0 or not args.run_id.strip():
            raise ValueError("Supply a nonblank run ID and a finite nonnegative stale threshold")
        selected, source = load_replay_source(args.manifest, args.max_rows)
        records = replay_records(
            selected, source, run_id=args.run_id, events_per_second=args.events_per_second,
            stale_seconds=args.stale_seconds,
        )
        producer = create_producer(args.bootstrap_servers)
        try:
            counts = publish_records(records, producer, args.topic)
        finally:
            producer.close(timeout=35)
    except KeyboardInterrupt:
        print("Publication interrupted; some records may already be in Kafka.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Publication failed; partial delivery is possible: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"run_id": args.run_id, "topic": args.topic, "acknowledged": counts}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
