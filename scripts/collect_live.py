"""Pull a bounded OpenSky session and publish it to the dedicated live topic."""

import argparse
import json
import os
from pathlib import Path
import sys
from uuid import uuid4

from .kafka_sink import create_producer
from .live_collector import collect_session
from .live_config import LiveConfig
from .sample_io import ROOT


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=f"live-{uuid4().hex[:12]}")
    parser.add_argument("--bounds", nargs=4, type=float, default=(8, 102, 24, 110),
                        metavar=("SOUTH", "WEST", "NORTH", "EAST"))
    parser.add_argument("--max-attempts", type=int, default=3, help="Includes failed requests and retries")
    parser.add_argument("--duration-seconds", type=float, default=60, help="Stop scheduling requests after this duration")
    parser.add_argument("--interval-seconds", type=float, default=10)
    parser.add_argument("--timeout-seconds", type=float, default=10)
    parser.add_argument("--backoff-seconds", type=float, default=5)
    parser.add_argument("--stale-seconds", type=float, default=30)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--output-dir", type=Path, help="New evidence directory; existing directories are refused")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    producer = None
    try:
        config = LiveConfig(**{name: getattr(args, name) for name in LiveConfig.__dataclass_fields__})
        directory = args.output_dir or ROOT / "data/reports/live" / config.run_id
        if directory.exists():
            raise FileExistsError("Evidence directory exists; choose a new run ID/output directory")
        producer = create_producer(args.bootstrap_servers)
        report = collect_session(config, producer, directory, token=os.environ.get("OPENSKY_ACCESS_TOKEN"))
        print(json.dumps(report, sort_keys=True, allow_nan=False))
        return 0 if report["status"] == "completed" else 130 if report["status"] == "interrupted" else 1
    except Exception as exc:
        print(f"Live collection failed ({type(exc).__name__}); inspect session.json if created. Use a new run ID.", file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
