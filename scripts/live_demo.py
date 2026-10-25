"""Verify real OpenSky collection through Kafka and the separate HDFS live archive."""

import argparse
import json
from pathlib import Path
import sys
from uuid import uuid4

from .checkpoint_demo import require_values
from .demo_runtime import DemoError, checked_compose, result_json, wait_for_archive
from .live_config import LIVE_ARCHIVE, LIVE_TOPIC, LiveConfig
from .sample_io import ROOT, write_json


def run_live_demo(config: LiveConfig, output_dir: Path, *, build=True):
    output_dir.mkdir(parents=True, exist_ok=False)
    log = output_dir / "demo.log"
    print(f"Starting the live pipeline. Evidence: {output_dir}", flush=True)
    checked_compose(["--profile", "live", "up", "-d", "--build" if build else "--no-build"], log, timeout=900)
    print(f"Collecting at most {config.max_attempts} requests in the selected region...", flush=True)
    result = checked_compose([
        "run", "--rm", "live-collector", "--run-id", config.run_id,
        "--bounds", *map(str, config.bounds), "--max-attempts", str(config.max_attempts),
        "--duration-seconds", str(config.duration_seconds), "--interval-seconds", str(config.interval_seconds),
        "--timeout-seconds", str(config.timeout_seconds), "--backoff-seconds", str(config.backoff_seconds),
        "--stale-seconds", str(config.stale_seconds),
    ], log, timeout=config.duration_seconds + 90)
    session = result_json(result)
    write_json(output_dir / "session.json", session)
    require_values(session, {"run_id": config.run_id, "status": "completed", "uncertain_deliveries": 0}, "Collector")
    acknowledged = session["acknowledged"]
    expected_rows = sum(acknowledged.values())
    if acknowledged.get("observation", 0) == 0:
        raise DemoError("No observations returned; session evidence is saved, but this does not verify live delivery")
    print(f"Checking {expected_rows} acknowledged deliveries in HDFS...", flush=True)
    archive = wait_for_archive(config.run_id, log, 180, expected_rows=expected_rows, path=LIVE_ARCHIVE)
    expected_statuses = {name: count for name, count in {
        "observation": acknowledged.get("observation", 0), "source_rejected": acknowledged.get("rejected", 0),
    }.items() if count}
    require_values(archive, {"run_id": config.run_id, "rows": expected_rows,
                            "unique_kafka_offsets": expected_rows, "status_counts": expected_statuses,
                            "mode_counts": {"live": expected_rows}, "topic_counts": {LIVE_TOPIC: expected_rows}}, "Live archive")
    write_json(output_dir / "archive.json", archive)
    summary = {"status": "passed", "run_id": config.run_id, "acknowledged": acknowledged,
               "archived_rows": archive["rows"], "unique_kafka_offsets": archive["unique_kafka_offsets"],
               "source_path": LIVE_ARCHIVE, "evidence_directory": str(output_dir)}
    write_json(output_dir / "summary.json", summary)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=f"live-{uuid4().hex[:12]}")
    parser.add_argument("--bounds", nargs=4, type=float, default=(8, 102, 24, 110),
                        metavar=("SOUTH", "WEST", "NORTH", "EAST"))
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = LiveConfig(args.run_id, bounds=tuple(args.bounds), max_attempts=args.max_attempts)
        output_dir = ROOT / "data/reports/live-demo" / config.run_id
        print(json.dumps(run_live_demo(config, output_dir, build=not args.no_build), sort_keys=True))
    except (OSError, ValueError, DemoError) as exc:
        print(f"Live demo failed: {exc}. Inspect data/reports/live/ and data/reports/live-demo/. Use a new run ID.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
