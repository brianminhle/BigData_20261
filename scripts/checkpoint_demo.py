"""Run the tracked fixture through Compose and save checkpoint evidence locally."""

import argparse
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import re
import sys
from uuid import uuid4

from airtraffic.demo_runtime import DemoError, checked_compose, result_json, wait_for_archive
from .profile_sample import profile_manifest
from airtraffic.sample_io import ROOT, read_json, write_json

FIXTURE = ROOT / "data/fixtures/checkpoint_manifest.json"


def require_values(result: dict, expected: dict, step: str) -> None:
    for name, value in expected.items():
        if result.get(name) != value:
            raise DemoError(f"{step}: expected {name}={value!r}, found {result.get(name)!r}")


def run_demo(run_id: str, output_dir: Path, *, build: bool, timeout_seconds: float) -> dict:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,79}", run_id):
        raise ValueError("Run ID must be 1–80 letters, digits, dots, dashes or underscores, starting with a letter/digit")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("--timeout-seconds must be finite and positive")
    profile = profile_manifest(FIXTURE, max_rows=10, stale_seconds=30)
    output_dir.mkdir(parents=True, exist_ok=False)
    log_path = output_dir / "demo.log"
    write_json(output_dir / "profile.json", profile)
    write_json(output_dir / "run.json", {
        "run_id": run_id, "fixture_sha256": read_json(FIXTURE)["sha256"],
        "started_at": datetime.now(timezone.utc).isoformat(),
    })

    print(f"Starting Compose. Evidence: {output_dir}", flush=True)
    checked_compose(["up", "-d", "--build" if build else "--no-build"], log_path, timeout=900)
    print("Publishing the 10-row fixture once...", flush=True)
    producer = result_json(checked_compose([
        "run", "--rm", "producer", "--manifest", "/workspace/data/fixtures/checkpoint_manifest.json",
        "--run-id", run_id, "--max-rows", "10", "--events-per-second", "10",
    ], log_path))
    require_values(producer, {"run_id": run_id, "acknowledged": {"observation": 8, "rejected": 2}}, "Producer")
    write_json(output_dir / "producer.json", producer)

    print("Waiting for committed HDFS records...", flush=True)
    archive = wait_for_archive(run_id, log_path, timeout_seconds)
    require_values(archive, {
        "run_id": run_id, "rows": 10, "unique_kafka_offsets": 10, "unique_observations": 6,
        "status_counts": {"observation": 8, "source_rejected": 2},
    }, "Archive")
    write_json(output_dir / "archive.json", archive)

    print("Computing traffic and trajectory summaries...", flush=True)
    analytics = result_json(checked_compose([
        "run", "--rm", "analytics", "--run-id", run_id, "--limit", "20",
    ], log_path))
    require_values(analytics, {
        "run_id": run_id, "archived_rows": 10, "eligible_positions": 3,
        "traffic_group_count": 2, "trajectory_segment_count": 2,
    }, "Analytics")
    write_json(output_dir / "analytics.json", analytics)

    print("Saving and verifying batch Parquet outputs...", flush=True)
    output_path = f"hdfs://namenode:9000/airtraffic/batch/replay-v1/{run_id}/v1"
    batch = result_json(checked_compose([
        "run", "--rm", "batch", "--run-id", run_id, "--output", output_path,
    ], log_path))
    require_values(batch, {
        "run_id": run_id, "archived_rows": 10, "eligible_positions": 3, "output_path": output_path,
    }, "Batch")
    for name in ("traffic", "trajectory_segments"):
        require_values(batch.get("datasets", {}).get(name, {}), {"row_count": 2}, f"Batch {name}")
    write_json(output_dir / "batch.json", batch)
    summary = {
        "status": "passed", "run_id": run_id, "archived_rows": 10,
        "eligible_positions": 3, "traffic_groups": 2, "trajectory_segments": 2,
        "batch_output": output_path, "evidence_directory": str(output_dir),
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_json(output_dir / "summary.json", summary)
    return summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=f"checkpoint-{uuid4().hex[:12]}")
    parser.add_argument("--output-dir", type=Path, help="New local evidence directory; existing directories are refused")
    parser.add_argument("--no-build", action="store_true", help="Use already-built local images")
    parser.add_argument("--timeout-seconds", type=float, default=180, help="Maximum wait for archive readiness")
    args = parser.parse_args(argv)
    output_dir = args.output_dir or ROOT / "data/reports/checkpoint" / args.run_id
    try:
        summary = run_demo(args.run_id, output_dir, build=not args.no_build, timeout_seconds=args.timeout_seconds)
    except (OSError, ValueError, DemoError) as exc:
        print(f"Demo failed: {exc}. Evidence: {output_dir}", file=sys.stderr)
        print("Partial delivery is possible; use a new run ID for another demo.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Demo interrupted; services may still be running and delivery may be partial.", file=sys.stderr)
        return 130
    print(json.dumps(summary, sort_keys=True))
    print("Services remain running for the UIs. Stop them with: docker compose stop")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
