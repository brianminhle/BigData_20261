"""Demonstrate real OpenSky collection, streaming archival, and saved Spark analytics."""

import argparse
import json
from pathlib import Path
import sys
from uuid import uuid4

from .checkpoint_demo import require_values
from airtraffic.demo_runtime import DemoError, checked_compose, result_json, wait_for_archive
from airtraffic.live_config import LIVE_ARCHIVE, LIVE_TOPIC, LiveConfig
from airtraffic.live_demo_report import print_source_evidence, write_report
from airtraffic.sample_io import ROOT, write_json


def collect_live_input(config, output_dir, log):
    print(f"Requesting real snapshots from {config.url}", flush=True)
    print(f"At most {config.max_attempts} HTTP requests; {config.interval_seconds:g}s between requests.", flush=True)
    result = checked_compose([
        "run", "--rm", "live-collector", "--run-id", config.run_id,
        "--bounds", *map(str, config.bounds), "--max-attempts", str(config.max_attempts),
        "--duration-seconds", str(config.duration_seconds), "--interval-seconds", str(config.interval_seconds),
        "--timeout-seconds", str(config.timeout_seconds), "--backoff-seconds", str(config.backoff_seconds),
        "--stale-seconds", str(config.stale_seconds),
    ], log, timeout=config.duration_seconds + 90)
    session = result_json(result)
    write_json(output_dir / "session.json", session)
    require_values(session, {"run_id": config.run_id, "status": "completed", "uncertain_deliveries": 0,
                             "mode": "live", "topic": LIVE_TOPIC, "source_url": config.url}, "Collector")
    if not any(attempt.get("http_status") == 200 and attempt.get("outcome") == "ok"
               for attempt in session.get("attempts", [])):
        raise DemoError("No successful source response recorded; inspect session.json")
    print_source_evidence(session)
    return session


def save_live_analytics(config, output_dir, log, expected_rows):
    output_path = f"hdfs://namenode:9000/airtraffic/batch/live-v1/{config.run_id}/v1"
    print("Spark is calculating traffic and trajectories from this live HDFS run...", flush=True)
    batch = result_json(checked_compose([
        "run", "--rm", "batch", "--path", LIVE_ARCHIVE, "--run-id", config.run_id,
        "--output", output_path, "--max-position-age-seconds", str(config.stale_seconds),
    ], log, timeout=300))
    write_json(output_dir / "batch.json", batch)
    require_values(batch, {"run_id": config.run_id, "source_path": LIVE_ARCHIVE,
                           "archived_rows": expected_rows, "output_path": output_path}, "Live batch")
    print("Reading the saved Parquet tables back for the demonstration...", flush=True)
    saved = result_json(checked_compose([
        "run", "--rm", "batch-inspect", "--output", output_path, "--limit", "20",
    ], log))
    write_json(output_dir / "saved-results.json", saved)
    require_values(saved, {"status": "verified", "run_id": config.run_id,
                           "output_path": output_path}, "Saved live results")
    for name in ("traffic", "trajectory_segments"):
        count = batch.get("datasets", {}).get(name, {}).get("row_count")
        if type(count) is not int or count < 0:
            raise DemoError(f"Live batch has no valid row count for {name}")
        require_values(saved.get("datasets", {}).get(name, {}), {"row_count": count}, f"Saved {name}")
    return batch, saved


def run_live_demo(config: LiveConfig, output_dir: Path, *, build=True):
    output_dir.mkdir(parents=True, exist_ok=False)
    log = output_dir / "demo.log"
    print(f"Starting the live pipeline. Evidence: {output_dir}", flush=True)
    checked_compose(["--profile", "live", "up", "-d", "--build" if build else "--no-build"], log, timeout=900)
    session = collect_live_input(config, output_dir, log)
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
    batch, saved = save_live_analytics(config, output_dir, log, expected_rows)
    report_path = write_report(output_dir, session, archive, batch, saved)
    summary = {"status": "passed", "run_id": config.run_id, "acknowledged": acknowledged,
               "archived_rows": archive["rows"], "unique_kafka_offsets": archive["unique_kafka_offsets"],
               "source_url": config.url, "source_path": LIVE_ARCHIVE,
               "eligible_positions": batch["eligible_positions"],
               "traffic_groups": batch["datasets"]["traffic"]["row_count"],
               "trajectory_segments": batch["datasets"]["trajectory_segments"]["row_count"],
               "batch_output": batch["output_path"], "report": str(report_path),
               "evidence_directory": str(output_dir)}
    write_json(output_dir / "summary.json", summary)
    print(f"Verified: {expected_rows} Kafka deliveries → {archive['rows']} HDFS rows → "
          f"{summary['traffic_groups']} traffic rows + {summary['trajectory_segments']} trajectory rows.", flush=True)
    print(f"Open the presentation report: {report_path}", flush=True)
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=f"live-{uuid4().hex[:12]}")
    parser.add_argument("--bounds", nargs=4, type=float, default=(8, 102, 24, 110),
                        metavar=("SOUTH", "WEST", "NORTH", "EAST"))
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--duration-seconds", type=float, default=60)
    parser.add_argument("--interval-seconds", type=float, default=10)
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = LiveConfig(args.run_id, bounds=tuple(args.bounds), max_attempts=args.max_attempts,
                            duration_seconds=args.duration_seconds, interval_seconds=args.interval_seconds)
        output_dir = ROOT / "data/reports/live-demo" / config.run_id
        print(json.dumps(run_live_demo(config, output_dir, build=not args.no_build), sort_keys=True))
    except (OSError, ValueError, DemoError) as exc:
        print(f"Live demo failed: {exc}. Inspect data/reports/live/ and data/reports/live-demo/. Use a new run ID.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
