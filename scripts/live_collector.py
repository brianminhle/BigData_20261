"""Collect a finite session with durable response evidence and delivery accounting."""

from dataclasses import asdict
import hashlib
from pathlib import Path
import time
from urllib.error import URLError

from .kafka_sink import DeliveryProgress, publish_records
from .live_adapter import normalize_response
from .live_config import LIVE_TOPIC, LiveConfig
from .observations import Observation
from .opensky_http import fetch_snapshot
from .sample_io import write_json


def freshness(records, snapshot_time, received_at, stale_seconds):
    observations = [record for record in records if isinstance(record, Observation)]
    ages = [received_at - record.position_updated_at for record in observations
            if record.position_updated_at is not None and record.latitude is not None and record.longitude is not None]
    return {
        "snapshot_age_seconds": received_at - snapshot_time,
        "positions_with_age": len(ages),
        "fresh_positions_at_receipt": sum(0 <= age <= stale_seconds for age in ages),
        "stale_positions_at_receipt": sum(age > stale_seconds for age in ages),
        "future_positions_at_receipt": sum(age < 0 for age in ages),
        "missing_positions_or_times": len(observations) - len(ages),
        "min_position_age_seconds": min(ages, default=None),
        "max_position_age_seconds": max(ages, default=None),
    }


def _save_response(response, attempt, directory):
    filename = f"response-{attempt['number']:04d}.body"
    (directory / filename).write_bytes(response.body)
    attempt.update({
        "http_status": response.status, "received_at": response.received_at,
        "rate_limit": response.headers, "response_file": filename,
        "response_sha256": hashlib.sha256(response.body).hexdigest(),
        "response_truncated": response.truncated,
    })


def _process_attempt(config, producer, directory, attempt, progress, fetch, token, timeout):
    try:
        response = fetch(config.url, timeout, token)
    except (OSError, URLError) as exc:
        attempt["error"] = f"transport:{type(exc).__name__}"
        return "retry"
    _save_response(response, attempt, directory)
    if response.status != 200:
        attempt["error"] = f"http:{response.status}"
        return "retry" if response.status >= 500 else "rate_limit" if response.status == 429 else "http_error"
    if response.truncated:
        attempt["error"] = "response_size_limit"
        return "invalid_response"
    try:
        snapshot_time, records = normalize_response(
            response.body, source_url=config.url, run_id=config.run_id,
            received_at=response.received_at, stale_seconds=config.stale_seconds,
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        attempt["error"] = f"invalid_response:{type(exc).__name__}"
        return "invalid_response"
    attempt.update({
        "snapshot_time": snapshot_time, "normalized_records": len(records),
        "freshness": freshness(records, snapshot_time, response.received_at, config.stale_seconds),
    })
    try:
        attempt["acknowledged"] = publish_records(records, producer, LIVE_TOPIC, progress=progress)
    except Exception as exc:
        # Some sends may have reached Kafka even when their acknowledgments were lost.
        attempt["error"] = f"publication:{type(exc).__name__}"
        return "publication_error"
    return "ok"


def collect_session(config: LiveConfig, producer, output_dir: Path, *, token=None,
                    fetch=fetch_snapshot, monotonic=time.monotonic, sleep=time.sleep):
    output_dir.mkdir(parents=True, exist_ok=False)
    progress = DeliveryProgress()
    report = {
        "run_id": config.run_id, "mode": "live", "topic": LIVE_TOPIC,
        "settings": asdict(config), "source_url": config.url,
        "authentication": "bearer" if token else "anonymous",
        "estimated_credit_budget": config.max_attempts * config.estimated_credits_per_attempt,
        "started_at": time.time(), "status": "running", "attempts": [],
    }

    def save():
        report.update({"attempted_deliveries": progress.attempted,
                       "acknowledged": dict(progress.acknowledged), "uncertain_deliveries": progress.uncertain,
                       "unattempted_records": sum(item.get("normalized_records", 0) for item in report["attempts"]) - progress.attempted})
        write_json(output_dir / "session.json", report)

    started = monotonic()
    deadline = started + config.duration_seconds
    next_attempt = started
    failures = 0
    stop_reason = "attempt_budget"
    outcome = "retry"
    save()
    try:
        for number in range(1, config.max_attempts + 1):
            if next_attempt >= deadline:
                stop_reason = "duration"
                break
            sleep(max(0, next_attempt - monotonic()))
            remaining = deadline - monotonic()
            if remaining <= 0:
                stop_reason = "duration"
                break
            attempt = {"number": number, "started_at": time.time()}
            report["attempts"].append(attempt)
            outcome = _process_attempt(config, producer, output_dir, attempt, progress, fetch, token,
                                       min(config.timeout_seconds, remaining))
            attempt["outcome"] = outcome
            save()
            if outcome not in ("ok", "retry"):
                stop_reason = outcome
                break
            failures = failures + 1 if outcome == "retry" else 0
            delay = max(config.interval_seconds, min(60, config.backoff_seconds * 2 ** min(failures, 6)))
            next_attempt = monotonic() + delay
        report["status"] = "completed" if outcome == "ok" else "failed"
    except KeyboardInterrupt:
        report["status"] = "interrupted"
        stop_reason = "interrupted"
    except Exception as exc:
        report["status"] = "failed"
        stop_reason = f"collector:{type(exc).__name__}"
        raise
    finally:
        report.update({"stop_reason": stop_reason, "completed_at": time.time(),
                       "elapsed_seconds": monotonic() - started})
        save()
    return report
