"""Profile a bounded prefix of the original observations, without exporting rows."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sys

if __package__:
    from .sample_io import DEFAULT_MANIFEST, FIELDS, SampleError, iter_rows, read_json, verified_source, write_json
    from .sample_values import BOOLEANS, NULLS, NUMERIC, numeric_value
else:
    from sample_io import DEFAULT_MANIFEST, FIELDS, SampleError, iter_rows, read_json, verified_source, write_json
    from sample_values import BOOLEANS, NULLS, NUMERIC, numeric_value


def utc(timestamp: float | None) -> str | None:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat() if timestamp is not None else None


def profile(path: Path, max_rows: int = 100000, stale_seconds: float = 30) -> dict:
    if max_rows <= 0 or not math.isfinite(stale_seconds) or stale_seconds < 0:
        raise SampleError("--max-rows must be positive and --stale-seconds must be finite and nonnegative")
    missing = Counter({field: 0 for field in FIELDS})
    invalid = Counter({field: 0 for field in FIELDS})
    members = Counter()
    extra_columns = set()
    aircraft = set()
    seen = {}
    previous_by_aircraft = {}
    time_min = time_max = previous_time = None
    bounds = None
    counts = Counter({key: 0 for key in (
        "malformed_rows", "rows_with_invalid_values", "rows_with_missing_values",
        "usable_identity_and_time", "usable_positions", "rows_with_missing_position",
        "duplicate_keys", "duplicates_matching_first_payload", "duplicates_conflicting_with_first_payload",
        "source_time_decreases", "per_aircraft_time_decreases",
    )})
    ages = {field: {"comparable_rows": 0, "older_than_threshold": 0, "after_observation_time": 0, "max_age_seconds": None}
            for field in ("lastposupdate", "lastcontact")}
    rows_profiled = 0
    truncated = False
    rows = iter_rows(path)
    try:
        for member, _, row in rows:
            if rows_profiled >= max_rows:
                truncated = True
                break
            rows_profiled += 1
            members[member] += 1
            extra_columns.update(key for key in row if key is not None and key not in FIELDS)
            malformed = None in row or any(row.get(field) is None for field in FIELDS)
            counts["malformed_rows"] += malformed
            parsed = {}
            row_missing = row_invalid = False
            for field in FIELDS:
                text = (row.get(field) or "").strip()
                if text.lower() in NULLS:
                    missing[field] += 1
                    row_missing = True
                    continue
                valid = True
                if field in NUMERIC:
                    value = numeric_value(field, text)
                    valid = value is not None
                    if valid:
                        parsed[field] = value
                elif field == "icao24":
                    valid = bool(re.fullmatch(r"[0-9a-fA-F]{6}", text))
                    if valid:
                        parsed[field] = text.lower()
                elif field in BOOLEANS:
                    valid = text.lower() in {"true", "false", "1", "0"}
                elif field == "squawk":
                    valid = bool(re.fullmatch(r"[0-7]{1,4}", text))
                if not valid:
                    invalid[field] += 1
                    row_invalid = True
            counts["rows_with_missing_values"] += row_missing
            counts["rows_with_invalid_values"] += row_invalid
            identifier = parsed.get("icao24")
            timestamp = parsed.get("time")
            if identifier is not None:
                aircraft.add(identifier)
            if timestamp is not None:
                time_min = timestamp if time_min is None else min(time_min, timestamp)
                time_max = timestamp if time_max is None else max(time_max, timestamp)
                counts["source_time_decreases"] += previous_time is not None and timestamp < previous_time
                previous_time = timestamp
                for field, summary in ages.items():
                    update = parsed.get(field)
                    if update is not None:
                        age = timestamp - update
                        summary["comparable_rows"] += 1
                        summary["older_than_threshold"] += age > stale_seconds
                        summary["after_observation_time"] += age < 0
                        summary["max_age_seconds"] = age if summary["max_age_seconds"] is None else max(age, summary["max_age_seconds"])
            if identifier is not None and timestamp is not None and not malformed:
                counts["usable_identity_and_time"] += 1
                previous = previous_by_aircraft.get(identifier)
                counts["per_aircraft_time_decreases"] += previous is not None and timestamp < previous
                previous_by_aircraft[identifier] = timestamp
                key = (identifier, timestamp)
                # Preserve original cell text in the fingerprint; whitespace changes count as conflicts.
                fingerprint = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).digest()
                if key in seen:
                    counts["duplicate_keys"] += 1
                    kind = "duplicates_matching_first_payload" if seen[key] == fingerprint else "duplicates_conflicting_with_first_payload"
                    counts[kind] += 1
                else:
                    seen[key] = fingerprint
            if any((row.get(field) or "").strip().lower() in NULLS for field in ("lat", "lon")):
                counts["rows_with_missing_position"] += 1
            if all(field in parsed for field in ("lat", "lon")) and not malformed:
                counts["usable_positions"] += 1
                lat, lon = parsed["lat"], parsed["lon"]
                if bounds is None:
                    bounds = {"min_lat": lat, "max_lat": lat, "min_lon": lon, "max_lon": lon}
                else:
                    bounds["min_lat"], bounds["max_lat"] = min(bounds["min_lat"], lat), max(bounds["max_lat"], lat)
                    bounds["min_lon"], bounds["max_lon"] = min(bounds["min_lon"], lon), max(bounds["max_lon"], lon)
    finally:
        rows.close()
    if not rows_profiled:
        raise SampleError("No observations found")
    return {
        "profile_version": 1,
        "scope": {"strategy": "first_rows_in_archive_order", "max_rows": max_rows,
                  "rows_profiled": rows_profiled, "truncated": truncated,
                  "total_source_rows": None if truncated else rows_profiled},
        "required_columns": list(FIELDS), "extra_columns": sorted(extra_columns),
        "member_rows_profiled": dict(members),
        "missing_by_field": dict(missing), "invalid_by_field": dict(invalid),
        "quality_counts": dict(counts),
        "distinct_aircraft_in_profile": len(aircraft),
        "distinct_aircraft_time_keys_in_profile": len(seen),
        "observation_time": {"min_epoch_seconds": time_min, "max_epoch_seconds": time_max,
                             "min_utc": utc(time_min), "max_utc": utc(time_max)},
        "position_bounds": bounds,
        "freshness": {"threshold_seconds": stale_seconds, "fields": ages},
    }


def profile_manifest(manifest_path: Path, max_rows: int, stale_seconds: float) -> dict:
    manifest, path = verified_source(manifest_path)
    report = profile(path, max_rows, stale_seconds)
    report["source"] = {key: manifest.get(key) for key in ("dataset_id", "provider", "source_url", "bytes", "sha256", "schema_url", "terms_url")}
    report["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--max-rows", type=int, default=100000, help="Maximum rows inspected (default: 100000)")
    parser.add_argument("--stale-seconds", type=float, default=30, help="Report update ages above this threshold (default: 30)")
    parser.add_argument("--output", type=Path, help="Report JSON (default: data/reports/<manifest name>.profile.json)")
    args = parser.parse_args()
    output = args.output or args.manifest.parent.parent / "reports" / f"{args.manifest.stem}.profile.json"
    print(f"Verifying the archive and inspecting up to {args.max_rows:,} rows...", flush=True)
    try:
        manifest = read_json(args.manifest)
        raw = args.manifest.parent / manifest.get("local_path", "")
        if output.resolve() in {args.manifest.resolve(), raw.resolve()}:
            raise SampleError("Report output cannot overwrite the manifest or source archive")
        report = profile_manifest(args.manifest, args.max_rows, args.stale_seconds)
        write_json(output, report)
    except (OSError, ValueError) as exc:
        print(f"Profile failed: {exc}", file=sys.stderr)
        return 1
    scope = report["scope"]
    print(f"Profiled {scope['rows_profiled']:,} rows ({'partial file' if scope['truncated'] else 'complete file'}); report: {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
