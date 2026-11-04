"""Readable source evidence and saved analytical results for a live presentation."""

from datetime import datetime, timezone
from pathlib import Path


def utc_time(timestamp):
    if timestamp is None:
        return "unavailable"
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec="seconds")


def print_source_evidence(session):
    for attempt in session["attempts"]:
        freshness = attempt.get("freshness", {})
        print(
            f"Request {attempt['number']}: HTTP {attempt.get('http_status', 'unavailable')}, "
            f"source time {utc_time(attempt.get('snapshot_time'))}, "
            f"received {utc_time(attempt.get('received_at'))}, "
            f"{attempt.get('normalized_records', 0)} records; "
            f"{freshness.get('fresh_positions_at_receipt', 0)} fresh / "
            f"{freshness.get('stale_positions_at_receipt', 0)} stale positions at receipt.", flush=True,
        )


def table(columns, rows):
    def cell(value):
        return str(value).replace("|", "\\|").replace("\n", " ")

    return ["| " + " | ".join(columns) + " |", "| " + " | ".join("---" for _ in columns) + " |", *[
        "| " + " | ".join(cell(value) for value in row) + " |" for row in rows
    ]]


def write_report(output_dir: Path, session, archive, batch, saved):
    lines = [
        "# Live OpenSky demonstration", "",
        f"Run: `{session['run_id']}`. This run requested the OpenSky REST API directly.", "",
        "OpenSky → Python collector → Kafka → Spark Structured Streaming → HDFS → Spark batch → saved tables.", "",
        f"Source: {session['source_url']}", "",
        "## Source requests", "",
    ]
    lines.extend(table(
        ["Request", "HTTP", "Source snapshot (UTC)", "Received (UTC)", "Records", "Fresh / stale at receipt"],
        [[attempt["number"], attempt.get("http_status", "unavailable"),
          utc_time(attempt.get("snapshot_time")), utc_time(attempt.get("received_at")),
          attempt.get("normalized_records", 0),
          f"{attempt.get('freshness', {}).get('fresh_positions_at_receipt', 0)} / "
          f"{attempt.get('freshness', {}).get('stale_positions_at_receipt', 0)}"]
         for attempt in session["attempts"]],
    ))
    lines.extend([
        "", "HTTP response bodies and SHA-256 hashes are retained in "
        f"`data/reports/live/{session['run_id']}/`. [Session details](session.json) include freshness and quota headers.",
        "", "## Delivery and computation", "",
    ])
    lines.extend(table(["Measurement", "Result"], [
        ["Kafka topic", session["topic"]],
        ["Kafka acknowledged observations", session["acknowledged"].get("observation", 0)],
        ["Kafka acknowledged rejections", session["acknowledged"].get("rejected", 0)],
        ["Committed HDFS rows", archive["rows"]],
        ["Unique Kafka offsets in HDFS", archive["unique_kafka_offsets"]],
        ["Eligible positions after deduplication and filtering", batch["eligible_positions"]],
        ["Saved traffic rows", batch["datasets"]["traffic"]["row_count"]],
        ["Saved trajectory segments", batch["datasets"]["trajectory_segments"]["row_count"]],
    ]))
    lines.extend([
        "", f"Archive: `{batch['source_path']}`", "", f"Saved results: `{batch['output_path']}`", "",
        "Spark streaming archives the messages. A separate Spark batch job then calculates the tables below. "
        "Both tables are read back after writing; a separate reader checks the completed version.", "",
        f"Analytics requires positions no older than {batch['max_position_age_seconds']:g} seconds at the "
        "source snapshot time. Receipt freshness above is measured against our collection clock. "
        "Source counts, duplicate snapshots, and stale positions can change between runs.", "",
        "## Saved traffic preview (up to 20 rows)", "",
    ])
    traffic = saved["datasets"]["traffic"]["rows"]
    lines.extend(table(["Minute (UTC)", "Latitude cell", "Longitude cell", "Aircraft", "Mean speed (m/s)"], [
        [utc_time(row["minute_start"]), row["latitude_cell"], row["longitude_cell"],
         row["aircraft_count"], round(row["mean_ground_speed_mps"], 2)
         if row["mean_ground_speed_mps"] is not None else "unavailable"] for row in traffic
    ]) if traffic else ["No traffic rows: no positions passed the analytics filters."])
    lines.extend(["", "## Saved trajectory preview (up to 20 rows)", ""])
    segments = saved["datasets"]["trajectory_segments"]["rows"]
    lines.extend(table(["Aircraft (ICAO24)", "Segment", "Position updates", "Observed distance (km)", "Unmeasured edges"], [
        [row["icao24"], row["segment_number"], row["position_updates"],
         round(row["observed_distance_km"], 4), row["unmeasured_edges"]] for row in segments
    ]) if segments else ["No trajectory rows: no positions passed the analytics filters."])
    lines.extend([
        "", "A short session may contain only one position update per aircraft, yielding zero measured distance. "
        "These are observed segments, not complete flights. A successful ingestion run can have empty analytical "
        "tables when all positions are stale or missing.", "",
        "## What to say", "",
        '“We requested aircraft observations directly from OpenSky. The request table shows when the provider '
        'observed them and when we received them. Kafka acknowledged our messages; Spark streamed the same '
        'number into HDFS. Spark then calculated and saved these traffic and trajectory tables from that live run. '
        'MongoDB latest state and the map are our next stages.”', "",
        "Evidence: [archive](archive.json), [batch manifest](batch.json), "
        "[saved table rows](saved-results.json), [execution log](demo.log).", "",
    ])
    path = output_dir / "report.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path
