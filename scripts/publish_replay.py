"""Publish bounded State Vector or raw ADS-B historical replay to separate Kafka topics."""

import argparse
import json
from pathlib import Path
import sys
from uuid import uuid4

from airtraffic.raw_adsb import RAW_TOPIC, replay_raw
from airtraffic.kafka_sink import create_producer, publish_records
from airtraffic.replay import replay_records, validate_replay_settings
from airtraffic.replay_source import load_replay_source
from airtraffic.sample_io import DEFAULT_MANIFEST


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--events-per-second", type=float, default=100)
    parser.add_argument("--stale-seconds", type=float, default=30)
    parser.add_argument("--run-id", default=f"replay-{uuid4().hex}")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic")
    parser.add_argument("--raw-avro", type=Path, help="Pinned OpenSky raw Avro sample")
    parser.add_argument("--start-record", type=int, default=0, help="Zero-based raw Avro record offset")
    args = parser.parse_args(argv)
    try:
        validate_replay_settings(args.max_rows, args.events_per_second, args.stale_seconds, args.run_id)
        args.topic = args.topic or (RAW_TOPIC if args.raw_avro else "airtraffic.replay.v1")
        if args.raw_avro:
            records = replay_raw(args.raw_avro, run_id=args.run_id, max_rows=args.max_rows,
                                 start_record=args.start_record, events_per_second=args.events_per_second)
        else:
            selected, source = load_replay_source(args.manifest, args.max_rows)
            records = replay_records(selected, source, run_id=args.run_id,
                                     events_per_second=args.events_per_second, stale_seconds=args.stale_seconds)
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
