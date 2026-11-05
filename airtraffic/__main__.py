"""Publish a bounded OpenSky raw ADS-B Avro replay to Kafka."""

import argparse
import json
from pathlib import Path
import sys
from uuid import uuid4

from airtraffic.ingestion.source import RAW_TOPIC, replay_raw
from airtraffic.ingestion.kafka import create_producer, publish_records


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--events-per-second", type=float, default=100)
    parser.add_argument("--run-id", default=f"replay-{uuid4().hex}")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--raw-avro", type=Path, default=Path("/workspace/data/raw/raw20150421_sample.avro"))
    parser.add_argument("--start-record", type=int, default=0, help="Zero-based raw Avro record offset")
    args = parser.parse_args(argv)
    try:
        records = replay_raw(args.raw_avro, run_id=args.run_id, max_rows=args.max_rows,
                             start_record=args.start_record, events_per_second=args.events_per_second)
        producer = create_producer(args.bootstrap_servers)
        try:
            counts = publish_records(records, producer, RAW_TOPIC)
        finally:
            producer.close(timeout=35)
    except KeyboardInterrupt:
        print("Publication interrupted; some records may already be in Kafka.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Publication failed; partial delivery is possible: {exc}", file=sys.stderr)
        return 1
    print(json.dumps({"run_id": args.run_id, "topic": RAW_TOPIC, "acknowledged": counts}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
