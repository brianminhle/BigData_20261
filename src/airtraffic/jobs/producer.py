"""Publish a bounded OpenSky raw ADS-B Avro replay to Kafka."""

import argparse
import json
from pathlib import Path
import sys
from uuid import uuid4

from airtraffic.config import KAFKA_BOOTSTRAP_SERVERS, RAW_AVRO, RAW_TOPIC
from airtraffic.ingestion.source import replay_raw
from airtraffic.ingestion.kafka import create_producer, publish_records


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-rows", type=int, default=1000)
    parser.add_argument("--events-per-second", type=float, default=100)
    parser.add_argument("--run-id", default=f"replay-{uuid4().hex}")
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--raw-avro", type=Path, default=Path(RAW_AVRO))
    parser.add_argument("--start-record", type=int, default=0, help="Zero-based raw Avro record offset")
    args = parser.parse_args(argv)
    try:
        records = replay_raw(args.raw_avro, run_id=args.run_id, max_rows=args.max_rows,
                             start_record=args.start_record, events_per_second=args.events_per_second)
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
