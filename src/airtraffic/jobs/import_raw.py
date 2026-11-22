"""Poll a raw-file source and archive complete Avro objects in MinIO."""

import argparse
from dataclasses import asdict
import json
import logging
import math
import os
import re
import signal
from threading import Event

from airtraffic.ingestion.catalog import s3_files, sample_files
from airtraffic.ingestion.importer import poll


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["opensky-raw", "opensky-sample", "s3"],
                        default="opensky-raw")
    parser.add_argument("--source-endpoint", help="Public S3 origin, e.g. https://s3.example.org")
    parser.add_argument("--source-bucket")
    parser.add_argument("--source-prefix", default="raw/")
    parser.add_argument("--dataset", default="external_raw")
    parser.add_argument("--interval-seconds", type=float, default=300)
    parser.add_argument("--max-file-bytes", type=int, default=1024 ** 3)
    parser.add_argument("--work-dir", help="Temporary download directory; one file at a time")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--discover-only", action="store_true", help="List sources without writing to MinIO")
    parser.add_argument("--endpoint", default=os.getenv("AIRTRAFFIC_MINIO_ENDPOINT"))
    parser.add_argument("--bucket", default=os.getenv("AIRTRAFFIC_MINIO_BUCKET", "airtraffic-raw"))
    parser.add_argument("--insecure", action="store_true", help="Use HTTP to destination MinIO (default HTTPS)")
    args = parser.parse_args(argv)
    if not math.isfinite(args.interval_seconds) or args.interval_seconds <= 0:
        parser.error("--interval-seconds must be finite and positive")
    if args.max_file_bytes <= 0:
        parser.error("--max-file-bytes must be positive")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.dataset):
        parser.error("--dataset must contain only letters, digits, underscores and hyphens")
    if args.source == "s3" and not (args.source_endpoint and args.source_bucket):
        parser.error("--source s3 requires --source-endpoint and --source-bucket")
    if not args.discover_only and not args.endpoint:
        parser.error("Provide --endpoint host:port or AIRTRAFFIC_MINIO_ENDPOINT")
    return args


def discover(args):
    if args.source == "opensky-sample":
        return sample_files()
    if args.source == "opensky-raw":
        return s3_files("https://s3.opensky-network.org", "data-samples", "raw/", "opensky_raw")
    return s3_files(args.source_endpoint, args.source_bucket, args.source_prefix, args.dataset)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.source == "opensky-sample":
        logging.warning("Historical sample from 2015; polling cannot create fresh live data")
    if args.discover_only:
        files = discover(args)
        print(json.dumps({"source": args.source, "files": [asdict(item) for item in files],
                          "file_count": len(files)}, indent=2))
        return 0

    from minio import Minio
    from urllib3 import PoolManager, Timeout
    from airtraffic.storage.raw import RawObjectStore

    access_key = os.getenv("AIRTRAFFIC_MINIO_ACCESS_KEY")
    secret_key = os.getenv("AIRTRAFFIC_MINIO_SECRET_KEY")
    if not access_key or not secret_key:
        raise ValueError("Set AIRTRAFFIC_MINIO_ACCESS_KEY and AIRTRAFFIC_MINIO_SECRET_KEY")
    client = Minio(args.endpoint, access_key=access_key, secret_key=secret_key,
                   secure=not args.insecure,
                   http_client=PoolManager(timeout=Timeout(connect=10, read=60), retries=2))
    store = RawObjectStore(client, args.bucket)
    store.ensure_bucket()
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    return poll(lambda: discover(args), store, interval=args.interval_seconds, stop=stop,
                once=args.once, max_bytes=args.max_file_bytes, work_dir=args.work_dir)


if __name__ == "__main__":
    raise SystemExit(main())
