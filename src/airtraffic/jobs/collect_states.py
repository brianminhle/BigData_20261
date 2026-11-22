"""Archive OpenSky snapshots in MinIO before publishing their observations."""

import argparse
import logging
import math
import os
import signal
from threading import Event
import time

from airtraffic.ingestion.kafka import create_producer
from airtraffic.ingestion.opensky_api import BoundingBox, OpenSkyClient, RateLimited
from airtraffic.ingestion.state_outbox import collect_once, publish_pending
from airtraffic.storage.snapshots import SnapshotStore


LOGGER = logging.getLogger(__name__)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=os.getenv("AIRTRAFFIC_KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"))
    parser.add_argument("--topic", default=os.getenv("AIRTRAFFIC_STATES_TOPIC", "airtraffic.states.opensky.v1"))
    parser.add_argument("--interval-seconds", type=float, default=120)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--replay-pending", action="store_true", help="Recover MinIO outbox without calling OpenSky")
    parser.add_argument("--endpoint", default=os.getenv("AIRTRAFFIC_MINIO_ENDPOINT"))
    parser.add_argument("--bucket", default=os.getenv("AIRTRAFFIC_STATES_BUCKET", "airtraffic-states"))
    parser.add_argument("--insecure", action="store_true", help="Use HTTP for local MinIO")
    parser.add_argument("--bbox", nargs=4, type=float, default=[8, 24, 102, 110],
                        metavar=("LAT_MIN", "LAT_MAX", "LON_MIN", "LON_MAX"))
    args = parser.parse_args(argv)
    if not math.isfinite(args.interval_seconds) or args.interval_seconds < 120:
        parser.error("--interval-seconds must be at least 120")
    try:
        BoundingBox(*args.bbox)
    except ValueError as error:
        parser.error(str(error))
    return args


def collect_loop(client, producer, box, topic, interval, stop, store, once=False):
    while not stop.is_set():
        started = time.monotonic()
        retry_after = 0
        try:
            collect_once(client, producer, box, topic, store)
        except RateLimited as error:
            retry_after = error.retry_after
            LOGGER.warning("OpenSky rate limited; wait at least %ss", retry_after)
            if once:
                return 1
        except Exception:
            LOGGER.exception("OpenSky collection failed")
            if once:
                return 1
        if once:
            return 0
        stop.wait(max(1, interval - (time.monotonic() - started), retry_after))
    return 0


def main(argv=None):
    args = parse_args(argv)
    client_id = os.getenv("AIRTRAFFIC_OPENSKY_CLIENT_ID")
    client_secret = os.getenv("AIRTRAFFIC_OPENSKY_CLIENT_SECRET")
    if not args.replay_pending and (not client_id or not client_secret):
        raise ValueError("Set AIRTRAFFIC_OPENSKY_CLIENT_ID and AIRTRAFFIC_OPENSKY_CLIENT_SECRET")
    access_key = os.getenv("AIRTRAFFIC_MINIO_ACCESS_KEY")
    secret_key = os.getenv("AIRTRAFFIC_MINIO_SECRET_KEY")
    if not args.endpoint or not access_key or not secret_key:
        raise ValueError("Set MinIO endpoint and AIRTRAFFIC_MINIO_ACCESS_KEY / AIRTRAFFIC_MINIO_SECRET_KEY")
    from minio import Minio
    from urllib3 import PoolManager, Timeout

    store = SnapshotStore(Minio(args.endpoint, access_key=access_key, secret_key=secret_key,
                                secure=not args.insecure,
                                http_client=PoolManager(timeout=Timeout(connect=10, read=60), retries=2)), args.bucket)
    store.ensure_bucket()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    box = BoundingBox(*args.bbox)
    LOGGER.info("Bounding box cost=%s credits/request, max %.0f credits/day at configured interval",
                box.credit_cost, box.credit_cost * 86400 / args.interval_seconds)
    client = OpenSkyClient(client_id, client_secret)
    producer = create_producer(args.bootstrap_servers)
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        if args.replay_pending:
            publish_pending(store, producer, args.topic)
            return 0
        return collect_loop(client, producer, box, args.topic, args.interval_seconds, stop, store, args.once)
    finally:
        producer.close(timeout=35)


if __name__ == "__main__":
    raise SystemExit(main())
