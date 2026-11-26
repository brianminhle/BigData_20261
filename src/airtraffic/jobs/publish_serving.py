"""Single-writer periodic publisher; partial failures keep the previous view active."""

import argparse
import logging
import signal
from threading import Event

from airtraffic.storage.spark_session import lake_session
from airtraffic.serving.lake_reader import serving_view
from airtraffic.serving.mongo import ServingStore, connect_database


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='s3a://airtraffic-states/stream-v1')
    parser.add_argument('--analytics-prefix', default='s3a://airtraffic-states/analytics')
    parser.add_argument('--hours', type=int, choices=range(1, 169), default=24)
    parser.add_argument('--interval-seconds', type=int, default=120)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--insecure', action='store_true')
    args = parser.parse_args()
    if args.interval_seconds < 30:
        parser.error('interval-seconds must be at least 30')
    logging.basicConfig(level=logging.INFO)
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    database = connect_database()
    store = ServingStore(database)
    spark = lake_session('airtraffic-serving-publisher', args.insecure)
    try:
        store.ensure_indexes()
        while not stop.is_set():
            try:
                view = serving_view(spark, args.root, args.analytics_prefix, args.hours)
                generation = store.publish(view)
                store.prune()
                logging.info('Published generation=%s windows=%d cells=%d movement=%d',
                             generation, len(view['windows']), len(view['cells']), len(view['movement']))
            except Exception as error:
                # Error type only: database connection errors can contain credentials/addresses.
                logging.error('Serving publication failed (%s); previous generation remains active', type(error).__name__)
                if args.once:
                    raise SystemExit(1) from None
            if args.once:
                break
            stop.wait(args.interval_seconds)
    finally:
        database.client.close()
        spark.stop()


if __name__ == '__main__':
    main()
