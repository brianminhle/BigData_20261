"""Schedule bounded analytics over the most recent Silver observations."""

import argparse
import logging
import signal
from threading import Event
import uuid
from pyspark.sql import functions as F

from airtraffic.analytics.config import parse_args
from airtraffic.pipelines.analytics_batch import run_analytics
from airtraffic.storage.spark_session import lake_session


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='s3a://airtraffic-states/stream-v1')
    parser.add_argument('--output-prefix', default='s3a://airtraffic-states/analytics')
    parser.add_argument('--hours', type=int, choices=range(1,169), default=24)
    parser.add_argument('--interval-seconds', type=int, default=0, help='0 runs once')
    parser.add_argument('--insecure', action='store_true')
    options = parser.parse_args()
    if options.interval_seconds != 0 and options.interval_seconds < 300:
        parser.error('Use 0 or an interval of at least 300 seconds')
    stop = Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    spark = lake_session('airtraffic-recent-analytics', options.insecure)
    previous = None
    try:
        while not stop.is_set():
            try:
                stats = spark.read.parquet(options.root.rstrip('/')+'/silver').agg(F.max('snapshot_time'),F.count('*')).first()
                signature = tuple(stats)
                if stats[0] is not None and signature != previous:
                    end = (stats[0] // 300 + 1) * 300
                    arguments = ['--root',options.root,'--output', options.output_prefix.rstrip('/')+'/recent-'+uuid.uuid4().hex,
                                 '--start',str(max(0,end-options.hours*3600)),'--end',str(end)]
                    if options.insecure:
                        arguments.append('--insecure')
                    run_analytics(spark,parse_args(arguments))
                    previous = signature
                    logging.warning('Recent analytics complete latest_snapshot=%s observations=%s', *signature)
            except Exception as error:
                logging.error('Recent analytics failed (%s)',type(error).__name__)
                if not options.interval_seconds:
                    raise SystemExit(1) from None
            if not options.interval_seconds:
                break
            stop.wait(options.interval_seconds)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
