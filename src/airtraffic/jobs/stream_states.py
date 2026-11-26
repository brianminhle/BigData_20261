"""Run one Spark state-vector stage; credentials are supplied through environment."""

import argparse
import json
import os
import signal

from pyspark.sql import SparkSession
from airtraffic.ingestion.opensky_api import BoundingBox
from airtraffic.pipelines.state_stream import start_stage


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', choices=['archive', 'dedup', 'windows', 'all'], required=True)
    parser.add_argument('--output', required=True, help='Dedicated output root; never share across topics or configurations')
    parser.add_argument('--bootstrap-servers', default=os.getenv('AIRTRAFFIC_KAFKA_BOOTSTRAP_SERVERS', 'localhost:19093'))
    parser.add_argument('--topic', default=os.getenv('AIRTRAFFIC_STATES_TOPIC', 'airtraffic.states.opensky.v1'))
    parser.add_argument('--bbox', nargs=4, type=float, default=[8, 24, 102, 110])
    parser.add_argument('--watermark', choices=['5 minutes', '10 minutes', '30 minutes'], default='10 minutes')
    parser.add_argument('--available-now', action='store_true')
    parser.add_argument('--insecure', action='store_true')
    args = parser.parse_args()
    args.box = BoundingBox(*args.bbox)
    builder = (SparkSession.builder.appName(f'airtraffic-states-{args.stage}')
               .config('spark.sql.session.timeZone', 'UTC')
               .config('spark.sql.shuffle.partitions', '4'))
    if args.output.startswith('s3a://'):
        settings = {
            'endpoint': os.environ['AIRTRAFFIC_MINIO_ENDPOINT'],
            'access.key': os.environ['AIRTRAFFIC_MINIO_ACCESS_KEY'],
            'secret.key': os.environ['AIRTRAFFIC_MINIO_SECRET_KEY'],
            'path.style.access': 'true', 'connection.ssl.enabled': str(not args.insecure).lower(),
            'aws.credentials.provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
        }
        for key, value in settings.items():
            builder = builder.config(f'spark.hadoop.fs.s3a.{key}', value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        stages = ['archive', 'dedup', 'windows'] if args.stage == 'all' else [args.stage]
        queries = []
        def stop_queries(*_):
            for query in queries:
                query.stop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, stop_queries)
        for stage in stages:
            stage_args = argparse.Namespace(**vars(args))
            stage_args.stage = stage
            query = start_stage(spark, stage_args)
            queries.append(query)
            if args.available_now:
                query.awaitTermination()
        if not args.available_now:
            reported = {}
            while not spark.streams.awaitAnyTermination(30):
                for stage, query in zip(stages, queries):
                    progress = query.lastProgress
                    if progress and reported.get(stage) != progress['batchId']:
                        reported[stage] = progress['batchId']
                        print(json.dumps({'stage': stage, 'batch_id': progress['batchId'],
                                          'input_rows': progress['numInputRows'],
                                          'event_time': progress.get('eventTime', {}),
                                          'state': progress.get('stateOperators', [])}), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
