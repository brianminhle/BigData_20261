"""Export lake aggregates atomically for the lightweight dashboard server."""

import argparse
import json
import os
from pathlib import Path
from pyspark.sql import SparkSession
from airtraffic.serving.aggregate import build_view


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='s3a://airtraffic-states/stream-v1')
    parser.add_argument('--output', required=True)
    parser.add_argument('--hours', type=int, choices=range(1, 169), default=24)
    parser.add_argument('--insecure', action='store_true')
    args = parser.parse_args()
    builder = SparkSession.builder.appName('airtraffic-dashboard-export').config('spark.sql.session.timeZone', 'UTC')
    if args.root.startswith('s3a://'):
        settings = {'endpoint': os.environ['AIRTRAFFIC_MINIO_ENDPOINT'],
                    'access.key': os.environ['AIRTRAFFIC_MINIO_ACCESS_KEY'],
                    'secret.key': os.environ['AIRTRAFFIC_MINIO_SECRET_KEY'],
                    'path.style.access': 'true', 'connection.ssl.enabled': str(not args.insecure).lower()}
        for key, value in settings.items():
            builder = builder.config(f'spark.hadoop.fs.s3a.{key}', value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        silver = spark.read.parquet(args.root.rstrip('/') + '/silver')
        gold = spark.read.parquet(args.root.rstrip('/') + '/gold')
        view = build_view(silver, gold, args.hours)
        target = Path(args.output)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix('.tmp')
        temporary.write_text(json.dumps(view, allow_nan=False))
        temporary.replace(target)
        print(json.dumps({'windows': len(view['windows']), 'cells': len(view['cells']),
                          'latest_snapshot': view['latest_snapshot']}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
