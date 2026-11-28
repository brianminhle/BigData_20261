"""Bounded acceptance on frozen real data; outputs never enter the serving prefix."""

import argparse
import json
import math
import os
import time
from collections import Counter
from urllib.parse import urlparse

from minio import Minio
from pyspark.sql import functions as F

from airtraffic.analytics.config import parse_args
from airtraffic.pipelines.analytics_batch import run_analytics, write_text
from airtraffic.storage.spark_session import lake_session


def read_json(spark, path):
    return json.loads('\n'.join(row.value for row in spark.read.text(path).collect()))


def compare_value(left, right):
    if isinstance(left, float) and isinstance(right, float):
        return (math.isnan(left) and math.isnan(right)) or math.isclose(left, right, rel_tol=1e-9, abs_tol=1e-9)
    return left == right


def event_metrics(root):
    uri = urlparse(root)
    client = Minio(os.environ['AIRTRAFFIC_MINIO_ENDPOINT'],
                   access_key=os.environ['AIRTRAFFIC_MINIO_ACCESS_KEY'],
                   secret_key=os.environ['AIRTRAFFIC_MINIO_SECRET_KEY'], secure=False)
    results = []
    for obj in client.list_objects(uri.netloc, prefix=uri.path.lstrip('/') + '/events/', recursive=True):
        if obj.object_name.endswith('/') or obj.object_name.endswith('.inprogress'):
            continue
        response = client.get_object(uri.netloc, obj.object_name)
        try:
            events = [json.loads(line) for line in response.read().decode().splitlines() if line]
        finally:
            response.close()
            response.release_conn()
        tasks = Counter()
        totals = Counter()
        started = ended = None
        master = None
        for event in events:
            kind = event['Event']
            if kind == 'SparkListenerApplicationStart':
                started = event['Timestamp']
            elif kind == 'SparkListenerApplicationEnd':
                ended = event['Timestamp']
            elif kind == 'SparkListenerEnvironmentUpdate':
                master = event['Spark Properties'].get('spark.master')
            elif kind == 'SparkListenerTaskEnd':
                if event['Task End Reason']['Reason'] != 'Success':
                    totals['failed_task_attempts'] += 1
                    continue
                tasks[event['Task Info']['Executor ID']] += 1
                metrics = event['Task Metrics']
                for key in ('Executor Run Time', 'Executor CPU Time', 'Memory Bytes Spilled', 'Disk Bytes Spilled'):
                    totals[key] += metrics.get(key, 0)
                totals['Input Bytes'] += metrics.get('Input Metrics', {}).get('Bytes Read', 0)
                shuffle = metrics.get('Shuffle Read Metrics', {})
                totals['Shuffle Read Bytes'] += shuffle.get('Remote Bytes Read', 0) + shuffle.get('Local Bytes Read', 0)
                totals['Shuffle Write Bytes'] += metrics.get('Shuffle Write Metrics', {}).get('Shuffle Bytes Written', 0)
                totals['Peak Task Execution Memory'] = max(totals['Peak Task Execution Memory'], metrics.get('Peak Execution Memory', 0))
        if started is not None:
            assert ended is not None, 'Event log has no completed application'
            results.append({'master': master, 'duration_ms': ended - started,
                            'successful_tasks_by_executor': dict(tasks), 'metrics': dict(totals)})
    distributed = [result for result in results if result['master'].startswith('k8s://')]
    assert len(distributed) == 1, 'Expected one distributed application event log'
    assert len(distributed[0]['successful_tasks_by_executor']) >= 2, 'Both executors must execute tasks'
    assert 'driver' not in distributed[0]['successful_tasks_by_executor'], 'Distributed tasks ran on driver'
    assert len([result for result in results if result['master'] == 'local[2]']) == 1
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['snapshot', 'local', 'distributed', 'compare'])
    parser.add_argument('--source', default='s3a://airtraffic-states/stream-v1')
    parser.add_argument('--root', required=True, help='New acceptance prefix, outside production analytics')
    options = parser.parse_args()
    root = options.root.rstrip('/')
    spark = lake_session('airtraffic-batch-acceptance-' + options.phase, insecure=True)
    try:
        if options.phase == 'snapshot':
            fs = spark._jvm.org.apache.hadoop.fs.Path(root).getFileSystem(spark._jsc.hadoopConfiguration())
            assert not fs.exists(spark._jvm.org.apache.hadoop.fs.Path(root)), 'Acceptance prefix already exists'
            tables = {}
            for table in ('silver', 'gold'):
                # Reading via Spark respects the streaming sink's committed-file metadata.
                frame = spark.read.parquet(options.source.rstrip('/') + '/' + table)
                frame.repartition(4).write.mode('errorifexists').parquet(root + '/snapshot/' + table)
                tables[table] = spark.read.parquet(root + '/snapshot/' + table).count()
            assert tables['silver'] > 0 and tables['gold'] > 0, 'Real input tables must be nonempty'
            bounds = spark.read.parquet(root + '/snapshot/silver').agg(F.min('snapshot_time'), F.max('snapshot_time')).first()
            manifest = {'source': options.source, 'tables': tables,
                        'start': bounds[0] // 300 * 300, 'end': (bounds[1] // 300 + 1) * 300,
                        'data_kind': 'frozen real observations; no synthetic expansion'}
            fs.mkdirs(spark._jvm.org.apache.hadoop.fs.Path(root + '/events'))
            write_text(spark, root + '/snapshot.json', json.dumps(manifest, indent=2))
            print(json.dumps(manifest), flush=True)
        elif options.phase in ('local', 'distributed'):
            master = spark.sparkContext.master
            assert master == 'local[2]' if options.phase == 'local' else master.startswith('k8s://')
            manifest = read_json(spark, root + '/snapshot.json')
            args = parse_args(['--root', root + '/snapshot', '--output', root + '/' + options.phase,
                               '--start', str(manifest['start']), '--end', str(manifest['end']), '--insecure'])
            started = time.monotonic()
            report = run_analytics(spark, args)
            evidence = {'master': master, 'application_id': spark.sparkContext.applicationId,
                        'analytics_seconds': time.monotonic() - started, 'report': report}
            write_text(spark, root + '/' + options.phase + '/execution.json', json.dumps(evidence, indent=2))
            print(json.dumps(evidence), flush=True)
        else:
            local = read_json(spark, root + '/local/report.json')
            distributed = read_json(spark, root + '/distributed/report.json')
            assert local == distributed, 'Report counts differ'
            for name, count in local['counts'].items():
                assert count <= 100000, 'Acceptance comparison is bounded; use a scalable comparison for larger inputs'
                left = spark.read.parquet(root + '/local/' + name)
                right = spark.read.parquet(root + '/distributed/' + name)
                assert left.schema.simpleString() == right.schema.simpleString(), 'Schema mismatch: ' + name
                left_rows = left.orderBy(*left.columns).collect()
                right_rows = right.orderBy(*left.columns).collect()
                assert len(left_rows) == len(right_rows) == count, 'Count mismatch: ' + name
                assert all(all(compare_value(a, b) for a, b in zip(l, r)) for l, r in zip(left_rows, right_rows)), 'Value mismatch: ' + name
            result = {'status': 'PASS', 'comparison': 'all rows and columns; float rtol/atol=1e-9',
                      'report': local, 'applications': event_metrics(root)}
            write_text(spark, root + '/acceptance.json', json.dumps(result, indent=2))
            print('BATCH_DISTRIBUTED_ACCEPTANCE_PASS ' + json.dumps(result), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
