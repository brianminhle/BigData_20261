"""Generate observations, batch window reference and quality report locally."""

import argparse
import json
from airtraffic.pipelines.state_batch import process_snapshots
from airtraffic.storage.state_connection import connect_store


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint')
    parser.add_argument('--bucket')
    parser.add_argument('--insecure', action='store_true')
    parser.add_argument('--output', required=True, help='New output directory; must not already exist')
    parser.add_argument('--expected-interval', type=int, default=120)
    args = parser.parse_args(argv)
    if args.expected_interval <= 0:
        parser.error('--expected-interval must be positive')
    store = connect_store(args.endpoint, args.bucket, args.insecure)
    store.ensure_bucket()
    report = process_snapshots(store, args.output, args.expected_interval)
    print(json.dumps({key: value for key, value in report.items() if key != 'sources'}, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
