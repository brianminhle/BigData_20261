"""Explicit run bounds and business thresholds for reproducible batch outputs."""

import argparse
import math


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', default='s3a://airtraffic-states/stream-v1')
    parser.add_argument('--output', required=True, help='New immutable run directory')
    parser.add_argument('--start', type=int, required=True, help='Inclusive snapshot time, Unix seconds')
    parser.add_argument('--end', type=int, required=True, help='Exclusive snapshot time, Unix seconds')
    parser.add_argument('--gap-seconds', type=int, default=600)
    parser.add_argument('--max-speed-mps', type=float, default=400)
    parser.add_argument('--baseline-seconds', type=int, default=3600)
    parser.add_argument('--shuffle-partitions', type=int, default=4)
    parser.add_argument('--insecure', action='store_true')
    args = parser.parse_args(argv)
    if args.start < 0 or args.end <= args.start or args.start % 300 or args.end % 300:
        parser.error('start/end must form a positive range aligned to 5 minutes')
    if min(args.gap_seconds, args.baseline_seconds, args.shuffle_partitions) <= 0:
        parser.error('Thresholds and partition count must be positive')
    if not math.isfinite(args.max_speed_mps) or args.max_speed_mps <= 0:
        parser.error('max-speed-mps must be finite and positive')
    return args
