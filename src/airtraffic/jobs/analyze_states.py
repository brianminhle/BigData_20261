"""Execute bounded aircraft analytics against Silver and Gold lake tables."""

import json
from airtraffic.analytics.config import parse_args
from airtraffic.pipelines.analytics_batch import run_analytics
from airtraffic.storage.spark_session import lake_session


def main():
    args = parse_args()
    spark = lake_session('airtraffic-analytics', args.insecure, args.shuffle_partitions)
    try:
        print(json.dumps(run_analytics(spark, args)), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
