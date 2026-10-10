"""Kafka to append-only HDFS Parquet, with a persistent streaming checkpoint."""

import argparse
import signal

from pyspark.sql import SparkSession

from archive_schema import archive_rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="airtraffic.replay.v1")
    parser.add_argument("--output", default="hdfs://namenode:9000/airtraffic/bronze/replay-v1")
    parser.add_argument("--checkpoint", default="hdfs://namenode:9000/airtraffic/checkpoints/archive-replay-v1")
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    if args.output.rstrip("/") == args.checkpoint.rstrip("/"):
        parser.error("Output and checkpoint must be different directories")
    spark = (SparkSession.builder.appName("airtraffic-raw-archive")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.hadoop.dfs.replication", "2")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        incoming = (spark.readStream.format("kafka")
                    .option("kafka.bootstrap.servers", args.bootstrap_servers)
                    .option("subscribe", args.topic).option("startingOffsets", "earliest")
                    .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 10000).load())
        writer = (archive_rows(incoming).writeStream.format("parquet").outputMode("append")
                  .partitionBy("ingest_date").option("path", args.output)
                  .option("checkpointLocation", args.checkpoint).queryName("raw_replay_archive"))
        writer = writer.trigger(availableNow=True) if args.available_now else writer.trigger(processingTime="2 seconds")
        query = writer.start()
        signal.signal(signal.SIGTERM, lambda *_: query.stop())
        query.awaitTermination()
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
