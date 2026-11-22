"""Runtime defaults for the current local HDFS deployment."""

import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("AIRTRAFFIC_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RAW_TOPIC = os.getenv("AIRTRAFFIC_RAW_TOPIC", "airtraffic.raw.replay.v1")
RAW_ARCHIVE = os.getenv(
    "AIRTRAFFIC_RAW_ARCHIVE", "hdfs://namenode:9000/airtraffic/bronze/raw-adsb-v1"
)
RAW_CHECKPOINT = os.getenv(
    "AIRTRAFFIC_RAW_CHECKPOINT",
    "hdfs://namenode:9000/airtraffic/checkpoints/archive-raw-adsb-v1",
)
STREAMING_ROOT = os.getenv(
    "AIRTRAFFIC_STREAMING_ROOT", "hdfs://namenode:9000/airtraffic/streaming"
)
RAW_AVRO = os.getenv("AIRTRAFFIC_RAW_AVRO", "/workspace/data/raw/raw20150421_sample.avro")
