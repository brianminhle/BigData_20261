"""Extract raw reception fields while retaining every original Kafka delivery."""

from pyspark.sql import functions as F

RAW_ENVELOPE_SCHEMA = """
schema_version INT, record_type STRING, mode STRING, run_id STRING, dataset_id STRING,
reception_id STRING, delivery_id STRING, source_sha256 STRING, source_url STRING,
record_index LONG, raw_message STRING, receiver_id INT, source_reception_time DOUBLE,
event_time DOUBLE, event_time_basis STRING, replay_released_at DOUBLE,
source_record_json STRING, source_payload_sha256 STRING, _corrupt_record STRING
"""


def archive_rows(kafka_rows):
    rows = kafka_rows.select(
        "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").alias("kafka_key"), F.col("value").alias("payload"),
        F.current_timestamp().alias("processed_at"),
        F.from_json(F.col("value").cast("string"), RAW_ENVELOPE_SCHEMA).alias("envelope"),
    )
    return rows.withColumn("ingest_date", F.to_date("processed_at")).withColumn(
        "envelope_status",
        F.when(F.col("envelope").isNull() | F.col("envelope._corrupt_record").isNotNull(), "malformed_json")
        .when(F.col("envelope.schema_version").isNull() | (F.col("envelope.schema_version") != 1), "unsupported_schema")
        .when(F.col("envelope.record_type") == "raw_reception", "raw_reception")
        .otherwise("unknown_record_type"),
    )
