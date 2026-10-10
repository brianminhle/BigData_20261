"""Extract searchable metadata while retaining original Kafka bytes unconditionally."""

from pyspark.sql import functions as F

ENVELOPE_SCHEMA = """
schema_version INT, record_type STRING, source STRING, mode STRING, run_id STRING,
observation_id STRING, icao24 STRING, observed_at LONG, received_at DOUBLE,
position_updated_at DOUBLE, last_contact_at DOUBLE, latitude DOUBLE, longitude DOUBLE,
ground_speed_mps DOUBLE, track_deg DOUBLE, vertical_rate_mps DOUBLE,
barometric_altitude_m DOUBLE, geometric_altitude_m DOUBLE, callsign STRING,
on_ground BOOLEAN, alert BOOLEAN, spi BOOLEAN, squawk STRING,
source_payload_sha256 STRING, quality_flags ARRAY<STRING>, reasons ARRAY<STRING>,
provenance STRUCT<provider:STRING,dataset_id:STRING,source_url:STRING,
archive_sha256:STRING,member:STRING,record_index:LONG,adapter_version:STRING>,
_corrupt_record STRING
"""


def archive_rows(kafka_rows):
    rows = kafka_rows.select(
        "topic", "partition", "offset", F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").alias("kafka_key"), F.col("value").alias("payload"),
        F.current_timestamp().alias("processed_at"),
        F.from_json(F.col("value").cast("string"), ENVELOPE_SCHEMA).alias("envelope"),
    )
    return rows.withColumn("ingest_date", F.to_date("processed_at")).withColumn(
        "envelope_status",
        F.when(F.col("envelope").isNull() | F.col("envelope._corrupt_record").isNotNull(), "malformed_json")
        .when(F.col("envelope.schema_version").isNull() | (F.col("envelope.schema_version") != 1), "unsupported_schema")
        .when(F.col("envelope.record_type") == "rejected", "source_rejected")
        .when(F.col("envelope.record_type") == "observation", "observation")
        .otherwise("unknown_record_type"),
    )
