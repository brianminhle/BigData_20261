"""Validate raw receptions and group conservative transmission candidates."""

from pyspark.sql import functions as F, Window
from airtraffic.preprocessing.decode import DECODE_SCHEMA, POLICY, decode


def receptions(bronze):
    deliveries = bronze.select("topic", "partition", "offset", "payload", "envelope.*")
    if deliveries.where("reception_id is null or source_sha256 is null").take(1):
        raise ValueError("Bronze contains an envelope without source identity")
    if (
        deliveries.groupBy("reception_id")
        .agg(F.countDistinct("source_record_json").alias("variants"))
        .where("variants > 1")
        .take(1)
    ):
        raise ValueError("Conflicting content for one reception ID")
    order = Window.partitionBy("reception_id").orderBy("topic", "partition", "offset")
    unique = (
        deliveries.withColumn("_rank", F.row_number().over(order))
        .where("_rank = 1")
        .drop(
            "_rank",
            "payload",
            "delivery_id",
            "run_id",
            "replay_released_at",
            "topic",
            "partition",
            "offset",
        )
    )
    if unique.where(
        "record_index < 0 or receiver_id is null or source_record_json is null"
    ).take(1):
        raise ValueError("Missing source record provenance")
    identity = F.sha2(
        F.concat_ws(":", "source_sha256", F.col("record_index").cast("string")), 256
    )
    if unique.where(~F.col("reception_id").eqNullSafe(identity)).take(1):
        raise ValueError("Invalid stable reception identity")
    classified = unique.withColumn(
        "decoded",
        F.udf(decode, DECODE_SCHEMA)("raw_message", "event_time", "source_record_json"),
    )
    classified = classified.select("*", "decoded.*").drop("decoded", "_corrupt_record")
    return (
        deliveries.select(
            "topic", "partition", "offset", "reception_id", "delivery_id"
        ),
        classified,
    )


def transmissions(valid):
    # Fixed bins cannot chain indefinitely. A repeated receiver makes the whole bin ambiguous.
    keyed = valid.withColumn("message", F.upper("raw_message")).withColumn(
        "bin", F.floor(F.col("event_time") * 100).cast("long")
    )
    counts = keyed.groupBy("source_sha256", "message", "bin").agg(
        F.count("*").alias("members"), F.countDistinct("receiver_id").alias("receivers")
    )
    keyed = keyed.join(counts, ["source_sha256", "message", "bin"]).withColumn(
        "ambiguous", F.col("members") != F.col("receivers")
    )
    keyed = keyed.withColumn(
        "transmission_id",
        F.when(F.col("ambiguous"), F.col("reception_id")).otherwise(
            F.sha2(
                F.concat_ws(":", F.lit(POLICY), "source_sha256", "message", "bin"), 256
            )
        ),
    )
    lineage = keyed.select(
        "transmission_id", "reception_id", "receiver_id", "event_time", "ambiguous"
    )
    order = Window.partitionBy("transmission_id").orderBy("event_time", "reception_id")
    representatives = (
        keyed.withColumn("_rank", F.row_number().over(order))
        .where("_rank = 1")
        .drop("_rank", "members", "receivers", "bin")
    )
    return representatives, lineage

