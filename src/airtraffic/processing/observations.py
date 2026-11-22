"""Temporal CPR reconstruction and timestamped aircraft states."""

from pyspark.sql import functions as F, Window
from airtraffic.processing.decode import POLICY, position

def reconstruct(transmissions):
    ordered = (
        Window.partitionBy("source_sha256", "icao24")
        .orderBy("event_time", "transmission_id")
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    frames = transmissions.where("category = 'position'")
    item = F.struct("raw_message", "event_time", "transmission_id")
    frames = frames.withColumn(
        "_even",
        F.last(F.when(F.col("parity") == 0, item), ignorenulls=True).over(ordered),
    )
    frames = frames.withColumn(
        "_odd",
        F.last(F.when(F.col("parity") == 1, item), ignorenulls=True).over(ordered),
    )
    frames = frames.withColumn(
        "partner", F.when(F.col("parity") == 0, F.col("_odd")).otherwise(F.col("_even"))
    )
    frames = frames.withColumn(
        "solution",
        F.udf(position, "latitude double, longitude double, outcome string")(
            "raw_message", "partner.raw_message", "event_time", "partner.event_time"
        ),
    )
    quality = frames.select(
        "transmission_id",
        "icao24",
        "event_time",
        F.col("partner.transmission_id").alias("partner_id"),
        "solution.*",
    ).withColumn("policy_version", F.lit(POLICY))
    positions = frames.where("solution.outcome = 'decoded'").select(
        "source_sha256",
        "icao24",
        "event_time",
        "transmission_id",
        F.col("partner.transmission_id").alias("partner_id"),
        "altitude_ft",
        "ambiguous",
        "solution.latitude",
        "solution.longitude",
    )
    positions = positions.withColumn(
        "position_id", F.sha2(F.concat_ws(":", F.lit(POLICY), "transmission_id"), 256)
    ).withColumn("policy_version", F.lit(POLICY))
    return positions, quality


def states(transmissions, positions):
    order = (
        Window.partitionBy("source_sha256", "icao24")
        .orderBy("event_time", "transmission_id")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    events = transmissions
    fields = ["callsign", "speed_knots", "track_deg", "vertical_rate_fpm"]
    for field in fields:
        events = events.withColumn(
            "_" + field,
            F.last(
                F.when(
                    F.col(field).isNotNull(),
                    F.struct(
                        F.col(field).alias("value"),
                        F.col("event_time").alias("updated_at"),
                        F.col("transmission_id").alias("source_id"),
                    ),
                ),
                ignorenulls=True,
            ).over(order),
        )
    joined = positions.join(
        events.select("transmission_id", *["_" + f for f in fields]), "transmission_id"
    )
    for field in fields:
        ttl = 300 if field == "callsign" else 30
        fresh = F.col("event_time") - F.col("_" + field + ".updated_at") <= ttl
        joined = (
            joined.withColumn(field, F.when(fresh, F.col("_" + field + ".value")))
            .withColumn(field + "_updated_at", F.col("_" + field + ".updated_at"))
            .withColumn(field + "_source_id", F.col("_" + field + ".source_id"))
            .withColumn(
                field + "_status",
                F.when(F.col("_" + field).isNull(), "missing")
                .when(fresh, "fresh")
                .otherwise("stale"),
            )
            .drop("_" + field)
        )
    return joined
