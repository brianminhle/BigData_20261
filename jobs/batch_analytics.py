"""Batch traffic and trajectory summaries from committed replay archive rows."""

from pyspark.sql import Window, functions as F


def eligible_positions(archive_rows, run_id, max_position_age_seconds=30):
    """Choose one payload per source snapshot and retain fresh, located positions."""
    candidates = archive_rows.where(
        (F.col("envelope_status") == "observation")
        & (F.col("envelope.run_id") == run_id)
        & F.col("envelope.observation_id").isNotNull()
    )
    snapshot_order = Window.partitionBy("envelope.observation_id").orderBy(
        F.col("envelope.source_payload_sha256").asc_nulls_last(),
        F.col("topic"), F.col("partition"), F.col("offset"),
    )
    unique = candidates.withColumn("snapshot_rank", F.row_number().over(snapshot_order)).where(
        F.col("snapshot_rank") == 1
    )
    positions = unique.select(
        F.col("envelope.run_id").alias("run_id"),
        F.col("envelope.observation_id").alias("observation_id"),
        F.col("envelope.source_payload_sha256").alias("source_payload_sha256"),
        F.col("envelope.icao24").alias("icao24"),
        F.col("envelope.observed_at").alias("observed_at"),
        F.col("envelope.position_updated_at").alias("position_updated_at"),
        F.col("envelope.latitude").alias("latitude"),
        F.col("envelope.longitude").alias("longitude"),
        F.when(
            (F.col("envelope.ground_speed_mps") >= 0)
            & (F.col("envelope.ground_speed_mps") < float("inf"))
            & ~F.isnan("envelope.ground_speed_mps"),
            F.col("envelope.ground_speed_mps"),
        ).alias("ground_speed_mps"),
    )
    position_age = F.col("observed_at") - F.col("position_updated_at")
    return positions.where(
        F.col("icao24").isNotNull()
        & (F.col("observed_at") > 0)
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & ~F.isnan("latitude")
        & ~F.isnan("longitude")
        & F.col("position_updated_at").isNotNull()
        & ~F.isnan("position_updated_at")
        & position_age.between(0, max_position_age_seconds)
    )


def minute_traffic(positions):
    """Count one representative per aircraft, minute, and one-degree cell."""
    located = positions.withColumn(
        "minute_start", (F.floor(F.col("observed_at") / 60) * 60).cast("long")
    ).withColumn(
        "latitude_cell", F.least(F.floor("latitude"), F.lit(89)).cast("int")
    ).withColumn(
        "longitude_cell", F.least(F.floor("longitude"), F.lit(179)).cast("int")
    )
    key = ["run_id", "minute_start", "latitude_cell", "longitude_cell", "icao24"]
    latest = Window.partitionBy(*key).orderBy(
        F.col("observed_at").desc(),
        F.col("position_updated_at").desc(),
        F.col("source_payload_sha256").asc_nulls_last(),
    )
    representatives = located.withColumn("representative_rank", F.row_number().over(latest)).where(
        F.col("representative_rank") == 1
    )
    return representatives.groupBy(*key[:-1]).agg(
        F.count("*").alias("aircraft_count"),
        F.count("ground_speed_mps").alias("speed_sample_count"),
        F.avg("ground_speed_mps").alias("mean_ground_speed_mps"),
    )


def _distance_km(latitude, longitude, previous_latitude, previous_longitude):
    latitude_delta = F.radians(latitude - previous_latitude)
    longitude_delta = F.radians(longitude - previous_longitude)
    haversine = (
        F.pow(F.sin(latitude_delta / 2), 2)
        + F.cos(F.radians(latitude)) * F.cos(F.radians(previous_latitude))
        * F.pow(F.sin(longitude_delta / 2), 2)
    )
    return 12742.0 * F.asin(F.sqrt(F.least(haversine, F.lit(1.0))))


def trajectory_segments(positions, segment_gap_seconds=600, distance_gap_seconds=60):
    """Summarize observed position updates; never bridge long or cross-day gaps."""
    update_order = Window.partitionBy("run_id", "icao24", "position_updated_at").orderBy(
        F.col("observed_at").desc(), F.col("source_payload_sha256").asc_nulls_last()
    )
    points = positions.withColumn("update_rank", F.row_number().over(update_order)).where(
        F.col("update_rank") == 1
    )
    aircraft_order = Window.partitionBy("run_id", "icao24").orderBy("position_updated_at")
    points = (points
              .withColumn("previous_update_at", F.lag("position_updated_at").over(aircraft_order))
              .withColumn("previous_latitude", F.lag("latitude").over(aircraft_order))
              .withColumn("previous_longitude", F.lag("longitude").over(aircraft_order)))
    gap_seconds = F.col("position_updated_at") - F.col("previous_update_at")
    same_utc_day = (F.floor(F.col("position_updated_at") / 86400)
                    == F.floor(F.col("previous_update_at") / 86400))
    points = points.withColumn(
        "segment_start",
        F.when(F.col("previous_update_at").isNull() | ~same_utc_day | (gap_seconds > segment_gap_seconds), 1)
        .otherwise(0),
    ).withColumn(
        "edge_distance_km",
        F.when(same_utc_day & (gap_seconds <= distance_gap_seconds)
               & (gap_seconds <= segment_gap_seconds), _distance_km(
            F.col("latitude"), F.col("longitude"), F.col("previous_latitude"), F.col("previous_longitude")
        )),
    )
    through_point = aircraft_order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    points = points.withColumn("segment_number", F.sum("segment_start").over(through_point))
    return points.groupBy("run_id", "icao24", "segment_number").agg(
        F.count("*").alias("position_updates"),
        F.min("position_updated_at").alias("first_position_at"),
        F.max("position_updated_at").alias("last_position_at"),
        F.sum(F.coalesce("edge_distance_km", F.lit(0.0))).alias("observed_distance_km"),
        F.sum(F.when(F.col("previous_update_at").isNotNull()
                     & (F.col("segment_start") == 0)
                     & F.col("edge_distance_km").isNull(), 1).otherwise(0)).alias("unmeasured_edges"),
    )
