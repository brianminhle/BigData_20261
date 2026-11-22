"""Shared Bronze snapshot and Silver reconstruction transforms."""

import hashlib
import json

from pyspark import StorageLevel
from pyspark.sql import functions as F

from airtraffic.processing.decode import POLICY
from airtraffic.storage.silver import persist_datasets, validate_output_location
from airtraffic.processing.receptions import receptions, transmissions
from airtraffic.processing.observations import reconstruct, states


def input_snapshot(bronze, run_id):
    count = bronze.count()
    if not count:
        raise ValueError("No input deliveries")
    offsets = [
        r.asDict()
        for r in bronze.groupBy("topic", "partition")
        .agg(
            F.min("offset").alias("first"),
            F.max("offset").alias("last"),
            F.count("*").alias("count"),
        )
        .orderBy("topic", "partition")
        .collect()
    ]
    if bronze.select("topic", "partition", "offset").distinct().count() != count:
        raise ValueError("Repeated Kafka offsets in Bronze snapshot")
    input_files = sorted(bronze.inputFiles())
    snapshot = hashlib.sha256(
        json.dumps(
            dict(run_id=run_id, files=input_files, offsets=offsets), sort_keys=True
        ).encode()
    ).hexdigest()
    return dict(
        input_deliveries=count,
        offsets=offsets,
        input_files=input_files,
        input_snapshot_id=snapshot,
    )


def preprocess(bronze):
    delivery, classified = receptions(bronze)
    classified = classified.persist(StorageLevel.DISK_ONLY)
    valid = classified.where("status = 'valid'")
    tx, lineage = transmissions(valid)
    tx = tx.persist(StorageLevel.DISK_ONLY)
    positions, quality = reconstruct(tx)
    members = lineage.groupBy("transmission_id").agg(
        F.sort_array(F.collect_set("reception_id")).alias("target_receptions")
    )
    positions = positions.join(members, "transmission_id")
    partners = members.select(
        F.col("transmission_id").alias("partner_id"),
        F.col("target_receptions").alias("partner_receptions"),
    )
    positions = (
        positions.join(partners, "partner_id")
        .withColumn(
            "source_reception_ids",
            F.sort_array(F.array_union("target_receptions", "partner_receptions")),
        )
        .drop("target_receptions", "partner_receptions")
        .persist(StorageLevel.DISK_ONLY)
    )
    datasets = dict(
        deliveries=delivery,
        receptions=classified,
        rejected=classified.where("status != 'valid'"),
        transmissions=tx,
        transmission_lineage=lineage,
        positions=positions,
        states=states(tx, positions),
        reconstruction_quality=quality,
    )
    return datasets


def run_batch(spark, source_path, output_path, run_id):
    """Reconstruct, validate and publish one immutable Silver version."""
    validate_output_location(spark, source_path, output_path)
    bronze = (
        spark.read.parquet(source_path)
        .where(F.col("envelope.run_id") == run_id)
        .persist(StorageLevel.DISK_ONLY)
    )
    snapshot = input_snapshot(bronze, run_id)
    datasets = preprocess(bronze)
    classified = datasets["receptions"]
    valid = classified.where("status = 'valid'")
    lineage = datasets["transmission_lineage"]
    positions = datasets["positions"]
    quality = datasets["reconstruction_quality"]
    tx = datasets["transmissions"]
    reception_count = classified.count()
    valid_count = valid.count()
    if lineage.count() != valid_count:
        raise ValueError(
            "Transmission lineage does not cover every valid reception"
        )
    if positions.count() != positions.select("position_id").distinct().count():
        raise ValueError("Canonical position identities are not unique")
    if quality.count() != tx.where("category = 'position'").count():
        raise ValueError("Missing reconstruction outcomes")
    return persist_datasets(
        spark,
        output_path,
        datasets,
        dict(
            run_id=run_id,
            source=source_path,
            **snapshot,
            unique_receptions=reception_count,
            repeated_deliveries=snapshot["input_deliveries"] - reception_count,
            valid_receptions=valid_count,
            policy_version=POLICY,
            decoder="pyModeS 2.21.1",
        ),
    )
