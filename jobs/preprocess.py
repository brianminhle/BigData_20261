"""Publish a validated immutable Silver version from one Bronze replay run."""

import argparse
import json
import hashlib
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from airtraffic.ingestion.source import RAW_ARCHIVE
from airtraffic.preprocessing.decode import POLICY
from airtraffic.preprocessing.storage import persist_datasets, validate_output_location
from airtraffic.preprocessing.receptions import receptions, transmissions
from airtraffic.preprocessing.observations import reconstruct, states


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source", default=RAW_ARCHIVE)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("raw-adsb-silver")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.parquet.block.size", "33554432")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        validate_output_location(spark, args.source, args.output)
        bronze = (
            spark.read.parquet(args.source)
            .where(F.col("envelope.run_id") == args.run_id)
            .persist(StorageLevel.DISK_ONLY)
        )
        snapshot = input_snapshot(bronze, args.run_id)
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
        manifest = persist_datasets(
            spark,
            args.output,
            datasets,
            dict(
                run_id=args.run_id,
                source=args.source,
                **snapshot,
                unique_receptions=reception_count,
                repeated_deliveries=snapshot["input_deliveries"] - reception_count,
                valid_receptions=valid_count,
                policy_version=POLICY,
                decoder="pyModeS 2.21.1",
            ),
        )
        print(
            json.dumps(
                {
                    name: item["row_count"]
                    for name, item in manifest["datasets"].items()
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
