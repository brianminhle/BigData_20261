"""Event-time finalized streaming epochs with durable Spark DataFrame state."""

import json
import math
import time
from pyspark import StorageLevel
from pyspark.sql import functions as F, Window
from airtraffic.storage.silver import _filesystem, persist_datasets
from airtraffic.pipelines.batch import preprocess
from airtraffic.processing.decode import POLICY


def read_manifest(spark, path):
    filesystem, location = _filesystem(spark, path + "/_manifest.json")
    stream = filesystem.open(location)
    try:
        return json.loads(bytes(stream.readAllBytes()).decode("utf-8"))
    finally:
        stream.close()


def retained_receptions(datasets, cutoff):
    """Keep the CPR horizon plus fixed last-value summaries for each aircraft."""
    tx = datasets["transmissions"]
    order = Window.partitionBy("source_sha256", "icao24").orderBy(
        F.desc("event_time"), F.desc("transmission_id")
    )
    ids = tx.where(F.col("event_time") >= cutoff - 10).select("transmission_id")
    predicates = ["parity = 0", "parity = 1"] + [
        f"{name} is not null"
        for name in ("callsign", "speed_knots", "track_deg", "vertical_rate_fpm")
    ]
    for predicate in predicates:
        last = (
            tx.where(predicate)
            .where(F.col("event_time") < cutoff)
            .withColumn("_rank", F.row_number().over(order))
            .where("_rank = 1")
        )
        ids = ids.union(last.select("transmission_id"))
    # Retain the entire grouping bin, including ambiguous sibling transmissions.
    # Otherwise a repeated-receiver bin could become unambiguous on recovery.
    bins = tx.withColumn("_bin", F.floor(F.col("event_time") * 100))
    selected = (
        bins.join(ids.distinct(), "transmission_id")
        .select("source_sha256", "message", "_bin")
        .distinct()
    )
    ids = bins.join(selected, ["source_sha256", "message", "_bin"]).select(
        "transmission_id"
    )
    return (
        datasets["transmission_lineage"]
        .join(ids.distinct(), "transmission_id")
        .select("reception_id")
        .distinct()
    )


class StreamingEpochs:
    """One query/run owns one output directory and checkpoint; no concurrent writers."""

    def __init__(self, spark, output, run_id, allowance=30.0, expected=None):
        self.spark, self.output = spark, output.rstrip("/")
        self.contract = dict(
            run_id=run_id,
            allowance=allowance,
            expected=expected,
            policy=POLICY,
        )
        self.allowance, self.expected = allowance, expected
        self.completed = False
        self.completed_epoch = -1

    def __call__(self, batch, epoch):
        started = time.monotonic()
        batch = batch.persist(StorageLevel.DISK_ONLY)
        try:
            self.process(batch, epoch, started)
        finally:
            batch.unpersist()
            self.spark.catalog.clearCache()

    def process(self, batch, epoch, started):
        count = batch.count()
        if not count:
            return
        offsets = [
            r.asDict()
            for r in batch.groupBy("topic", "partition")
            .agg(
                F.min("offset").alias("first"),
                F.max("offset").alias("last"),
                F.count("*").alias("count"),
            )
            .orderBy("topic", "partition")
            .collect()
        ]
        target = f"{self.output}/{epoch:020d}"
        fs, root = _filesystem(self.spark, self.output)
        _, marker = _filesystem(self.spark, target + "/_SUCCESS")
        if fs.exists(marker):
            committed = read_manifest(self.spark, target)
            if (
                committed["contract"] != self.contract
                or committed["offsets"] != offsets
            ):
                raise ValueError(
                    "Committed epoch belongs to different inputs or settings"
                )
            self.completed = committed["complete"]
            self.completed_epoch = epoch
            return
        previous = []
        if fs.exists(root):
            for entry in fs.listStatus(root):
                name = entry.getPath().getName()
                if name.isdigit():
                    _, done = _filesystem(
                        self.spark, entry.getPath().toString() + "/_SUCCESS"
                    )
                    if fs.exists(done):
                        previous.append((int(name), entry.getPath().toString()))
        old = dict(cutoff=-1.0, max_event=-1.0, deliveries=0, complete=False)
        if previous:
            last_epoch, path = max(previous)
            if last_epoch >= epoch:
                raise ValueError("Checkpoint/output epoch order differs")
            old = read_manifest(self.spark, path)
            if old["contract"] != self.contract:
                raise ValueError("Cannot change the streaming contract on restart")
        # Null timestamps remain classified, but cannot participate in event-time state.
        late = batch.where(F.col("envelope.event_time") < old["cutoff"])
        accepted = batch.where(
            F.col("envelope.event_time").isNull()
            | (F.col("envelope.event_time") >= old["cutoff"])
        )
        if old["complete"]:
            late, accepted = batch, batch.limit(0)
        staging = f"{self.output}/_incoming-{epoch:020d}"
        accepted.write.mode("overwrite").parquet(staging)
        inputs = [staging] + ([path + "/state"] if previous else [])
        combined = self.spark.read.parquet(*inputs).persist(StorageLevel.DISK_ONLY)
        source_hashes = combined.select("envelope.source_sha256").distinct().take(2)
        if len(source_hashes) > 1:
            raise ValueError("One streaming run must have one source time domain")
        total = old["deliveries"] + count
        if self.expected is not None and total > self.expected and not old["complete"]:
            raise ValueError("Replay exceeded declared completion count")
        complete = old["complete"] or (
            self.expected is not None and total == self.expected
        )
        datasets = preprocess(combined)
        classified = datasets["receptions"]
        maximum = (
            classified.where(
                (F.col("event_time") > 0) & (F.col("event_time") < float("inf"))
            )
            .agg(F.max("event_time"))
            .first()[0]
        )
        maximum = max(old["max_event"], maximum if maximum is not None else -1.0)
        cutoff = max(old["cutoff"], math.floor((maximum - self.allowance) * 100) / 100)
        if complete:
            cutoff = max(cutoff, maximum + 1)
        # Finalize only closed 10 ms bins. No provisional points are published.
        output = {}
        for name in ("positions", "states", "transmissions", "reconstruction_quality"):
            output[name] = datasets[name].where(
                (F.col("event_time") >= old["cutoff"]) & (F.col("event_time") < cutoff)
            )
        output["transmission_lineage"] = datasets["transmission_lineage"].join(
            output["transmissions"].select("transmission_id"), "transmission_id"
        )
        output["receptions"] = classified.join(
            accepted.select(
                F.col("envelope.reception_id").alias("reception_id")
            ).distinct(),
            "reception_id",
        )
        output["late_deliveries"] = late
        ids = retained_receptions(datasets, cutoff)
        marked = combined.join(
            ids.withColumn("_keep", F.lit(True)),
            combined.envelope.reception_id == ids.reception_id,
            "left",
        )
        state = marked.where(
            (
                (F.col("envelope.event_time") >= cutoff)
                & (F.col("envelope.event_time") < float("inf"))
            )
            | F.col("_keep").isNotNull()
        ).drop("reception_id", "_keep")
        # Deduplicate source reception state deterministically by its original Kafka offset.
        rank = Window.partitionBy("envelope.reception_id").orderBy(
            "topic", "partition", "offset"
        )
        state = (
            state.withColumn("_rank", F.row_number().over(rank))
            .where("_rank = 1")
            .drop("_rank")
        )
        output["state"] = state.limit(0) if complete else state
        metadata = dict(
            contract=self.contract,
            offsets=offsets,
            cutoff=cutoff,
            max_event=maximum,
            deliveries=total,
            complete=complete,
            late_deliveries=late.count(),
            state_rows=output["state"].count(),
            seconds_before_write=time.monotonic() - started,
        )
        _, location = _filesystem(self.spark, target)
        if fs.exists(location):
            fs.delete(location, True)  # Only this incomplete epoch is retryable.
        persist_datasets(self.spark, target, output, metadata)
        _, staged = _filesystem(self.spark, staging)
        fs.delete(staged, True)
        self.completed = complete
        self.completed_epoch = epoch
        print(
            json.dumps(
                dict(epoch=epoch, epoch_seconds=time.monotonic() - started, **metadata),
                sort_keys=True,
            ),
            flush=True,
        )
