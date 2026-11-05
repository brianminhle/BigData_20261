# Raw ADS-B data pipeline

This project processes pinned OpenSky historical raw ADS-B receptions through **Kafka → Spark Structured Streaming → HDFS Bronze → Spark Batch → HDFS Silver**. S5 ingestion, S6 batch preprocessing and S7 streaming reconstruction are verified locally. Serving, corrections, and distributed deployment remain later milestones in [the plan](docs/plan.md).

## Run S5

The sample is 27.9 MB. Download it into the ignored `data/raw/` directory:

```bash
mkdir -p data/raw
curl --fail --location 'https://raw.githubusercontent.com/openskynetwork/osky-sample/1b179c1c541b178944010a2eb6fa5189a4f9193d/avro/raw20150421_sample.avro' \
  -o data/raw/raw20150421_sample.avro
docker compose up -d --build
RUN_ID="raw-$(date +%s)"
docker compose run --rm producer --run-id "$RUN_ID" \
  --max-rows 1000 --events-per-second 100
docker compose run --rm inspect --run-id "$RUN_ID" --expect-rows 1000
```

The producer verifies the pinned SHA-256 (`f8adc737ff6b2da6446bcaece62586eec91210339ece0147ca699d39555d96af`) before releasing records. It supports `--start-record` and `--max-rows` for bounded file-order replay. If inspection runs before Spark commits the batch, wait briefly and retry. `docker compose stop` stops services without deleting HDFS or Kafka volumes.

The Kafka topic is `airtraffic.raw.replay.v1`. Bronze is `/airtraffic/bronze/raw-adsb-v1` in HDFS; its Spark checkpoint is `/airtraffic/checkpoints/archive-raw-adsb-v1`. The Parquet rows retain Kafka payload bytes and offsets, plus parsed source identity and clocks. All Avro fields, including nullable receiver metadata, remain in `source_record_json`; `raw_message` preserves the original message text. Replaying a reception keeps its `reception_id` and creates a new `delivery_id`.

A verified run archived [2,100 deliveries representing 1,100 unique receptions](data/verification/s5_raw_20261104.json), including replay after archive process termination. The recorded run used a bounded prefix of the five-minute sample. No CPR decoding or deduplication is performed in S5.

## Code layout

[Repository structure](docs/repository-structure.md) maps every active file to the source, ingestion, preprocessing, and storage flow.

## Batch preprocessing (S6)

Build the updated image once (`docker compose build producer`). A Silver version is produced from one retained Bronze run:

```bash
docker compose run --rm batch --run-id YOUR_BRONZE_RUN \
  --output hdfs://namenode:9000/airtraffic/silver/YOUR_VERSION
```

The output must be new. Consumers must require the **root** `_SUCCESS` and `_manifest.json`; individual table markers do not publish a version. The manifest records the Bronze file snapshot, selected run and Kafka offset ranges, input count, snapshot fingerprint, decoder and policy version. Every table is read back and compared before publishing completion. The local batch service uses a 1,536 MiB JVM heap within a 3 GiB container, disk-backed caches and 32 MiB Parquet row groups. Keep the source archive immutable during processing. Malformed source identities or conflicting content under one reception ID fail the version instead of silently selecting a record.

Tables: `deliveries` maps Kafka offsets to source reception IDs; `receptions` retains one classified row per reception; `rejected` retains invalid, unsupported and unverified receptions with distinct statuses; `transmissions` and `transmission_lineage` preserve conservative candidate grouping; `positions` contains canonical points and source reception IDs; `states` adds timestamped attributes; `reconstruction_quality` records every eligible position event, including unresolved cases. Reception rows retain the full source JSON.

Policy `raw-silver-v1` uses pyModeS 2.21.1. It decodes CRC-valid DF17 airborne barometric positions, identification and ground velocity. Other formats are unsupported; SBS-3 frames with zero parity are unverified, not declared corrupt. Transmission candidates group exact payloads only inside a fixed 10 ms server-time bin and only when each member has a different receiver. Bins with repeated receiver IDs remain separate and ambiguous. Bin boundaries can split a transmission; these are candidates, not proven radio transmissions.

Spark windows select the latest preceding opposite-parity candidate within 10 seconds, ordered by `(event_time, transmission_id)` per source file and aircraft. There is no fallback after a zone mismatch. Each target candidate has at most one canonical point; its ID hashes policy and target identity. pyModeS chooses the odd frame when times tie. Altitude is barometric feet; velocity is knots, track degrees and vertical rate feet/minute. Callsign expires after 300 seconds and velocity attributes after 30 seconds. Stale values become null while last-update timestamps and source IDs remain visible. Missing attributes remain null. States are emitted at reconstructed position times.

The complete local sample was replayed with `--run-id s6-full-2015 --max-rows 304131 --events-per-second 20000`, inspected with `--expect-rows 304131`, and processed into `/airtraffic/silver/full-2015-v3`. Two full batch runs produced identical rows in all eight tables: **304,131 receptions → 46,244 canonical positions for 254 aircraft**. After the code was reorganized, a new full version at `/airtraffic/silver/full-2015-refactor-v1` matched `/airtraffic/silver/full-2015-v3` in all eight tables; the bounded run also matched its earlier version. Every Silver source record was compared to the original Avro. See [the S6 results](docs/plan.md#s6--batch-reconstruction-as-the-correctness-reference-verified-locally) for accounting and limitations.

Run the small correctness fixtures against the actual Spark transforms:

```bash
docker compose run --no-deps --rm -v "$PWD/tests:/workspace/tests:ro" \
  --entrypoint /opt/spark/bin/spark-submit batch \
  --master 'local[1]' --driver-memory 512m tests/test_silver.py
```

## Stateful streaming preprocessing (S7)

The `stream` tool reads the existing Kafka topic and runs the S6 transforms on incoming receptions plus durable retained state. Each query selects one run and one source file. Start it with a new version:

```bash
docker compose run --no-deps --rm stream --run-id s6-full-2015 \
  --version YOUR_STREAM_VERSION --watermark-seconds 30 \
  --expected-deliveries 304131 --max-offsets 100000 --available-now
```

Omit `--available-now` for continuous consumption. Omit `--expected-deliveries` for an open-ended run; the final watermark tail then remains pending. A declared completion count includes duplicate and late deliveries and must describe a closed replay. It flushes the tail without artificial telemetry. `--starting-offsets` optionally accepts Kafka offset JSON for a new query. Existing checkpoints take precedence on restart.

S7 uses application-managed event-time state in `foreachBatch`, not Spark's native state-store operators. A committed epoch advances a monotonic watermark from the selected run's maximum finite positive source timestamp minus the allowance, rounded down to a 10 ms bin boundary. The previous epoch's watermark rejects late inputs; the new boundary finalizes closed bins. Thus arrivals within one micro-batch are processed together. No provisional positions are published. Unrelated runs are filtered before event-time progress; multiple source hashes in one run are rejected.

Live state retains pending receptions, the 10-second CPR horizon, and the last pre-watermark frame for each parity and measured attribute per aircraft. This preserves S6's expired-partner reasons and stale attribute timestamps. State is bounded per aircraft by the pending window and fixed summaries, not by total query lifetime; an unbounded number of distinct aircraft can still grow the summary population. Replay completion clears live state. Historical epoch files remain on HDFS for audit/recovery and are not automatically pruned.

Output and checkpoint are separate children of `/airtraffic/streaming/<version>/`. Each `epochs/<epoch>/` contains positions, states, transmissions, transmission lineage, reception classifications, late deliveries, retained state and a manifest. Require each epoch's root `_SUCCESS` before reading it. Reception classifications can recur across epochs; finalized canonical positions do not. Canonical table schemas remain compatible with S6; the manifest binds the run, policy and watermark contract. Bronze independently archives all raw deliveries, including late data.

The sink commits output and retained state together. Retry removes only the current incomplete epoch; a completed epoch with matching contract and Kafka offset accounting is a no-op. Use one writer and its original checkpoint per version. This is a tested idempotent epoch boundary, not a general distributed transaction guarantee. Epoch manifests expose watermark, cumulative deliveries, late count, state rows and pre-write time; logs also report total epoch duration. These are not Spark native `stateOperators` metrics.

Service integration tests (existing Kafka and HDFS must be running):

```bash
docker compose run --no-deps --rm -v "$PWD/tests:/workspace/tests:ro" \
  --entrypoint /opt/spark/bin/spark-submit stream --master 'local[2]' \
  --driver-memory 1536m --conf spark.jars.ivy=/opt/spark/ivy \
  --conf spark.jars.ivySettings=/opt/spark/ivysettings.xml \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 tests/test_streaming.py
```

Verified S7 full output: `/airtraffic/streaming/s7-full-v3`, with 304,131 deliveries across four epochs and 46,244 positions. Positions, states, transmissions, lineage and quality tables matched S6 by full-row comparison. Peak live state was 34,187 reception rows; completion cleared it. Detailed results and limitations are recorded in [the plan](docs/plan.md).
