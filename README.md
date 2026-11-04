# Big Data Storage and Real-Time Processing Platform for Air-Traffic Telemetry

A course project for a current aircraft map, historical trajectory segments, and regional traffic analytics.

**Status:** S1–S4 are verified locally with processed OpenSky State Vectors: **historical replay or bounded live collection → separate Kafka topics → Spark Structured Streaming → HDFS Parquet**, plus saved batch traffic and trajectory tables. S5 historical raw ADS-B ingestion into Kafka and HDFS is also verified locally on a bounded real sample. Raw reconstruction, serving, reconciliation, and Kubernetes remain planned.

The planned stack is **Raw-first Lambda architecture with Kafka, Spark, HDFS/Parquet, MongoDB, FastAPI/Streamlit, and Kubernetes**. Historical raw ADS-B replay is the primary input; the existing State Vector pipeline remains a separate verified checkpoint.

## Raw ADS-B ingestion (S5)

Verified on 4 November 2026: **2,100 acknowledged deliveries → 2,100 HDFS rows**, including duplicate receptions and recovery after SIGKILL. See [recorded evidence](data/verification/s5_raw_20261104.json).

Acquire the pinned 2015 sample (27.9 MB) once; the producer checks its SHA-256 on every replay:

```bash
mkdir -p data/raw
curl --fail --location 'https://raw.githubusercontent.com/openskynetwork/osky-sample/1b179c1c541b178944010a2eb6fa5189a4f9193d/avro/raw20150421_sample.avro' \
  -o data/raw/raw20150421_sample.avro
docker compose build producer
docker compose --profile raw up -d --no-build raw-archive
RUN_ID="raw-$(date +%s)"
docker compose run --no-deps --rm producer \
  --raw-avro /workspace/data/raw/raw20150421_sample.avro \
  --run-id "$RUN_ID" --max-rows 1000 --events-per-second 100
docker compose run --no-deps --rm inspect --raw-adsb \
  --path hdfs://namenode:9000/airtraffic/bronze/raw-adsb-v1 \
  --run-id "$RUN_ID" --expect-rows 1000
```

Wait for a committed streaming batch before inspection; if the expected count is not yet visible, inspect `docker compose logs raw-archive` and retry. Stop services with `docker compose --profile raw stop`; persistent volumes retain the archive and checkpoint.

The source checksum is `f8adc737ff6b2da6446bcaece62586eec91210339ece0147ca699d39555d96af`. Replay reads Avro in file order at a bounded rate, with `--start-record` (zero-based) and `--max-rows` (1–100,000). It does not reorder, filter, deduplicate, or decode messages. Republishing the same records creates additional deliveries with stable reception IDs and new delivery IDs.

Raw data uses `airtraffic.raw.replay.v1`, `/airtraffic/bronze/raw-adsb-v1`, and `/airtraffic/checkpoints/archive-raw-adsb-v1`. Restart `raw-archive` with the same output and checkpoint to resume Kafka offsets. Never share these locations with the State Vector archive.

Each envelope retains `raw_message` exactly as the Avro string and `source_record_json` with **all** source fields, including nullable receiver metadata and receiver-local clocks. `reception_id` hashes the source file SHA-256 and record index. `event_time` explicitly uses `timeAtServer` as a proxy; it is not a radio transmission timestamp. `source_reception_time`, `replay_released_at`, Kafka timestamp, and Spark processing time remain separate. Nonfinite source doubles remain in the source JSON text; searchable event-time fields become null. Bronze preserves original Kafka payload bytes, keys, partitions and offsets without a watermark or deduplication.

## First checkpoint: 26 October 2026

The [checkpoint plan](docs/plan.md) records completed work and next steps. The [architecture diagram](docs/architecture.md) distinguishes the running local pipeline from the planned Lambda components. **Present the real OpenSky demo below first.** A small synthetic fixture is also included for repeatable correctness checks without contacting the source:

For a teacher-facing explanation, code map, six-commit history, evidence, and presentation script, use the [first checkpoint guide](docs/checkpoint-guide.md). The project work order remains in the single [active plan](docs/plan.md).

```bash
python3 -m unittest discover -s tests -v
python3 -m scripts.replay_sample \
  --manifest data/fixtures/checkpoint_manifest.json \
  --max-rows 10 --events-per-second 1000 --run-id checkpoint-offline \
  > /tmp/airtraffic-checkpoint.jsonl
wc -l /tmp/airtraffic-checkpoint.jsonl
```

The replay writes 10 JSONL records: eight observations and two rejected source rows. The fixture exercises missing positions, stale positions, duplicate/conflicting identities, and invalid values. It is synthetic; the separate [OpenSky reference profile](data/reference_profile.json) describes the first 100,000 rows of a pinned real archive.

## Run the pipeline

Use Docker Engine with Compose v2, or Docker Desktop. Start with about 8 GB available to Docker and 10 GB free disk for images/builds and this small sample. Run from the repository root:

```bash
python3 -m scripts.live_demo
```

This builds/starts the services, requests real OpenSky snapshots, publishes them to Kafka, verifies the Spark/HDFS archive, and calculates and saves traffic and trajectory tables **from that live run**. It then reads the saved tables and writes `data/reports/live-demo/<run-id>/report.md` for presentation. `summary.json` records the counts; `demo.log` retains execution output. The runner declares success only after source, archive, batch, and saved-result checks pass. Initial builds need network access; use `--no-build` once images are built.

### Live collection demonstration

```bash
python3 -m scripts.live_demo --no-build
```

This makes at most **two requests** over the Vietnam bounding box. The terminal prints the actual endpoint, HTTP status, source/receipt timestamps, and position freshness. Broker-acknowledged counts must match committed HDFS rows and unique offsets in the live topic/mode. Spark then writes verified tables under `/airtraffic/batch/live-v1/<run-id>/v1` and reads them back. Open `report.md` for request evidence and result tables, or `saved-results.json` for the table rows. Raw API responses and their hashes are under `data/reports/live/<run-id>/`.

For a longer presentation session, use `python3 -m scripts.live_demo --no-build --max-attempts 6 --duration-seconds 120 --interval-seconds 10`. Requests are bounded; this does not run continuously. A short session may produce zero measured distance, and stale/missing positions can produce empty analytical tables even when ingestion succeeds. Counts come from the source. The demo never substitutes a fixture when OpenSky fails.

Each invocation generates a fresh run ID. The runner leaves archive services running for the UIs; the collector stops when its session finishes. Use `docker compose --profile live stop` afterward to stop both paths while retaining data. For a failed run, inspect `demo.log` and the collector's `session.json`, then use a new run ID; partial delivery or output can exist.

On October 25, two real responses returned eight observations each: **16 acknowledged → 16 archived**, zero uncertain deliveries. Each response contained three positions no older than 30 seconds and five stale positions. Live counts and availability change; keep the fixture demo as the repeatable fallback.

On October 26, the expanded demo passed with **19 real observations → 19 HDFS rows → 12 eligible positions → 3 saved traffic rows + 7 trajectory segments**. Both saved tables were read back. See the [measured evidence](data/verification/live_demo_20261026.json). The local presentation report is `data/reports/live-demo/live-8196ea97138b/report.md`.

### Repeatable fixture check

```bash
python3 -m scripts.checkpoint_demo --no-build
```

This uses the tracked synthetic input and checks **10 archived records → 3 eligible positions → 2 traffic rows + 2 trajectory rows**. It saves verified batch results under `/airtraffic/batch/replay-v1/<run-id>/v1`; local evidence is under `data/reports/checkpoint/<run-id>/`. Use it to demonstrate duplicate/invalid-data handling, or as an explicitly labeled offline fallback. The October 25 run `checkpoint-s3-20261025-final` passed; saved values also survived service restart. See the [progress/evidence table](docs/plan.md#verified-progress--25-october-2026).

### Manual live collection

For a custom bounded collection:

```bash
mkdir -p data/reports
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
docker compose --profile live up -d --build
RUN_ID="live-$(date +%s)"
docker compose run --rm live-collector --run-id "$RUN_ID" \
  --bounds 20 104 22 107 --max-attempts 3 \
  --duration-seconds 60 --interval-seconds 10 --timeout-seconds 10
docker compose run --rm inspect --run-id "$RUN_ID" \
  --path hdfs://namenode:9000/airtraffic/bronze/live-v1
```

Bounds are `SOUTH WEST NORTH EAST`; the example selects the Hanoi area. HTTP failures/retries count toward the same attempt limit. Duration stops new requests; an in-flight HTTP/Kafka operation has its own timeout and may finish afterward. Authentication/quota errors stop collection. Optional OAuth bearer authentication uses `OPENSKY_ACCESS_TOKEN` from your environment; obtain it through the [official OpenSky authentication instructions](https://openskynetwork.github.io/opensky-api/rest.html#authentication). Anonymous mode needs no credentials. The collector does not refresh expired tokens. See the [live data contract and limits](data/README.md#live-opensky-collection).

### Manual checkpoint steps

The tracked fixture runs through Kafka, Spark, and HDFS without downloading the real archive:

```bash
docker compose up -d --build
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
RUN_ID="checkpoint-$(date +%s)"
docker compose run --rm producer \
  --manifest /workspace/data/fixtures/checkpoint_manifest.json \
  --max-rows 10 --events-per-second 10 --run-id "$RUN_ID"
docker compose run --rm inspect --run-id "$RUN_ID" --expect-rows 10
docker compose run --rm analytics --run-id "$RUN_ID" --limit 20
docker compose run --rm batch --run-id "$RUN_ID" \
  --output "hdfs://namenode:9000/airtraffic/batch/replay-v1/$RUN_ID/v1"
docker compose run --rm batch-inspect \
  --output "hdfs://namenode:9000/airtraffic/batch/replay-v1/$RUN_ID/v1"
```

In the October 23 Linux/amd64 smoke test, Kafka acknowledged eight observations and two rejections. HDFS inspection found 10 rows with 10 unique Kafka offsets. Analytics found three eligible positions, two minute/cell traffic groups, and two trajectory segments. The `analytics` command prints a preview; `batch` saves both result tables and verifies them by reading them back. Existing output directories are refused; choose a new version for recomputation. This creates batch files, with MongoDB/API publication still planned.

### Real OpenSky archive

To try a larger real-data replay, download the pinned archive and use a new run ID:

```bash
python3 scripts/download_sample.py
docker compose up -d --build

# Match the owner of the downloaded files, including on macOS.
export LOCAL_UID="$(id -u)" LOCAL_GID="$(id -g)"
docker compose run --rm producer --run-id my-first-run --max-rows 1000 --events-per-second 100
docker compose run --rm inspect --run-id my-first-run --expect-rows 1000
docker compose run --rm analytics --run-id my-first-run --limit 20
```

The first build downloads several GB of public images and connector dependencies. The producer prints broker-acknowledged counts. Inspection reads committed Parquet from HDFS and checks row counts and unique Kafka offsets. Allow Spark to finish its current batch before inspection; if the expected count is not yet visible, check its logs and retry inspection. Use a **new run ID for a new experiment**; republishing a run adds deliveries.

- [Spark UI](http://localhost:4040): running streaming query and jobs.
- [Live Spark UI](http://localhost:4041): separate live archive when the `live` profile is enabled.
- [HDFS UI](http://localhost:9870): two DataNodes and stored files.
- `docker compose logs -f archive`: Spark logs.
- `docker compose stop`: stop the services while retaining Kafka, HDFS, and checkpoints.

On October 10, 2026, the local Linux/amd64 check archived **1,000/1,000 events**. After killing Spark and queuing another 100 events, restart archived all 100 and left the original run at 1,000 unique Kafka offsets. HDFS uses replication two on this single host; this does not demonstrate resilience to losing the host. Spark currently runs `local[2]`; distributed executors and MacBook/ARM64 runtime validation remain future work.

See the [data guide](data/README.md#kafka-spark-and-hdfs) for storage paths, recovery rules, verification commands, and limitations.

## Verification

```bash
python3 -m unittest discover -s tests -v
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit batch \
  --master 'local[1]' --driver-memory 512m tests/test_batch_storage.py -v
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit inspect \
  --master 'local[1]' --driver-memory 512m tests/test_archive_spark.py -v
```

The October 25 host run passed 64 tests and skipped 13 that require Spark. Separately, seven storage and three archive tests passed in the Spark image. These cover full fixture materialization, null/duplicate value comparison, incomplete publication, source clocks, live rejections, bounded HTTP errors, partial delivery accounting, and false-success prevention. The [tracked aggregate evidence](data/verification/s3_s4_20261025.json) records actual pipeline and restart results. Live API requests occur only when running the collector/demo, not in the test suite.

The October 26 host run passed 69 tests and skipped the same 13 Spark tests. New checks prevent a live demo from passing with missing source evidence, the wrong archive/run, or failed/mismatched saved results. The real live demo also exercised Spark through both saved table read-backs; its results are recorded above.

## Run acquisition and offline checks

Use Python 3.10 or newer from the repository root. These tools use the standard library:

```bash
python3 scripts/download_sample.py
python3 scripts/profile_sample.py
python3 -m scripts.replay_sample --max-rows 100 --events-per-second 10
python3 -m unittest discover -s tests -v
```

The downloader fetches a pinned OpenSky historical hour from June 27, 2022, approximately 100.5 MiB, and records its origin and checksum. The profiler inspects the first 100,000 rows by default; it does not measure the entire file.

JSONL replay verifies the archive, selects a bounded prefix, orders it by source snapshot time, and releases one JSON record per line at the requested rate. Original event times stay unchanged; `received_at` records the new release time. Invalid rows become explicit rejection records. The Kafka producer uses the same loader, adapter, and replay clock directly, without republishing old receipt timestamps from a saved JSONL file.

See the [data guide](data/README.md) for fields, report interpretation, download options, and troubleshooting. The [reference profile](data/reference_profile.json) records aggregate findings from the verified sample.

## Repository contents

- `scripts/`: command entry points for acquisition, replay, and checkpoint demos.
- `airtraffic/`: shared source readers, observation contract, replay, collection, and Kafka publishing logic.
- `compose.yaml`, `deploy/`: local Kafka/HDFS/Spark services and pinned image builds.
- `jobs/`: streaming raw archive, batch inspection, traffic/trajectory analytics, and verified Parquet output.
- `tests/`: acquisition, normalization, replay, publication, and Spark transformation checks with synthetic observations.
- `data/`: source configuration, aggregate reference report, and acquisition guide; downloaded telemetry and generated artifacts are ignored.
- `docs/plan.md`: the single project plan, with current progress and a technical appendix; `docs/architecture.md`: system diagrams.
