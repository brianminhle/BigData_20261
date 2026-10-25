# Big Data Storage and Real-Time Processing Platform for Air-Traffic Telemetry

A course project for a current aircraft map, historical trajectory segments, and regional traffic analytics.

**Status:** S1–S4 are verified locally: **historical replay or bounded live OpenSky collection → separate Kafka topics → Spark Structured Streaming → HDFS Parquet**, plus saved and verified batch traffic/trajectory tables. Next are MongoDB latest-aircraft state (S5) and the API/map (S6). Advanced analytics, full Lambda reconciliation, and Kubernetes remain planned.

The planned stack is **Lambda architecture with Kafka, Spark, HDFS/Parquet, MongoDB, FastAPI/Streamlit, and Kubernetes**. It will combine bounded live collection with historical replay and reference-data enrichment.

## First checkpoint: 26 October 2026

The [checkpoint plan](docs/plan.md) records completed work and next steps. The [architecture diagram](docs/architecture.md) distinguishes the running local pipeline from the planned Lambda components. A small synthetic fixture is included so a fresh clone can run an offline demonstration:

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
python3 -m scripts.checkpoint_demo
```

This command builds/starts Compose, publishes the tracked fixture once, waits for Spark, checks the expected counts, and saves verified batch Parquet in HDFS. It prints the evidence directory under `data/reports/checkpoint/<run-id>/`; open `summary.json`, `analytics.json`, and `batch.json` to show the result. `demo.log` contains command output if a step fails. Only a fully successful run produces `summary.json` with `status: passed`.

Verified on October 25: run `checkpoint-s3-20261025-final` passed with ten archived records, three eligible positions, two saved traffic rows, and two saved trajectory rows. A Spark comparison hang was fixed; saved rows matched after service restart. Existing output versions and incomplete publications were correctly refused. See the [progress/evidence table](docs/plan.md#verified-progress--25-october-2026).

Use `python3 -m scripts.checkpoint_demo --no-build` once images are built. Each invocation generates a fresh run ID. The fixture needs no download; initial image builds need network access. The runner leaves services running for the Spark/HDFS UIs; stop them afterward with `docker compose stop`. For a failed run, inspect the log and use a new run ID; partial delivery or output can exist.

### Live collection demonstration

```bash
python3 -m scripts.live_demo --no-build
```

This starts the additional live topic/archive, makes at most **two requests** over the Vietnam bounding box, and compares broker-acknowledged records with committed HDFS rows, unique offsets, mode, and topic. Reports are under `data/reports/live-demo/<run-id>/`; raw responses and freshness details are under `data/reports/live/<run-id>/`. Only a verified nonempty delivery produces a passing summary. It leaves the archive services running; the collector stops when its session finishes. Use `docker compose --profile live stop` afterward to stop both paths.

On October 25, two real responses returned eight observations each: **16 acknowledged → 16 archived**, zero uncertain deliveries. Each response contained three positions no older than 30 seconds and five stale positions. Live counts and availability change; keep the fixture demo as the repeatable fallback.

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

- `scripts/`: acquisition, observation contract, replay, and the checkpoint demo runner.
- `compose.yaml`, `deploy/`: local Kafka/HDFS/Spark services and pinned image builds.
- `jobs/`: streaming raw archive, batch inspection, traffic/trajectory analytics, and verified Parquet output.
- `tests/`: acquisition, normalization, replay, publication, and Spark transformation checks with synthetic observations.
- `data/`: source configuration, aggregate reference report, and acquisition guide; downloaded telemetry and generated artifacts are ignored.
- `docs/plan.md`: the single project plan, with current progress and a technical appendix; `docs/architecture.md`: system diagrams.
