# Big Data Storage and Real-Time Processing Platform for Air-Traffic Telemetry

A course project for a current aircraft map, historical trajectory segments, and regional traffic analytics.

**Status:** The local pipeline works: **historical replay → Kafka → Spark Structured Streaming → HDFS Parquet**. It includes two HDFS DataNodes, persistent checkpoints, recovery checks, and read-only Spark batch traffic and trajectory summaries over the archive. Live collection, MongoDB serving, the dashboard, advanced analytics, and Kubernetes remain planned.

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

### Checkpoint fixture

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
```

In the October 23 Linux/amd64 smoke test, Kafka acknowledged eight observations and two rejections. HDFS inspection found 10 rows with 10 unique Kafka offsets. The batch command found three eligible positions, two minute/cell traffic groups, and two trajectory segments. It reads committed Bronze Parquet and prints JSON; it does not publish a serving dataset.

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
- [HDFS UI](http://localhost:9870): two DataNodes and stored files.
- `docker compose logs -f archive`: Spark logs.
- `docker compose stop`: stop the services while retaining Kafka, HDFS, and checkpoints.

On October 10, 2026, the local Linux/amd64 check archived **1,000/1,000 events**. After killing Spark and queuing another 100 events, restart archived all 100 and left the original run at 1,000 unique Kafka offsets. HDFS uses replication two on this single host; this does not demonstrate resilience to losing the host. Spark currently runs `local[2]`; distributed executors and MacBook/ARM64 runtime validation remain future work.

See the [data guide](data/README.md#kafka-spark-and-hdfs) for storage paths, recovery rules, verification commands, and limitations.

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

- `scripts/`: acquisition, shared value rules, observation contract, historical adapter, and replay commands.
- `compose.yaml`, `deploy/`: local Kafka/HDFS/Spark services and pinned image builds.
- `jobs/`: streaming raw archive, batch inspection, and read-only traffic/trajectory analytics using Spark.
- `tests/`: acquisition, normalization, replay, publication, and Spark transformation checks with synthetic observations.
- `data/`: source configuration, aggregate reference report, and acquisition guide; downloaded telemetry and generated artifacts are ignored.
