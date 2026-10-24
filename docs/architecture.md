# Air-traffic platform architecture

**Checkpoint version:** October 25, 2026, for the October 26 review. This note separates implemented code from the planned Lambda system. Current verification status and next steps are in [plan.md](plan.md); detailed design and acceptance criteria are in [design.md](design.md).

## Implemented data flow

```mermaid
flowchart LR
    SOURCE[Pinned OpenSky archive or tracked fixture] --> VERIFY[Checksum verification and CSV adapter]
    VERIFY --> REPLAY[Bounded, timestamp-ordered replay]
    REPLAY --> PRODUCER[Kafka producer]
    PRODUCER --> TOPIC[Kafka airtraffic.replay.v1]
    TOPIC --> ARCHIVE[Spark Structured Streaming archive]
    ARCHIVE --> BRONZE[HDFS committed Bronze Parquet]
    ARCHIVE --> CHECKPOINT[HDFS Spark checkpoint]
    BRONZE --> INSPECT[Spark batch inspector]
    BRONZE --> ANALYTICS[Spark batch analytics]
    ANALYTICS --> SUMMARY[Bounded JSON summary]
    ANALYTICS --> OUTPUT[Versioned batch Parquet and completion manifest]
    VERIFY --> PROFILE[Bounded quality profile]
```

| Component | Current role and boundary |
| --- | --- |
| [Source configuration](../data/sources/opensky_sample.json), [checkpoint fixture](../data/fixtures/checkpoint_manifest.json), and [acquisition tools](../data/README.md) | Pin and verify a June 2022 OpenSky state archive or the tracked 10-row synthetic fixture. The real archive bytes and local manifest are excluded from Git; the source configuration and aggregate profile are tracked. |
| [Observation adapter and replay](../scripts/observations.py) | Keep source snapshot, position-update, last-contact, and new receipt times distinct. Stable observation IDs identify source/aircraft/snapshot; invalid rows become rejection records. Replay sorts only a bounded archive prefix. |
| [Kafka producer](../scripts/publish_replay.py) | Publishes new replay records to the three-partition `airtraffic.replay.v1` topic with broker acknowledgments. Kafka retains the topic for seven days; one broker provides no broker failover. |
| [Spark archive](../jobs/archive_stream.py) | Reads Kafka with two-second triggers, labels envelope status, and preserves original key/payload bytes plus topic, partition, offset, and processing time. It appends every row to Parquet, including malformed or rejected records. |
| HDFS Bronze and checkpoint | Bronze path: `/airtraffic/bronze/replay-v1`, partitioned by UTC ingestion date. Query checkpoint: `/airtraffic/checkpoints/archive-replay-v1`. The [batch inspector](../jobs/inspect_archive.py) reads committed Parquet from the root and checks row and offset counts for a run. |
| [Batch analytics](../jobs/batch_analytics.py) and [analytics command](../jobs/inspect_analytics.py) | Read committed Bronze for one run, select eligible positions, and print bounded one-degree-cell/minute traffic and trajectory segment summaries as JSON. This prototype does not yet write Silver/Gold tables or publish serving views. |
| [Batch writer](../jobs/materialize_analytics.py) | Saves traffic and trajectory Parquet to a new output directory, compares read-back schemas/counts/values, then writes `_manifest.json` and root `_SUCCESS`. Five Spark storage tests pass; the first full HDFS demo timed out, so this integration remains pending. |

The batch writer reads the committed input visible when its job starts. Wait for the expected run count before writing a version. `_RESERVED` is a permanent ownership marker; only root `_SUCCESS` indicates that both datasets finished verification. Existing output directories are refused, including partial failed versions. Later input needs a new version. MongoDB/API publication remains planned.

The deployed [Compose topology](../compose.yaml) has one Kafka broker, one archive Spark process using `local[2]`, one HDFS NameNode, and two DataNode containers on the same physical host. Inspection and analytics launch short-lived Spark jobs using `local[1]`. HDFS writes data and checkpoints with replication two across those DataNodes. Named volumes retain Kafka, HDFS, and checkpoint data when services stop. The two replicas help exercise DataNode behavior; they do not survive loss of the host. This is a local integration setup, not a multi-host Kubernetes deployment.

The archive is a loss-preserving first slice. Its `envelope_status` is a parse/type label; it does not revalidate every measurement or deduplicate source observations. Spark checkpoints and the file sink's commit metadata support restarting the same query from retained Kafka offsets. Reusing the checkpoint with a different query or losing unread offsets after Kafka retention can break recovery. A producer restart or republishing a run may add source deliveries, so unique Kafka offsets and unique observations are separate measures.

The batch prototype chooses one payload for each observation ID using the smallest source-payload fingerprint before position filtering. If conflicting payloads exist, this deterministic choice can exclude an otherwise usable position. Eligible positions need coordinates and a position-update age from zero to 30 seconds. Traffic uses one representative per aircraft, UTC minute, and one-degree grid cell. Trajectories use distinct position-update times, break at UTC-day boundaries or gaps over ten minutes, and count distance only across gaps of at most 60 seconds. These are prototype rules for the checkpoint; cell size, conflict policy, coverage, and publication will need review before final products.

## Target Lambda system

```mermaid
flowchart LR
    LIVE[Bounded OpenSky live collector] --> KAFKA[Kafka observations]
    REPLAY[Historical replay] --> KAFKA
    KAFKA --> RAW[Spark raw archive → HDFS Bronze]
    KAFKA --> SPEED[Spark current state and windows]
    SPEED --> HOT[MongoDB provisional views]
    RAW --> BATCH[Spark batch clean, enrich, and aggregate]
    REFERENCE[Versioned airport reference] --> BATCH
    BATCH --> GOLD[HDFS versioned Silver and Gold]
    GOLD --> PUBLISH[Validate and publish serving version]
    PUBLISH --> FINAL[MongoDB batch views]
    HOT --> API[FastAPI reconciled reads]
    FINAL --> API
    API --> UI[Streamlit map and charts]
```

The current implementation supplies the **historical replay → Kafka → raw archive** branch and a first batch read of that archive. Live collection, MongoDB speed/serving layers, versioned batch publication, API/dashboard, and Kubernetes are planned. Batch and speed results will share source identity and business rules; the API will select a published batch result for its declared coverage and otherwise use a provisional speed result. It must not add overlapping counts. Historical trajectory gaps and stale positions must remain visible instead of being filled or presented as current.

The target has three user-facing products: latest observed positions with age, recorded trajectory segments with coverage gaps, and regional traffic summaries. The planned system describes *observed* traffic, not complete flights or complete airspace coverage. See [design.md](design.md#5-lambda-data-flow-storage-and-correctness) for state rules, publication, storage, and recovery design.

## Evidence and next integration boundary

The [data guide](../data/README.md#kafka-spark-and-hdfs) records a local 1,000-event archival read-back and a 100-event restart backlog on Linux/amd64. Both had as many unique Kafka offsets as archived rows. On October 23, an end-to-end Compose run with the tracked 10-row synthetic fixture acknowledged eight observations and two rejections. The archive inspector found ten rows and ten unique Kafka offsets; batch analytics found three eligible positions, two one-aircraft traffic groups, and two trajectory segments. The archived rows contained six distinct observation IDs because the fixture deliberately repeats source snapshot identities. These runs demonstrate the bounded archive and batch-read paths. They do not measure dashboard latency, streaming window correctness, host failure recovery, or distributed Spark executors.

The next integration boundary is a current-state Spark query with deterministic MongoDB writes and a small read API/map, followed by the bounded live collector and an independently validated airport reference. Each layer will need a small correctness fixture and an end-to-end check before its diagram edge is described as implemented.
