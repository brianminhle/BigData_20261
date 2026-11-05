# Repository structure

This is the active Raw ADS-B codebase through S7. Read the files in data-flow order: **source → Kafka ingestion → HDFS Bronze → batch preprocessing → HDFS Silver**. S1–S4 State Vector prototype code is available in Git history and is not part of the active runtime.

## Files and responsibilities

```text
BigData_20261/
├── README.md                         Quick start, commands, and processing policy
├── compose.yaml                      Kafka, HDFS, Spark, and one-shot tool services
├── .gitignore                        Local data, caches, and generated-file exclusions
├── .dockerignore                     Files excluded from container build context
├── airtraffic/                       Application logic
│   ├── __init__.py                    Python package marker
│   ├── __main__.py                    Producer CLI: python -m airtraffic
│   ├── ingestion/
│   │   ├── __init__.py                Python package marker
│   │   ├── source.py                  Pinned Avro source, identities, provenance, replay
│   │   ├── kafka.py                   Broker-acknowledged Kafka publication
│   │   └── bronze.py                  Kafka record → loss-preserving Bronze row
│   └── preprocessing/
│       ├── __init__.py                Python package marker
│       ├── decode.py                  Validation and pyModeS decoding policy
│       ├── receptions.py              Delivery handling and receiver candidate grouping
│       ├── observations.py            Temporal CPR positions and aircraft states
│       ├── streaming.py               Stateful epochs, watermark, late data and recovery
│       └── storage.py                 Silver read-back checks and version publication
├── jobs/                             Spark entrypoints
│   ├── archive.py                     Continuous Kafka → Bronze archive
│   ├── inspect_bronze.py              Bronze count and offset inspection
│   ├── stream.py                      Kafka → finalized streaming observations
│   └── preprocess.py                  Bronze → Silver batch orchestration
├── tests/
│   ├── test_streaming.py              Kafka/HDFS state and retry integration tests
│   └── test_silver.py                 Small fixtures for real pipeline transforms
├── deploy/
│   ├── hdfs/
│   │   ├── Dockerfile                 HDFS container image
│   │   ├── core-site.xml              Hadoop filesystem connection settings
│   │   ├── hdfs-site.xml              HDFS storage and replication settings
│   │   └── start.sh                   NameNode/DataNode startup
│   └── spark/
│       ├── Dockerfile                 Spark image and pinned Python dependencies
│       └── ivysettings.xml            Spark Kafka connector resolution
├── data/
│   ├── raw/                           Local Avro sample; ignored by Git
│   └── verification/
│       └── s5_raw_20261104.json       Committed S5 recovery/count evidence
└── docs/
    ├── plan.md                        Milestones, scope, and S5/S6 evidence
    └── repository-structure.md        This guide
```

`AGENTS.md` and `Guideline.md` are local workspace guidance files. They are not application modules and are not currently tracked by Git. Python caches and Docker/HDFS generated files are omitted from the tree.

## How a reception moves through the code

| Step | Code | Result |
| --- | --- | --- |
| Read and replay | `airtraffic/ingestion/source.py`, `airtraffic/__main__.py` | Check the pinned Avro file; emit source receptions with stable IDs and separate source/replay times. |
| Publish | `airtraffic/ingestion/kafka.py` | Wait for broker acknowledgments on `airtraffic.raw.replay.v1`. |
| Archive | `airtraffic/ingestion/bronze.py`, `jobs/archive.py` | Spark Structured Streaming appends every Kafka delivery to Bronze Parquet and resumes from its checkpoint. |
| Clean and reconstruct | `airtraffic/preprocessing/decode.py`, `receptions.py`, `observations.py`, `jobs/preprocess.py` | Classify receptions, link deliveries and receiver candidates, decode supported frames, build canonical positions and timestamped states. |
| Publish Silver | `airtraffic/preprocessing/storage.py` | Write and read back eight Parquet tables; create root `_SUCCESS` only after successful checks. |

Bronze preserves raw deliveries, including repeated or incomplete receptions. Cleaning happens in the S6 batch job: it records statuses and reasons, then uses eligible receptions for reconstruction. It does not rewrite Bronze or invent missing aircraft attributes.

## Where data lives

| Data | Location | In Git? |
| --- | --- | --- |
| Pinned 2015 Avro sample | `data/raw/raw20150421_sample.avro` | No; ignored local input |
| Kafka replay | `airtraffic.raw.replay.v1` in the Compose broker | No; Docker volume |
| Raw Bronze archive | `hdfs://namenode:9000/airtraffic/bronze/raw-adsb-v1` | No; HDFS volume |
| Archive recovery checkpoint | `hdfs://namenode:9000/airtraffic/checkpoints/archive-raw-adsb-v1` | No; HDFS volume |
| Versioned Silver output | `hdfs://namenode:9000/airtraffic/silver/<version>` | No; HDFS volume |
| S5 verification summary | `data/verification/s5_raw_20261104.json` | Yes |

Each completed Silver version contains `deliveries`, `receptions`, `rejected`, `transmissions`, `transmission_lineage`, `positions`, `states`, and `reconstruction_quality`, plus `_manifest.json` and root `_SUCCESS`. Consumers should require the root marker. The verified full versions are `full-2015-v3` and `full-2015-refactor-v1`; the refactor produced identical rows in all eight tables. They contain 304,131 source receptions and 46,244 canonical positions for 254 aircraft.

## Runtime and commands

`compose.yaml` starts one Kafka broker, one HDFS NameNode, two DataNodes, and a continuously running Spark archive. `producer`, `inspect`, `batch`, and `stream` are one-shot tool services. Both DataNodes and Spark run on the same local host; this is not a multi-host deployment.

```bash
docker compose up -d --build
docker compose run --rm producer --run-id example-raw --max-rows 1000 --events-per-second 100
docker compose run --rm inspect --run-id example-raw --expect-rows 1000
docker compose run --rm batch --run-id example-raw \
  --output hdfs://namenode:9000/airtraffic/silver/example-v1
```

The archive may need a moment to commit the replay before inspection. Choose a **new** Silver output path for every batch run. The producer requires the ignored local sample; the [README](../README.md) explains how to obtain and verify it. The existing [plan](plan.md) records completed stages and future work.

## Current boundary

S5 raw ingestion, S6 batch preprocessing and S7 streaming preprocessing are implemented. The repository does not yet contain MongoDB serving, an API or dashboard, trajectory analytics, Lambda correction, or Kubernetes deployment.

S7 stores dedicated checkpoints and committed state/output epochs under `/airtraffic/streaming/<version>/`. See the [README S7 section](../README.md#stateful-streaming-preprocessing-s7) for finalization, retention and recovery semantics.
