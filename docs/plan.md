# Project Progress: Air-Traffic Platform

**Updated:** 25 October 2026 · **First checkpoint:** 26 October 2026

Read this page to see what works, what we can demonstrate, and what to build next. Detailed technical requirements are in [design.md](design.md); the [architecture diagram](architecture.md) shows component connections.

## What are we building?

We want to answer three questions using aircraft observations:

1. Where were aircraft most recently observed? A map with position age.
2. Where did they move? Recorded trajectories with gaps shown clearly.
3. How busy was each region? Aircraft counts and movement summaries over time.

Our working pipeline already takes a small input file through Kafka, Spark, and HDFS, then calculates traffic and trajectory summaries. The map and live collection are future work.

```text
CSV input → replay → Kafka → Spark archive → HDFS raw history → Spark batch analytics
```

**Terms:** replay means sending saved observations through the pipeline again; Kafka queues those messages; Spark processes them; HDFS stores them. Bronze means the preserved raw history. Batch analytics calculates results from that stored history.

## Where we are now

“Done” means the stated local scope has been implemented and checked. It does not mean that the whole final project is complete.

| Area | Status | What this means |
| --- | --- | --- |
| Repository and runnable code | Done locally | Python tools, tests, Docker Compose, and run instructions exist. |
| Small demonstration dataset | Done | Ten tracked synthetic rows, including duplicates, missing positions, and invalid records; no data download required. |
| Real historical data | Partial | One 100.5 MiB OpenSky archive verified locally; first 100,000 rows profiled. Full coverage and larger replay remain. |
| Kafka → Spark → HDFS | Done locally | Input is archived with its original payload and Kafka offsets; restart/backlog recovery was exercised. |
| Batch traffic and trajectories | Partial | Spark calculates useful summaries; the final enriched and reconciled products remain. |
| Saved batch files and automated demo | Implemented; integration pending | Five Spark storage tests pass. The first full demo timed out during batch publication; rerun successfully before presenting saved outputs as verified. |
| Architecture | Done for checkpoint | Implemented components and planned Lambda components are documented separately. |
| Live aircraft collector | Not started | Needs a bounded OpenSky collector, source adapter, and request limits. |
| MongoDB, API, and map | Not started | Needed to expose latest positions and historical results to users. |
| Airport reference data | Not started | Needed to add airport context to observations. |
| Kubernetes and distributed Spark | Not started | Current services run on one machine; Spark uses local execution. |
| Advanced analytics and comparisons | Not started | Statistics, MLlib, GraphFrames, and measured performance comparisons remain course work. |

## Evidence we can explain

The earlier verified fixture run on 23 October used `checkpoint-20261023-01`:

| Check | Result | Meaning |
| --- | --- | --- |
| Producer acknowledgments | 8 observations + 2 rejections | Invalid source rows are retained as rejection records. |
| Archived deliveries | 10 rows, 10 unique Kafka offsets | All ten published records reached the archive. |
| Observation identities | 6 distinct IDs | The fixture deliberately repeats aircraft/snapshot identities. |
| Eligible positions | 3 | Duplicate identities, missing positions, and stale positions reduce the usable rows. |
| Traffic groups | 2 | Counts are grouped by UTC minute and one-degree geographic cell. |
| Trajectory segments | 2 | Recorded movement is grouped into segments using time-gap rules. |

The real archive checksum was rechecked on 23 October, and a new first-100,000-row profile matched [reference_profile.json](../data/reference_profile.json). Those rows contain 4,472 aircraft and 90,790 usable coordinate pairs. These are prefix counts, not full-file totals.

Earlier local recovery evidence: a 1,000-record run was archived, then a separate 100-record backlog was recovered after restarting Spark. See the [data guide](../data/README.md#kafka-spark-and-hdfs).

New code saves traffic and trajectory tables as Parquet, checks their values by reading them back, and creates a completion marker. Five Spark tests cover successful writes, empty output, overwrite protection, partial failures, and changed values. The host suite currently passes 43 tests, with 10 Spark tests skipped outside the container. The first automated run (`checkpoint-20261023-persist-01`) reached the archive and analytics stages but timed out after 180 seconds during batch publication; it has no passing `summary.json`. Completing that full run is the remaining demo check.

## Show this at the checkpoint

Run this from the repository root; Docker must be running:

```bash
python3 -m scripts.checkpoint_demo
```

The command starts Compose, verifies the fixture, publishes it once, waits for the archive, computes summaries, and saves batch Parquet results. It checks expected counts and saves JSON evidence plus logs under `data/reports/checkpoint/<run_id>/`. These generated files stay outside Git. The first build downloads images and dependencies; run it before class. Subsequent runs can use `--no-build`; each run generates a fresh ID by default.

Use `summary.json` for the overall result, `archive.json` for delivered rows, `analytics.json` for sample results, and `batch.json` for saved output paths. A failed run keeps its available logs but does not write a passing summary. See the [README](../README.md#run-the-pipeline) for individual commands and troubleshooting.

1. Show the repository and explain the working pipeline using [architecture.md](architecture.md).
2. Show the [synthetic fixture](../data/fixtures/checkpoint_states.csv) and explain why it contains bad and repeated rows.
3. Run the fixture through the pipeline; show archived counts and batch results.
4. Explain the real archive's source and bounded profile, then show the next tasks below.

The [Spark UI](http://localhost:4040) shows processing; the [HDFS UI](http://localhost:9870) shows storage and DataNodes. Services remain running after the demo. Use `docker compose stop` when finished; named volumes retain the stored data.

For an offline fallback without Docker:

```bash
python3 -m scripts.replay_sample \
  --manifest data/fixtures/checkpoint_manifest.json \
  --max-rows 10 --events-per-second 1000 --run-id checkpoint-offline \
  > /tmp/airtraffic-checkpoint.jsonl
wc -l /tmp/airtraffic-checkpoint.jsonl
```

Expected: **10 JSONL records**. This fallback demonstrates input verification and replay only. The full demonstration needs Docker and the Kafka/Spark/HDFS services.

## Next three steps, in order

| Order | Task | We can call it done when… |
| --- | --- | --- |
| 1 | Serve latest positions | A Spark query writes deterministic latest-aircraft records to MongoDB; a small API/map displays positions and their age; a fixture proves older arrivals cannot replace newer state. |
| 2 | Collect a short live session | A collector enforces a region, request budget, timeouts, and retries; live input has separate run/topic/checkpoint state; a bounded session reaches the existing pipeline. |
| 3 | Improve historical products | Airport reference data is verified, a batch product includes reference context, and the API selects a published batch result without adding overlapping provisional counts. |

Alongside these steps, check team hardware and try a tiny ARM64/K3s deployment before scheduling the full cluster. Larger data, fault injection, Spark comparisons, and course analytics follow; see the [delivery milestones](design.md#9-implementation-milestones-and-schedule).

## Keep this page up to date

After finishing a task, change its status, record the command and observed result, and update the next three steps. Keep detailed design rules in [design.md](design.md) and runnable commands in the [README](../README.md). Record a task as done only after checking its stated result.

| Date | Change | Verification |
| --- | --- | --- |
| 23 October 2026 | Added reproducible fixture and first batch analytics. | Ten archived rows; three eligible positions; two traffic groups; two trajectory segments. |
| 23 October 2026 | Reorganized documentation into this tracker and a detailed design reference. | Existing design and course requirements retained in `docs/design.md`. |
| 25 October 2026 | Added immutable batch Parquet output and an automated demo with saved evidence. | Five Spark storage tests pass; full HDFS publication/demo verification remains pending after a timeout. |

The local prototype does not yet demonstrate a complete Lambda serving system, a live map, a five-second display target, or operation across multiple hosts.
