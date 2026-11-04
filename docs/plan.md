# Project Plan — Raw ADS-B Air Traffic Reconstruction & Analytics

**Updated:** 4 November 2026

**Active direction:** **Raw-first Lambda Architecture** using historical Raw Mode S/ADS-B messages for **both Spark Streaming (Kafka replay) and Spark Batch (HDFS reprocessing)**.

**Implementation status:** S5 raw Avro → Kafka → Spark → HDFS Bronze is verified locally on a bounded real sample (4 November 2026). S6–S11 remain planned. The October 26 State Vector checkpoint is retained in [checkpoint-guide.md](checkpoint-guide.md).

> **One-sentence project goal:** Build a replay-based air-traffic analytics platform that reconstructs observed aircraft positions from raw ADS-B receptions, delivers fast *provisional* traffic information, and recomputes/republishes affected history when late or conflicting input changes the result.

**Next milestone: S6 — define and implement canonical batch preprocessing/reconstruction.** S5 evidence is recorded in [s5_raw_20261104.json](../data/verification/s5_raw_20261104.json); runnable ingestion commands are in the [README](../README.md#raw-ads-b-ingestion-s5).

## 1. What the system will actually deliver

This is a **historical replay and data-quality analytics platform**, not a global live radar or operational air-traffic-control service. A user selects a recorded dataset/run and a simulation time. The platform shows what can be reconstructed from the captured raw messages, and explains the limits of those observations.

| Product | Question answered | Visible output and value |
| --- | --- | --- |
| **Aircraft Replay Map** | Which aircraft positions can our raw-message pipeline reconstruct at this simulation time? | Aircraft markers, timestamp, decoded location, altitude/velocity when available, position age, validity and source lineage; explicit **Historical Replay** label. |
| **Trajectory Explorer** | What portions of an aircraft's observed movement are supported, and where are the gaps? | Ordered reconstructed positions, segments, coverage gaps, altitude/speed timeline, unresolved points and provenance; **no invented continuous path**. |
| **Regional Traffic Analytics** | How many distinct aircraft were observed in each region/time window, and how do provisional and recomputed results differ? | H3-cell and minute counts, time-series/heatmap, coverage, *provisional* vs *recomputed* values and correction magnitude. |
| **Data Quality & Correction History** | Why was a position missing or changed, and what data/processing rule caused a correction? | Raw reception/message-type counts, potential duplicate receptions, CPR success/failure, late arrivals, state size, affected aircraft/cells/windows and published version. |

**Concrete user story:** During a raw Kafka replay, the dashboard may initially display **4 observed aircraft** in one cell/minute. A CPR message with an older event time arrives after the streaming watermark, so streaming cannot reconstruct an additional position. HDFS still retains the raw reception. A bounded batch recomputation later reconstructs the position and republishes that cell/minute as **5**. The dashboard shows **4 provisional → 5 recomputed**, a correction reason, and the input/rule version. **4 and 5 are illustrative fixture values, not a claim about measured real-world traffic.**

### What success means

1. Real raw ADS-B messages are the **primary input** for the end-to-end demonstration, not merely an optional decoder study.
2. Spark batch and streaming agree on the **same canonical position definition** for equivalent in-order, sufficiently complete input; any late-data difference is measured and explainable.
3. Regional counts are correct for the **observed cohort and declared coverage**, not advertised as total aircraft in the sky.
4. A correction is observable, reproducible, idempotent and does not double-count old/new results.
5. The final pipeline runs through Kafka, Spark, HDFS, MongoDB, API/dashboard and a distributed Kubernetes/K3s deployment, with required Spark techniques and real measured comparisons.

## 2. Data choice, scope and semantics

### 2.1 One primary data family, two processing paths

**Primary source:** pinned **historical Raw Mode S/ADS-B receptions**, initially OpenSky public Avro samples/archives. A raw reception is a sensor's captured frame and associated metadata. It is **not** a complete aircraft state; multiple sensors may receive one transmission, and distinct transmissions may arrive close together.

- **Streaming path:** timestamp-controlled **historical replay → Kafka → Spark Structured Streaming** for stateful decoding/reconstruction and provisional serving.
- **Batch path:** **all retained raw receptions in HDFS Bronze → Spark batch** for canonical reconstruction, historical analytics and correction of a fixed, versioned input snapshot.
- **Reference data, not a second production feed:** OpenSky historical **State Vectors** when an **overlapping aircraft/time/region** reference exists; optional OurAirports snapshots for contextual spatial joins.
- **Existing OpenSky live REST collector:** keep working code and its verified checkpoint evidence, but it is a **legacy/optional comparison demo**, **not** the primary raw source and **not** evidence of raw live streaming.
- **True live raw ADS-B:** optional extension requiring a suitable receiver/feed. **Not required** for the project to demonstrate Spark streaming; a historical replay must be clearly labeled as such.

### 2.2 Data availability and feasibility gate

| Data / experiment | What has been reported | What it does **not** prove |
| --- | --- | --- |
| **2015 public raw Avro sample** | 304,131 sensor receptions, 27,876,685 bytes, ~301 seconds, 17 receivers; reported 47,129 decoded-position outputs under a bounded CPR rule. | Not an hour/day of continuous trajectories; decoded outputs do not prove geographic ground truth. |
| **2017 raw archive prefix** | 16 MiB of a larger ~3.83 GB compressed object; 1,127,250 complete receptions recovered from the prefix, covering ~14.8 seconds; reported 73,850 decoded-position outputs. | **Not** the full archive, a random sample, or proof that the full hour can be downloaded/processed on available hardware. |
| **Exploratory streaming test** | 240 selected real events, 408 unique valid **CPR candidate pairs** in an ordered scenario; late-arrival injection yielded 313; duplicate delivery yielded 528 physical outputs but 408 unique pairs. | File source + local Spark/Parquet probe, **not** Kafka-to-MongoDB exactly-once effects; candidate pairs are not canonical positions. |

These are **team-provided feasibility findings**, not completed raw integration milestones. The 2015 sample and 2017 object must be pinned with checksum, source URI, timestamp range and acquisition terms. The 2017 prefix was intentionally incomplete. Reproduce selected calculations and record decoder/code versions before using these findings as final report benchmarks.

**S5 go/no-go gate:** obtain an interval of real raw data long and continuous enough for the agreed products; record bytes, event-time span, receiver count, aircraft count, eligible messages, reconstructable positions, trajectory gaps, download/storage cost and true replay rate. Choose the coverage requirement **before** claiming multi-hour trajectories, sustained traffic trends or large-scale processing. If coverage is insufficient, narrow the **historical analytics claim** and explicitly revise scope; do not silently switch the core back to State Vectors.

### 2.3 Data identities that must never be conflated

| Logical level | Identity / required attributes | Why it matters |
| --- | --- | --- |
| **Raw reception** | `dataset_id`, source file/record index, sensor ID, captured bytes, reception/server timestamps, run/replay metadata | One message may be received by multiple receivers; keep all original deliveries. |
| **Transmission candidate** | Stable grouping identifier, aircraft/DF/TC/payload evidence, receiver-member references, grouping rule version | A close-in-time payload match is a **candidate**, not proven ground truth; sensor clock semantics differ. |
| **Decoded message** | Aircraft ICAO24 when known, message category, decoded attributes, event time, validation outcomes | Callsign, speed, barometric altitude and CPR positions can have different clocks and availability. |
| **Canonical position observation** | Stable output ID, aircraft, event/position timestamp, coordinates, quality, chosen CPR pair or other documented method, lineage, rule version | **One canonical output per declared rule**; do not equate every accepted even/odd pair with a distinct position. |
| **Aircraft state / trajectory point** | Aircraft and simulation time, latest attribute values with per-field timestamps, gaps and freshness | A state can combine attributes sent at different times; don't make them look simultaneous. |
| **Regional metric** | Dataset/run, H3 cell, event-time window, metric rule version, input snapshot, status/version | A distinct aircraft may appear in two cells in one minute; never sum cell counts as union distinct. |

**Critical parity-policy issue from feasibility:** the batch prototype selected the latest **preceding** opposite-parity CPR message within 10 seconds, while the streaming probe emitted **all** valid pairs within ±10 seconds. Those outputs are **not equivalent**. Select one deterministic, tested canonicalization policy and implement the same interpretation in both layers. State lifetime/watermark limits can cause explicit streaming omissions; batch then explains/recomputes them. Keep unsupported/valid-but-incomplete/corrupt frames in separate quality categories; sensor-dependent CRC/parity handling must not blindly reject legitimate receiver formats.

## 3. Target architecture — why both batch and streaming exist

```mermaid
flowchart TD
    INPUT[Historical Raw ADS-B Avro] --> REPLAY[Timestamp-based Replay Producer]
    REPLAY --> KAFKA[Kafka raw.replay topic]
    KAFKA --> ARCHIVE[Spark Append-only Raw Archive]
    ARCHIVE --> BRONZE[(HDFS Bronze: all receptions)]
    KAFKA --> SPEED[Spark Stateful Reconstruction]
    SPEED --> HOT[(MongoDB: provisional states / windows)]
    BRONZE --> BATCH[Spark Batch Canonical Reconstruction]
    BATCH --> GOLD[(HDFS Silver/Gold: canonical positions and metrics)]
    GOLD --> PUB[Validate and publish new version]
    PUB --> FINAL[(MongoDB: recomputed results / correction log)]
    HOT --> API[FastAPI reconciled reads]
    FINAL --> API
    API --> UI[Dashboard: replay / trajectories / traffic / quality]
```

| Path | Purpose | What must be proven |
| --- | --- | --- |
| **Stream** | Respond quickly to raw messages and reconstruct positions/windows while a replay is running. | Bounded state, determinism for accepted inputs, watermark behavior, checkpoint restart, idempotent serving writes. |
| **Archive** | Retain **every raw delivery**, including late, invalid and unresolved records. | Input completeness against acknowledged offsets, immutable provenance, restart behavior; **no watermark deletion in Bronze**. |
| **Batch** | Reconstruct against a fixed committed raw input snapshot and recompute full affected windows/trajectories. | Independent reference and correct canonicalization; versioned immutable outputs; affected old/new cells updated. |
| **Serving** | Show provisional results until a validated batch version covers the key; then show the published result. | **Replace, never add** overlapping stream/batch counts; no partial-version visibility; corrections show source and reason. |

**Why Lambda instead of batch-only?** Batch-only cannot continuously update the replay dashboard without waiting for a recomputation. **Why not streaming-only?** Correctness under arbitrarily late arrivals would require a separate reprocessing/replay-and-repair mechanism anyway. A Kappa architecture with retained log replay is a valid alternative; the report should compare it conceptually and avoid claiming Lambda is universally superior.

**Important constraints:** Kafka partitions do not create a global order; partition by an aircraft/run-aware key where suitable and make deterministic tie rules explicit. Spark streaming query-wide watermark behavior may require isolation of dataset/run/time domains. Spark 3.5.x has restrictions on chaining stateful operators: use tested query boundaries and dedicated checkpoints. `foreachBatch`/MongoDB writes require application-level idempotency; Kafka + HDFS + MongoDB are not one atomic transaction.

## 4. One active execution roadmap

**Legacy S1–S4 remain completed historical work, not Raw ADS-B milestones.** Their reusable Kafka/HDFS/batch-publishing code is a starting point. All **new** development follows **S5 → S6 → S7 → S8 → S9 → S10 → S11**.

| Stage | Outcome / acceptance gate | Status |
| --- | --- | --- |
| **S1–S4 — legacy State Vector baseline** | Verified source adapters, historical/live ingestion, Kafka → Spark → HDFS archival, saved Parquet analytics and checkpoint demo. | **Verified locally (old pipeline only)** |
| **S5 — Raw acquisition, contracts and replay/archive** | Pinned 2015 sample; stable reception IDs; Kafka replay and HDFS raw archival; source-field verification and crash/restart accounting. | **Verified locally, bounded prefix — 4 November 2026** |
| **S6 — Spark batch reference reconstruction** | Validated/gap-aware raw decoding, reception handling, deterministic CPR pairing, canonical positions and immutable, checked Silver outputs. | Planned |
| **S7 — Spark streaming raw reconstruction** | Same canonical rules under replay; bounded state, late/duplicate/reorder tests, restart; measurable differences against S6. | Planned; exploratory *file-source* probe only |
| **S8 — Serving and first replay map** | Idempotent MongoDB state, FastAPI and a map driven solely by **our raw-derived canonical output**; visible simulation clock and quality. | Planned |
| **S9 — Analytics, Lambda corrections and advanced Spark** | Trajectories, H3 traffic/coverage, quality dashboard, provisional-vs-recomputed correction demo; required Spark experiments. | Planned |
| **S10 — Kubernetes, distributed performance and operations** | K3s multi-host distributed Spark/HDFS deployment, resource/scale/recovery/monitoring/security evidence. | Planned; feasibility work starts with S5 |
| **S11 — Final integration and report** | Reproducible raw-to-dashboard demo, measured correctness/performance, report and lesson evidence. | Planned |

### S5 — Raw data and source contract **(verified locally)**

Run `s5-20261104`: 1,000 initial deliveries, SIGKILL archive, 1,000 duplicate receptions plus 100 new receptions, then resume the same checkpoint. Result: **2,100 acknowledged = 2,100 archived = 2,100 unique Kafka offsets**, representing **1,100 unique receptions**. All archived source fields were compared to Avro. All 82 existing tests passed in the Spark image; the legacy fixture integration also passed. Temporary verification code was removed. This verifies bounded ingestion and process recovery, not the full dataset or distributed host failure.

- Acquire and pin raw Avro files/chunks; document URL, SHA-256, format/schema, licensing/usage constraints, compressed/decompressed size, time range and holes. Do not assume a partial HTTP range equals the whole file.
- Preserve source fields, receiver metadata, timestamp bases, nulls and repeated payloads; verify source-to-archive equality. DF/type-code classification, parity validation and CPR opportunity profiling belong to S6. Receiver-local timestamp fields are retained without assuming a global clock.
- Define stable `dataset_id`, `reception_id`, `run_id`, delivery ID, event-time selection and invalid/unresolved categories. Decide how a replay's simulation clock and seeded late/duplicate delivery faults are recorded.
- Implement a **new raw replay topic** and append-only Spark/HDFS Bronze writer, reusing existing Kafka/Spark infrastructure without letting raw and State Vector envelopes share semantics or checkpoints.
- **Exit:** a repeatable command ingests **real raw messages** from a known interval, archives all acknowledged deliveries, validates offsets/counts and resumes after restart. Source coverage supports the next chosen experiments.

### S6 — Batch reconstruction as the correctness reference

- Implement Spark parsing/receiver-aware validation, eligible frame filtering, candidate reception grouping (without unjustified destructive dedup), deterministic CPR even/odd matching, pyModeS-assisted decode verification and canonical output rules.
- Distinguish **position event**, **CPR pair** and **canonical position**. Assemble velocity, altitude and identification only with explicit per-field observation timestamps/availability; no invented attributes.
- Build small independent reference fixtures for tie cases, zone mismatch, expired/no-opposite CPR, invalid frame, multisensor candidates and out-of-order input. Profile output completeness and gap distribution for the real interval.
- Save immutable versioned Silver (canonical positions, rejects/quality, provenance) and Gold-ready records in HDFS. Check schema/count/value invariants and `input_snapshot_id` before publishing completion.
- **Exit:** reproducible real-data decoded positions and independently checked canonicalization; one rule version and explicit unresolved reasons. This is the **reference** used to judge streaming correctness.

### S7 — Streaming reconstruction on the *same raw data*

- Replay S5 receptions via Kafka in controlled event-time order/rate, plus separately seeded duplicates, rearrangements and late arrivals. Use aircraft/run isolation and proper event-time state limits.
- Reproduce S6 canonical policy for inputs still within the configured watermark/state retention. Deduplicate output effects using stable canonical IDs, not counts of matching pairs.
- Record watermark progress, state growth, decode success, unresolved rate, Kafka lag, processing latency and output cardinality. Test restart, reprocessing and deterministic tie behavior; use dedicated checkpoints and exact source/run isolation.
- Compare **canonical positions and downstream metric representatives**, not only the streaming probe's 408 candidate pairs. Explain and quantify every allowed late-data omission against bounded S6 batch recomputation.
- **Exit:** ordered and within-allowance replay agree with batch for defined eligible cases; deliberately late input produces a controlled, documented completeness difference that batch can repair. A file-source prototype is **not** sufficient acceptance.

### S8 — MongoDB + API + first usable Historical Replay Map

- Publish latest **raw-derived reconstructed aircraft states** from S7 into MongoDB using stable keys, version guards, idempotent upserts and partial-failure retry tests. Never update a new state with a stale event solely because it arrived later.
- Add bounded FastAPI endpoints for available dataset/run/time, aircraft details/positions, coverage and health; Streamlit map uses the API rather than querying HDFS directly.
- Show **Historical Replay**, simulation time, location timestamp, quality/lineage, missing/stale/unresolved states; handle idle/failed replay transparently.
- Measure `displayed_at - replay_received_at` rather than subtracting 2015/2017 event times from today's wall clock.
- **Exit:** a real raw message can be traced through decoder → canonical position → MongoDB → visible map. No State Vector bypass is used to satisfy this gate.

### S9 — Business analytics, Lambda corrections and Spark evidence

- Build canonical trajectory points/segments and honest gap metrics. Build distinct aircraft **observed per H3 cell/minute**, time series and explicit coverage statistics. Missing coverage is **not** proof of zero traffic.
- Implement streaming provisional metrics and immutable batch recomputation for chosen committed input snapshots, including late/revised canonical positions. Recompute both **old and new** affected cells/windows and nearby trajectory segments where necessary.
- Validate full output versions before switching the active serving pointer; API prefers batch within published coverage, otherwise provisional. Never add both versions. Store a correction reason, affected keys and version/lineage.
- Deliver the four dashboard views in section 1. Demonstrate one reproducible seeded lateness case (e.g. **4 → 5**) and a replay where no correction is needed. Separate injected faults from naturally observed receiver effects.
- Execute the Spark course evidence matrix in section 6 with real code, independent fixtures and measured plans/results. **Exit:** corrections are end-to-end correct and transparent, and advanced Spark claims are backed by actual measurements.

### S10 — Distributed infrastructure and evaluation

- Test resource and compatibility feasibility of available team machines early (ARM64, Java/Scala/Python, Avro/pyModeS, Kafka connectors, MongoDB, GraphFrames); keep a small local fallback.
- Deploy Kafka, Spark executors, HDFS, MongoDB, API/dashboard and jobs with persistent storage to K3s. Validate **executors/data replicas on different physical hosts** where feasible; multiple containers on one laptop do not demonstrate host failure tolerance.
- Instrument input/decoded/canonical throughput, processing and display latency, watermark/state behavior, shuffle/spill, storage, Kafka backlog, HDFS health, MongoDB latency and failures.
- Test one declared executor/worker/DataNode interruption, checkpoint restart, partial MongoDB write and restore; report remaining single points of failure. Compare alternatives on equal logical workloads.
- **Exit:** a teammate can reproduce a **distributed** raw replay-to-dashboard/correction run and its recorded resource/correctness metrics.

### S11 — Final project evidence and delivery

- Reproduce the source-to-dashboard demo from pinned real raw input, plus a clearly labeled injected late/duplicate case and a restart/recovery case.
- Report receptions, inferred transmissions, canonical positions, aircraft, Kafka deliveries and physical replicated bytes as **different quantities**. Show actual coverage, achieved/missed targets and limitations.
- Link requirement → runnable command/test → exact input/seed/config → measured result/executed plan → lesson/report section. Include security, provenance, privacy and source-access constraints.
- Keep the old October 26 State Vector demo separately as an **earlier checkpoint**, not a claim that the new raw pipeline was already completed.

## 5. Product contracts and metrics

| Metric / UI status | Exact meaning / rule |
| --- | --- |
| `observed_aircraft_count` | Distinct `icao24` from **eligible canonical positions** within a specified H3 cell and event-time window; a given aircraft can count in multiple cells, never infer union distinct by summing cell counts. |
| `provisional` | Streaming result at a stated watermark/processing version. It can be incomplete for late inputs. |
| `recomputed` | Batch result for a **fixed, declared raw-input snapshot** and rules; not a claim that no future data can exist. |
| `correction_delta` | The published recomputed value minus the provisional value for the **same dataset/run/rule/cell/window**; version selection replaces, does not add. |
| `position_quality` | Validation and decode outcome plus provenance, receiver/time limitations and canonical CPR rule. Distinguish invalid, unsupported, incomplete and stale. |
| `replay_visibility_latency` | Time from replay release/receipt to visible result for a specified cohort; not the historical aircraft observation age. |
| `trajectory_coverage` | Valid observed points, time span and justified gaps/segments; unsupported intervals are not interpolated or assumed flown. |

**Dashboard layout:** (1) Historical Replay Map; (2) Aircraft Trajectory Explorer; (3) Regional Traffic + provisional/recomputed switch/comparison; (4) Data Quality & Correction Log. No separate OpenSky live State Vector map is needed for raw-first acceptance. Real-time raw reception and optional airport context remain extensions.

## 6. Academic Spark coverage and experiments

Every claim below needs **runnable code + independent correctness fixture + executed plan/metrics + comparison/lesson**; a technology appearing in an architecture diagram does not satisfy the course requirement.

| Spark requirement / technique | Concrete Raw ADS-B application |
| --- | --- |
| Window functions, ranking, lag/lead | Canonical CPR candidate selection, per-aircraft ordered positions, gap/trajectory segmentation. |
| Advanced aggregation, pivot/unpivot | Message types by receiver/hour, decode failures, H3 traffic counts and coverage distributions. |
| Custom/Pandas aggregation; UDF | Bounded reception/trajectory statistics; measured CPR/H3 parsing/assignment choices against built-ins/reference implementation. |
| Broadcast and sort-merge joins; multi-join optimization | Receiver/reference enrichment and large canonical/reception or trajectory memberships; verify actual join strategy and input cardinality. |
| Partition pruning/bucketing/caching/query optimization | Event-day raw/position layout, repeated analytics workloads, explain plans, shuffle/spill/memory comparisons. |
| Structured Streaming/watermark/state/output modes | Pairing within event-time tolerance, state eviction, provisional representatives and drop measurements. |
| Exactly-once **effects** | Checkpoint/file commits plus stable IDs and idempotent MongoDB upserts under duplicate micro-batches and partial retries; explicitly state transaction boundary. |
| Statistics/time series | Gap-aware traffic/decode distributions and regional observed-count trends over continuous coverage only. |
| MLlib | Optional small next-window observed-count study **only if** sustained data coverage exists; chronological split and baseline. |
| GraphFrames | Bounded H3-cell movement graph from supported consecutive canonical positions; not inferred airline routes. |

**Priority comparisons:** (E1) canonical CPR candidate-selection methods / correctness; (E2) raw reception grouping sensitivity; (E3) streaming 10 s vs 30 s lateness allowance, completeness versus state/latency; (E4) recovery by checkpoint versus isolated raw replay; (E5) Spark join/partition/cache alternatives; (E6) 1 versus multiple executors; (E7) storage format/layout/replication. Freeze logical inputs/rules, record resources and actual executed plans, compare output equality before claiming speedups, repeat meaningful benchmarks and report failures. The exact experiment count is a project choice, not a stated fixed course minimum.

**Engineering priority:** correctness of raw-to-canonical reconstruction and Lambda correction **before** ML or GraphFrames. If source coverage is too short for time-series forecasting, do not fabricate long continuous observations; seek instructor-approved scope interpretation and record it.

## 7. Existing code, checkpoint evidence and document ownership

### What is **already implemented** (October 26 State Vector prototype)

The local Compose setup verified State Vector CSV replay and a bounded live REST collector, Kafka acknowledgment, separate Spark/HDFS archival paths and immutable batch Parquet traffic/trajectory tables. The synthetic ten-row fixture archived ten deliveries and generated three eligible positions. The October 26 real REST run archived **19** State Vector observations and saved **3 traffic rows and 7 trajectory segments** from **12 eligible positions**. These are valid infrastructure/code-reuse evidence, **not** evidence of raw ADS-B decoding.

Reuse the Kafka publisher, archive/checkpoint patterns, HDFS paths, Parquet publication validation and demo/test harness when compatible. Implement **new schemas/topics and reconstruction jobs**, rather than renaming `State Vector` input/output fields to claim they are raw frames.

### Raw exploratory evidence (separate from the running app)

A teammate's reported standalone probes decoded 2015 and short 2017 raw samples and ran a file-source Spark streaming pairing experiment. These experiments show possible feasibility but **do not yet establish** Kafka raw ingestion, correct batch–stream canonical equality, raw-derived MongoDB publication, true raw live, distributed execution or a final dashboard. See section 2.2 for exact reported boundaries.

### Keep documentation consistent

- **This `plan.md` is the only active roadmap.** `architecture.md` documents **legacy implementation versus new Raw-first target**. `checkpoint-guide.md` describes **only** the October 26 historical checkpoint and should never override stages S5–S11.
- Existing `README.md` and `data/README.md` commands remain **legacy** until corresponding raw commands are implemented. Do not advertise imagined commands for S5–S11 as runnable.
- Source files and tests do **not** change merely because these documents change. Mark each newly built stage as Planned / In Progress / Verified with date, command/run ID, input/coverage, measured output and evidence location.

### References and source starting points

- [OpenSky public raw sample](https://github.com/openskynetwork/osky-sample) (reported pinned commit `1b179c1c541b178944010a2eb6fa5189a4f9193d`).
- [OpenSky 2017 anonymized raw archive](https://s3.opensky-network.org/data-samples/raw/20170109_16_anonymized.avro.bz2) (large object; the reported prefix is **not** the full archive).
- [OpenSky REST API reference](https://openskynetwork.github.io/opensky-api/rest.html) (**processed State Vectors**, not raw live frames).
- [Apache Spark Structured Streaming documentation](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html); [MongoDB single-document atomicity](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/).
- [OurAirports data](https://ourairports.com/data/) for optional contextual reference snapshots.
- Course-provided guidelines: advanced Spark operations, queue, distributed storage, NoSQL, Kubernetes/cloud, analytical comparisons and lessons learned; retain the supplied course file unchanged.
