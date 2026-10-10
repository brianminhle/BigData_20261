# Data acquisition, replay, and pipeline checks

This guide documents the working OpenSky downloader, quality profiler, observation adapter, replay, Kafka/Spark/HDFS pipeline, and verified findings.

## Run it

From the repository root, using Python 3.10 or newer:

```bash
python3 scripts/download_sample.py
python3 scripts/profile_sample.py
```

The downloader reads [sources/opensky_sample.json](sources/opensky_sample.json). The configured archive represents June 27, 2022, 04:00–05:00 UTC and is 105,400,320 bytes, approximately 100.5 MiB. Its TAR container holds a gzipped CSV. The original bytes stay unchanged; the profiler reads the CSV inside the archive without extracting it to disk.

```text
data/
  sources/opensky_sample.json                  # Tracked source configuration
  raw/states_2022-06-27-04.csv.tar              # Downloaded archive, ignored
  manifests/opensky_states_2022-06-27_04.json   # Origin and checksum, ignored
  reports/opensky_states_2022-06-27_04.profile.json  # Quality summary, ignored
```

The manifest identifies the file through its URL, byte count, and SHA-256 checksum. Its archive path is relative to the manifest directory so that the entire data directory can be moved. The profiler verifies the file against this manifest before reading it. A checksum verifies the bytes we pinned; it is not an OpenSky signature.

The tools also support CSV and gzipped CSV files, and TAR archives containing either, while expecting the OpenSky telemetry schema. Another telemetry file needs its own JSON configuration with a distinct dataset identifier and filename, an HTTPS URL, the expected byte count, and an optional known SHA-256 digest. Airport reference files need a separate schema/validator; the existing profiler cannot interpret them as telemetry.

## Understand an observation

Each row is an aircraft state snapshot, not a complete flight. Key source fields are:

| Field | Meaning for this project |
| --- | --- |
| `icao24` | Aircraft transponder identifier; read as text, preserving leading zeros. |
| `time` | Snapshot timestamp in Unix seconds, used as the initial observation time. |
| `lat`, `lon` | Position in degrees; missing values remain missing. |
| `velocity` | Ground speed in metres per second. |
| `heading` | Track angle in degrees. |
| `vertrate` | Vertical speed in metres per second; negative values indicate descent. |
| `baroaltitude`, `geoaltitude` | Barometric and geometric altitude in metres; keep separate. |
| `callsign` | Flight callsign when available; source strings can have padding. |
| `onground`, `alert`, `spi` | Boolean state indicators. |
| `squawk` | Transponder code; treat as text, not a decimal measurement. |
| `lastposupdate` | Timestamp of the position update, potentially older than the snapshot. |
| `lastcontact` | Timestamp of the last received message, potentially different from both other times. |

For example, a row observed at `04:00:10` might still carry a position last updated at `03:59:30`. Replaying that row does not make its position fresh. The historical adapter retains these separate timestamps for later current-state processing.

Source definitions: [OpenSky sample README](https://s3.opensky-network.org/data-samples/states/README.txt).

## Normalize and replay historical observations

From the repository root:

```bash
# Release 100 rows at up to 10 rows/second; JSONL goes to the terminal.
python3 -m scripts.replay_sample --max-rows 100 --events-per-second 10

# Save a run for inspection; choose a new output filename for each run.
python3 -m scripts.replay_sample --max-rows 1000 --events-per-second 100 \
  --run-id demo-001 --output data/samples/demo-001.jsonl
```

Each line is one JSON object. Run metadata and accepted/rejected counts go to stderr, leaving stdout suitable for piping. `--manifest` selects a downloaded source; its size and SHA-256 must match before any row is released. No API request is made. Files under `data/samples/` are ignored by Git. Existing output files are never overwritten; an interrupted run can leave a partial JSONL file.

The [version-one contract](../scripts/observations.py) and [historical adapter](../scripts/historical.py) produce:

| Fields | Behavior |
| --- | --- |
| `schema_version`, `source`, `mode`, `run_id` | Version 1, `opensky`, `replay`, and a supplied or generated run identifier. |
| `observation_id`, `icao24` | `opensky:state:<lowercase aircraft>:<snapshot seconds>`; independent of run, receipt time, dataset filename, and row location. Leading zeros survive. |
| `observed_at` | Original CSV `time`, as integer UTC Unix seconds. |
| `position_updated_at`, `last_contact_at` | Original update times, retaining fractional seconds and nulls. |
| `received_at` | Wall-clock Unix seconds sampled at replay release, before normalization. |
| Measurements | Nullable `latitude`/`longitude` in degrees, `track_deg`, speed/rate in `_mps`, separate altitudes in `_m`, trimmed callsign, nullable booleans, and textual squawk. |
| `provenance` | Provider, dataset, source URL, archive checksum, member name, one-based CSV record index, and adapter version. |
| `source_payload_sha256` | Fingerprint of original parsed cell text, including unknown columns and extra cells; preserves distinctions such as whitespace. Archive checksum identifies the original file bytes. |
| `quality_flags` | Missing coordinates/update times, updates after snapshot time, or position/contact older than `--stale-seconds` (default 30). Flags do not discard valid observations. |

Valid rows have `record_type: "observation"`. Missing mandatory identity/time, malformed rows, and invalid non-null values produce `record_type: "rejected"`, with `reasons`, original `raw_fields`, `extra_values`, and provenance. Missing optional measurements stay null; zero, false, negative altitude, and descent remain valid. A broken archive or invalid CSV header stops the command instead of fabricating row records.

Duplicates remain in the output. Equal source/aircraft/snapshot identities keep the same observation ID; differing payload fingerprints expose conflicts for later processing. The adapter does not yet resolve conflicts or implement a current-state table.

The replayer reads **only the first `--max-rows` source rows** (default 1,000; prototype cap 100,000) into memory and sorts that selection by snapshot time, aircraft, member, and record index. Rows with invalid snapshot times go last. This is not a globally sorted replay or a full-file validation. The rate includes rejection records; slow consumers can reduce the achieved rate, and replay does not burst to catch up. It uses constant-rate release, not the original spacing between events.

For this prototype, the replay simulation clock is the current accepted record's `observed_at`; source-age flags compare against that time, never today's date. Identical input and settings preserve ordering, IDs, values, and flags; wall-clock receipt times and automatically generated run IDs vary. Fault injection, large-scale ordering, live ingestion, and measured display latency remain planned.

## Kafka, Spark, and HDFS

The local [Compose deployment](../compose.yaml) runs Kafka 3.9.1, Spark 3.5.7 with its matching Scala 2.12 Kafka connector, and Hadoop 3.4.2. Base images are pinned by digest; the producer uses `kafka-python==2.2.15`. Spark uses Java 17; HDFS uses Java 11. Spark's bundled Hadoop client is 3.3.4; the successful local run verifies this client/server combination for this path. Connector settings follow the [Spark Kafka integration guide](https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html).

Run commands are in the root [README](../README.md#run-the-pipeline). Services publish UIs and the host Kafka listener only on loopback. HDFS and internal Kafka traffic remain on the Compose network. This is a local, unauthenticated teaching deployment. The Kafka topic `airtraffic.replay.v1` has three partitions, replication one, and seven-day retention. A single Kafka broker cannot demonstrate broker failover.

The producer releases newly normalized observations directly to Kafka. Keys include mode, run, and aircraft, keeping an aircraft's records together within a run. Rejected rows use their source fingerprint when an aircraft key is unavailable. Publication uses broker acknowledgments, bounded pending requests, delivery timeouts, and producer-session idempotence. An application restart or republishing a run can still add duplicates; failure reports may indicate partial delivery. See the [producer's delivery guarantees](https://kafka-python.readthedocs.io/en/2.2.15/apidoc/KafkaProducer.html).

The Spark query uses a two-second micro-batch trigger and reads up to 10,000 Kafka offsets per trigger. It keeps original payload/key bytes, topic/partition/offset, Kafka timestamp, batch processing time, and typed envelope metadata. It labels malformed JSON, unsupported schema versions, source rejections, and unknown record types. It does not filter or deduplicate raw input; `observation` labels a parsed record type, not a new independent validation of every measurement.

| Stored artifact | Location |
| --- | --- |
| Raw committed Parquet | `hdfs://namenode:9000/airtraffic/bronze/replay-v1`, partitioned by UTC ingestion date. |
| Spark checkpoint and source offsets | `hdfs://namenode:9000/airtraffic/checkpoints/archive-replay-v1`. |
| File sink commit metadata | `_spark_metadata` under the Parquet root. |
| Kafka/HDFS persistence | Project-scoped Docker named volumes; preserved by `docker compose stop` and ordinary `down`. |

Both the data and checkpoint use HDFS replication two across two DataNode containers. They share one physical host in this prototype. NameNode formatting happens only for an empty metadata volume; a partial directory is refused rather than reformatted. There is no automated NameNode checkpoint/backup service yet. The Hadoop build copies the Java distribution from Apache's amd64 image into a native Temurin runtime and removes architecture-specific native libraries. No donor-stage x86 process runs on ARM, but actual ARM64 testing is still pending.

Keep output and checkpoint directories paired. Do not reuse a checkpoint for a different topic/query or run two writers against this pair. Spark resumes offsets from the checkpoint; `startingOffsets=earliest` applies to a fresh checkpoint. Kafka retention can remove unread events, in which case `failOnDataLoss=true` stops the query. Read the Parquet **root** so Spark honors committed-file metadata; globbing individual files can include uncommitted files after a failed batch. Recovery behavior follows the [Structured Streaming file-sink model](https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html).

Inspect HDFS health and run the Spark-specific checks:

```bash
docker compose exec namenode hdfs dfs -ls -R /airtraffic/bronze/replay-v1
docker compose exec namenode hdfs fsck /airtraffic/bronze/replay-v1
docker compose run --rm --no-deps --entrypoint /opt/spark/bin/spark-submit inspect \
  --master 'local[1]' --driver-memory 512m tests/test_archive_spark.py
```

The host's standard-library test command skips the two Spark checks when PySpark is unavailable; the container command actually executes them. The batch inspector reports distinct Kafka offsets, distinct observation IDs, quality counts, and `receipt_to_batch_start_p95_seconds`. This last metric is diagnostic: it excludes completion of the Parquet write, serving, and display, and can include deliberate recovery downtime. It is **not** the five-second collector-to-display result.

The October 10 local smoke test published 1,000 observations at a requested 100/s and read back 1,000 rows with 1,000 unique Kafka offsets. Flags matched the input: 80 missing positions and five stale positions. Killing Spark after that run, publishing 100 more observations while it was stopped, and restarting it archived the backlog without duplicating the first run's offsets. This verifies that specific restart case, not every possible mid-write failure. Aggregate local results are in `data/reports/pipeline-demo-001.json` and `data/reports/pipeline-recovery-001.json`.

The Spark build uses Maven Central at `repo.maven.apache.org` and resolves connector dependencies during the build. Downloaded source files can be owner-only; the producer's `LOCAL_UID`/`LOCAL_GID` settings allow reads without changing their permissions.

## Read the report

By default, the profiler examines the **first 100,000 rows in archive order**. This is a bounded initial inspection, not a random sample or a measurement of the full hour. In particular, its aircraft counts, time range, and geographical bounds describe only those rows.

- `scope.rows_profiled` is the inspected count. `truncated: true` and `total_source_rows: null` mean more source rows exist.
- `missing_by_field` counts blanks and the case-insensitive null markers `null`, `none`, and `\N`.
- `invalid_by_field` counts nonfinite or invalid numbers, out-of-range coordinates, negative ground speed, invalid track angles, identifiers, booleans, and squawk codes. Negative altitude and vertical speed are allowed. Boolean validation accepts `true`, `false`, `1`, and `0`.
- `malformed_rows` counts rows with too many or too few cells. These rows are excluded from usable identity/time keys and usable positions. Field-level missing/invalid counts still include them.
- `usable_positions` means both coordinates are present and within range on a well-formed row. It does not imply a fresh position or valid aircraft identity.
- Duplicate keys use `(icao24, time)`, with the identifier trimmed and case-folded. Matching/conflicting payload counts compare each repeat against the first row for that key, using original cell text, including whitespace. The tool reports duplicates without removing them.
- `source_time_decreases` counts adjacent decreases between valid snapshot times in source order. `per_aircraft_time_decreases` applies the same check to well-formed rows for each valid aircraft identifier. These are diagnostics, not an assumption that files are already sorted.
- `freshness` compares the snapshot time with each update time. The default threshold is 30 seconds; ages above it are reported, and updates after the snapshot time are counted separately. No rows are discarded based on this threshold.

The profiler reads one extra row to determine whether it stopped before the end. Distinct-aircraft and duplicate detection keep state in memory proportional to the inspected rows. Start with the default limit; this script is for acquisition checks, while full-scale profiling belongs in the later Spark pipeline. A prefix scan does not validate the unexamined CSV rows or a gzip trailer it has not reached.

For a different limit or output location:

```bash
python3 scripts/profile_sample.py --max-rows 10000 --stale-seconds 60
python3 scripts/profile_sample.py --output /tmp/opensky-profile.json
```

## Reproducibility and troubleshooting

### First verified run

The [reference report](reference_profile.json) records the first successful run on October 7, 2026. It contains aggregate statistics and source provenance, without observation rows or aircraft identifiers.

| Measurement | First 100,000 rows only |
| --- | --- |
| Snapshot time range | 2022-06-27 04:00:10–04:04:00 UTC |
| Distinct aircraft identifiers | 4,472 |
| Rows missing latitude and longitude | 9,210 (9.21%) |
| Rows with usable coordinates | 90,790 |
| Position updates more than 30 seconds old | 5,219 of 90,790 rows with a position-update timestamp |
| Last contacts more than 30 seconds old | 4,770 of 100,000 rows |
| Duplicate aircraft/time keys | 0 |
| Malformed rows or rows failing the implemented value checks | 0 |

These findings support keeping positions nullable and tracking their age separately from the snapshot time. They do not establish quality rates, total observations, or distinct-aircraft counts for the entire file or other days. Full-file row count and decompressed size have not been measured.

### Download behavior

The downloader checks the expected size and pinned checksum, probes the CSV header, and publishes the archive only after these checks pass. Downloads have a 256 MiB default limit and a 30-second network-operation timeout. Re-running verifies and reuses an existing file without contacting the server.

For a slow connection, use `python3 scripts/download_sample.py --timeout 120`. An interrupted transfer is restarted from the beginning; resume and automatic retries are not implemented. Abrupt process termination can leave a hidden `.download-*` temporary file in `data/raw/`; after confirming no downloader is running, it can be removed.

On a checksum mismatch, the tool leaves the existing archive untouched. Inspect it, or use a fresh directory to fetch another copy:

```bash
python3 scripts/download_sample.py --data-dir /tmp/airtraffic-data
python3 scripts/profile_sample.py --manifest /tmp/airtraffic-data/manifests/opensky_states_2022-06-27_04.json
```

Do not update the pinned checksum merely to suppress a mismatch; first establish why the source bytes differ. A changed source should have reviewed provenance.

Offline checks use synthetic observations only:

```bash
python3 -m unittest discover -s tests -v
```

## Source and attribution

Data provider: **The OpenSky Network**. See the [scientific datasets page](https://opensky-network.org/data/scientific), [sample schema](https://s3.opensky-network.org/data-samples/states/README.txt), and [dataset terms](https://s3.opensky-network.org/data-samples/states/LICENSE.txt).

This repository stores acquisition code, provenance configuration, and aggregate findings. Downloaded telemetry remains local and is excluded from Git. The dataset has its own terms, independent of this project's code.

## Live source availability check

A manual check on **October 10, 2026** requested two snapshots for Vietnam and surrounding airspace, approximately 12 seconds apart. Both returned HTTP 200 and 18 aircraft; 10 aircraft rows changed. This establishes that newly changing snapshots were accessible at the time of the check. It does not establish continuous availability or complete regional coverage.

Snapshot ages at receipt were approximately 2.86 and 5.59 seconds. The oldest position was already 251 and 261 seconds old relative to its snapshot. These are different measures: receiving a new response does not make every aircraft position new. The generated check report is a local artifact under `data/reports/`, excluded from Git.
