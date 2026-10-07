# First milestone: download and understand the data

Before building distributed storage or processing, we need a real input file and an understanding of its contents. This milestone downloads one historical hour from the OpenSky Network and produces an aggregate quality report.

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

The tools also support CSV and gzipped CSV files, and TAR archives containing either. A new source needs its own JSON configuration with a distinct dataset identifier and filename, an HTTPS URL, the expected byte count, and an optional known SHA-256 digest.

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

For example, a row observed at `04:00:10` might still carry a position last updated at `03:59:30`. Replaying that row does not make its position fresh. Later normalization and current-state processing must retain the separate timestamps.

Source definitions: [OpenSky sample README](https://s3.opensky-network.org/data-samples/states/README.txt).

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

## Next step

Use the measured quality findings to define the normalized observation schema and a deterministic replay reader. Preserve the three source timestamps, identifiers, missing values, and original source reference. Kafka, HDFS, and processing jobs will be connected after that input contract is defined.
