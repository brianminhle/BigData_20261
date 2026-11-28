# Spark batch driver/executor acceptance — 2026-11-27

Functional acceptance passed on the existing one-node Ubuntu Hyper-V cluster.
The Spark 3.5.7 driver ran inside a Kubernetes Job in client deploy mode with
`k8s://https://kubernetes.default.svc:443`; two independent executor pods
executed the analytics tasks. Streaming and publisher remain `local[2]`.

## Configuration and scope

- Node: `airtraffic-k8s`, Kubernetes 1.37.2, 6 vCPU, 12 GiB assigned RAM.
- Driver: 500m CPU request, 1 CPU limit, 1536 MiB memory, 768 MiB JVM heap.
- Two executors: each 500m CPU request, 1 CPU limit, 768 MiB heap plus 384 MiB
  overhead. Aggregate batch requests: 1.5 CPU and 3840 MiB memory.
- Application image: existing `airtraffic-state-stream:local`, built from
  application commit `2a07ce3`; the pipeline code was not changed for this test.
- Executor template restores `/opt/entrypoint.sh`, uses UID/GID 185, read-only
  root filesystem and a 2-GiB disk-backed emptyDir. API tokens are not mounted
  into executors. The driver uses namespace-scoped pod/Service/ConfigMap RBAC.
- Driver address and owner pod name come from Downward API. Executor cleanup
  was verified after completion. Spark PVC ownership/reuse are disabled; its
  ServiceAccount cannot delete PVC collections or directly read Secrets.

The initial run produced correct data but logged a denied PVC cleanup request
on Spark shutdown. The final configuration disables those unused PVC features;
the full test was repeated successfully without Spark ERROR/Traceback logs.
No PVC deletion permission was added.

## Frozen-input comparison

Final run ID: `batch-check-20261127161308` (UTC timestamp).

Input/output prefix:
`s3a://airtraffic-states/acceptance/batch-check-20261127161308`.
The `acceptance` prefix is outside the production analytics prefix consumed by
publisher. Snapshot creation read the streaming sink through Spark so only
committed files were included, then wrote an immutable copy for both runs.

- Real Silver input: **4,949 observations**.
- Gold input: **1,051 rows**.
- Interval: `[1795710000, 1795796100)` in Unix seconds, aligned to five minutes.
- No synthetic expansion or observation duplication was used.
- Baseline and distributed runs were sequential with four shuffle partitions.

| Output | Rows in both runs | Result |
| --- | ---: | --- |
| movement | 3,677 | All values match |
| gaps | 452 | All values match |
| density | 1,051 | All values match |
| quality | 348 | All values match |
| quality_wide | 174 | All values match |
| quality_long | 696 | All values match |

The comparison checks report equality, column types and every row/column;
floating-point values use relative and absolute tolerances of `1e-9`. It is
bounded to 100,000 rows per output table. Both runs write their normal root
`_SUCCESS`, SQL executed plans and report, plus acceptance execution metadata.

## Execution evidence

| Measurement | local[2] | Kubernetes executors |
| --- | ---: | ---: |
| Successful tasks | 67 on driver | 22 on executor 1; 45 on executor 2 |
| Analytics function duration | 19.605 s | 22.956 s |
| Spark application event duration | 25.947 s | 33.823 s |
| Sum of executor task runtime | 14,377 ms | 15,857 ms |
| Input bytes read across tasks | 9,882,442 | 9,882,442 |
| Shuffle bytes read / written | 866,391 / 866,391 | 866,364 / 866,364 |
| Memory / disk bytes spilled | 0 / 0 | 0 / 0 |
| Maximum task execution memory metric | 26,214,400 bytes | 51,380,224 bytes |

Distributed application ID: `spark-f137d3b8e64f4a2b867549c57128eb66`.
Local application ID: `local-1795796044014`.
There were no distributed tasks on the driver. The small shuffle-byte variation
does not imply differing records: the full result comparison passed.
Task execution memory is a Spark task metric, not container peak RSS.

During the first functional run with the same memory limits, a kubelet sample
reported driver working set 510.2 MiB and executor working sets 633.4/418.5 MiB;
Ubuntu reported approximately 6.1 GiB available memory. This is one sample,
not a measured peak or a long-running capacity guarantee.

Logs, Job specs/results and executor pod snapshots are retained in ignored
`deploy/k8s/local/batch-check-20261127161308/`. MinIO retains `snapshot.json`,
`local/`, `distributed/`, `events/` and `acceptance.json` at the prefix above.
Spark event logs were parsed to verify both executor IDs completed tasks.

## Production entrypoint and activation

Job `batch-distributed-promotion-202611271616` was created from the suspended
CronJob and completed successfully from 16:15:54 to 16:17:21 UTC (87 seconds).
It ran the normal `analyze_recent_states.py` entrypoint on the live lake and
reported 4,956 observations. Both executors registered; shutdown logged no
Spark ERROR/Traceback and left no executor pods.

Its completed output was
`s3a://airtraffic-states/analytics/recent-c102ffaed2d1481dae9b121d7149ce00`.
After the next publisher cycle, `/api/overview?hours=24` exposed exactly that
`analytics_run` and `/readyz` returned `{"status":"ok"}`. The existing publisher,
MongoDB and API consumed the distributed result without configuration changes.

The live `airtraffic-recent-analytics` CronJob was then resumed:
`*/15 * * * *`, `suspend: false`, `Forbid`, with no active Job at final check.
The overlay itself retains `suspend: true` as a repeatable activation gate;
reapplying it requires explicitly resuming after checks. All five application
Deployments and Mongo StatefulSet were ready; all three application PVCs were
Bound. The next timer-triggered run was not awaited; the identical CronJob
template was validated through the promotion Job.

## Limits and reproduction

This proves distributed Spark execution across pods on **one physical PC/VM**.
It does not establish multi-node resilience, HA or a Big Data speedup. The
distributed job was slower on this small input and had additional executor CPU
limits; these timings are not a controlled equal-resource benchmark.

VM access and Kubernetes readiness were confirmed at the start. Tailscale is
installed but remained logged out. Windows reported its last boot as
2026-11-16; a Windows reboot recovery test was not performed in this task.

Follow [the overlay guide](../deploy/k8s/overlays/batch-distributed/README.md)
for configuration, sequential acceptance, promotion and rollback. The
PowerShell 7 verification script leaves the CronJob suspended on success or
failure so the normal entrypoint can be checked before resuming its schedule.
Streaming migration, executor-loss testing, larger benchmarks, backup/restore
and central freshness/failure alerts are subsequent work.
