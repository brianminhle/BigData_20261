# Spark batch with Kubernetes executors

This overlay extends `../pc` and changes only the recent-analytics CronJob, plus
its Spark configuration and namespaced RBAC. Streaming and publisher continue
using `local[2]`; MongoDB, API and dashboard keep their current configuration.

The CronJob pod runs the Spark driver in **client deploy mode** with the
Kubernetes master, not local mode. Spark creates two executor pods. The driver
advertises its pod IP through the Downward API on ports 7078/7079. Its pod name
is passed to Spark so executor pods have an owner reference and can be garbage
collected when the driver is removed. No external driver Service is needed on
this pod network. CronJob `Forbid`, a 120-second scheduling deadline and a
15-minute Job deadline bound recurring runs.

| Process | CPU request / limit | Memory request / limit |
| --- | --- | --- |
| Driver | 500m / 1 CPU | 1536 MiB / 1536 MiB; JVM heap 768 MiB |
| Each of 2 executors | 500m / 1 CPU | 1152 MiB / 1152 MiB; heap 768 MiB + overhead 384 MiB |
| Total batch | 1500m / 3 CPUs | 3840 MiB |

Each executor has one Spark task slot. This is a small functional configuration
for the 6-vCPU, 12-GiB VM, not a production sizing recommendation. Existing
services also consume resources; aggregate limits can exceed VM capacity.
Watch actual memory and pod termination reasons. Do not run baseline and
distributed batches together. EmptyDir shuffle storage is bounded at 2 GiB per
executor; the driver retains its existing 4-GiB temporary volume.

The driver ServiceAccount can manage pods, Services and ConfigMaps only in
`airtraffic`; it has no direct Secret-read or cluster-wide permissions. Pod
creation itself is a powerful permission: namespace isolation is still needed
for untrusted workloads. Executors have an unprivileged ServiceAccount and no
mounted API token. MinIO credentials remain supplied by the existing Secret;
they are never put in these tracked files.

Spark's driver PVC ownership and reuse are explicitly disabled because this
batch uses only emptyDir. This avoids Spark 3.5.7's default PVC list/cleanup
requests without granting the driver deletion rights over application PVCs.

The current `airtraffic-state-stream:local` image already includes Spark 3.5.7,
Hadoop S3A, Kafka connectors and the application. Its default ENTRYPOINT starts
local streaming, so the executor template explicitly restores
`/opt/entrypoint.sh` for Spark's `executor` arguments. No application rebuild is
needed for this overlay. Before adding a worker, distribute the same image to
that node or publish an immutable image to a registry and update both driver
image and `spark.kubernetes.container.image`.

## Apply and validate

Use PowerShell 7, the private kubeconfig and explicit context from the operations guide.
Render and server-validate before applying. The `pc` overlay remains the local
batch fallback; applying it later will revert the batch configuration.
This overlay deliberately declares `suspend: true` as an activation gate.
Reapplying it suspends the schedule; resume explicitly after validation. Before
applying to an existing installation, allow any active batch Job to finish.

```powershell
$kc = '.\deploy\k8s\local\bin\kubectl.exe'
$cfg = 'deploy/k8s/local/admin.conf'
& $kc --kubeconfig $cfg --context airtraffic-kubeadm apply --dry-run=server -k deploy/k8s/overlays/batch-distributed
& $kc --kubeconfig $cfg --context airtraffic-kubeadm apply -k deploy/k8s/overlays/batch-distributed
```

Before acceptance, suspend `airtraffic-recent-analytics` and wait for any active
batch Job to finish. `verify-spark-batch.ps1` refuses to run against an enabled
schedule or another active analytics driver. It creates sequential Jobs for:

1. Freezing committed Silver/Gold Parquet from the live lake into a unique
   `s3a://airtraffic-states/acceptance/batch-check-.../snapshot` prefix.
2. Running the existing `run_analytics` pipeline with `local[2]` on that snapshot.
3. Running the same code, interval and four shuffle partitions on two executors.
4. Comparing reports, schemas and every row/column in all six output tables,
   with float tolerance `rtol=atol=1e-9`, then verifying event logs contain
   successful tasks from at least two executor IDs and none from the driver.

```powershell
& $kc --kubeconfig $cfg --context airtraffic-kubeadm -n airtraffic patch cronjob airtraffic-recent-analytics --type merge -p '{"spec":{"suspend":true}}'
.\deploy\k8s\cluster\verify-spark-batch.ps1
```

Input manifests, outputs, SQL plans, execution durations and uncompressed Spark
event logs are retained under the unique acceptance prefix. Local ignored
`deploy/k8s/local/batch-check-.../` contains Job manifests, logs, results and
executor pod snapshots. These are real observations, with no synthetic data
expansion. Comparison collects at most 100,000 rows per output table and is
intended for functional acceptance, not large-scale benchmarking.

The script leaves the schedule suspended even on success. After PASS, create
one Job from the CronJob to verify the normal recent-analytics entrypoint,
confirm publisher/API readiness, then enable the schedule. Keep one-off jobs
sequential. On failure, inspect logs and correct the cause before resuming.

```powershell
& $kc --kubeconfig $cfg --context airtraffic-kubeadm -n airtraffic create job batch-distributed-promotion --from=cronjob/airtraffic-recent-analytics
# Wait for Complete, inspect logs and API readiness before enabling.
& $kc --kubeconfig $cfg --context airtraffic-kubeadm -n airtraffic patch cronjob airtraffic-recent-analytics --type merge -p '{"spec":{"suspend":false}}'
```

Run IDs and Job names must be unique when repeating acceptance. Completed test
Jobs and their ConfigMap may be removed by exact name after saving evidence;
do not delete production data or checkpoints. Spark should remove executor
pods on normal completion; inspect leftovers after failures.

## Interpretation and rollback

Two executors on this single VM prove Spark executes across separate pods.
They do not prove multiple physical hosts, high availability or faster Big Data
processing. Local and distributed drivers have the same memory/CPU limits, but
the distributed run adds two executor CPU limits; recorded times are not a
controlled equal-resource benchmark. A future benchmark needs larger input,
fixed total budgets, repeated runs and memory/shuffle measurements.

To roll back, suspend the CronJob, wait for its active Job to finish, then
restore the saved CronJob spec or the original `pc` batch manifest. Do not
delete the lake, checkpoint or Mongo volumes. Restore the schedule only after
checking the selected spec. During the initial migration, the original full
CronJob was saved to ignored `deploy/k8s/local/batch-before-distributed.json`.

Reference: [Spark 3.5.7 Kubernetes client mode, pod templates and authentication](https://spark.apache.org/docs/3.5.7/running-on-kubernetes.html).
