# Air Traffic Analytics on this PC — operations record

This document records deployment on 2026-11-26 and recovery checks on 2026-11-27. It does not claim multi-host availability or physical data replication.

## Platform and access

- Host: Windows 11 Pro, Intel i5-12600K, 32 GiB RAM, Lexar NM790 SSD.
- VM: `airtraffic-k8s`, Ubuntu 24.04 LTS, 6 vCPU, 12 GiB RAM, 60 GiB dynamic VHDX at `D:\VMs\AirTraffic-K8s\airtraffic-k8s.vhdx`. Hyper-V starts it automatically and requests a guest shutdown when Windows shuts down.
- Kubernetes: kubeadm/kubelet/kubectl `1.37.2-1.1`, containerd `2.2.1`, Flannel `v0.28.10`. The applied Flannel manifest has SHA-256 `393BDBCE53EB50C8EB97DD513B218518B7D6E7985DC7B889BE956E112A711084`.
- API endpoint: `172.23.47.137:6443` via Hyper-V Default Switch. After the first guest reboot changed its DHCP address and stopped kubeadm/etcd from binding, `/etc/netplan/99-airtraffic-k8s.yaml` was added in Ubuntu to retain `172.23.47.137/20` alongside DHCP. A second guest reboot verified that the control plane recovers without manual address repair. The Default Switch subnet itself can still change after Windows restarts; the attempted dedicated management switch was canceled at UAC. This is not a stable multi-host endpoint.
- Tailscale v1.104.7 was installed on the Ubuntu VM on 2026-11-27 and `tailscaled` is active. Device authentication is pending; the VM has no Tailscale IP yet. Tailscale is installed on Windows but its client was in `NoState` at inspection. Kubernetes still advertises `172.23.47.137`; joining a remote worker requires a planned control-plane and Flannel network migration after both devices have joined the same tailnet.
- Windows management client: `deploy/k8s/local/bin/kubectl.exe` v1.37.2, verified against the official SHA-256 file. Kubeconfig: ignored file `deploy/k8s/local/admin.conf`, context `airtraffic-kubeadm`. This kubeconfig is cluster-admin material; keep it private.

Use both kubeconfig and context for every cluster mutation:

```powershell
$kc = '.\deploy\k8s\local\bin\kubectl.exe'
$cfg = 'deploy/k8s/local/admin.conf'
& $kc --kubeconfig $cfg --context airtraffic-kubeadm get nodes
& $kc --kubeconfig $cfg --context airtraffic-kubeadm -n airtraffic get pods,pvc
```

`deploy/k8s/cluster/bootstrap-ubuntu-24.04.sh` records host prerequisites and pinned package installation. `deploy/k8s/overlays/pc` combines the application runtime with single-node Kafka/MinIO and HTTP MinIO settings. App images were built from commit `2a07ce3` inside the VM and imported into containerd as local tags; future worker nodes need a registry or image import. The MinIO image is a pinned archived community distribution used only inside this local cluster. Plan a maintained build before exposing services or depending on long-term security updates.

## Storage and resources

### Web interfaces

- Headlamp v0.46.0 is deployed in namespace `headlamp` as a ClusterIP Service. It is the Kubernetes management UI for nodes, workloads, logs, Jobs, storage and RBAC. From Windows, run `& $kc --kubeconfig $cfg --context airtraffic-kubeadm -n headlamp port-forward service/headlamp 18082:80 --address 127.0.0.1`, then open `http://127.0.0.1:18082/`. Run `deploy/k8s/cluster/new-headlamp-token.ps1` from PowerShell to copy an eight-hour admin login token to the clipboard, then paste it into Headlamp. The login ServiceAccount has `cluster-admin`; the Headlamp server's own ServiceAccount has no cluster-wide privileges. No Ingress or NodePort exposes Headlamp. A port-forward lasts only while its terminal process runs.
- The application dashboard is served by `airtraffic-api` at `http://127.0.0.1:18081/` with the API port-forward described below. Spark's live job UI can be forwarded from `deployment/airtraffic-stream` as `18083:4040` and opened at `http://127.0.0.1:18083/`.
- Kafka and MongoDB have no separate web UI in this deployment. The pinned MinIO community build serves the S3 API on port 9000; its port 9001 console does not listen, so no MinIO web UI is advertised. Its management UI was removed from that community release; use the S3/`mc` interface for object operations.

The StorageClass `airtraffic-local-retain` has no dynamic provisioner. Static local PVs are bound to `airtraffic-k8s`, with `Retain` reclaim policy:

| Volume | Guest path | Claim | Nominal capacity |
| --- | --- | --- | ---: |
| Kafka | `/srv/airtraffic/kafka` | `airtraffic/kafka-data` | 10 GiB |
| MinIO | `/srv/airtraffic/minio` | `airtraffic/minio-data` | 20 GiB |
| MongoDB | `/srv/airtraffic/mongo` | `airtraffic/database-mongo-0` | 10 GiB |
| Smoke test | `/srv/airtraffic/checks` | `airtraffic-infra-check/readiness-storage` | 64 MiB |

All paths are inside the Ubuntu VHDX on the PC's D: SSD. Local PV capacity is a scheduling declaration, not a filesystem quota. Pod recreation and VM reboot can retain data if the VHDX remains intact; losing the VM disk or PC loses all copies. These volumes cannot fail over to a future worker without data migration. The cluster has one control plane, one Kafka broker, one MinIO instance and one MongoDB instance.

The VM's 12 GiB RAM accommodates the measured small run, but Spark, Kafka and MongoDB run together. Watch `free -h`, `df -h /` in the VM and `kubectl ... get pods` during longer tests. At deployment, the guest had about 45 GiB free after image builds/pulls; the host D: had about 67 GiB free. Neither figure is a capacity guarantee.

## Current operating state

- Namespace `airtraffic` contains Kafka, MinIO, MongoDB, Spark stream, publisher and API. Collector (`*/2 * * * *`) and recent-analytics (`*/15 * * * *`) CronJobs were enabled after bounded one-shot acceptance. The collector logs estimate 3 OpenSky credits per request, or at most 2160 credits per day at this interval.
- The Kafka topic is `airtraffic.states.opensky.v1` (3 partitions, replication factor 1, 7-day retention). MinIO bucket is `airtraffic-states`. Stream root/checkpoints are `s3a://airtraffic-states/stream-v1`.
- The API is ClusterIP only. From Windows: `& $kc --kubeconfig $cfg --context airtraffic-kubeadm -n airtraffic port-forward service/airtraffic-api 18081:18080 --address 127.0.0.1`, then open `http://127.0.0.1:18081/`.
- Secrets were created from ignored `.env` files in `deploy/k8s/local/`. Do not commit those files, the kubeconfig, seed ISO or SSH private key. The OAuth client secret was supplied in chat, so rotate it if this conversation is shared outside the project team.

## Acceptance evidence so far

- Node `Ready`; CoreDNS and Flannel rolled out; DNS resolved `kubernetes.default.svc.cluster.local`.
- A BusyBox client reached a different pod directly and through a Service by DNS. Image pull succeeded.
- Readiness PVC bound; a Job wrote a marker; after the Job and pod were deleted and recreated, the same marker was read from the PVC.
- Authenticated OpenSky one-shots collected 6, 7 and 8 real observations. Spark dedup and Mongo/API `/api/overview?hours=24` returned 21 observations across three windows. After the later snapshot advanced the 10-minute event-time watermark, the Gold stage emitted closed windows; the publisher served 10 Gold cells. `/readyz` returned HTTP 200.
- Recent batch analytics completed first on 6 observations and then, after stream catch-up, on all 21. Publisher exposed the committed analytics run through the API.
- MongoDB and MinIO pod rollouts retained their PVCs and serving data. Kafka offsets before/after its pod rollout remained `0:4, 1:5, 2:4`. Replaying the MinIO outbox after restarts left those offsets unchanged. Spark stream was also restarted and resumed from its checkpoint.

The first VM reboot on 2026-11-27 exposed a DHCP change. After adding the persistent secondary address, a second guest reboot returned the node to `Ready` with all six application services running and all three PVCs `Bound`. Kafka offsets increased from `0:6, 1:11, 2:14` before the first reboot to `0:9, 1:15, 2:25` after the second as scheduled ingestion continued. The API returned 40 observations, four windows and 10 Gold cells; `/readyz` and dashboard `/` returned HTTP 200. No backup or restore to another disk has yet been tested.

## Recovery and redeployment

1. Check VM state in Hyper-V, then connect by SSH (`airtraffic` user, private key in the Windows user's `.ssh` directory). The static secondary address is `172.23.47.137`; DHCP may assign another address too. If Windows changes the Default Switch subnet, connect to the guest's new DHCP address and plan a controlled address/certificate repair or a dedicated stable switch. Do not reset the cluster or PVs as a shortcut.
2. Check `kubectl ... get nodes`, `kubectl ... get pods -A`, `kubectl ... get pvc -n airtraffic`, and recent namespace events. Check `journalctl -u kubelet` and `journalctl -u containerd` inside the VM for node/runtime faults.
3. The MinIO snapshot outbox can be replayed with the collector's `--replay-pending --insecure` mode. It will send only snapshots without a receipt; Kafka consumers deduplicate observation IDs. Do not delete receipts, Kafka data or Spark checkpoints to force a test pass.
4. Reapply `deploy/k8s/resources` and `deploy/k8s/overlays/pc` only after checking the target context and existing PV/PVC state. Static PV directories must exist on the correct node before pods schedule. Keep local app image tags available in containerd or replace them with pinned registry digests.
5. Before a destructive host or VM change, make an offline copy of the VHDX to another physical disk and verify it can be restored. That backup has not yet been performed; the current deployment has no protection against disk failure.
