# Kubernetes deployment audit — 2026-11-26

Status: application acceptance and Ubuntu VM reboot recovery verified; Windows host restart and off-disk recovery remain unverified.

## Repository

- Branch `main` was fast-forwarded from `5848069` to `2a07ce3` after checking local changes. Deployment manifests, scripts and operations records were added afterward; they remain uncommitted for review.
- No `AGENTS.md` found in the repository or its parent directories.
- The active architecture in `docs/project-blueprint.md` is OpenSky state vectors → collector → MinIO snapshots/outbox → Kafka → Spark streaming/batch analytics → MongoDB publication → API/dashboard.
- Commit `2a07ce3` added the Spark, MongoDB, API/dashboard code and application manifests. Local test evidence is in `docs/local-verification.md`; current Kubernetes evidence is in `docs/kubernetes-operations-2026-11-26.md`.
- `compose.yaml` runs the older raw ADS-B replay pipeline with Kafka, HDFS and local-mode Spark. The newer state-vector application manifests from `2a07ce3` needed Kafka/MinIO and static PVs; these were added in `deploy/k8s/apps/infra` and applied through `deploy/k8s/overlays/pc`.
- No local source data or credentials were present in the repository at initial inspection. OAuth credentials supplied later were stored only in ignored local secret files and the Kubernetes Secret, not Git.
- The supplied task attachment ends mid-sentence in section 7; remaining requirements are needed.

## Host and infrastructure

- Windows 11 Pro build 22621, x86-64, Intel Core i5-12600K, 10 physical cores / 16 logical processors, 32 GiB installed RAM, approximately 15.5 GiB available at inspection.
- One Lexar NM790 SSD; C: NTFS approximately 9.2 GiB free, D: NTFS approximately 80.3 GiB free (values are time-sensitive).
- Hyper-V is enabled. Elevated inspection found no pre-existing VMs and two pre-existing internal switches (`Default Switch` and `WSL (Hyper-V firewall)`).
- Ubuntu 24.04.1 WSL2 is available with systemd; no K3s binary or existing K3s data found. WSL2 currently has a NAT address and no stable LAN address.
- Docker CLI 29.5.2 and kubectl client 1.34.1 are installed. Docker Desktop daemon is stopped. Kubectl had no current context at initial inspection.
- Ethernet LAN address observed: 192.168.1.19. DNS and HTTPS connectivity to public sites worked. Tailscale interface was present but had only a link-local IPv4 address at inspection.

## Decision and constraints

The user chose upstream Kubernetes via kubeadm and authorized an Ubuntu VM. VM `airtraffic-k8s` now runs Ubuntu 24.04 LTS on a dynamic 60 GiB VHDX on D:, with 6 vCPU and 12 GiB RAM. The official Ubuntu cloud image was verified against Ubuntu's SHA-256 list. Cloud-init and SSH succeeded; the guest root filesystem is approximately 58 GiB. Containerd 2.2.1 is active with systemd cgroups, and kubeadm/kubelet/kubectl 1.37.2-1.1 are installed and held. Kubernetes control-plane images were pulled. The available D: capacity is limited, and C: has too little headroom for a default Docker Desktop image store.

The VM currently uses the Hyper-V Default Switch and DHCP address `172.23.47.137`; this address can change after a host restart. An attempted isolated management switch creation was canceled at the Windows UAC prompt, so it was not created. The cluster was then initialized on the current DHCP address to continue the one-PC deployment without changing host networking. Node, networking, storage and a first end-to-end data run have been verified; stable multi-host access, off-PC backup and full failure recovery have not.

Official references: [kubeadm installation](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/), [containerd runtime](https://kubernetes.io/docs/setup/production-environment/container-runtimes/), [version skew policy](https://kubernetes.io/releases/version-skew-policy/), [Hyper-V VM creation](https://learn.microsoft.com/en-us/windows-server/virtualization/hyper-v/get-started/create-a-virtual-machine-in-hyper-v).

## Verification attempted

- Six OpenSky state unit tests passed in Ubuntu WSL2.
- The complete unit suite could not load four test modules because the WSL Python environment lacks `pyspark`, `minio` and `fastavro`. This is an environment dependency failure, not a passing suite.
- Twenty-six OpenSky unit tests passed in the built application image in the VM.
- Kubernetes API, Ready node, CoreDNS, Flannel, pod-to-pod/Service networking and image pulls passed. A retained PVC marker survived Job/pod recreation.
- Kafka, MinIO, MongoDB, stream, publisher and API ran on the VM. Three authenticated OpenSky one-shot Jobs acknowledged 6, 7 and 8 observations. Spark dedup and the API returned 21 observations across three windows and 10 closed-window Gold cells. The API dashboard HTML and JavaScript returned HTTP 200; `/readyz` returned HTTP 200.
- Kafka, MinIO, MongoDB and stream pods were recreated. Kafka offsets and PVC bindings persisted; MongoDB serving remained available after its restart. A replay-pending Job did not change Kafka offsets.
- Batch analytics completed on 21 observations. The collector and analytics CronJobs were enabled for ongoing operation.
- The first VM reboot changed DHCP from `172.23.47.137` to `172.23.34.130`, preventing etcd/kubeadm from binding their configured address. A persistent secondary `172.23.47.137/20` address was added with netplan. A second Ubuntu reboot recovered the node and workloads without manual repair; the API returned 40 observations, four windows and 10 Gold cells, and all three application PVCs remained bound. Windows host restart and off-disk backup remain unverified.
