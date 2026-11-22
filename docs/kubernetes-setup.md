# Kubernetes bằng kubeadm: một máy trước, thêm worker sau

Xem [sơ đồ thư mục K8s](../deploy/k8s/README.md) để biết vai trò từng file.

Phạm vi: chuẩn bị cụm Kubernetes upstream, chưa triển khai pipeline. Một node
chạy control plane, etcd và workload; sau này thêm worker qua `kubeadm join`.
Một control plane vẫn là điểm lỗi duy nhất, chưa phải HA. Compose tiếp tục là
runtime ứng dụng. Các template chưa được kiểm chứng trên cụm đang chạy.

Theo ưu tiên mới, thực hiện nền cụm này trước khi chuyển pipeline sang MinIO.
Đề xuất dùng VM Ubuntu riêng trên PC Windows ở nhà khi có thể. Máy Ubuntu hiện
tại gia nhập làm worker trực tiếp trên host, không cần VM. Cài Tailscale
**bên trong VM** và trên từng Linux worker; không chỉ cài trên Windows/macOS host.
Các node dùng IP Tailscale để truy cập API server và giao tiếp pod liên node.
Bắt đầu quản trị bằng `kubectl`; sau khi node Ready và bài kiểm tra DNS/PVC đạt,
chạy Rancher Docker trên VM Ubuntu quản lý riêng, có Tailscale, rồi import cụm.
Manifest của project dùng `kubectl apply -k`. Giai đoạn lab không dùng Helm.

Ghi cấu hình thực vào bản sao `deploy/k8s/cluster/inventory.yaml.example` đặt tại
`deploy/k8s/local/inventory.yaml`. Đây là bảng thông tin triển khai, không phải
manifest để apply hoặc inventory Ansible. Chưa điền máy đích/IP thì không chạy
các lệnh thay đổi host ở dưới.

### Mạng Tailscale khi các node ở nhiều nơi

Sau khi Tailscale chạy, ghi `tailscale ip -4` của từng **Linux node** vào
inventory. Điền IP Tailscale của control plane vào `init.yaml` tại
`advertiseAddress`, `controlPlaneEndpoint`, `certSANs` và `node-ip`. Điền IP
Tailscale tương ứng của worker vào `join.yaml`, kể cả `node-ip`. Địa chỉ control
plane cần giữ ổn định; thay đổi nó sau `kubeadm init` liên quan tới certificate
và cấu hình join. Không dùng IP Tailscale của Windows host làm `node-ip` cho VM.

Tailscale phải hoạt động trước khi kubelet/kubeadm khởi động sau reboot. Khi có
worker, cấu hình DaemonSet Flannel dùng `--iface=tailscale0`; giữ bản manifest
đã chỉnh và checksum trong `deploy/k8s/local/`. Flannel VXLAN dùng UDP 8472
giữa các node, nên kiểm tra quyền truy cập trong tailnet và firewall của node.
Pod CIDR/service CIDR phải không trùng các mạng LAN/VPN đang định tuyến.

Tailscale có MTU tối đa 1280; VXLAN thêm overhead. Kiểm tra MTU pod và kết nối
pod-pod liên node với cả gói nhỏ và lớn trước khi đặt Kafka hoặc MinIO lên cụm.
`tailscale ping`/`tailscale status` cho biết kết nối direct hay relay. Relay
có thể làm Spark shuffle và MinIO replication chậm. MacBook không cần tham gia
cụm để phát triển hoặc quản trị qua Rancher/kubectl. Nếu muốn chạy pod trên
MacBook thì cần Linux VM ARM64 và image tương thích; máy ngủ/đóng nắp sẽ rời
cụm, nên không dùng laptop làm nơi đặt volume bền vững.

## 1. Máy đích và mạng

Hướng dẫn lệnh dưới đây dành cho **Ubuntu 24.04 có systemd, máy/VM mới dành cho
cụm**. Không chạy trong sandbox IDE, không ghi đè runtime trên máy đang phục vụ
Docker/cluster khác. Nếu máy hiện tại đang chạy Compose, nên tạo VM Linux riêng
với mạng truy cập Internet và Tailscale chạy bên trong VM.

- Dự trù VM Kubernetes ban đầu 4 vCPU, 8 GiB RAM và ít nhất 40 GiB đĩa; tăng tài nguyên theo inventory trước khi chạy pipeline. VM Rancher riêng dự trù 2 vCPU, 4 GiB RAM và khoảng 20 GiB đĩa, cần đối chiếu yêu cầu bản Rancher sẽ cài.
- Mỗi node có IP Tailscale ổn định, hostname/MAC/product UUID riêng, giờ đồng bộ.
- Pod CIDR `10.244.0.0/16`, service CIDR `10.96.0.0/12` không được trùng LAN/VPN/Docker.
- Các node cần DNS/HTTPS ra ngoài để tải package, image và manifest.
- Đĩa cho `/var/lib/etcd`, `/var/lib/containerd`, `/var/lib/kubelet` cần đủ chỗ.

| Cổng đích | Phạm vi truy cập |
| --- | --- |
| TCP 6443 control plane | Worker và máy quản trị |
| TCP 10250 các node | Control plane đến kubelet |
| UDP 8472 các node | Giữa IP Tailscale của các node, Flannel VXLAN |
| TCP 2379–2380 | etcd/control plane; không mở cho Internet hoặc worker |
| TCP 10257, 10259 | Control plane nội bộ; giữ bind loopback mặc định |

Cho phép forwarding lưu lượng pod/service trong firewall. Chưa cần NodePort,
ingress hay LoadBalancer. Flannel cung cấp kết nối pod; không thực thi
NetworkPolicy. Khi cần cô lập workload, chọn giải pháp mạng/policy phù hợp.

## 2. Chuẩn bị mọi node

Chạy từ gốc repository trên máy đích. Các file host nằm trong `deploy/k8s/cluster/`.
Tắt swap cho phiên hiện tại, rồi chỉnh `/etc/fstab` và dịch vụ zram/swap nếu có
để swap không bật lại sau reboot. Kiểm tra `swapon --show` không còn kết quả.

```bash
sudo swapoff -a
sudo install -m 0644 deploy/k8s/cluster/modules-load.conf /etc/modules-load.d/airtraffic-k8s.conf
sudo modprobe overlay
sudo modprobe br_netfilter
sudo install -m 0644 deploy/k8s/cluster/sysctl.conf /etc/sysctl.d/99-airtraffic-k8s.conf
sudo sysctl --system
sudo apt-get update
sudo apt-get install -y ca-certificates curl gpg containerd
mkdir -p deploy/k8s/local
containerd config default > deploy/k8s/local/containerd.toml
sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' deploy/k8s/local/containerd.toml
grep SystemdCgroup deploy/k8s/local/containerd.toml
```

Kiểm tra file sinh ra có `SystemdCgroup = true` và `disabled_plugins` không chứa
`cri`. Sinh từ binary đã cài giúp dùng đúng schema của containerd 1.x/2.x.
Các lệnh sau chỉ dành cho host mới, không ghi đè cấu hình containerd đang sử dụng:

```bash
sudo install -d -m 0755 /etc/containerd
sudo install -m 0644 deploy/k8s/local/containerd.toml /etc/containerd/config.toml
sudo systemctl enable --now containerd
sudo systemctl restart containerd
sudo systemctl is-active containerd
```

## 3. Cài kubeadm, kubelet, kubectl

Baseline của hướng dẫn là Kubernetes **1.35**, dùng bản patch có trong repo lúc
cài và ghi lại phiên bản; không tuyên bố đây là bản mới nhất. Mọi node dùng cùng
phiên bản đã ghi, tránh tự nâng cấp độc lập. Template dùng API kubeadm `v1beta4`.

```bash
sudo install -d -m 0755 /etc/apt/keyrings
curl --fail --location https://pkgs.k8s.io/core:/stable:/v1.35/deb/Release.key -o deploy/k8s/local/Release.key
sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg deploy/k8s/local/Release.key
echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.35/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list
sudo apt-get update
sudo apt-get install -y kubelet kubeadm kubectl
sudo apt-mark hold kubelet kubeadm kubectl
sudo systemctl enable kubelet
kubeadm version -o short > deploy/k8s/local/kubernetes-version.txt
dpkg-query -W -f='${Version}\n' kubeadm > deploy/k8s/local/kubernetes-package-version.txt
```

Kubelet có thể restart liên tục trước init/join; lúc này chưa có cấu hình cụm.
Thư mục `deploy/k8s/local/` được Git bỏ qua; dùng cho cấu hình thật và bằng
chứng kiểm tra, không commit token hoặc kubeconfig quản trị.

## 4. Khởi tạo control plane

```bash
cp deploy/k8s/cluster/init.yaml.example deploy/k8s/local/init.yaml
nano deploy/k8s/local/init.yaml
```

Thay mọi `REPLACE_WITH_CONTROL_PLANE_IP` bằng IP LAN; thay phiên bản bằng nội dung
`kubernetes-version.txt` (có tiền tố `v`). Endpoint hiện là IP cố định; nếu sau này
cần HA, phải thiết kế endpoint/load balancer ổn định riêng. Với nhiều card mạng,
đảm bảo IP này là interface các node dùng để liên lạc.

```bash
grep REPLACE_WITH deploy/k8s/local/init.yaml
```

Chỉ tiếp tục khi không còn placeholder. `validate` kiểm tra schema, còn dry-run
kiểm tra thêm điều kiện host; không bỏ qua lỗi preflight:

```bash
sudo kubeadm config validate --config deploy/k8s/local/init.yaml
sudo kubeadm init --config deploy/k8s/local/init.yaml --dry-run
sudo kubeadm config images pull --config deploy/k8s/local/init.yaml
sudo kubeadm init --config deploy/k8s/local/init.yaml
```

Dùng kubeconfig cụm này một cách tường minh. Định nghĩa lại hàm khi mở terminal:

```bash
kc() { sudo kubectl --kubeconfig /etc/kubernetes/admin.conf "$@"; }
kc get nodes -o wide
```

Node có thể NotReady và CoreDNS Pending cho đến khi cài CNI. Kubeadm không cài
sẵn mạng pod, provisioner lưu trữ hoặc metrics-server.

## 5. Mạng pod và scheduling trên một máy

Dùng Flannel `v0.27.4` làm cấu hình tham chiếu cố định. Tải manifest, đọc lại
trước khi apply; thêm `--iface=tailscale0` vào danh sách `args` của container
`kube-flannel` trong DaemonSet, xác nhận đúng vị trí rồi lưu checksum của bản
đã chỉnh. Đây chưa phải xác nhận
tương thích đã đo trên máy đích.

```bash
curl --fail --location https://github.com/flannel-io/flannel/releases/download/v0.27.4/kube-flannel.yml -o deploy/k8s/local/kube-flannel.yml
less deploy/k8s/local/kube-flannel.yml
# Edit the kube-flannel DaemonSet args to include --iface=tailscale0.
nano deploy/k8s/local/kube-flannel.yml
rg -n -C 5 -- '--iface=tailscale0' deploy/k8s/local/kube-flannel.yml
sha256sum deploy/k8s/local/kube-flannel.yml > deploy/k8s/local/flannel.sha256
kc apply -f deploy/k8s/local/kube-flannel.yml
kc taint nodes airtraffic-control-1 node-role.kubernetes.io/control-plane:NoSchedule-
kc rollout status daemonset/kube-flannel-ds -n kube-flannel --timeout=240s
kc wait --for=condition=Ready node/airtraffic-control-1 --timeout=240s
kc rollout status deployment/coredns -n kube-system --timeout=180s
```

Manifest Flannel dùng `10.244.0.0/16`; nếu đổi podSubnet, sửa Network trong
ConfigMap Flannel tương ứng trước apply. Với nhiều NIC/VPN, cấu hình `--iface`
của flanneld theo mạng giữa node. Lệnh bỏ taint chỉ áp dụng node đầu tiên để
workload chạy được trong cụm một máy. Khi có worker, có thể khôi phục taint sau
khi xem xét workload/PV đang nằm trên control plane.

## 6. Namespace, local PV và smoke check

`deploy/k8s/resources/` tạo namespace `airtraffic` và StorageClass
`airtraffic-local-retain` với `kubernetes.io/no-provisioner`. Cần tạo PV thủ công:
không có dynamic provisioner ngầm định. PV mẫu chỉ dành cho bài kiểm tra.

Trên node lưu trữ (hiện là control plane):

```bash
sudo install -d -m 0755 /var/lib/airtraffic/infra-check
kc get nodes -L kubernetes.io/hostname
cp deploy/k8s/checks/local-pv.yaml.example deploy/k8s/local/local-pv.yaml
nano deploy/k8s/local/local-pv.yaml
```

Thay `REPLACE_WITH_NODE_HOSTNAME_LABEL` bằng giá trị label hostname vừa xem.
Không dùng tên node giả định nếu khác label. PV có nodeAffinity và claimRef
dành riêng cho PVC kiểm tra. Sau khi hết placeholder:

```bash
kc kustomize deploy/k8s/resources
kc apply --dry-run=server -k deploy/k8s/resources
kc apply -k deploy/k8s/resources
kc apply --dry-run=server -f deploy/k8s/local/local-pv.yaml
kc apply -f deploy/k8s/local/local-pv.yaml
kc apply -k deploy/k8s/checks
kc wait -n airtraffic-infra-check --for=condition=complete job/cluster-readiness --timeout=240s
kc logs -n airtraffic-infra-check job/cluster-readiness
kc get pvc -n airtraffic-infra-check
```

Job phải resolve được DNS Kubernetes, PVC Bound, log có `PVC write/read verified`.
Với WaitForFirstConsumer, không đợi PVC Bound trước khi tạo pod. Giữ PVC và
tạo lại Job để kiểm tra dữ liệu tồn tại qua vòng đời pod:

```bash
kc delete job cluster-readiness -n airtraffic-infra-check --wait=true
kc apply -k deploy/k8s/checks
kc wait -n airtraffic-infra-check --for=condition=complete job/cluster-readiness --timeout=240s
kc logs -n airtraffic-infra-check job/cluster-readiness
kc get nodes -o wide > deploy/k8s/local/nodes.txt
kc logs -n airtraffic-infra-check job/cluster-readiness > deploy/k8s/local/smoke.log
```

Lần hai phải có `Existing PVC marker verified after pod recreation`. Storage
local không replication/failover; capacity trong PV không tạo disk quota. Retain
giữ dữ liệu khi xóa PVC. Có thể xóa riêng Job để giữ bài kiểm tra cho lần sau.
Nếu xóa namespace smoke, PV sẽ Released và cần xử lý thủ công trước khi dùng lại;
không xóa thư mục etcd/containerd để dọn bài kiểm tra.

## 7. Thêm worker

Chuẩn bị host/runtime và apt repository theo mục 1–3 trên worker. Thay bước cài
package bằng bản chính xác ghi từ control plane (copy file version sang worker):

```bash
K8S_PACKAGE_VERSION=$(cat deploy/k8s/local/kubernetes-package-version.txt)
sudo apt-get install -y "kubelet=$K8S_PACKAGE_VERSION" "kubeadm=$K8S_PACKAGE_VERSION" "kubectl=$K8S_PACKAGE_VERSION"
sudo apt-mark hold kubelet kubeadm kubectl
sudo systemctl enable kubelet
```

Trên control plane, tạo token có hạn và ghi lệnh join vào file riêng, quyền 0600:

```bash
(umask 077; sudo kubeadm token create --ttl 2h --print-join-command > deploy/k8s/local/join-command.txt)
```

Chuyển thông tin này an toàn sang worker. Copy `deploy/k8s/cluster/join.yaml.example`
thành `deploy/k8s/local/join.yaml`, quyền `0600`. Điền endpoint, token, CA
public-key hash từ lệnh join, IP worker và tên worker duy nhất; giữ kiểm tra CA.
Token hết hạn thì tạo lại. Không commit hoặc chia sẻ công khai file chứa token.

```bash
chmod 0600 deploy/k8s/local/join.yaml
sudo kubeadm config validate --config deploy/k8s/local/join.yaml
sudo kubeadm join --config deploy/k8s/local/join.yaml --dry-run
sudo kubeadm join --config deploy/k8s/local/join.yaml
```

Trên control plane kiểm tra worker Ready, Flannel chạy trên worker. Thêm worker
không chuyển hoặc nhân bản PV cũ. Cần kiểm tra riêng pod-to-pod liên node,
scheduling trên từng máy và lỗi host trước khi tuyên bố triển khai phân tán.

## 8. Tiêu chí sẵn sàng và xử lý lỗi

- Node Ready, kube-system hoạt động, Flannel/CoreDNS rollout thành công.
- Smoke DNS/PVC thành công hai lần và có bằng chứng lưu trong `local/`.
- Reboot VM có kiểm soát, chờ node Ready rồi tạo lại Job; marker cũ vẫn đọc được.
- Kubeconfig trên máy quản trị truy cập đúng API endpoint, không chỉ truy cập từ node.
- Đã ghi phiên bản, IP/subnet và dung lượng đĩa; chưa triển khai ứng dụng.

Khi lỗi dùng `kc get pods -A -o wide`, `kc get events -A --sort-by=.lastTimestamp`,
`kc describe pods -n airtraffic-infra-check`, `sudo journalctl -u kubelet -n 100`
và `sudo journalctl -u containerd -n 100`. Metrics-server là addon riêng chưa cài,
nên `kubectl top` chưa phải tiêu chí ở giai đoạn này.

Tích hợp sau: image có code, registry, CPU architecture, MinIO distributed,
Kafka/MongoDB với storage bền vững, Spark driver/executor + RBAC và monitoring. Spark hiện chạy
`local[...]`; dựng Kubernetes chưa tự biến job thành xử lý phân tán.

## Nguồn chính thức

- [Cài kubeadm](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/install-kubeadm/)
- [Tạo cụm và scheduling một node](https://kubernetes.io/docs/setup/production-environment/tools/kubeadm/create-cluster-kubeadm/)
- [Container runtime và systemd cgroup](https://kubernetes.io/docs/setup/production-environment/container-runtimes/)
- [Kubeadm API v1beta4](https://kubernetes.io/docs/reference/config-api/kubeadm-config.v1beta4/)
- [Flannel v0.27.4](https://github.com/flannel-io/flannel/releases/tag/v0.27.4)
- [Local volumes](https://kubernetes.io/docs/concepts/storage/volumes/#local)
