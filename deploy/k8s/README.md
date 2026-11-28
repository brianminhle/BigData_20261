# Chuẩn bị Kubernetes

Toàn bộ cấu hình K8s nằm ở đây, dùng **kubeadm**. Cụm Ubuntu Hyper-V một node đã
triển khai ứng dụng; xem [hồ sơ vận hành](../../docs/kubernetes-operations-2026-11-26.md).
Overlay `overlays/pc` dùng batch local; `overlays/batch-distributed` chạy batch
với driver và hai executor pod. Xem [hướng dẫn batch phân tán](overlays/batch-distributed/README.md)
để triển khai, nghiệm thu và rollback.

```text
k8s/
├── cluster/       1. Chuẩn bị máy, tạo control plane, thêm worker
├── resources/     2. Tạo namespace ứng dụng và StorageClass
├── checks/        3. Kiểm tra DNS và dữ liệu trên đĩa
└── local/         Bản cấu hình theo máy và kết quả chạy; tự tạo, Git bỏ qua
```

| File | Mục đích |
| --- | --- |
| `cluster/inventory.yaml.example` | Ghi máy/VM, host vật lý, IP, tài nguyên và phiên bản; không apply vào Kubernetes |
| `cluster/modules-load.conf` | Module Linux cần cho container và mạng pod |
| `cluster/sysctl.conf` | Thiết lập forwarding mạng trên host |
| `cluster/init.yaml.example` | Mẫu tạo control plane đầu tiên |
| `cluster/join.yaml.example` | Mẫu thêm worker sau này |
| `resources/namespace.yaml` | Namespace `airtraffic` |
| `resources/storage-class.yaml` | StorageClass cho local PV được tạo thủ công |
| `checks/local-pv.yaml.example` | Mẫu PV kiểm tra, cần điền node chứa đĩa |
| `checks/namespace.yaml`, `pvc.yaml`, `job.yaml` | Namespace kiểm tra, yêu cầu đĩa và Job kiểm tra |

`kustomization.yaml` trong `resources/` và `checks/` liệt kê các manifest áp dụng
cùng nhau bằng `kubectl apply -k`. File `.example` phải được copy sang `local/`
và điền thông tin máy trước khi sử dụng. Không apply cả thư mục `k8s/` một lần:
cấu hình host, kubeadm và Kubernetes API có cách dùng khác nhau.

Thực hiện theo [hướng dẫn dựng cụm](../../docs/kubernetes-setup.md), theo thứ tự:
chuẩn bị host → containerd → kubeadm → mạng pod → resources → checks.
`local/` chứa cả token và cấu hình riêng, không commit vào Git.
