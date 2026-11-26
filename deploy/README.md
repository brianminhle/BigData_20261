# Cấu hình triển khai

| Thư mục | Dùng để làm gì? | Khi nào cần? |
| --- | --- | --- |
| `local/` | Kafka + MinIO + collector/batch state vectors trên một máy | Hướng phát triển hiện tại |
| `images/states/` | Image package cho collector và batch local | State-vector pipeline |
| `images/hdfs/` | Build image HDFS và cấu hình Hadoop | Pipeline Compose hiện tại |
| `images/spark/` | Build image Spark có cài package `airtraffic` | Pipeline Compose hiện tại |
| `k8s/` | Chuẩn bị cụm Kubernetes bằng kubeadm | Dựng cụm một máy, thêm worker sau |

Nếu đang chuẩn bị Kubernetes, bắt đầu tại [k8s/README.md](k8s/README.md).
`compose.yaml` ở gốc repository sử dụng hai thư mục image trong `images/`.

Hướng hiện tại: [local/README.md](local/README.md). File Compose ở gốc vẫn là runtime raw/HDFS cũ. Kubernetes app được chuẩn bị tại [k8s/apps/states](k8s/apps/states/README.md), chưa chạy trên cụm.
