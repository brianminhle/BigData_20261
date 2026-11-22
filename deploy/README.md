# Cấu hình triển khai

| Thư mục | Dùng để làm gì? | Khi nào cần? |
| --- | --- | --- |
| `images/hdfs/` | Build image HDFS và cấu hình Hadoop | Pipeline Compose hiện tại |
| `images/spark/` | Build image Spark có cài package `airtraffic` | Pipeline Compose hiện tại |
| `k8s/` | Chuẩn bị cụm Kubernetes bằng kubeadm | Dựng cụm một máy, thêm worker sau |

Nếu đang chuẩn bị Kubernetes, bắt đầu tại [k8s/README.md](k8s/README.md).
`compose.yaml` ở gốc repository sử dụng hai thư mục image trong `images/`.
