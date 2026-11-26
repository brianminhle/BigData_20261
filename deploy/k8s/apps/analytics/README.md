# Analytics Job: deployment handoff

Job mặc định `suspend: true`; không tự đọc dữ liệu hoặc ghi output khi apply. Đây là **Spark local[2] trong một pod** để nghiệm thu ứng dụng trên K8s; chưa phải Spark executors phân tán. Muốn chạy phân tán cần chốt Spark-on-K8s driver/executor, service account, RBAC và cấu hình mạng ở nhánh deploy.

Điều kiện:

1. Namespace `airtraffic`, MinIO/Silver/Gold tồn tại và endpoint truy cập được từ pod.
2. ConfigMap `airtraffic-states`, Secret `airtraffic-states-credentials` từ cấu hình ingestion. Quyền S3 tối thiểu đọc nguồn và ghi prefix analytics; không cần OpenSky credentials cho analytics.
3. Build `deploy/images/state-stream/Dockerfile`, đưa image lên registry hoặc load vào node. Sửa image trong kustomization thành tag/digest thực; tag `local` chỉ cho nghiệm thu local.
4. Sao chép `run.env.example` thành file riêng, điền khoảng [start,end) căn theo 300 giây và output run ID mới. Không overwrite output cũ, kể cả lần trước thất bại.
5. Mặc định S3 dùng TLS. Nếu môi trường thử nghiệm MinIO dùng HTTP, thêm `--insecure` vào args; không tự tắt TLS cho cluster.

```bash
kubectl -n airtraffic create configmap airtraffic-analytics-run --from-env-file=/path/to/run.env
kubectl apply -k deploy/k8s/apps/analytics
# Sau khi kiểm tra image, endpoint và run config:
kubectl -n airtraffic patch job airtraffic-analytics --type=merge -p '{"spec":{"suspend":false}}'
kubectl -n airtraffic logs -f job/airtraffic-analytics
```

Không retry tự động: output đang ghi dở không được coi là hoàn tất. Chỉ dùng run có `_SUCCESS` tại gốc. Lần chạy tiếp theo dùng Job name và output mới; dọn Job cũ theo chính sách vận hành.

Giới hạn: requests/limits là cấu hình khởi đầu, chưa đo trên cụm. `emptyDir` cần đủ chỗ cho shuffle/spill. Chưa bật CronJob vì khoảng thời gian, lịch và backfill cần được chốt trước khi tự động hóa.

Lưu ý đã phát hiện khi kiểm tra image: Snappy giải nén native `.so` vào thư mục tạm. `/tmp` phải cho phép nạp thư viện native; mount `noexec` gây `UnsatisfiedLinkError`. Docker nghiệm thu dùng tmpfs có `exec`; K8s cần xác nhận mount policy thực tế của `emptyDir`. Không bỏ filesystem chỉ đọc ở root để xử lý vấn đề này.
