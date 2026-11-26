# State collector trên cụm tương lai

Manifest chỉ chuẩn bị ứng dụng, không tạo MinIO/Kafka hoặc thay phần kubeadm. CronJob mặc định `suspend: true`. Chưa chạy trên Kubernetes.

Trước khi bật: tạo namespace bằng `deploy/k8s/resources`, triển khai MinIO/Kafka với DNS trong ConfigMap hoặc sửa endpoints, tạo bucket/topic, cung cấp quyền API, build/push image rồi thay image trong Kustomization. Default MinIO dùng HTTPS; cấu hình TLS tin cậy cho endpoint trước khi chạy. Credentials phải có quyền đọc/list/ghi object, đọc bucket; không dùng tài khoản root local cho production.

Copy `credentials.env.example` vào `deploy/k8s/local/states-credentials.env` (đã ignore), điền secrets tại máy, rồi tạo Secret:

```bash
kubectl -n airtraffic create secret generic airtraffic-states-credentials \
  --from-env-file=deploy/k8s/local/states-credentials.env
kubectl kustomize deploy/k8s/apps/states
kubectl apply -k deploy/k8s/apps/states
```

Chỉ bật sau khi kiểm tra endpoint, credentials và quyền sử dụng OpenSky. `Forbid` ngăn chồng Job của cùng CronJob, không khóa collector khác; tắt collector local nếu dùng chung bucket. Job có deadline 110 giây, snapshot chưa có receipt sẽ được thử lại lượt sau. CronJob một lần không lưu thời hạn 429 xuyên các Job; khi hết quota phải suspend/tăng lịch trước khi tiếp tục, không xem lịch 2 phút là cơ chế tự backfill. Lịch phù hợp backlog nhỏ; backlog lớn cần worker chạy lâu hơn.

Các lệnh trên là hướng dẫn triển khai tương lai. Kiểm tra schema chỉ xác nhận cấu trúc manifest, không xác nhận DNS, PVC, TLS, image có sẵn trên mọi node hoặc kết nối dịch vụ.
