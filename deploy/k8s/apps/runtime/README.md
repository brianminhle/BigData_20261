# Runtime ứng dụng để bàn giao Kubernetes

Bundle gồm collector, stream, MongoDB, publisher, API/dashboard và CronJob batch gần nhất. Không tạo cluster, Kafka hoặc MinIO. Cần namespace `airtraffic`, DNS/service cho Kafka/MinIO, bucket/topic và storage class/PV từ nhánh hạ tầng. MongoDB ở đây là **một node có PVC**, không phải replica set HA. Spark stream/publisher chạy `local[2]` trong pod; đây là deployment ứng dụng trên K8s, chưa phải Spark executors phân tán.

Riêng batch có [overlay driver/executor](../../overlays/batch-distributed/README.md)
và script nghiệm thu trên dữ liệu thật. Overlay đó thay CronJob batch; stream và
publisher vẫn chạy local. Hồ sơ cụm thật nằm trong
[tài liệu vận hành](../../../../docs/kubernetes-operations-2026-11-26.md).

## Chuẩn bị một lần

1. `make runtime-build`; tag/push ba image `airtraffic-states:local`, `airtraffic-state-stream:local`, `airtraffic-serving:local` tới registry của cụm hoặc load vào node. Dùng overlay Kustomize `images` để thay bằng tag/digest đã phát hành.
2. Kiểm tra `airtraffic-states` ConfigMap: Kafka bootstrap, MinIO endpoint, topic/bucket. Các job mặc định MinIO TLS. Nếu hạ tầng thử nghiệm dùng HTTP, thêm `--insecure` cho collector/stream/publisher/batch trong overlay; không sửa các checkpoint khi đổi endpoint/topic/bbox.
3. Tạo Secret `airtraffic-states-credentials` theo [states/README.md](../states/README.md). Stream/publisher/batch chỉ nhận hai key MinIO, không nhận OAuth credentials.
4. Điền hai file `.env` từ các `.env.example` tại đây, giữ ngoài Git. Mật khẩu `publisher`/`reader` trong URI phải khớp bootstrap; percent-encode nếu có ký tự đặc biệt. URI có thể trỏ tới MongoDB managed thay vì StatefulSet; khi đó bỏ base `../mongo` trong overlay.
5. Mongo PVC yêu cầu storage class mặc định có provisioner; nếu không có, chỉ định storageClassName/PV phù hợp. Cấp phát tối thiểu 10Gi và theo dõi tăng trưởng; dữ liệu Mongo phục vụ có thể tái tạo từ lake.

```bash
kubectl -n airtraffic create secret generic airtraffic-mongo-bootstrap --from-env-file=/secure/mongo-bootstrap.env
kubectl -n airtraffic create secret generic airtraffic-serving-credentials --from-env-file=/secure/serving-credentials.env
kubectl kustomize deploy/k8s/apps/runtime
kubectl apply -k deploy/k8s/apps/runtime
```

Mongo init script chỉ chạy khi volume trống. Đổi Secret không tự đổi mật khẩu user trong database đã có: cần quy trình rotate user/URI đồng bộ, không xóa PVC để đổi mật khẩu.

## Kích hoạt và nghiệm thu

Collector và batch trong base hiện có `suspend: false`. Khi triển khai mới, đặt
`suspend: true` trước khi apply để tránh chạy khi cấu hình chưa xong. Sau khi kiểm
tra images, Secrets, endpoint và quyền bucket/topic, bật lịch bằng:

```bash
kubectl -n airtraffic patch cronjob collect-opensky-states --type=merge -p '{"spec":{"suspend":false}}'
kubectl -n airtraffic patch cronjob airtraffic-recent-analytics --type=merge -p '{"spec":{"suspend":false}}'
kubectl -n airtraffic rollout status deployment/airtraffic-api
kubectl -n airtraffic port-forward service/airtraffic-api 18081:18080
```

API readiness chỉ thành công khi Mongo có publication hoàn tất. Cụm mới có thể cần chờ snapshot đầu và ba stage xử lý xong. Publisher retry khi lake chưa có Parquet; không công bố dữ liệu rỗng thay cho lỗi đọc. Service là ClusterIP; chưa đưa API ra Internet hoặc tích hợp đăng nhập.

Luồng tự động: collector mỗi 2 phút → streaming trigger 30 giây → publisher đọc lake mỗi 120 giây sau chu kỳ trước → Mongo → dashboard poll 30 giây. Batch gần nhất chạy mỗi 15 phút, publisher chỉ chọn run có root `_SUCCESS`. Lịch không phải SLA; thời gian tính toán và watermark tạo thêm độ trễ.

## Ràng buộc vận hành

- Chỉ một stream owner cho mỗi checkpoint và một publisher cho mỗi serving database. Deployment dùng `replicas: 1`, strategy Recreate; không scale hai workload này bằng HPA. API có thể nhiều replicas vì chỉ đọc Mongo.
- Publication dùng generation bất biến + atomic pointer, không dùng distributed transaction. Giữ generation đã nghỉ và staging lỗi ít nhất một giờ rồi dọn. Không xóa generation bằng tay khi còn request đọc.
- `/livez` kiểm tra tiến trình API; `/readyz` kiểm tra publication truy cập được. Publisher log success/failure và serving `checked_at` cho phép theo dõi độ mới. Chưa có hệ thống alert trung tâm.
- `/tmp` phải writable và cho phép nạp native library Snappy; root filesystem của API/Spark read-only. EmptyDir 4Gi phục vụ shuffle/spill, cần điều chỉnh theo tải.
- Image Mongo cần khởi tạo volume/user bằng entrypoint chuẩn; pod security của cluster phải phù hợp image hoặc dùng Mongo managed/operator đã chuẩn hóa riêng.
- Giới hạn phục vụ hiện tại: tối đa 20.000 dòng mỗi dataset/view, mặc định 24 giờ; movement API phân trang ≤1.000 dòng. Vượt giới hạn khiến publisher giữ phiên bản cũ và log lỗi, không cắt dữ liệu âm thầm.

## Phạm vi đã/ chưa kiểm chứng

Đã kiểm tra local trên dịch vụ Mongo/MinIO thật, quyền read-only, ghi dở/retry,
API và trình duyệt. Cụm một node đã nghiệm thu DNS, PVC, pipeline và reboot VM;
xem hồ sơ vận hành để biết bằng chứng và giới hạn. Chưa chứng minh HA, TLS cho
MinIO, phục hồi sang ổ đĩa khác hoặc hiệu năng tải lớn.
