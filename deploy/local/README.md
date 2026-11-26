# Chạy state pipeline trên máy hiện tại

Không cần Kubernetes. Docker Compose này tách biệt root `compose.yaml` (raw/HDFS cũ). Cần Docker Engine, Compose v2, Internet cho API và lần build; thử với khoảng 3 GiB RAM trống cho services/jobs, cộng dung lượng image. Đây là cấu hình một broker/một MinIO dành cho phát triển, không chứng minh HA hoặc phân tán.

## Các lệnh từ gốc repo

```bash
make states-up
make states-test
make states-check-api
make states-report
make states-recover
make states-stop
```

- `states-up`: build package vào image, chạy MinIO/Kafka và tạo bucket/topic bằng job `init`.
- `states-check-api`: một request anonymous thật → MinIO → Kafka; không bật lịch thu liên tục. Không cần OAuth cho kiểm tra này.
- `states-report`: đọc snapshots đã lưu, chuẩn hóa JSONL, xuất báo cáo chất lượng và đếm aircraft theo ô 1°/cửa sổ 5 phút. Output ở `data/state-reports/run-.../` (Git ignore).
- `states-recover`: phát lại snapshots chưa có receipt, không gọi OpenSky.
- `states-stop`: dừng containers, giữ dữ liệu/volumes để chạy tiếp. Muốn dọn cả dữ liệu: `docker compose -f deploy/local/compose.yaml down -v`.

Xác nhận end-to-end sau khi có ít nhất một snapshot:

```bash
docker compose -f deploy/local/compose.yaml run --rm --no-deps \
  --entrypoint python3 -v "$PWD/tests:/tests:ro" collector \
  /tests/integration/test_states_local.py
```

Job đối chiếu ID, hàng nguồn, timestamp, tham chiếu MinIO và receipt với Kafka. Tắt collector định kỳ khi chạy phép đối chiếu này để input không thay đổi giữa lúc kiểm tra.

## Cấu hình

MinIO API `localhost:19000`, console `localhost:19001`; Kafka từ host `localhost:19093`, trong network `kafka:9092`. Ports chỉ bind loopback. MinIO dùng credentials mặc định **chỉ local** `local-airtraffic` / `local-development-only`. Có thể override `LOCAL_MINIO_USER`, `LOCAL_MINIO_PASSWORD` qua shell hoặc `--env-file` riêng; không đổi credentials tùy tiện khi tái sử dụng volume đã có.

Nếu port đã được dùng, đổi `LOCAL_MINIO_PORT`, `LOCAL_MINIO_CONSOLE_PORT`, `LOCAL_KAFKA_PORT`. `LOCAL_UID`/`LOCAL_GID` mặc định 1000; đặt theo `id -u`/`id -g` nếu khác để job ghi báo cáo vào host. Tạo `data/state-reports` trước khi chạy batch trực tiếp; Makefile tự tạo thư mục này.

Sau khi được cấp quyền sử dụng API và có OAuth credentials trong môi trường:

```bash
docker compose -f deploy/local/compose.yaml run --rm --no-deps collector \
  --insecure --interval-seconds 120
```

Default bbox là 8–24°N, 102–110°E. Có thể truyền `--bbox LAT_MIN LAT_MAX LON_MIN LON_MAX`. Collector hiện chỉ một instance trên cùng bucket. Anonymous bị giới hạn ở `--once`, không tự fallback nếu OAuth lỗi. Backfill chưa được triển khai.

## Báo cáo và định nghĩa

- `observations.jsonl`: một observation/content revision, gồm timestamps, đơn vị nguồn (mét, m/s, độ, Unix seconds), quality flags và source reference. Không thay null bằng giá trị phỏng đoán. Trường số sai kiểu/phạm vi được chuyển null kèm flag; bản gốc luôn còn ở MinIO.
- `quality.json`: snapshot/row counts, số máy bay khác nhau, trường thiếu/sai, vị trí cũ >15 giây, số lần lặp cùng position update và các khoảng snapshot lớn hơn 1,5 lần lịch dự kiến.
- `aircraft_windows.jsonl`: số ICAO24 phân biệt theo bbox scope, ô 1° và cửa sổ UTC 5 phút. Chỉ dùng ICAO24 hợp lệ và tọa độ trong bbox. Vị trí stale vẫn được tính là quan sát snapshot và có flag riêng; không diễn giải thành vị trí mới.
- `_SUCCESS`: chỉ có khi cả batch hoàn tất. Dùng thư mục output mới mỗi lần; lỗi giữa chừng không công bố thành công.

Snapshot gap là khoảng trống trong tập đã lưu, không tự chứng minh API outage. Vị trí lặp không đồng nghĩa message bị gửi trùng. Batch tham chiếu chạy Python local, giữ tập ID trong RAM và quét toàn bộ snapshots; chỉ dành cho giai đoạn kiểm chứng dữ liệu. Chưa thay Spark analytics ở quy mô lớn hoặc streaming native watermark.

## Phân phối MinIO và kiểm tra deploy

Image MinIO local là bản lưu trữ Bitnami ghim digest, dùng để xác nhận giao thức S3. Chưa chốt phân phối production; không dùng bản local này làm tiêu chí nghiệm thu bảo mật/HA. Image app tái sử dụng Spark base ghim của repo để giảm khác biệt môi trường; job local không khởi chạy Spark.

`make states-config` và `make config` kiểm tra Compose mới/cũ. Kubernetes được chuẩn bị riêng trong `deploy/k8s/`; collector manifest nằm ở `apps/states`, mặc định suspend. Tham khảo README ở đó trước khi áp dụng lên cụm.
