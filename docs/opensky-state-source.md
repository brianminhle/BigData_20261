# Ingestion OpenSky state vectors

Luồng chính: **API → lưu snapshot vào MinIO → phát observations vào Kafka**. Code ở `ingestion/state_outbox.py`, `storage/snapshots.py`, `jobs/collect_states.py`. OAuth API ở `ingestion/opensky_api.py` và Kafka publisher ở `ingestion/state_kafka.py`.

## Chạy

Cần Python 3.10+, MinIO bucket `airtraffic-states` có sẵn và Kafka topic `airtraffic.states.opensky.v1` có sẵn. Credentials cung cấp bằng môi trường, không commit; CLI không tự nạp `.env`. Kafka bootstrap phải truy cập được từ nơi chạy CLI.

```bash
pip install .
export AIRTRAFFIC_MINIO_ENDPOINT=localhost:9000
export AIRTRAFFIC_STATES_BUCKET=airtraffic-states
export AIRTRAFFIC_KAFKA_BOOTSTRAP_SERVERS=localhost:19092
# Export AIRTRAFFIC_MINIO_ACCESS_KEY và AIRTRAFFIC_MINIO_SECRET_KEY.
# Export AIRTRAFFIC_OPENSKY_CLIENT_ID và AIRTRAFFIC_OPENSKY_CLIENT_SECRET.

airtraffic-collect-states --once --insecure
airtraffic-collect-states --interval-seconds 120 --insecure

# Chỉ phục hồi snapshot chưa gửi xong; không cần OpenSky credentials.
airtraffic-collect-states --replay-pending --insecure
```

`--insecure` dùng HTTP cho MinIO local, mặc định HTTPS. `--endpoint` nhận host:port. `--bucket` chọn bucket; `--topic` chọn topic. `--bbox LAT_MIN LAT_MAX LON_MIN LON_MAX` đổi vùng; mặc định `8 24 102 110`. Topic init của Compose chỉ tạo raw topic cũ, cần tạo state topic riêng bằng công cụ quản trị Kafka. Không chạy collector nhiều replica trên cùng bucket/prefix.

[REST API OpenSky](https://openskynetwork.github.io/opensky-api/rest.html) cung cấp OAuth2 và quota. Vùng mặc định tốn 3 credits/request: 120 giây khoảng 2.160 credits/ngày, chưa tính ứng dụng khác dùng cùng tài khoản. Collector tôn trọng header retry của 429. [Điều khoản OpenSky](https://opensky-network.org/about/terms-of-use) yêu cầu thỏa thuận bằng văn bản cho vận hành API trong hệ thống tự động; nhóm cần xác nhận trước khi chạy thật.

## Lưu trữ và phục hồi

```text
snapshots/opensky/<sha256>.json
published/<hash-topic>/<sha256>.json
```

Snapshot object chứa JSON response gốc (giữ cấu trúc/giá trị, không giữ whitespace HTTP), bbox, thời điểm nhận và checksum. Mỗi PUT hoàn tất là một object đầy đủ. Chỉ gửi Kafka sau khi snapshot đã lưu; đọc lại và kiểm checksum trước khi phát. Có receipt sau khi tất cả Kafka futures đã ACK; receipt phân biệt theo topic. Không thay broker/cluster đằng sau cùng tên topic với cùng bucket nếu muốn phát lại toàn bộ: receipt không phân biệt cluster. Dùng bucket mới hoặc topic mới cho replay sang cluster khác.

Khi khởi động và trước mỗi lần lấy dữ liệu mới, collector quét snapshot chưa có receipt để gửi lại. Nếu bị lỗi giữa chừng, có thể gửi trùng một phần snapshot. Event ID ổn định cho phép consumer dedup; Kafka key không tự loại bỏ duplicate. `--replay-pending` hoàn thành khi outbox đã drain hoặc thoát lỗi; `--once` drain rồi thực hiện một lần lấy snapshot mới.

Schema event v2 giữ `snapshot_time` làm `event_time`, `time_position` và `last_contact` trong `state`, cùng `collected_at`, `source_row`, `source_object` (bucket, key, sha256). ID thay đổi nếu nội dung hàng thay đổi ngay cả khi timestamp không đổi. Hai snapshot khác thời điểm nhưng cùng vị trí vẫn là hai observation hợp lệ.

Empty snapshot được lưu và ghi receipt với số ACK bằng 0. Snapshot có hàng sai schema được giữ lại và chặn lượt publish trước khi gửi bất kỳ hàng nào của snapshot đó. Không có cơ chế tự bỏ qua/quarantine lỗi; cần kiểm tra object và sửa adapter nếu schema nguồn thay đổi.

## Giới hạn hiện tại

- At-least-once, chưa có transaction xuyên MinIO/Kafka hay lock giữa các collector.
- Quét toàn bộ snapshot/receipt mỗi lượt; cần index outbox khi lịch sử lớn.
- Nếu API response chưa lưu được vào MinIO và tiến trình chết thì response có thể mất; không tuyên bố bảo toàn dữ liệu chưa được lưu.
- Khi Kafka/backlog lỗi, ngừng gọi API cho đến khi phục hồi; các snapshot đã bỏ lỡ chưa được backfill tự động. Lịch polling không bảo đảm có quan sát cho mọi máy bay trong vùng.
- Thứ tự recovery theo object listing không bảo đảm thứ tự event time. Consumer phải dùng timestamp nguồn.
- Giữ cả snapshots và receipts; không xóa snapshot khi còn cần truy xuất nguồn hoặc tính batch.
- Spark consumer, quality/window queries và dashboard mới chưa được triển khai. Không đưa topic state vectors vào raw archive/decoder hiện có.

## Kiểm tra

```bash
PYTHONPATH=src python -m unittest discover -s tests/unit -p 'test_opensky_*.py' -v
```

Tests dùng JSON fixture và S3/Kafka giả lập: lỗi ghi trước gửi, ACK thất bại, crash trước receipt, restart không gọi API, dedup lần thu lại, changed content, empty/malformed snapshot và checksum sai. End-to-end local với API và dịch vụ thật đã kiểm tra; xem [kết quả](local-verification.md).

## Chuẩn hóa và kiểm tra local

Đã có `airtraffic-process-states` đọc snapshot MinIO và xuất observations/quality/batch windows. Xem [local runbook](../deploy/local/README.md). `airtraffic-collect-states --once --anonymous --insecure` cho phép một request public để kiểm tra; không chạy polling anonymous liên tục.
