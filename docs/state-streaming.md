# Xử lý state vectors bằng Spark

Luồng chính: OpenSky `/states/all` → snapshot JSON trên MinIO → Kafka → Spark → Parquet trên MinIO. Collector hiện có chịu trách nhiệm gọi API, lưu snapshot và publish. Spark không gọi API.

## Ba stage độc lập

| Stage | Đọc | Ghi | Xử lý |
|---|---|---|---|
| `archive` | Kafka từ earliest khi chưa có checkpoint | `bronze/` | Giữ payload, topic/partition/offset, dữ liệu chuẩn hóa hoặc lỗi validation |
| `dedup` | Bronze | `silver/` | Event time theo snapshot, watermark 10 phút, dedup `observation_id` |
| `windows` | Silver | `gold/` | Đếm ICAO duy nhất theo ô 1° × 1°, cửa sổ 5 phút |

Mỗi stage dùng checkpoint riêng và Spark file sink metadata. Không chạy hai instance cùng stage/output. Đọc Parquet qua Spark tại đúng thư mục đích để tôn trọng `_spark_metadata`, không gom tất cả object bằng wildcard.

Envelope version 2 được đối chiếu với source row, ID và bounding box cấu hình; mặc định 8–24°N, 102–110°E. Không đoán vùng từ tọa độ máy bay. Thay bbox phải cấu hình cả collector lẫn consumer. `_contract` ngăn tái sử dụng output/checkpoint với topic, broker, bbox hoặc watermark khác. Khi thay logic/schema cần output root mới và replay nguồn được giữ lại.

## Chạy trên PC

```bash
make states-up
make states-stream-build
make states-stream-once
make states-stream-test
```

`states-stream-once` xử lý backlog qua cả ba stage rồi thoát. Không gọi OpenSky. `states-stream-test` cần snapshot đã có trong bucket, tạo topic/prefix verification riêng, replay dữ liệu thật và thêm fixture được đánh dấu bằng prefix verification. Topic test được xóa; kết quả kiểm tra được giữ trong prefix đó.

Để chạy liên tục trên PC, khởi tạo một lượt bằng `states-stream-once`, rồi chạy cả ba query trong một Spark JVM để tiết kiệm bộ nhớ:

```bash
docker compose -f deploy/local/compose.yaml run --rm --no-deps stream \
  --stage all --output s3a://airtraffic-states/stream-v1 --insecure
```

Mỗi query vẫn có checkpoint riêng. Log tiến độ gồm stage, batch ID, số dòng đầu vào, watermark và thống kê state. `--stage all --available-now` chạy tuần tự ba stage rồi thoát. Có thể chạy từng stage riêng khi cần tách tài nguyên, nhưng không chạy đồng thời với `all` trên cùng output root.

Đây là môi trường local, chưa chứng minh HA hoặc triển khai Kubernetes.

## Ý nghĩa kết quả và giới hạn

- Event time là `snapshot_time`, không phải thời điểm máy bay phát tín hiệu. `time_position` dùng để đánh giá tuổi vị trí.
- Trường thiếu/sai giữ null và quality flag. ICAO sai hoặc vị trí ngoài bbox không được đếm. Vị trí cũ vẫn được đếm như batch reference, có cờ `stale_position`.
- Gold chỉ xuất cửa sổ đã hoàn tất theo watermark; watermark cần event mới tiến lên, không tự tiến theo đồng hồ khi nguồn im lặng. Hai snapshot gần nhau có thể chưa tạo dòng Gold nào.
- Dedup có trạng thái giới hạn theo watermark, không phải tập ID lưu vĩnh viễn. Bản ghi quá muộn vẫn nằm trong snapshot/Bronze nhưng không vào Silver; xử lý bù cần batch riêng.
- Đếm distinct dùng `collect_set` theo ô/cửa sổ, không đếm số message; bộ nhớ phụ thuộc số ICAO trong cửa sổ.
- Kiểm tra với batch trên Silver cho số đếm hiện thời; không được gọi đó là Gold streaming đã chốt. Integration test đưa event tương lai có kiểm soát vào topic test để xác minh Gold hoàn tất, không trộn fixture vào topic chính.
- Snapshot rỗng vẫn nằm trong kho snapshot nhưng không sinh observation; watermark không tiến chỉ nhờ snapshot rỗng.

## Credentials OpenSky

Đặt file tải từ OpenSky tại `credentials.json` ở gốc repo, chứa `clientId` và `clientSecret`. File đã được ignore; không đưa vào Docker image. Collector nhận `--credentials-file` hoặc hai biến môi trường hiện có.

```bash
make states-build
make states-check-auth
make states-stream-once
```

`states-check-auth` mount file chỉ đọc và gọi đúng một lần. Muốn chạy collector định kỳ, dùng lệnh tương tự nhưng bỏ `--once` (mặc định 120 giây):

```bash
docker compose -f deploy/local/compose.yaml run --rm --no-deps \
  -v "$PWD/credentials.json:/run/secrets/opensky.json:ro" \
  collector --insecure --credentials-file /run/secrets/opensky.json
```

Không cần truyền secret qua command line hoặc commit `.env`. Collector Compose chạy UID của người dùng local; nếu UID khác 1000, đặt `LOCAL_UID` và `LOCAL_GID` tương ứng để đọc file quyền 600.
