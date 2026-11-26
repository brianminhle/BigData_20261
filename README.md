# Air Traffic Analytics

Nguồn chính là **OpenSky state vectors**, phân tích mật độ theo không gian–thời gian, hành trình quan sát được và chất lượng dữ liệu. [Blueprint hiện tại](docs/project-blueprint.md) là kế hoạch thực thi; raw ADS-B/CPR đã rời phạm vi chính.

[Sơ đồ tổng thể hệ thống (draw.io)](docs/system-overview.drawio) gồm luồng state vectors hiện tại, kiến trúc đích và trang riêng cho raw replay cũ.

## Ingestion hiện tại

```text
OpenSky API → Collector → MinIO snapshots → Kafka state vectors
```

Snapshot được lưu trước khi phát Kafka; outbox trong MinIO cho phép gửi lại sau lỗi/restart. Có thể có duplicate khi crash sau ACK, consumer cần dedup theo observation ID. [Cấu hình và recovery](docs/opensky-state-source.md) mô tả cách chạy `airtraffic-collect-states`, bucket/topic cần có và các giới hạn. Luồng local có phép kiểm tra API/MinIO/Kafka thật; xem [kết quả kiểm tra](docs/local-verification.md). Đã có [Spark consumer state vectors](docs/state-streaming.md): Kafka → Bronze → Silver có watermark/dedup → Gold cửa sổ 5 phút, cùng checkpoint trên MinIO.

## Chạy local và kiểm tra dữ liệu

Không cần Kubernetes: xem [hướng dẫn local](deploy/local/README.md).

```bash
make states-up
make states-test
make states-check-api
make states-report
make states-stop
```

Lệnh API thực hiện một lần anonymous. Report nằm ở `data/state-reports/`: observations chuẩn hóa, quality JSON và batch reference số aircraft theo ô/cửa sổ. Đây là batch local, chưa triển khai Spark consumer state vectors.

## Runtime raw cũ để tham khảo

Các lệnh bên dưới là pipeline raw replay/HDFS cũ, không phải pipeline state vectors mới. Deploy Kubernetes vẫn được làm ở nhánh riêng.

## Chạy runtime raw replay hiện có

Cần Docker Compose và file Avro mẫu. File nguồn là dữ liệu local, không commit vào Git.

```bash
mkdir -p data/raw
curl --fail --location \
  'https://raw.githubusercontent.com/openskynetwork/osky-sample/1b179c1c541b178944010a2eb6fa5189a4f9193d/avro/raw20150421_sample.avro' \
  -o data/raw/raw20150421_sample.avro
docker compose up -d --build
RUN_ID="raw-$(date +%s)"
docker compose run --rm producer --run-id "$RUN_ID" \
  --max-rows 1000 --events-per-second 100
docker compose run --rm inspect --run-id "$RUN_ID" --expect-rows 1000
docker compose run --rm batch --run-id "$RUN_ID" \
  --output "hdfs://namenode:9000/airtraffic/silver/${RUN_ID}-v1"
```

Producer kiểm tra SHA-256 cố định của mẫu trước khi phát. Archive chạy liên tục; nếu `inspect` chưa thấy đủ record, chờ archive commit rồi chạy lại. Mỗi batch run cần output path mới. Dừng service bằng `docker compose stop` để giữ volume Kafka/HDFS.

## Luồng raw replay và code đang có

```text
Avro → producer → Kafka → archive → Bronze
                       └→ stream → streaming epochs
Bronze → batch → Silver version
```

- `src/airtraffic/ingestion/`: đọc nguồn, gửi Kafka, giữ thông tin gốc trong Bronze.
- `src/airtraffic/processing/`: giải mã ADS-B và dựng reception, position, state.
- `src/airtraffic/pipelines/`: biến đổi batch dùng chung và quản lý streaming epoch.
- `src/airtraffic/storage/`: kiểm tra và công bố output Silver.
- `src/airtraffic/jobs/`: CLI và Spark entrypoints; package được cài sẵn trong image.
- `deploy/images/`: image local; `deploy/k8s/`: chuẩn bị cụm, chưa triển khai pipeline.

[Hướng dẫn cấu trúc repo](docs/repository-structure.md) giải thích ranh giới từng phần. [Hướng dẫn Kubernetes](docs/kubernetes-setup.md) chỉ áp dụng cho bước chuẩn bị cụm; không biến Compose thành triển khai phân tán.

## Trạng thái

Pipeline raw replay local có ingestion, Bronze archive, batch reconstruction và streaming epochs. Importer source → MinIO landing đã có code và unit tests; chưa kiểm tra với server MinIO thật. Đây là runtime raw cũ; lộ trình hiện tại nằm trong blueprint state vectors. Các cấu hình Kubernetes thuộc nhánh deploy. Input `data/raw/` và cấu hình riêng trong `deploy/k8s/local/` được Git bỏ qua.

## Cấu hình local

Copy `.env.example` thành `.env` nếu cần thay default. Compose truyền các biến `AIRTRAFFIC_*` vào container; `topic-init` dùng cùng topic và bootstrap server với các job. Địa chỉ phải truy cập được **từ container**, ví dụ `kafka:9092`. Thứ tự ưu tiên của job là CLI → biến môi trường → default trong package.

Producer và stream hỗ trợ `--bootstrap-servers`, `--topic`. Stream thêm `--output-root`; archive có `--output`, `--checkpoint`; batch có `--source`, `--output`. Nếu override topic bằng CLI, cần tạo topic tương ứng và cấu hình các consumer đọc cùng topic.

## Kiểm tra image và hồi quy

```bash
make config
make test
```

`make test` build image, chạy kiểm tra cấu hình/CLI, importer raw và fixture giải mã, deduplication, temporal matching, attribute freshness, Silver publication. Chỉ mount thư mục test; source ứng dụng được lấy từ package trong image. Các kiểm tra này không cần khởi động Kafka/HDFS/MinIO hay tải Avro.

Kiểm tra streaming với dịch vụ thật chạy riêng, sau khi Kafka/HDFS sẵn sàng:

```bash
docker compose up -d kafka topic-init namenode datanode-1 datanode-2
make test-streaming
```

Test streaming tạo run riêng và ghi output/checkpoint vào storage đã cấu hình. Đây vẫn là kiểm tra runtime HDFS trong giai đoạn chuyển đổi; chưa xác nhận MinIO hoặc Kubernetes.

## Dashboard local

[Hướng dẫn truy vấn và dashboard](docs/dashboard.md): `make dashboard-export`, sau đó `make dashboard-serve` và mở http://localhost:18080. Dashboard đọc bản tổng hợp Silver/Gold; không gọi OpenSky.

## Batch analytics nâng cao

[Chuyển động, observation gaps, rolling density và quality analytics](docs/advanced-analytics.md). Chạy bằng `make analytics-run START=... END=... OUTPUT=...`; image dùng chung Spark, có [Job K8s để bàn giao](deploy/k8s/apps/analytics/README.md).

## MongoDB serving (luồng chính)

`make serving-build && make serving-up` bật publisher và dashboard tại http://localhost:18081, không gọi OpenSky. [Chi tiết](docs/mongodb-serving.md). `make runtime-build && make runtime-local-up` mới bật toàn bộ luồng có collector và batch định kỳ. [Bàn giao Kubernetes](deploy/k8s/apps/runtime/README.md). Dashboard file ở port 18080 vẫn là fallback thủ công.
