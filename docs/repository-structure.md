> Hướng hiện tại là state vectors theo [blueprint](project-blueprint.md). `storage/snapshots.py` và `ingestion/state_outbox.py` thực hiện MinIO-before-Kafka; các mô tả raw/Spark bên dưới là runtime cũ để tham khảo.

# Cấu trúc repository

[Project blueprint](project-blueprint.md) chốt luồng raw ADS-B → Kafka → Spark decode/reconstruction → MinIO/MongoDB/API/dashboard. Cây dưới đây mô tả **code raw ADS-B replay đang có**; adapter raw live chưa được triển khai và chưa có feed được cung cấp. Compose vẫn chạy pipeline HDFS local để kiểm tra hồi quy; deploy được phát triển trên nhánh riêng. State vectors xử lý sẵn không nằm trên luồng thay thế raw.

```text
BigData_20261/
├── pyproject.toml                 Python build backend
├── setup.cfg                      Package metadata, dependencies và CLI
├── .env.example                   Mẫu cấu hình local, không có secrets
├── Makefile                       Build image và chạy kiểm tra
├── compose.yaml                   Kafka, Spark và HDFS chạy local
├── src/airtraffic/
│   ├── config.py                  Runtime defaults, override bằng biến môi trường
│   ├── __main__.py                Tương thích `python -m airtraffic`
│   ├── ingestion/                Khám phá/tải raw Avro, polling importer, phát Kafka, Bronze
│   ├── processing/               Giải mã ADS-B, nhóm reception, dựng position/state
│   ├── pipelines/                Phép biến đổi batch dùng chung và streaming epochs
│   ├── storage/                  Raw object/manifest MinIO và công bố Silver version
│   └── jobs/                     CLI importer/producer và Spark entrypoints
├── tests/unit/                   Cấu hình, CLI, importer và source/storage failures
├── tests/integration/            Fixture Spark và kiểm tra Kafka/HDFS recovery
├── deploy/
│   ├── images/hdfs/             Dockerfile và cấu hình HDFS local
│   ├── images/spark/            Image Spark có cài package `airtraffic`
│   └── k8s/                     Chuẩn bị kubeadm và kiểm tra cụm; chưa deploy app
└── docs/                         Blueprint và hướng dẫn vận hành
```

## Ranh giới code

| Vị trí | Trách nhiệm |
| --- | --- |
| `ingestion/` | Bảo toàn bản tin nguồn, tạo danh tính reception/delivery và row Bronze. |
| `ingestion/opensky_api.py`, `state_kafka.py` | Thu thập state vectors OpenSky trên Kafka topic riêng; không chuyển chúng thành raw reception. |
| `processing/` | Quy tắc ADS-B thuần xử lý; không quyết định endpoint hoặc cách chạy job. |
| `pipelines/batch.py` | Tạo tám bảng Silver, kiểm tra và công bố batch; streaming dùng cùng hàm `preprocess`. |
| `pipelines/streaming.py` | State, watermark, epoch và recovery của streaming. |
| `storage/silver.py` | Ghi, đọc lại, kiểm tra và đánh dấu hoàn tất version. |
| `storage/raw.py` | Ghi raw object vào MinIO, công bố manifest sau upload; checkpoint cho importer. |
| `jobs/` | Nhận tham số, cấu hình Spark/Kafka và gọi pipeline. Không import ngược từ pipeline vào job. |
| `deploy/` | Image, cấu hình host/cụm và manifest; không chứa logic ADS-B. |

`src/airtraffic/config.py` giữ default của runtime HDFS local. CLI có thể ghi đè các giá trị liên quan; biến môi trường `AIRTRAFFIC_*` cung cấp cấu hình mặc định theo môi trường. Importer riêng dùng `AIRTRAFFIC_MINIO_*` như mô tả trong [raw ingestion](raw-ingestion.md); cấu hình đó chưa chuyển Parquet/checkpoint của Spark sang MinIO.

Compose truyền cấu hình vào các job và dùng cùng topic cho `topic-init`. `make test` build image rồi chạy unit/fixture tests; `make test-streaming` yêu cầu Kafka/HDFS đang chạy. Lệnh chi tiết nằm trong README.

## Chạy local

```bash
docker compose up -d --build
docker compose run --rm producer --run-id example-raw --max-rows 1000
docker compose run --rm inspect --run-id example-raw --expect-rows 1000
docker compose run --rm batch --run-id example-raw \
  --output hdfs://namenode:9000/airtraffic/silver/example-v1
```

Input Avro nằm tại `data/raw/` và không commit. Mỗi batch output cần một đường dẫn mới. Cách chạy local nằm trong [README](../README.md); hợp đồng dữ liệu và hướng triển khai tiếp theo nằm trong [blueprint](project-blueprint.md). Việc đổi cấu trúc này chưa xác nhận runtime MinIO hoặc Kubernetes.
