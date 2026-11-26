# Batch analytics và bàn giao triển khai

Job `airtraffic.jobs.analyze_states` đọc Silver/Gold đã commit, ghi một run riêng. Không thay checkpoint, không gọi OpenSky và không sửa luồng streaming đang có.

## Đầu ra và ý nghĩa

| Dataset | Phép biến đổi | Ý nghĩa |
|---|---|---|
| `movement` | row_number, lag, lead, công thức Haversine | Khoảng cách, tốc độ suy ra, thay đổi độ cao giữa các vị trí quan sát |
| `gaps` | lọc elapsed_seconds theo ngưỡng | Khoảng cách thời gian giữa hai vị trí hợp lệ vượt ngưỡng |
| `density` | range window, avg/stddev, dense_rank | Baseline mật độ một giờ trước đó, z-score và xếp hạng ô trong cùng cửa sổ |
| `quality` | groupBy, broadcast join bảng nhãn nhỏ | Số observations theo trạng thái chất lượng kèm mô tả/mức độ |
| `quality_wide` | pivot | Một dòng vùng/cửa sổ với các cột chất lượng |
| `quality_long` | unpivot | Dạng dài để đối chiếu tổng và phục vụ báo cáo |

Bảng nhãn chất lượng là dimension nội bộ, không phải nguồn sân bay bên ngoài. Các trạng thái loại trừ nhau theo thứ tự: ICAO sai → thiếu tọa độ → vị trí cũ → vị trí còn lại. `usable_position` chỉ nói về ba điều kiện này, không khẳng định mọi trường đều hợp lệ.

### Quy tắc chuyển động

- Phân nhóm theo scope + ICAO, không nối máy bay giữa hai bbox.
- Chỉ dùng vị trí hợp lệ, time_position không vượt snapshot_time. Timestamp vị trí giống nhau được chọn theo snapshot mới nhất, tie-break bằng observation_id để kết quả ổn định.
- Snapshot lặp lại vị trí cũ không sinh một đoạn chuyển động mới.
- Khoảng cách dùng bán kính Trái Đất 6.371.000 m. Tốc độ là khoảng cách thẳng trên mặt cầu chia thời gian, không phải tốc độ đường bay thực tế.
- Mặc định gap >600 giây; tốc độ suy ra >400 m/s được gắn cờ. Đây là ngưỡng kiểm tra chất lượng cấu hình được, không kết luận sự cố bay.
- Gap ở đây là khoảng giữa **vị trí hợp lệ**, không đồng nghĩa collector mất dữ liệu. Không suy ra đoạn trước/sau ranh giới run; không gọi các đoạn là chuyến bay hoàn chỉnh.

### Quy tắc mật độ

Baseline lấy các ô/cửa sổ đã xuất trong 3.600 giây trước cửa sổ hiện tại, loại chính cửa sổ hiện tại. Chỉ tính z-score khi có ≥3 mẫu và độ lệch chuẩn >0. Không điền các cửa sổ thiếu bằng 0. Baseline ít dữ liệu là hạn chế thực tế; z-score là chỉ số mô tả, chưa phải mô hình phát hiện bất thường đã được đánh giá.

## Chạy local

```bash
make states-stream-build
# Chỉ cần MinIO, không cần bật collector/Kafka:
docker compose -f deploy/local/compose.yaml up -d minio
make analytics-run START=1795414500 END=1795429500 \
  OUTPUT=s3a://airtraffic-states/analytics/my-unique-run
make analytics-test
```

START/END là Unix UTC seconds, căn theo 300 giây, khoảng [start,end). CLI còn có `--gap-seconds`, `--max-speed-mps`, `--baseline-seconds`, `--shuffle-partitions`; xem `src/airtraffic/analytics/config.py`.

Output không được tồn tại trước khi chạy. `run-config.json`, báo cáo số dòng và physical plans được lưu cùng run; chỉ gốc run có `_SUCCESS` mới được tiêu thụ. Lỗi giữa chừng để lại run chưa hoàn tất, cần run ID mới khi chạy lại. Không coi `_SUCCESS` của một dataset con là hoàn tất cả run.

## Hiệu năng đã triển khai và giới hạn

- Cache Silver và các đoạn chuyển động bằng MEMORY_AND_DISK do được sử dụng lại, unpersist khi kết thúc.
- Output Parquet partition theo `event_date`; truy vấn phải filter cột này để partition pruning. Input Silver hiện chưa partition theo ngày, nên chưa có input partition pruning.
- Broadcast join với dimension nhãn nhỏ có execution plan kiểm chứng. Không ép sort-merge join trên dữ liệu nhỏ để giả lập hiệu năng Big Data.
- Lưu physical plan từng đầu ra để xem shuffle/window/join. Chưa có benchmark nhiều GB, bucketing hay đánh giá tối ưu phân tán.

## Bàn giao K8s

[Job manifest và điều kiện chạy](../deploy/k8s/apps/analytics/README.md) có image, requests/limits, Secret/ConfigMap, thời hạn chạy, non-root và filesystem chỉ đọc với `/tmp` riêng. Job mặc định suspend để chờ endpoint/image/run config thật. Manifest này chạy Spark trong một pod; chưa phải cụm executor phân tán.

## Phạm vi guideline

Đã bổ sung analytic windows, rolling statistics, ranking, pivot/unpivot, broadcast enrichment, cache/persist và output partitioning. Custom aggregation, sort-merge/multiple-join optimization, bucketing, MLlib/GraphFrames, enrichment sân bay và batch correction vào serving store **chưa triển khai**. MongoDB serving và deployment phân tán vẫn là các mốc tiếp theo; không coi job này là hoàn tất toàn bộ project.
