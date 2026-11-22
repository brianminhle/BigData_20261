# Air Traffic Analytics — kế hoạch hiện tại

Cập nhật 22/11/2026. **Nguồn chính duy nhất: OpenSky state vectors.** Raw ADS-B/CPR đã rời phạm vi chính; [blueprint raw cũ](archive/raw-project-blueprint.md) được giữ làm tham khảo, không dùng để giao việc. Deploy Kubernetes được làm trên nhánh riêng theo [hướng dẫn cụm](kubernetes-setup.md).

## Bài toán

Theo dõi mật độ máy bay trong vùng quan sát theo không gian–thời gian, phân tích các đoạn hành trình quan sát được và chất lượng dữ liệu. Chỉ kết luận về các máy bay nguồn thực sự quan sát được, không suy ra toàn bộ lưu lượng hàng không hoặc chuyến bay thương mại.

Sản phẩm gồm bản đồ quan sát mới nhất kèm độ mới, mật độ theo ô địa lý/cửa sổ, lịch sử độ cao và vận tốc, khoảng mất quan sát và đối chiếu kết quả streaming với batch.

## Kiến trúc

```text
OpenSky API → Collector → MinIO snapshots (lưu trước)
                              ↓ outbox publisher
                            Kafka
                              ↓
                       Spark Streaming → kết quả cập nhật
MinIO snapshots → Spark Batch → kết quả tính lại
                              ↓
                      MongoDB → API → Dashboard
```

MinIO lưu response JSON nguồn và metadata, sau này lưu cả các lớp dữ liệu chuẩn hóa/tổng hợp. Kafka đưa từng observation vào xử lý luồng. MongoDB là serving store; chỉ publisher công bố output đã hoàn tất mới cập nhật dữ liệu phục vụ.

## Hợp đồng ingestion đã triển khai

- Mặc định poll 120 giây, bounding box 8–24°N, 102–110°E, có thể đổi bằng CLI. Vùng cuối cùng chọn theo coverage thực tế.
- Snapshot được lưu nguyên cấu trúc JSON nguồn cùng bbox và `collected_at`; chỉ sau khi MinIO xác nhận ghi thành công mới gửi Kafka.
- Tên object dựa trên SHA-256 của response và bbox. Thu lại cùng nội dung không ghi đè metadata lần thu đầu.
- Event schema v2 có `observation_id`, `snapshot_time`, `event_time`, `collected_at`, state fields, hàng nguồn và tham chiếu object. Event time mặc định là snapshot time; position time và last contact được giữ riêng.
- `observation_id` gồm scope, snapshot time và nội dung hàng nguồn; cùng timestamp nhưng trạng thái sửa đổi không bị gộp nhầm.
- Receipt theo snapshot và topic được ghi chỉ khi tất cả message đã ACK. Restart quét các snapshot chưa có receipt; `--replay-pending` không cần gọi API.
- At-least-once: crash sau ACK nhưng trước receipt có thể gửi lại. Consumer phải dedup theo ID; không tuyên bố exactly-once toàn pipeline.
- Chỉ một collector cho mỗi bucket/prefix. Chưa có distributed lock hoặc index outbox; hiện quét lịch sử và kiểm tra receipt nên chi phí tăng theo thời gian.
- Khi backlog không gửi được, collector dừng lấy snapshot mới. Ghi nhận khoảng thiếu do gián đoạn; chưa tự backfill.
- Snapshot lỗi schema hàng được giữ trong MinIO, không gửi dở do validation; cần xử lý lỗi trước khi tiếp tục backlog.

Hướng dẫn cấu hình và recovery tại [nguồn OpenSky](opensky-state-source.md). Cần quyền sử dụng OpenSky phù hợp và OAuth credentials để vận hành thật; code/test hiện không chứng minh coverage hay quyền truy cập.

## Các bước tiếp theo

1. **Xác nhận nguồn và tích hợp ingestion:** endpoint MinIO, topic Kafka, quyền/API credentials; kiểm tra một snapshot, replay pending, lỗi Kafka và restart trên dịch vụ thật.
2. **Chuẩn hóa state vectors:** kiểm tra null/range, tách quan sát hợp lệ và lý do chất lượng, lưu schema và provenance. Không suy diễn vị trí bị thiếu.
3. **Streaming analytics:** count aircraft theo ô địa lý và cửa sổ 5–15 phút; quản lý state theo aircraft/window để đếm phân biệt. Watermark native với số liệu progress thực tế. Snapshot rows và aircraft counts là hai metric khác nhau.
4. **Batch analytics:** `lag`, `lead`, SQL windows để phân tích độ cao/vận tốc và observation gaps; tính lại các cửa sổ từ snapshot đầy đủ để đối chiếu streaming.
5. **Serving:** công bố phiên bản kết quả vào MongoDB, API và dashboard; hiển thị thời gian dữ liệu, phạm vi quan sát và nguồn.
6. **Hiệu năng và phục hồi:** kiểm tra partition pruning, join strategies, state size, restart và xử lý phân tán trên cụm do nhánh deploy cung cấp. Báo khối lượng/throughput đo được; không tự gọi dữ liệu API là quy mô lớn khi chưa đo.

## Tiêu chí kiểm chứng

Có các case dữ liệu bình thường, duplicate do retry, đảo arrival order, late trong/ngoài watermark, restart sau ghi MinIO/trước ACK/sau ACK và batch correction. Case chủ động tạo phải được ghi nhãn. Dữ liệu vị trí cũ trong snapshot không mặc nhiên là late event. Batch reference phải dùng cùng input/phạm vi đã thu.

## Trạng thái code

Đã có collector OAuth2, MinIO snapshot outbox, Kafka publisher và unit tests cho failure/recovery. Chưa nghiệm thu API/MinIO/Kafka thật cho pipeline này. Spark analytics, serving và dashboard state vectors chưa có.

Code raw replay, decoder, CPR và Compose Kafka/Spark/HDFS vẫn nằm trong repo để tham khảo và hồi quy; chạy `docker compose up` hiện khởi động runtime cũ. Không xem đó là deployment đã hoàn thành của kiến trúc mới.
