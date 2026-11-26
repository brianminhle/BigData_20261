# MongoDB serving và tự động cập nhật

Luồng hiện tại: Spark committed Silver/Gold + batch `_SUCCESS` → Spark publisher → MongoDB → API → dashboard. Publisher không gọi OpenSky. API chỉ có quyền đọc Mongo, không nhận credentials lake hoặc nguồn.

## Chạy local

```bash
make serving-build
make serving-up
```

Dashboard: http://localhost:18081. Các dịch vụ này đọc lake đã có; publisher chạy định kỳ và analytics-worker kiểm tra dữ liệu mới mỗi 15 phút. Không khởi động collector. Muốn cả luồng tự động, sau khi có `credentials.json`:

```bash
make runtime-build
make runtime-local-up
# Dừng, giữ nguyên volumes:
make runtime-local-stop
```

Local có mật khẩu phát triển mặc định trong Compose, Mongo không publish port ra host. Không dùng mật khẩu mặc định cho cluster. File credentials chỉ mount vào collector. `runtime-local-up` là lệnh chủ động bắt đầu gọi API thật.

## Hợp đồng công bố

- Các collection `windows`, `cells`, `movement` lưu từng dòng cùng generation. Index hỗ trợ vùng/thời gian và ICAO/thời gian.
- Generation là hash nội dung canonical (không tính thời điểm chạy export). ID mỗi dòng ổn định trong generation; chạy lại cùng dữ liệu không thêm bản ghi trùng.
- Ghi toàn bộ dữ liệu, kiểm tra số dòng, rồi replace một document `publications/active`. API đọc metadata một lần và chỉ truy vấn generation đó.
- Nếu ngắt trước khi đổi active, dữ liệu cũ vẫn được phục vụ. Retry hoàn tất generation đã staging. Atomicity ở đây dựa trên [single-document atomic writes của MongoDB](https://www.mongodb.com/docs/manual/core/write-operations-atomicity/), không tuyên bố atomic transaction nhiều collection.
- Thời điểm nguồn, thời điểm tạo view, thời điểm publish và lần publisher kiểm tra thành công được tách riêng. Lake không đổi thì chỉ cập nhật `checked_at`.
- Giữ phiên bản đã nghỉ và staging lỗi ít nhất một giờ, rồi dọn; generation active không bị xóa. Yêu cầu một publisher duy nhất, không chạy hai instance cạnh tranh con trỏ.

Publisher chỉ đọc Spark file-sink metadata và batch run có marker hoàn tất tại gốc. Batch mới nhất được chọn theo thời điểm hoàn tất, không phải thời gian quan sát; dữ liệu batch có thể cũ hơn streaming và giao diện ghi rõ điều đó. Hiện chưa hỗ trợ ghép nhiều run batch hoặc tra cứu toàn bộ lịch sử ngoài cửa sổ serving.

## API

- `GET /api/overview?scope=...&hours=24`: mật độ/chất lượng; có thể thay hours bằng start/end UTC seconds căn 300 giây, khoảng [start,end).
- `GET /api/movement?icao24=040060&scope=...&hours=24&limit=200&offset=0`: vị trí và thông số đoạn chuyển động quan sát được.
- `GET /api/gaps?...`: cùng bộ lọc nhưng chỉ trả khoảng trống quan sát.
- `GET /livez`, `GET /readyz`: liveness và readiness.

Không có endpoint ghi hoặc đọc file tùy ý. Unknown scope/filter sai trả 400; backend chưa sẵn sàng trả 503, không giả dữ liệu trống.

Dashboard tự poll overview mỗi 30 giây. Tra cứu máy bay theo yêu cầu người dùng; đổi bộ lọc rồi tra cứu lại. Movement có pagination ở API; UI hiển thị tối đa trang đầu và ghi rõ khi còn dữ liệu.

## Giới hạn

Serving được thiết kế như bản tổng hợp có giới hạn, không tải toàn bộ lịch sử lake vào Mongo hoặc browser. Nếu dataset vượt 20.000 dòng, publication thất bại và bản trước vẫn giữ nguyên; cần giảm cửa sổ hoặc thiết kế publisher phân vùng cho quy mô lớn. Input Silver hiện chưa partition theo ngày nên mỗi chu kỳ vẫn có chi phí scan; chưa benchmark tải lớn.

[Hướng dẫn bàn giao Kubernetes](../deploy/k8s/apps/runtime/README.md) mô tả images, Secrets, endpoints, PVC và cách kích hoạt CronJobs. Bundle có Mongo standalone và Spark chạy trong từng pod; HA và executor phân tán chưa được triển khai.

Khi khởi tạo lake mới, publisher đợi commit log đầu tiên của cả Silver và Gold. Gold có thể đã commit nhưng rỗng khi chưa qua watermark; trường hợp đó vẫn phục vụ observations Silver và bản đồ chưa có ô đã chốt. Thư mục Parquet chưa có commit log không được coi là dữ liệu phục vụ hợp lệ.
