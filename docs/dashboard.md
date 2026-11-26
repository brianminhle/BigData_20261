> Luồng chính hiện dùng [MongoDB serving tự động](mongodb-serving.md) tại port 18081. Nội dung dưới đây mô tả chế độ xuất file thủ công để kiểm tra/fallback.

# Dashboard local

Dashboard đọc bản tổng hợp từ **Silver và Gold trên MinIO**. Spark xuất dữ liệu trong 24 giờ tính từ snapshot mới nhất (có thể chọn 1–168 giờ khi gọi exporter), giới hạn 20.000 dòng mỗi view để tránh đưa toàn bộ lake vào web server. Export đọc qua Spark file-sink metadata, không đọc các object chưa commit.

```bash
# MinIO phải đang chạy; không cần gọi thêm OpenSky.
make dashboard-export
make dashboard-serve
```

Mở http://localhost:18080. API: `/api/overview?scope=...&start=...&end=...`; start/end là Unix seconds, căn theo 300 giây, khoảng [start,end). Khi không chỉ định scope, chọn scope đầu tiên. Scope ID là định danh bbox của collector.

- Lưới tọa độ: distinct ICAO theo ô 1° × 1° từ Gold, chọn từng cửa sổ đã chốt; không có bản đồ nền.
- Biểu đồ: distinct ICAO toàn vùng trong từng cửa sổ từ Silver, không cộng distinct giữa các ô hay cửa sổ. Dữ liệu cửa sổ chưa chốt có thể thay đổi.
- Chất lượng: số observations, tỷ lệ có tọa độ, tỷ lệ vị trí cũ hơn 15 giây, đỉnh distinct của một cửa sổ trong khoảng chọn.
- Không có observations không đồng nghĩa không có máy bay. Ô không có dòng Gold không được coi là số 0 đã xác nhận.

Server chỉ đọc file tổng hợp `data/dashboard/view.json`, không nhận credentials MinIO/OpenSky, và chỉ bind loopback mặc định. Export thay thế file bằng rename khi hoàn tất để API không đọc file đang ghi dở. Chỉ chạy một exporter cho mỗi file đích.

**Độ mới:** nút “Đọc lại dữ liệu” tải bản xuất hiện có, không chạy Spark hoặc gọi API. Muốn cập nhật từ lake, chạy lại `make dashboard-export`. Dashboard hiển thị thời điểm xuất riêng với thời điểm snapshot. Đây là serving snapshot tối thiểu cho local; chưa phải dịch vụ truy vấn phân tán hoặc dashboard cập nhật tự động. Hai tầng được đọc tuần tự nên trong lúc stream đang chạy, Gold và Silver có thể lệch một vài microbatch.

Kiểm tra:

```bash
PYTHONPATH=src python3 -m unittest discover -s tests/unit -p test_dashboard.py -v
```

Spark aggregation test: `tests/integration/test_dashboard_aggregate.py`, chạy trong image Spark. Chứng minh cùng một ICAO xuất hiện nhiều lần/ô chỉ được đếm một lần trên toàn vùng/cửa sổ.

Nghiệm thu local 23/11/2026: 5 unit tests, Spark aggregation test và Chromium desktop/mobile đều đạt. Bản xuất cuối phiên một giờ chứa 407 observations tổng hợp trong 16 nhóm vùng/cửa sổ và 85 dòng ô/cửa sổ Gold. Dịch vụ web vẫn phục vụ bản này khi MinIO/Kafka/Spark đã dừng; ảnh kiểm tra được lưu ở `data/dashboard/dashboard-desktop.png` và `dashboard-mobile.png`.
