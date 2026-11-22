> Luồng raw đã rời phạm vi chính. Tài liệu này giữ lại để tham khảo importer cũ; xem [blueprint hiện tại](project-blueprint.md).

# Thu nhận file raw ADS-B định kỳ

## Kết luận kiểm tra nguồn ngày 22/11/2026

**Có thể kiểm tra nguồn mỗi 300 giây, nhưng chưa xác minh được nguồn công khai miễn phí cung cấp một file raw ADS-B mới mỗi 5 phút.** Polling là lịch chạy của importer; thời điểm xuất bản và độ mới của dữ liệu do nhà cung cấp quyết định.

| Nguồn đã kiểm tra | Dữ liệu thực tế | Dùng cho project |
| --- | --- | --- |
| [OpenSky osky-sample](https://github.com/openskynetwork/osky-sample) | Mẫu raw Avro ngày 21/04/2015, khoảng 5 phút; không phải file được cập nhật mỗi 5 phút | Có, để phát triển raw replay |
| [OpenSky scientific datasets](https://opensky-network.org/data/scientific) | Mô tả raw samples; bucket được liên kết từ trang hiện trỏ tới `s3.opensky-network.org/data-samples` | Importer dò được bucket; chưa có nguồn file mới để xác nhận live |
| [OpenSky Trino](https://openskynetwork.github.io/opensky-api/trino.html) | Kho lịch sử có các bảng `_data4` chứa `rawmsg`, `sensors`, `mintime`, `maxtime`, `msgcount`; cần đăng ký và được duyệt quyền truy cập | Ứng viên nhập bản tin lịch sử mới hơn theo phân vùng; chưa tích hợp và chưa rõ độ trễ công bố |
| [OpenSky snapshot 01/03/2026](https://opensky-network.org/data/scientific#d11) | Bản tải tĩnh một ngày của các bảng Trino, có dữ liệu gần hơn mẫu 2015 | Có thể khảo sát thêm khi cần tập dữ liệu lịch sử lớn; không tạo dữ liệu mới định kỳ |
| [adsb.lol historical](https://www.adsb.lol/docs/open-data/historical) | Archive hằng ngày, JSON trajectory/state đã giải mã | Không thay thế đầu vào raw frame |
| [ADSBItalia](https://www.adsbitalia.it/data-access/) | Nhà cung cấp quảng bá TCP Beast có chế độ Full Raw, trả phí | Khả năng lấy raw live khi có quyền truy cập; chưa kết nối hoặc kiểm chứng nội dung feed |

Kiểm tra trực tiếp [S3 listing của OpenSky](https://s3.opensky-network.org/data-samples?list-type=2&prefix=raw%2F) trả về duy nhất `raw/placeholder.txt`, kích thước 0. Importer bỏ qua file này. Đây là quan sát tại thời điểm kiểm tra, không phải cam kết về nội dung bucket về sau.

Đã tải lại mẫu GitHub bằng downloader mới: **27.876.685 bytes, 304.131 receptions**, SHA-256 khớp bản ghim trong repo. Dữ liệu này vẫn là Historical Replay. Không đổi timestamp năm 2015 thành thời gian hiện tại để gọi là live.

OpenSky vẫn xử lý dữ liệu lịch sử trong năm 2026: tài liệu Trino ghi thay đổi pipeline tháng 08/2026 và các khoảng mất dữ liệu tháng 01/2026. Điều đó **không xác nhận** độ mới của từng phân vùng raw hay quyền đọc của nhóm. Bảng `_data4` giữ `rawmsg` dạng hex nhưng đã gộp các reception trùng vào một hàng; `msgcount` là số lần thu, `sensors` chỉ giữ receiver cùng thời gian sớm/muộn. Không thể biến một hàng này thành đầy đủ reception gốc của từng receiver. Có thể tự giải mã `rawmsg` và ghép CPR để làm bài toán xử lý bản tin, nhưng phải ghi rõ provenance là **OpenSky grouped historical messages** thay vì raw reception trực tiếp. [Mô tả schema](https://openskynetwork.github.io/opensky-api/trino.html#shared-decoded-message-columns)

**Quyết định nguồn:** dùng raw Avro công khai cho development và demo replay hiện tại. Nếu được cấp quyền Trino, thử nhập một phân vùng giờ nhỏ có `rawmsg`, đo `MAX(mintime)` so với thời điểm query và kiểm tra xem những lần query sau có thêm hàng mới không; lúc đó mới quyết định lịch nhập thực tế. Feed Beast/receiver được cấp quyền vẫn là nguồn cần có để nghiệm thu raw reception live. Chưa chuyển sang state vectors vì như vậy bỏ qua bài toán decode và CPR của project.

Nếu cần dữ liệu mới thật theo từng 5 phút, cần có quyền sử dụng raw feed. Khi đó collector đọc liên tục và đóng file theo từng khoảng 300 giây; phải kiểm chứng receiver identity, clock basis và việc feed có pha dữ liệu tổng hợp như MLAT không. Beast collector chưa nằm trong phần code đã triển khai ở đây.

Trino có thể cung cấp dữ liệu lịch sử mới được công bố theo thời gian sau khi được cấp quyền, nhưng không phải feed realtime được xác nhận. Tài liệu OpenSky yêu cầu query luôn lọc theo partition `hour` và giới hạn truy vấn trên hệ thống dùng chung; không thiết kế vòng hỏi lại toàn bộ bảng cứ 300 giây. [Hướng dẫn hiệu năng](https://openskynetwork.github.io/opensky-api/trino.html#tools-performance-considerations)

## Luồng đã triển khai

```text
Danh sách file từ public S3 / mẫu OpenSky được ghim
    → kiểm tra mỗi 300 giây
    → bỏ qua source revision đã có manifest
    → tải vào file tạm, giới hạn dung lượng
    → kiểm tra ETag, kích thước, SHA-256 nếu nguồn có cung cấp
    → đọc hết Avro, kiểm tra raw schema, đếm reception và khoảng event time
    → upload file nguyên bản vào MinIO
    → kiểm tra size/checksum metadata của object
    → ghi manifest ready
```

Đây là nhánh **source → MinIO landing**. Producer hiện có vẫn đọc file local được ghim; chưa tự đọc manifest MinIO để phát Kafka. Spark Bronze/Silver vẫn dùng HDFS. Việc nối manifest vào producer là bước tiếp theo, không suy ra đã hoàn thành toàn bộ pipeline MinIO từ việc importer upload được file.

Code được tách theo trách nhiệm:

- `ingestion/catalog.py`: nguồn sample và khám phá file từ public S3, SDK xử lý phân trang.
- `ingestion/download.py`: tải có điều kiện theo ETag, kiểm tra dung lượng/checksum.
- `ingestion/validation.py`: kiểm tra container raw Avro, thống kê thời gian nguồn.
- `ingestion/importer.py`: chu kỳ nhập, bỏ qua file đã nhập và thử lại khi lỗi.
- `storage/raw.py`: ghi object và công bố manifest.
- `jobs/import_raw.py`: CLI, cấu hình và dừng vòng polling khi nhận SIGINT/SIGTERM.

Importer hiện nhận **Avro Object Container File có các trường `rawMessage`, `timeAtServer`, `sensorSerialNumber`**. Chưa hỗ trợ file Beast, AVR, gzip/tar hoặc JSON state vectors. Không loại reception có frame/CRC lỗi ở bước nhập; processing phía sau chịu trách nhiệm phân loại chúng.

## Cài và kiểm tra nguồn

Python 3.10 trở lên:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install .

# Chỉ liệt kê nguồn, không cần MinIO hoặc credentials.
airtraffic-import-raw --source opensky-raw --discover-only
airtraffic-import-raw --source opensky-sample --discover-only
```

`opensky-raw` dò bucket công khai thật; kết quả `file_count: 0` là không có file Avro tại nguồn, không phải đã thu được dữ liệu mới. `opensky-sample` trả về một file lịch sử cố định.

## Chạy với MinIO do nhánh deploy cung cấp

Cần bucket có sẵn và tài khoản được quyền đọc metadata, đọc/ghi object trong bucket. CLI không tạo cụm hoặc tự tạo bucket. Cấu hình credentials bằng biến môi trường `AIRTRAFFIC_MINIO_ACCESS_KEY` và `AIRTRAFFIC_MINIO_SECRET_KEY`, không đưa vào tham số lệnh hay commit vào Git.

```bash
export AIRTRAFFIC_MINIO_ENDPOINT=localhost:9000
export AIRTRAFFIC_MINIO_BUCKET=airtraffic-raw

# Sau khi đã đặt hai biến credentials trong shell:
airtraffic-import-raw --source opensky-sample --once --insecure

# Theo dõi bucket raw của OpenSky mỗi 300 giây:
airtraffic-import-raw --source opensky-raw --interval-seconds 300 --insecure
```

`--insecure` dùng HTTP cho MinIO local; mặc định kết nối HTTPS. Endpoint đích nhận `host:port`, không kèm scheme. CLI độc lập không tự nạp `.env`; cần export các biến vào môi trường chạy.

Với nguồn **public S3 tương thích** khác có quyền sử dụng và đúng schema raw Avro:

```bash
airtraffic-import-raw --source s3 \
  --source-endpoint https://s3.example.org \
  --source-bucket raw-captures --source-prefix raw/ \
  --dataset receiver_group_a --interval-seconds 300
```

URL trên là mẫu cấu hình, không phải feed đã xác minh. Adapter nguồn hiện truy cập S3 ẩn danh; credentials MinIO chỉ dành cho bucket đích. Không dùng tên file có hậu tố `.avro` làm bằng chứng dữ liệu raw: importer còn phải đọc và kiểm tra container.

## Lưu trữ, retry và thời gian

Trong bucket đích:

```text
raw/<dataset>/sha256/<content-sha256>.avro
manifests/<dataset>/<source-id>.json
```

`source-id` là hash của dataset, URL, kích thước, ETag, LastModified và checksum nguồn nếu có. Phiên bản nguồn thay đổi sẽ được xét lại. Nội dung giống nhau trong cùng dataset dùng chung raw object; mỗi source revision có manifest riêng để giữ provenance.

Manifest chứa URL/revision nguồn, checksum, object key, số receptions, event-time min/max và `imported_at`. Event time lấy từ `timeAtServer`, không từ LastModified hay thời gian tải. Manifest và raw object phải được giữ cùng nhau; đây là checkpoint để bỏ qua file đã nhập qua các lần khởi động.

- Upload raw hoàn tất và metadata hợp lệ mới ghi manifest `ready`; consumer về sau chỉ đọc qua manifest.
- Nếu ghi manifest lỗi, vòng sau thử lại và tái sử dụng raw object đã upload. Không công bố object đang tải dở.
- Một file lỗi không chặn các file còn lại trong lượt. File lỗi được thử lại ở chu kỳ sau; `--once` trả exit code 1 nếu có lỗi nhập hoặc liệt kê.
- Không có file mới là một lượt thành công với số nhập bằng 0. Chạy lại mẫu lịch sử báo `skipped`, không tạo bản sao theo giờ tải.
- Một tiến trình nhập tuần tự, không chạy chồng lượt. Nếu một lượt kéo dài hơn 300 giây, lượt sau bắt đầu sau khi lượt trước kết thúc; không cam kết thời gian hoàn tất tải 5 phút.
- Chạy **một importer cho mỗi nguồn/bucket đích**; chưa có distributed lock giữa các replica.
- Mặc định giới hạn 1 GiB/file; `--max-file-bytes` thay đổi giới hạn. `--work-dir` chọn nơi chứa file tạm; cần dung lượng đủ cho một file. File tạm được xóa khi lượt xử lý kết thúc, kể cả lỗi thông thường.
- Metadata checksum được đối chiếu sau upload; chưa tải ngược toàn bộ object để kiểm chứng lại checksum phía server. Không tự ghi đè object có metadata bất nhất.

## Kiểm tra

```bash
python -m unittest discover -s tests/unit -p 'test_raw_*.py' -v
make test
```

Bộ kiểm tra importer dùng Avro thật tạo tại chỗ, HTTP response giả lập và S3 client giả lập. Bao gồm checksum sai, download thiếu bytes, ETag thay đổi, Avro hỏng, state vectors bị từ chối, upload lỗi, lỗi manifest, retry, restart/dedup và chu kỳ 300 giây. Các test không ngủ thực tế 5 phút. Ngày 22/11/2026, `make test` build image thành công và pass **30 tests**: 20 importer/source, 3 cấu hình/CLI, 7 fixture Spark.

Đã kiểm tra trực tiếp nguồn OpenSky và tải mẫu công khai. Chưa chạy ghi/đọc tích hợp trên server MinIO thật; cần kiểm tra bước đó với endpoint của nhánh deploy trước khi nghiệm thu lưu trữ. Không dùng kết quả unit test để tuyên bố có raw live hoặc đã chuyển Spark sang MinIO.
