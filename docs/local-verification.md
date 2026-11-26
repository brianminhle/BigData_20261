# Kết quả kiểm tra local — 23/11/2026

Đã chạy package trong image `airtraffic-states:local`, dùng Docker Compose mới tại `deploy/local/compose.yaml`. MinIO và Kafka là dịch vụ thật, không mock. Hai request OpenSky là anonymous one-shot trong bbox 8–24°N, 102–110°E; không chạy collector định kỳ hoặc kiểm tra OAuth.

## Kết quả

| Phép kiểm tra | Kết quả |
| --- | --- |
| Build image/package và CLI | Thành công |
| Unit tests state ingestion/outbox/quality | 22 pass |
| API → MinIO → Kafka | 2 snapshots, lần lượt 17 và 13 hàng; 30 deliveries |
| Đối chiếu Kafka với MinIO | 30 observation IDs khớp, 0 duplicate deliveries; source rows/timestamps/object references khớp |
| Replay pending sau khi hoàn tất | Không phát lại snapshots đã có receipt |
| Batch local | 30 observations, 20 ICAO24 phân biệt, 10 nhóm ô/cửa sổ |
| Quality | 8 hàng thiếu barometric altitude, 17 stale positions (>15s), 1 repeated position |
| Compose | Cả root Compose cũ và Compose state mới render hợp lệ |
| Kubernetes | 8 resources qua kubeconform strict schema Kubernetes 1.32.0; Kustomize render thành công |

Khoảng snapshot 224 giây do hai lần gọi chủ động cách nhau trong buổi kiểm tra, **không phải bằng chứng API outage**. Mẫu nhỏ này không chứng minh coverage cả ngày, quota dài hạn hoặc quy mô Big Data.

Report tại `data/state-reports/live-20261123-final/`: `observations.jsonl`, `aircraft_windows.jsonl`, `quality.json`, `_SUCCESS`. Thư mục được Git ignore; các response nguồn giữ trong MinIO volume local. [Hướng dẫn chạy lại](../deploy/local/README.md).

## Phạm vi kiểm tra deploy

5 resources render từ `deploy/k8s/resources` và `deploy/k8s/checks`, 1 local PV mẫu, 2 resources từ `deploy/k8s/apps/states`. Mẫu PV vẫn cần thay hostname/path và tạo thư mục thật. Kubeadm init/join là template chưa điền thông số và chưa thực thi; không có cụm để kiểm chứng scheduling, DNS, TLS, mount volume hoặc restart nhiều node. CronJob ứng dụng mặc định suspend.

Consumer integration ban đầu bị chờ ở deferred `seek_to_beginning` của kafka-python. Đã đổi sang lấy beginning offsets rồi seek tường minh và thêm timeout tổng; đối chiếu sau đó pass. Đây là sửa test reader, không đổi dữ liệu nguồn.

Batch normalization chạy Python local với state trong RAM; chưa triển khai Spark streaming analytics, native watermark hoặc đánh giá hiệu năng phân tán. MinIO dùng distribution Bitnami lưu trữ ghim digest chỉ cho local test; chưa chốt image production. Kafka/MinIO được dừng sau kiểm tra, dữ liệu volume giữ lại để chạy tiếp.

## Spark và OAuth — 23/11/2026

Đã đọc `credentials.json` bằng mount chỉ đọc và gọi một lần `/states/all` qua OAuth. Snapshot mới có 12 observations, lưu tại `snapshots/opensky/0ce390bafcde725727651ea5c7bc894739a0c7fcbb1a316cf6cfeac8b42fcbf3.json`; Kafka ACK đủ 12. Không lưu thông tin đăng nhập trong báo cáo/image/Git.

Kiểm tra bằng image `airtraffic-state-stream:local`, Spark 3.5.7 + Kafka + S3A/MinIO thực:

- 26 unit tests thành công, gồm parser envelope và credentials.
- Replay ba snapshot thật: 42 observations sau dedup.
- 16 nhóm ô/cửa sổ khớp hoàn toàn Python batch reference.
- Gửi trùng trong lần đầu và sau restart: Silver không tăng dòng.
- JSON lỗi giữ ở Bronze cùng lỗi validation.
- Event đến sai thứ tự, trễ 60 giây trong watermark: vẫn vào Silver.
- Event chưa từng thấy nhưng cũ hơn watermark: giữ Bronze, không vào Silver.
- Native streaming Gold sau khi fixture tiến watermark: 16 dòng, khớp batch.

Prefix lưu kết quả tích hợp: `s3a://airtraffic-states/verification/states.verify.8038ed627bfb43fcaf0ed6cdc3930616`. Topic kiểm thử đã được xóa. Fixture chỉ tồn tại trong prefix kiểm thử; không gửi vào topic chính. Đây là kiểm tra correctness local, chưa phải đo throughput, HA hay thu thập dài hạn.

Cách tái chạy, ý nghĩa Gold và credential-file: [state-streaming.md](state-streaming.md).

Đã chạy cả ba stage trên topic chính và đọc lại Parquet qua Spark: `s3a://airtraffic-states/stream-v1` có **42 dòng Bronze, 42 dòng Silver, 10 dòng Gold đã chốt**. Sáu nhóm cửa sổ mới nhất chưa qua watermark nên chưa xuất Gold; không thêm event giả vào luồng chính để ép hoàn tất. Checkpoint được giữ cùng output root. Các dịch vụ local được dừng sau kiểm tra, volumes còn nguyên, không có collector gọi API nền.

## Phiên liên tục 1 giờ — 23/11/2026, 16:22–17:22 UTC+7

Phiên đã hoàn tất, collector dừng đúng lịch và ba stage được drain bằng checkpoint trước khi đối chiếu:

| Chỉ số | Trước | Sau |
|---|---:|---:|
| Snapshot lưu MinIO | 3 | 33 |
| Bronze | 42 | 407 |
| Silver | 42 | 407 |
| Gold đã chốt | 10 | 85 |

- 30 lần lấy/publish snapshot thành công, bổ sung 365 observations.
- Không ghi nhận lỗi collector hoặc HTTP rate limit.
- Kafka có 407 deliveries, không có duplicate trong phiên dữ liệu được kiểm tra.
- Silver có 407 ID duy nhất, khớp tập observation ID từ snapshot gốc; không có message bị quarantine.
- Tất cả Gold đã xuất khớp phép đếm batch trên nguồn lưu trữ. Cửa sổ cuối vẫn có thể chưa chốt do watermark.
- Log và báo cáo chi tiết nằm ở `data/state-reports/live-hour/` (ignored). `supervisor.log` kết thúc với `HOUR_RUN_COMPLETE`.

Đây là bằng chứng correctness và vận hành local trong một giờ; chưa phải kiểm thử tải hoặc HA.

Dashboard đọc bản tổng hợp Silver/Gold và đã được kiểm tra bằng Chromium desktop/mobile, bộ lọc thời gian, API input validation và Spark distinct aggregation. Xem [dashboard.md](dashboard.md).

## Batch analytics nâng cao — 23/11/2026

Image `airtraffic-state-stream:local` đã chạy job mới trên 407 observations thật, khoảng snapshot `[1795414500,1795429500)` UTC Unix seconds. Output: `s3a://airtraffic-states/analytics/verified-batch-20261123`.

| Output | Số dòng |
|---|---:|
| movement — vị trí sau loại lặp, có thuộc tính đoạn trước nếu tồn tại | 324 |
| gaps — khoảng giữa vị trí hợp lệ >600 giây | 11 |
| density — rolling statistics/ranking trên Gold | 85 |
| quality — nhãn chất lượng theo vùng/cửa sổ | 32 |
| quality_wide — pivot | 16 |
| quality_long — unpivot, bao gồm nhóm 0 | 64 |

Đã đọc lại root `_SUCCESS`, report và physical plans từ MinIO: có BroadcastHashJoin cho dimension chất lượng và InMemoryTableScan cho movement. 324 dòng movement **không có nghĩa 324 chuyến bay hoặc 324 đoạn có đủ hai đầu**; vị trí đầu mỗi chuỗi vẫn có dòng với khoảng cách/tốc độ null. Các gap cũng có thể phản ánh khoảng không thu thập chủ động trước phiên liên tục.

Semantic integration test bằng Spark đạt: loại vị trí lặp, tách scope, lag/lead, Haversine, gap, baseline không dùng mẫu hiện tại, broadcast join, pivot/unpivot bảo toàn tổng, output marker và từ chối overwrite. Output partition theo ngày có partition filter trong physical plan. Config CLI đã kiểm tra từ chối khoảng thời gian/ngưỡng sai.

Job K8s mới render được và qua kubeconform strict Kubernetes 1.32.0 (1 resource hợp lệ). Chưa apply lên cụm; vẫn suspend mặc định. Xem [advanced-analytics.md](advanced-analytics.md) và [bàn giao deployment](../deploy/k8s/apps/analytics/README.md).

Image build cuối cùng cũng vượt qua cùng semantic integration suite dưới UID/GID 185, root filesystem read-only, không Linux capabilities, no-new-privileges và chỉ `/tmp` writable. Test chạy mạng cô lập với địa chỉ loopback; tmpfs phải có `exec` để Snappy nạp native library. Đây là kiểm chứng container tương ứng các ràng buộc pod, không thay thế nghiệm thu Kubernetes thật.

## MongoDB serving và runtime bundle — 23/11/2026

Đã chạy MongoDB 7.0 với volume và authentication, publisher Spark, API có user read-only, cùng analytics worker local. Port dashboard MongoDB là 18081; không gọi thêm OpenSky trong lượt kiểm tra này.

Kết quả đối chiếu publication trên dữ liệu thật:

- 407 observations tổng hợp, 16 nhóm vùng/cửa sổ, 85 dòng mật độ Gold.
- 324 vị trí movement, 11 dòng có observation_gap.
- API reader bị MongoDB từ chối ghi (authorization error 13).
- Test Mongo trên database tạm xác nhận: publish lại nội dung không tạo bản ghi trùng; lỗi có kiểm soát sau staging không thay con trỏ active; kết nối/store mới retry hoàn tất; cleanup phiên bản đã nghỉ giữ nguyên active.
- Test Spark xác nhận publisher bỏ qua run thiếu root `_SUCCESS` và chọn run hoàn tất mới nhất.
- Scheduled recent-analytics chạy thành công trên 407 observations; publisher tự phát hiện run mới và đổi publication, không manual export/restart API.
- Chromium desktop/mobile: biểu đồ, ICAO lookup, HTTP validation và tự poll sau 30 giây đạt; API không phục vụ credentials.json.
- Cold start ba query trên root mới `verification/serving-cold-start`: Silver nhận 407 observations, watermark tiến tới 10:10:45 UTC và aggregation loại 85 cửa sổ đã hoàn tất khỏi state. Không sửa checkpoint chính.
- Runtime Kustomize render và kubeconform strict 1.32.0: 11 resources hợp lệ. Compose đầy đủ với runtime override render hợp lệ.

Cold-start container và Kafka thử nghiệm đã dừng. MinIO/Mongo/publisher/API/analytics-worker được giữ chạy để phục vụ dashboard và tự cập nhật khi lake có dữ liệu mới. Collector vẫn dừng; `make runtime-local-up` mới bật thu thập API thật. Ảnh kiểm tra ở `data/serving-verification/` (ignored).

Giới hạn: Mongo standalone, Spark local[2] trong pod, chưa deploy lên Kubernetes thật, chưa nghiệm thu HA/replication/executor phân tán hoặc tải lớn. API đọc serving snapshot có giới hạn; history vượt cửa sổ/budget cần đường truy vấn lake riêng.

Bổ sung kiểm tra khởi tạo mới: publisher yêu cầu có commit log Spark hợp lệ, không đọc thư mục Parquet chưa commit; đồng thời dùng schema tường minh để đọc Gold đã commit nhưng chưa có dòng vì watermark chưa qua. Test Spark cho cả hai trường hợp đạt. Dashboard cũng xóa kết quả tra cứu máy bay cũ khi bộ lọc hoặc generation đổi để tránh hiển thị sai phạm vi.
