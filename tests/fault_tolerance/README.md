# Kiểm thử fault tolerance Spark Standalone trên 2 laptop

Mục tiêu: chạy pipeline streaming thật trên một cluster Spark gồm hai máy, cố tình gây sự cố (mất worker, mất mạng, mất master, mất driver), rồi kiểm tra pipeline tự phục hồi và dữ liệu không bị mất hay trùng.

| Máy | Chạy gì |
|---|---|
| Laptop 1 | Kafka, MinIO, MongoDB, Spark Master, Worker 1, driver của job |
| Laptop 2 | Worker 2 |

Dữ liệu thử nằm riêng: `s3a://crypto-lake/ft/`, topic `ft-test`, database `crypto_analytics_ft`. Không đụng vào bảng, topic hay collection thật.

## Cách hai máy tìm thấy nhau

Mỗi service giữ nguyên tên như trong `docker-compose.yml`:

| Tên | Trên Laptop 1 trỏ tới | Trên Laptop 2 trỏ tới |
|---|---|---|
| `spark-master`, `spark-worker`, `spark-driver`, `kafka`, `minio`, `mongodb` | container tương ứng | IP Laptop 1 (`extra_hosts`) |
| `spark-worker-2` | IP Laptop 2 (`extra_hosts`) | chính container worker 2 |

Các cổng Spark được cố định và mở ra mạng LAN:
- Driver: 7078, 7079.
- Block manager của executor: 7090.
- Worker 2: 7100.

Nhờ vậy code pipeline và biến môi trường không phải sửa gì.

## Chuẩn bị, làm trước buổi thử

### Cả hai máy
1. Cùng kết nối **một mạng Wi-Fi** (router ở nhà). Wi-Fi trường hoặc quán cà phê thường chặn các máy nói chuyện với nhau.
2. Đặt mạng Wi-Fi là **Private**: Settings → Network & internet → Wi-Fi → tên mạng → Network profile type → Private.
3. Lấy IP Wi-Fi bằng lệnh `ipconfig`, dòng `IPv4 Address` của mục "Wireless LAN adapter Wi-Fi". IP có thể đổi mỗi lần kết nối, nên kiểm tra lại ở đầu mỗi buổi.
4. Mở firewall bằng PowerShell chạy quyền Administrator:
   ```powershell
   # Laptop 1
   New-NetFirewallRule -DisplayName "Spark FT test" -Direction Inbound -Protocol TCP -Profile Private -Action Allow `
     -LocalPort 7077,7078,7079,7090,8082,9000,27017,29092
   # Laptop 2
   New-NetFirewallRule -DisplayName "Spark FT test" -Direction Inbound -Protocol TCP -Profile Private -Action Allow `
     -LocalPort 7090,7100,8081
   ```
   Thử xong thì gỡ bằng `Remove-NetFirewallRule -DisplayName "Spark FT test"`.

### Laptop 2
1. Cài Docker Desktop.
2. Clone repo và checkout nhánh `test/spark-2node`. Worker cần thư mục `spark/` vì executor import code UDF từ đó.
3. Nạp image Spark từ file Nam gửi (qua Drive hoặc USB) để khỏi phải build:
   ```bash
   # Trên Laptop 1 (Nam đã làm)
   docker save crypto-analytics/spark:latest | gzip > spark-image.tar.gz
   # Trên Laptop 2, sau khi tải file về
   docker load -i spark-image.tar.gz
   docker image ls crypto-analytics/spark   # phải thấy tag latest
   ```
   Có thể kiểm tra file tải về không bị lỗi bằng `certutil -hashfile spark-image.tar.gz SHA256` (PowerShell), so với mã SHA-256 Nam gửi kèm.

## Chạy

### Laptop 1, trong thư mục repo, dùng Git Bash
```bash
export NODE2_IP=<IP Laptop 2>
docker compose up -d --no-recreate kafka minio mongodb
docker compose -f docker-compose.yml -f docker-compose.2node.yml up -d kafka-lan
docker compose -f docker-compose.yml -f docker-compose.2node.yml up -d --no-deps --force-recreate spark-master spark-worker
tests/fault_tolerance/ft.sh prepare     # chép dữ liệu _v3 sang ft/, tạo topic ft-test
```

Không tạo lại container `kafka`: nó không có volume, tạo lại sẽ mất hết message đang giữ. Container `kafka-lan` chỉ chuyển tiếp cổng 29092 vào Kafka.

### Laptop 2, trong thư mục repo
```bash
NODE1_IP=<IP Laptop 1> docker compose -f docker-compose.worker2.yml up -d
docker logs crypto-spark-worker-2 | grep "registered with master"
```

### Kiểm tra kết nối, trước khi chạy kịch bản
1. Từ Laptop 2, chạy bằng PowerShell: `Test-NetConnection <IP Laptop 1> -Port 7077`. Kết quả phải có `TcpTestSucceeded : True`. Làm tương tự với các cổng 9000, 27017, 29092, 7078.
2. Từ Laptop 1: `Test-NetConnection <IP Laptop 2> -Port 7100`, rồi cổng 7090.
3. Mở `http://localhost:8082` trên Laptop 1: phải thấy 2 worker ở trạng thái ALIVE.

### Laptop 1: bật driver và cấp dữ liệu
```bash
NODE2_IP=<IP Laptop 2> tests/fault_tolerance/ft.sh driver   # Spark UI: http://localhost:4041
tests/fault_tolerance/ft.sh feed 0 20                        # 20 đợt, mỗi phút 300 message
```

Lệnh `feed FROM COUNT` có thể chạy nhiều lần. Mỗi lần nhớ đặt `FROM` nối tiếp lần trước (ví dụ `feed 20 10`), để dữ liệu không trùng phút.

## Kịch bản

Ghi lại giờ bắt đầu gây sự cố. Log của driver có giờ của mọi sự kiện.

| # | Làm gì | Ở đâu | Kỳ vọng |
|---|---|---|---|
| K1 | Chạy bình thường khoảng 10 batch | — | Task chạy trên cả `spark-worker` và `spark-worker-2`, mỗi batch dưới 1 phút |
| K2 | `docker kill crypto-spark-worker-2` giữa batch, sau 1 phút `docker start crypto-spark-worker-2` | Laptop 2 | Spark phát hiện ngay, tính lại phần việc trên Worker 1; Worker 2 quay lại có executor mới |
| K3 | Tắt Wi-Fi của Laptop 2 giữa batch, sau 3 phút bật lại | Laptop 2 | Phát hiện chậm hơn K2 vì phải chờ hết thời gian chờ mạng; sau đó phục hồi |
| K4 | `docker kill crypto-spark-worker` (Worker 1) | Laptop 1 | Batch chạy tiếp trên Worker 2 |
| K5 | `docker kill crypto-spark-master` giữa lúc job chạy, rồi bật lại | Laptop 1 | Job vẫn chạy bằng các executor đang có, nhưng không xin được executor mới. Standalone không có HA nếu thiếu ZooKeeper |
| K6 | `docker kill ft-driver`, rồi `docker start ft-driver` | Laptop 1 | Batch đang dở được chạy lại từ checkpoint |

Sau mỗi kịch bản (hoặc một lần ở cuối), trên Laptop 1:
```bash
tests/fault_tolerance/ft.sh report   # thời gian batch, số task mỗi máy, sự kiện mất/thêm executor
tests/fault_tolerance/ft.sh verify   # đối chiếu Bronze/Silver/Gold/MongoDB
```

`verify` đạt khi:
- Bronze có `duplicated ... 0`.
- Mọi dòng `mismatch`, `missing`, `unexpected`, `docs differing` đều bằng 0.

Log và kết quả được lưu trong `tests/fault_tolerance/out/`. Thư mục này không commit.

## Dọn dẹp
```bash
# Laptop 1
tests/fault_tolerance/ft.sh stop
docker compose -f docker-compose.yml -f docker-compose.2node.yml stop kafka-lan spark-worker spark-master
# Laptop 2
NODE1_IP=x docker compose -f docker-compose.worker2.yml down
```

## Đã thử trước trên một máy (2026-11-28)

Worker 2 được giả lập trên Laptop 1, nằm trong một mạng Docker riêng. Mọi kết nối của nó đều đi qua IP Wi-Fi `192.168.1.252`, giống như máy thật.

| Kiểm tra | Kết quả |
|---|---|
| Worker 2 đăng ký với Master qua IP LAN | Đạt (ALIVE) |
| Executor trên Worker 2 đọc Kafka, ghi MinIO và MongoDB qua IP LAN | Đạt: 5 batch, 8.615 task, mỗi batch 35–62 giây |
| K2 rút gọn: kill Worker 2 giữa batch, 45 giây sau bật lại | Phát hiện ngay; có executor mới 2 giây sau khi bật; mất dữ liệu shuffle nên Spark tính lại; batch hoàn tất sau 115 giây |
| Đối chiếu sau đó | 1.500/1.500 message, 0 bản trùng; Silver, Gold, MongoDB khớp 100% |

Chưa thử được trên một máy:
- Chiều Laptop 1 → Laptop 2 qua mạng thật.
- Trao đổi shuffle giữa hai worker.
- Tắt Wi-Fi (K3).

Ba việc này phải làm trên hai máy thật.

## Hạn chế
- Laptop 1 vẫn là điểm lỗi duy nhất: Kafka (1 broker), MinIO, MongoDB, Master và driver đều ở đó.
- Không có Master HA (cần ZooKeeper).
- Tải là nhịp thời gian thực của 5 coin, không phải big data.
