# Task 5: Kafka → Spark Structured Streaming → MongoDB

## Mục tiêu

Spark đọc các event metadata do Task 2 gửi vào topic `cpg.metadata`, sau đó
ghi vào collection `cpg.source_metadata` của MongoDB.

Luồng:

```text
Task 2 Parser → cpg.metadata → Spark Structured Streaming → MongoDB
```

## Vì sao không bị trùng?

- Task 2 tạo `file_id` ổn định từ repository và đường dẫn file.
- Spark đổi `file_id` thành MongoDB `_id`.
- MongoDB Spark Connector dùng `operationType=replace` và
  `upsertDocument=true`.
- File cũ được cập nhật khi chạy lại, không chèn thêm document thứ hai.
- Spark lưu Kafka offsets trong checkpoint volume nên restart sẽ tiếp tục từ
  offset cũ.

## Thành phần

```text
docker-compose.task5.yml              Kafka + MongoDB + Spark
task5/metadata_stream.py              Spark Structured Streaming job
task5/metadata_event.schema.json      Hợp đồng JSON từ Task 2
task5/samples/                        Hai event dùng kiểm tra replay
task5/publish_sample_metadata.sh      Gửi event mẫu vào Kafka
task5/verify_task6_mongodb.sh         Kiểm tra Task 6 phía MongoDB
task5/test_metadata_contract.py       Test hợp đồng dữ liệu
```

## 1. Chạy môi trường

Yêu cầu: Docker Desktop đang chạy.

```bash
docker compose -f docker-compose.task5.yml up -d
```

Lần đầu Spark cần tải Kafka và MongoDB connectors nên có thể mất vài phút.

Theo dõi Spark:

```bash
docker compose -f docker-compose.task5.yml logs -f metadata-stream
```

Khi thành công sẽ thấy:

```text
[TASK 5] Streaming started: cpg.metadata -> cpg.source_metadata
```

## 2. Gửi event thật từ Task 2

Cài Kafka client:

```bash
python -m pip install kafka-python
```

Sau đó chạy Parser, dùng port Kafka dành cho chương trình trên máy host:

```bash
python task2/parser_service.py \
  --manifest artifacts/task1/python_manifest.jsonl \
  --repo-dir .work/repos/datasets \
  --kafka-bootstrap localhost:29092
```

Không thêm `--dry-run`, vì tùy chọn đó chỉ ghi JSONL ra ổ đĩa.

## 3. Kiểm tra MongoDB

```bash
docker compose -f docker-compose.task5.yml exec mongodb \
  mongosh cpg --eval \
  'db.source_metadata.findOne({}, {_id:1,path:1,content_sha256:1,kafka_offset:1})'
```

Đếm số file đã lưu:

```bash
docker compose -f docker-compose.task5.yml exec mongodb \
  mongosh cpg --eval 'db.source_metadata.countDocuments()'
```

Kiểm tra checkpoint:

```bash
docker compose -f docker-compose.task5.yml exec metadata-stream \
  sh -c 'find /opt/spark-checkpoints/cpg-metadata -type f | head'
```

## 4. Task 6 phía MongoDB

> Hợp đồng JSON hiện tại của Task 2 không có trường `revision` riêng. Trong
> kiểm tra bên dưới, `content_sha256` là revision của nội dung: cùng hash là
> cùng revision, hash khác là revision mới của cùng `file_id`.

### 4.1. Smoke test tự động bằng event mẫu

Script sau tự động kiểm tra cơ chế upsert và checkpoint:

1. Gửi metadata ban đầu hai lần.
2. Gửi metadata đã sửa hai lần với cùng `file_id`.
3. Kiểm tra MongoDB vẫn chỉ có một document.
4. Kiểm tra hash và số dòng được cập nhật.
5. Restart Spark và kiểm tra checkpoint không xử lý lại offset cũ.

```bash
bash task5/verify_task6_mongodb.sh
```

Kết quả cuối phải có:

```text
[6/6] PASS
MongoDB document count : 1
Kafka offset unchanged : ...
Checkpoint files       : ...
```

### 4.2. Bài kiểm tra chính thức bằng file Python thật

Khi làm báo cáo cuối, phải dùng file thật từ repository Hugging Face:

```bash
# Lần 1: parse file gốc
python task2/parser_service.py \
  --repo-dir .work/repos/datasets \
  --single-file src/datasets/some_small_file.py \
  --kafka-bootstrap localhost:29092
```

Lưu `content_sha256` và `line_count` hiện tại:

```bash
docker compose -f docker-compose.task5.yml exec mongodb \
  mongosh cpg --eval \
  'db.source_metadata.findOne({path:"src/datasets/some_small_file.py"})'
```

Sau đó thêm một hàm nhỏ vào file đó, chạy lại đúng lệnh Parser và kiểm tra:

```javascript
db.source_metadata.countDocuments({
  path: "src/datasets/some_small_file.py"
})
// Phải bằng 1
```

Document phải giữ nguyên `_id`, nhưng `content_sha256`, `line_count` và các
node/edge counts phải đổi theo nội dung mới.

Cuối cùng restart Spark:

```bash
docker compose -f docker-compose.task5.yml restart metadata-stream
```

Không gửi thêm event. Document và `kafka_offset` phải giữ nguyên, chứng minh
Spark tiếp tục từ checkpoint thay vì đọc lại các offset cũ.

## 5. Bằng chứng cần đưa vào Jupyter Book

- Screenshot topic `cpg.metadata` và một metadata message.
- Log `[TASK 5] Streaming started`.
- Kết quả `db.source_metadata.countDocuments()`.
- Một document có `_id`, `content_sha256` và `kafka_offset`.
- Kết quả `[6/6] PASS` của Task 6.
- Danh sách file trong checkpoint directory.

## Ghép với Docker Compose của Task 3

File Compose này là môi trường độc lập để Task 5 có thể phát triển song song.
Khi Task 3 có Kafka chung, chỉ giữ `mongodb`, `metadata-stream`, volumes và
dùng đúng địa chỉ Kafka nội bộ trong `KAFKA_BOOTSTRAP_SERVERS`.

## Tài liệu kỹ thuật

- Spark Structured Streaming + Kafka:
  <https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html>
- MongoDB Spark Connector:
  <https://www.mongodb.com/docs/spark-connector/current/>
- MongoDB streaming write:
  <https://www.mongodb.com/docs/spark-connector/current/streaming-mode/streaming-write/>
- MongoDB upsert/checkpoint options:
  <https://www.mongodb.com/docs/spark-connector/current/streaming-mode/streaming-write-config/>
