# Big Data Lab 04 — Incremental CPG Streaming

Pipeline xử lý từng file Python của `huggingface/datasets`, phát Code Property
Graph qua Kafka, ghi node/edge trực tiếp vào Neo4j và ghi metadata qua Spark
Structured Streaming vào MongoDB.

Jupyter Book nằm trong [`book/`](book/). Kiến trúc bắt buộc:

```text
Parser ─┬─> cpg.nodes ─┐
        ├─> cpg.edges ─┴─> Neo4j Kafka Connector ─> Neo4j
        ├─> cpg.metadata ─> Spark Structured Streaming ─> MongoDB
        └─> cpg.errors
```

Node/edge không đi qua Spark.

## Phiên bản và điều kiện cần

Các image/package đã pin trong Compose:

| Thành phần | Phiên bản |
|---|---|
| Apache Kafka | 4.3.1 |
| Neo4j Community + APOC | 5.20 |
| Neo4j Kafka Connector | 5.5.0 |
| Confluent Kafka Connect worker | 7.8.0 |
| Apache Spark | 4.1.2, Scala 2.13, Java 17 |
| MongoDB | 8.0 |
| MongoDB Spark Connector | 11.1.0 |

Cần Docker Desktop có Compose v2, Git, Python 3.10+ và một shell chạy được
script Bash. Các cổng mặc định: Kafka `9092`, Neo4j `7474/7687`, Kafka Connect
`8083`, MongoDB `27017`, Spark UI `4040`. Mongo Express `8081` là UI tùy chọn.

Mật khẩu `neo4j/cpgpassword` chỉ dành cho lab chạy local; không dùng cấu hình
này cho hệ thống public.

## Chạy thống nhất từ môi trường sạch

Chạy từ thư mục gốc repository:

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# PowerShell:   .venv\Scripts\Activate.ps1
python -m pip install -r task4/requirements.txt jsonschema

# Task 1: shallow clone và tạo manifest
python task1/discover_files.py

# Task 3–5: một Kafka chung, Neo4j/Kafka Connect, MongoDB/Spark
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml up -d

# Đăng ký hoặc cập nhật hai Neo4j sink connector
bash task4/scripts/register_connectors.sh

# Task 2: phát toàn bộ corpus thật vào Kafka
python task2/parser_service.py \
  --manifest artifacts/task1/python_manifest.jsonl \
  --repo-dir .work/repos/datasets \
  --kafka-bootstrap localhost:9092
```

Lần đầu Kafka Connect và Spark tải connector package, vì vậy cần chờ healthcheck
và log stream thay vì chỉ nhìn trạng thái `Up`:

```bash
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml ps
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml logs --tail 100 kafka-connect metadata-stream
```

Muốn mở Mongo Express để xem/chụp bằng chứng:

```bash
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml --profile ui up -d mongo-express
```

## Kiểm tra từng tầng

### Kafka và JSON contract

```bash
bash task3/list_topics.sh
bash task3/describe_topics.sh
bash task3/send_samples.sh
bash task3/consume_samples.sh
python -m unittest discover -s tests -v
python task5/test_metadata_contract.py
```

Hợp đồng chuẩn dùng `schema_version="1.0.0"`, `path`, `content_sha256`,
`source_id`, `target_id` và edge type `AST|CFG|DFG|CALL`. Chi tiết:
[`task3/TOPIC_CONTRACT.md`](task3/TOPIC_CONTRACT.md).

### Neo4j

```bash
curl http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status
curl http://localhost:8083/connectors/neo4j-sink-cpg-edges/status
python task4/verify_neo4j.py
```

Neo4j Browser: <http://localhost:7474>, đăng nhập `neo4j/cpgpassword`.

### MongoDB và Spark checkpoint

```bash
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml exec -T mongodb \
  mongosh --quiet cpg --eval 'db.source_metadata.countDocuments()'

docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml exec -T mongodb \
  mongosh --quiet cpg --eval \
  'db.source_metadata.findOne({}, {_id:1,file_id:1,path:1,content_sha256:1,kafka_offset:1})'

docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml exec -T metadata-stream sh -c \
  'find /opt/spark-checkpoints/cpg-metadata -type f | wc -l'
```

Spark UI: <http://localhost:4040>. Task 5 parse JSON bằng `StructType`, đặt
`_id=file_id`, rồi `replace` với `upsertDocument=true` trong từng micro-batch.
Named volume `spark-checkpoints` giữ checkpoint qua restart.

## Task 6 — replay đầy đủ

Contract không có trường `revision` riêng; `content_sha256` là revision nội
dung. Script dưới chạy baseline, replay cùng revision, sửa file thật, replay
revision mới, restart Spark, rồi phục hồi source và database về revision gốc:

```bash
python task6/replay_single_file.py \
  --file src/datasets/utils/experimental.py
```

PASS yêu cầu:

- `file_id` không đổi nhưng content hash đổi ở revision tạm.
- Neo4j có exact node/edge ID sets, không missing/stale/duplicate.
- MongoDB có đúng một document theo cả `_id` và `file_id`.
- MongoDB offset tăng khi có event mới.
- Offset và `processed_at` không đổi khi chỉ restart Spark.
- Cleanup phục hồi hash gốc.

Báo cáo máy đọc được được lưu tại
[`artifacts/task6/replay_result.json`](artifacts/task6/replay_result.json).

## Regression trước khi nộp

```bash
python -m compileall -q task1 task2 task4 task5 task6 tests
python -m unittest discover -s tests -v
python task5/test_metadata_contract.py
python task2/verify_corpus.py artifacts/task2 --expected-files 147

bash -n task3/*.sh task4/scripts/*.sh task5/*.sh
docker compose -f compose.yml config
docker compose -f docker-compose.task5.yml config
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml config
```

Không commit `.work/`, `.runtime/`, JSONL event dump, Kafka/MongoDB/Neo4j data,
Spark checkpoint, Ivy cache hoặc `book/_build/`.

## Build Jupyter Book

Book dùng notebook đã thực thi và lưu output thật. Sau khi chạy regression và
cập nhật các artifact:

```bash
python -m pip install -r book/requirements.txt
python book/make_notebooks.py
jupyter-book build book
```

Mở `book/_build/html/index.html`. Workflow
[`deploy-book.yml`](.github/workflows/deploy-book.yml) build và deploy cùng nội
dung lên GitHub Pages.

## Tắt an toàn

Giữ nguyên volumes để checkpoint và database không mất:

```bash
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml stop
```

Không dùng `down -v` nếu còn cần bằng chứng hoặc muốn kiểm tra resume checkpoint.
