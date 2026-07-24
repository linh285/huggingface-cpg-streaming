# Hướng dẫn tích hợp Task 4 và Task 6

Tài liệu chính và lệnh từ môi trường sạch nằm trong [`README.md`](README.md);
bằng chứng đã chạy nằm trong chương
[`book/task4.ipynb`](book/task4.ipynb) và
[`book/task6.ipynb`](book/task6.ipynb).

## Kiến trúc đúng

```text
Parser -> cpg.nodes -> Neo4j Kafka Connector -> Neo4j
Parser -> cpg.edges -> Neo4j Kafka Connector -> Neo4j
Parser -> cpg.metadata -> Spark Structured Streaming -> MongoDB
Parser -> cpg.errors
```

Task 4 không dùng Spark. Task 6 kiểm tra cả hai sink trên cùng Kafka.

## Chạy end-to-end

```bash
docker compose -f compose.yml -f task4/docker-compose.yml \
  -f task5/docker-compose.yml up -d
bash task4/scripts/register_connectors.sh

python task2/parser_service.py \
  --manifest artifacts/task1/python_manifest.jsonl \
  --repo-dir .work/repos/datasets \
  --kafka-bootstrap localhost:9092

python task4/verify_neo4j.py
python task6/replay_single_file.py \
  --file src/datasets/utils/experimental.py
```

## Điều kiện Task 6 PASS

- Connector và connector task đều `RUNNING`.
- Exact node/edge ID sets đúng, không missing, stale hoặc duplicate.
- Cùng `file_id` và cùng `content_sha256` vẫn có đúng một MongoDB document.
- Revision tạm giữ nguyên `file_id`, đổi `content_sha256` và cập nhật document.
- Restart Spark giữ nguyên offset/`processed_at`, checkpoint files còn tồn tại.
- Cleanup phục hồi source, Neo4j và MongoDB về content hash gốc.

MongoDB là phần bắt buộc; script không đánh dấu `SKIPPED`.
