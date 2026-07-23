# huggingface-cpg-streaming
Big Data Lab 04 - Incremental CPG Streaming Pipeline

## Task 5 — Kafka to Spark Structured Streaming to MongoDB

Task 2 publishes source metadata to Kafka topic `cpg.metadata`. Task 5 parses that
JSON with an explicit schema and writes it to MongoDB collection
`cpg.source_metadata`. Each document uses `_id = file_id`, so replaying the same
file replaces the existing document rather than inserting a duplicate.

### Start the system

```bash
docker compose -f docker-compose.task5.yml up -d
docker compose -f docker-compose.task5.yml logs -f metadata-stream
```

Wait for `[TASK 5] Streaming started` in the Spark logs. Kafka is available to
host processes at `localhost:29092`.

### Run Task 5 with Task 2 metadata

Install the Task 2 producer dependency once, then run the parser without
`--dry-run`:

```bash
python -m pip install kafka-python
python task2/parser_service.py \
  --manifest artifacts/task1/python_manifest.jsonl \
  --repo-dir .work/repos/datasets \
  --kafka-bootstrap localhost:29092
```

### Verify Task 6 MongoDB idempotency

From Git Bash, WSL, or another Bash shell:

```bash
bash task5/verify_task6_mongodb.sh
```

The current Task 2 contract has no separate `revision` field. The verification
uses `content_sha256` as the content revision: it sends one revision twice,
then a different hash twice for the same `file_id`, and confirms that exactly
one document for that file remains after a Spark restart.

### MongoDB checks

```bash
docker compose -f docker-compose.task5.yml exec mongodb \
  mongosh cpg --eval 'db.source_metadata.countDocuments()'

docker compose -f docker-compose.task5.yml exec mongodb \
  mongosh cpg --eval \
  'db.source_metadata.findOne({_id:"task6-demo-file"}, {_id:1,path:1,content_sha256:1,kafka_offset:1})'
```

See [`task5/README.md`](task5/README.md) for the detailed architecture and
troubleshooting notes.

## Implemented tasks

- `task1/`: repository cloning and Python file discovery.
- `task2/`: incremental CPG parser and Kafka event producer.
- `task5/`: Spark Structured Streaming from `cpg.metadata` to MongoDB,
  persistent checkpointing, and MongoDB replay verification.

Task 5 quick start:

```bash
docker compose -f docker-compose.task5.yml up -d
bash task5/verify_task6_mongodb.sh
```

See [`task5/README.md`](task5/README.md) for the complete instructions.
