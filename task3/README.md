# Task 3 – Kafka Topic Design & Event Contracts

## Overview

Task 3 provisions the **Apache Kafka message bus** for the CPG streaming pipeline.  
It defines four topics, their record-key strategy, cleanup policies, and the JSON event contracts consumed by downstream tasks (Neo4j, Spark/MongoDB).

---

## Folder Structure

```
task3/
├── README.md               # This document
├── TOPIC_CONTRACT.md       # Full data contract for downstream tasks
├── topics.env              # Shared environment variables
├── create_topics.sh        # Create the four Kafka topics (idempotent)
├── list_topics.sh          # List all topics
├── describe_topics.sh      # Describe partition/replication/config per topic
├── send_samples.sh         # Send sample events to Kafka (producer test)
├── consume_samples.sh      # Read one message per topic (consumer test)
├── samples/
│   ├── node-event.json     # Sample NODE_UPSERT event → cpg.nodes
│   ├── edge-event.json     # Sample EDGE_UPSERT event → cpg.edges
│   ├── metadata-event.json # Sample FILE_METADATA_UPSERT → cpg.metadata
│   └── error-event.json    # Sample PARSER_ERROR event → cpg.errors
└── schemas/
    ├── node-event.schema.json
    ├── edge-event.schema.json
    ├── metadata-event.schema.json
    └── error-event.schema.json
```

---

## Topic Design

| Topic | Record key | Cleanup | Consumer |
|---|---|---|---|
| `cpg.nodes` | `node_id` | compact | Neo4j Sink |
| `cpg.edges` | `edge_id` | compact | Neo4j Sink |
| `cpg.metadata` | `file_id` | compact | Spark → MongoDB |
| `cpg.errors` | `error_id` | delete (7 days) | Debug/monitoring |

---

## Quick-Start

### Prerequisites
- Docker 20.10+ with Docker Compose plugin
- Make sure port `9092` is free on the host

### 1. Start Kafka

```bash
docker compose up -d
docker compose ps        # wait until kafka is healthy
```

### 2. Create topics

```bash
chmod +x task3/*.sh
./task3/create_topics.sh
```

### 3. Verify topics

```bash
# List
./task3/list_topics.sh | tee artifacts/task3/topics_list.txt

# Describe (partition, replication, cleanup policy)
./task3/describe_topics.sh | tee artifacts/task3/topics_describe.txt
```

### 4. Validate sample JSON files

```bash
for file in task3/samples/*.json; do
    echo "Checking ${file}"
    python3 -m json.tool "${file}" > /dev/null
done
echo "All sample JSON files are valid JSON."
```

### 5. Send & consume samples

```bash
./task3/send_samples.sh
./task3/consume_samples.sh
```

Consumer output format: `<key> | <json-value>`

### 6. (Optional) Validate samples against JSON Schemas

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install jsonschema

python3 - <<'PYCODE'
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

for name in ["node-event", "edge-event", "metadata-event", "error-event"]:
    sample = json.loads(Path(f"task3/samples/{name}.json").read_text())
    schema = json.loads(Path(f"task3/schemas/{name}.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(sample)
    print(f"[OK] {name}.json")
PYCODE
```

---

## Artifacts

After running the verification steps, commit the following:

```
artifacts/task3/
├── topics_list.txt       # Output of list_topics.sh
└── topics_describe.txt   # Output of describe_topics.sh
```

---

## Handoff to Downstream Tasks

See [`TOPIC_CONTRACT.md`](TOPIC_CONTRACT.md) for the full contract.

| Task | Integration point |
|---|---|
| Task 2 – Parser | Publishes to `cpg.nodes`, `cpg.edges`, `cpg.metadata`, `cpg.errors` using the stable IDs and field names in this contract |
| Task 4 – Neo4j | Consumes `cpg.nodes` and `cpg.edges`; uses `node_id` / `edge_id` for MERGE upserts |
| Task 5 – Spark/MongoDB | Consumes `cpg.metadata`; upserts by `file_id` |
| Task 6 – Replay | Re-runs parser for the same file; identical stable IDs → log compaction deduplicates |
