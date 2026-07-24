# Task 2: Incremental CPG Parser Service

## Overview
The **Incremental CPG Parser Service** processes Python source files one-by-file, extracting Code Property Graph (CPG) nodes and edges:
- **AST Nodes**: Abstract Syntax Tree nodes (`FunctionDef`, `ClassDef`, `Assign`, `Call`, `Name`, etc.).
- **CFG Edges**: Control Flow Graph edges for statement sequences, `if`/`else` branches, and loop iterations.
- **DFG Edges**: Data Flow Graph edges mapping variable definitions to variable usages (`def-use` chains).
- **Call Edges**: Call graph edges connecting `Call` sites to function definitions and names.
- **Source Metadata**: File size, line count, content SHA-256, and parsing statistics.
- **Error Events**: Captures syntax or file reading errors cleanly.

All nodes, edges, metadata, and error events carry:
1. `schema_version`: `"1.0.0"`
2. `event_time`: ISO-8601 UTC timestamp.
3. **Deterministic IDs**: Derived using SHA-256 to guarantee **idempotent ingestion** into downstream databases (Neo4j & MongoDB).

---

## Folder Structure
```
task2/
├── cpg_parser.py        # Core CPG parser engine (AST, CFG, DFG, CALL)
├── event_contract.py    # Shared topic/field/event contract
├── kafka_producer.py    # Kafka event publisher & dry-run JSONL sink
├── parser_state.py      # Prior-revision ID state for DELETE events
├── parser_service.py    # Service CLI entry point
├── verify_corpus.py     # Distinct-ID and endpoint verification
├── README.md            # Documentation and execution guide
└── HANDOFF_TASK3.md     # Task 3 Kafka Topic design handoff
```

---

## Execution Instructions

### 1. Dry-Run Mode (Local JSONL Output)
Process all discovered Python files and output events to `artifacts/task2/`:
```bash
python task2/parser_service.py \
  --manifest artifacts/task1/python_manifest.jsonl \
  --repo-dir .work/repos/datasets \
  --output-dir artifacts/task2 \
  --dry-run
```

### 2. Kafka Mode (Publish directly to Kafka Broker)
Publish events directly to an Apache Kafka cluster:
```bash
python task2/parser_service.py \
  --manifest artifacts/task1/python_manifest.jsonl \
  --repo-dir .work/repos/datasets \
  --kafka-bootstrap localhost:9092
```

### 3. Single-File Replay Mode (for Task 6 Idempotency Test)
Reprocess a single modified source file:
```bash
python task2/parser_service.py \
  --repo-dir .work/repos/datasets \
  --single-file src/datasets/arrow_dataset.py \
  --output-dir artifacts/task2 \
  --dry-run
```

---

## Output Artifacts
When executed with `--dry-run`, the service creates the following files in `artifacts/task2/`:
- `nodes.jsonl`: Emitted CPG Node events.
- `edges.jsonl`: Emitted CPG Edge events (`AST`, `CFG`, `DFG`, `CALL`).
- `metadata.jsonl`: Emitted Source Metadata events.
- `errors.jsonl`: Emitted Parser Error events.
- `summary.json`: High-level summary of total files, nodes, edges, and breakdown.

Kafka mode bắt buộc `--kafka-bootstrap`; `--dry-run` không phải mặc định. Parser
phát `NODE_DELETE`/`EDGE_DELETE` từ state revision trước rồi mới phát UPSERT,
nhờ vậy file sửa không để node hoặc edge stale ở Neo4j.
