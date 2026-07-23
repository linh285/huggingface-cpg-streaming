# Kafka Topic Contract

> **Version:** 1.0.0 · **Date:** 2026-07-23 · **Project:** linh285/huggingface-cpg-streaming

## Topic Design

| Topic | Record key | Cleanup policy | Consumer |
|---|---|---|---|
| `cpg.nodes` | `node_id` | `compact` | Neo4j Sink Connector |
| `cpg.edges` | `edge_id` | `compact` | Neo4j Sink Connector |
| `cpg.metadata` | `file_id` | `compact` | Spark Streaming → MongoDB |
| `cpg.errors` | `error_id` | `delete` (retention 7 days) | Debug / Monitoring |

## Event Types

| Topic | Allowed `event_type` values |
|---|---|
| `cpg.nodes` | `NODE_UPSERT`, `NODE_DELETE` |
| `cpg.edges` | `EDGE_UPSERT`, `EDGE_DELETE` |
| `cpg.metadata` | `FILE_METADATA_UPSERT` |
| `cpg.errors` | `PARSER_ERROR` |

## Common Fields (all events)

| Field | Type | Source | Description |
|---|---|---|---|
| `schema_version` | `integer` | Task 3 | Schema version, starts at `1` |
| `event_type` | `string` | Task 2 | One of the event types above |
| `event_time` | `date-time` (ISO-8601 UTC) | Task 2 | When the event was emitted |
| `repository` | `string` | Task 1 | `huggingface/datasets` |
| `commit_sha` | `string` (≥7 chars) | Task 1 | Git commit snapshot |
| `file_id` | `string` | Task 1 | Stable SHA-256 based file identifier |
| `file_path` | `string` | Task 1 | Relative path within repository |
| `content_hash` | `string` | Task 1 | SHA-256 of file content |

## ID Ownership

| ID | Created by | Requirement |
|---|---|---|
| `file_id` | Task 1 | Task 2 preserves unchanged |
| `node_id` | Task 2 | Stable; derived from file path + node type + location |
| `edge_id` | Task 2 | Stable; derived from source/target/type |
| `error_id` | Task 2 | Hash of file/stage/error type/position |

## Edge Types

`edge_type` is restricted to: **`AST`**, **`CFG`**, **`DFG`**, **`CALL`**

## Partition & Replication

- **Partitions:** `1` (ordered delivery, easy lab debugging)
- **Replication factor:** `1` (single-broker cluster)

## Idempotency Rules

### Task 4 – Neo4j
- **Nodes:** `MERGE (n:CPGNode {node_id: event.node_id}) SET n += event`
- **Edges:** `MATCH (src:CPGNode {node_id: event.source_node_id}), (dst:CPGNode {node_id: event.target_node_id}) MERGE (src)-[r:EDGE {edge_id: event.edge_id, edge_type: event.edge_type}]->(dst)`

### Task 5 – MongoDB
- **Collection:** `source_metadata`
- **Upsert key:** `file_id`

### Task 6 – Replay
Running the parser again for the same `(file_id, content_hash)` produces identical `node_id`, `edge_id`, and `error_id` values — Kafka log compaction deduplicates automatically.

## Handoff Message

> Task 3 has finalised `cpg.nodes`, `cpg.edges`, `cpg.metadata`, and `cpg.errors`.  
> The full contract is in `task3/TOPIC_CONTRACT.md`.  
> Task 2 keeps `file_id` from Task 1 and uses stable `node_id` / `edge_id` as Kafka record keys.

---

*References: Apache Kafka 4.3 · JSON Schema Draft 2020-12 · Lab 04 – Spark Streaming*
