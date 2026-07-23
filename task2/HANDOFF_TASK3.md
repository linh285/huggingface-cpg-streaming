# Task 2 Handoff to Task 3 (Kafka Topic Design & Ingestion)

## Topic Mapping
The Parser Service emits four distinct categories of events to the following Kafka topics:

| Event Type | Kafka Topic Name | Primary Key | Description | Target Sink |
|------------|------------------|-------------|-------------|-------------|
| Node | `cpg.nodes` | `node_id` | AST & CPG Node topology | Neo4j Sink Connector |
| Edge | `cpg.edges` | `edge_id` | CFG, DFG, Call, & AST Edges | Neo4j Sink Connector |
| Metadata | `cpg.metadata` | `file_id` | File-level metadata & metrics | MongoDB Spark Streaming |
| Error | `cpg.errors` | `error_id` | Parse / File errors | Logging / Alerting Sink |

---

## JSON Event Schemas (Version `1.0.0`)

### 1. `cpg.nodes`
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-23T14:58:33.000Z",
  "event_type": "node",
  "node_id": "9f8e7d6c...",
  "file_id": "a1b2c3d4...",
  "node_type": "FunctionDef",
  "label": "FunctionDef:load_dataset",
  "lineno": 45,
  "col_offset": 0,
  "end_lineno": 90,
  "end_col_offset": 24,
  "code": "def load_dataset(path, ...):",
  "properties": {
    "name": "load_dataset",
    "args": ["path", "name"]
  }
}
```

### 2. `cpg.edges`
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-23T14:58:33.000Z",
  "event_type": "edge",
  "edge_id": "3b2a1c0d...",
  "file_id": "a1b2c3d4...",
  "source_id": "9f8e7d6c...",
  "target_id": "1a2b3c4d...",
  "edge_type": "CFG",
  "properties": {
    "label": "FLOWS_TO"
  }
}
```

### 3. `cpg.metadata`
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-23T14:58:33.000Z",
  "event_type": "metadata",
  "file_id": "a1b2c3d4...",
  "repository": "huggingface/datasets",
  "path": "src/datasets/load.py",
  "language": "python",
  "size_bytes": 15420,
  "line_count": 420,
  "content_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "ast_node_count": 310,
  "cfg_edge_count": 210,
  "dfg_edge_count": 145,
  "call_edge_count": 35,
  "status": "PARSED"
}
```

### 4. `cpg.errors`
```json
{
  "schema_version": "1.0.0",
  "event_time": "2026-07-23T14:58:33.000Z",
  "event_type": "error",
  "error_id": "c7b6a5f4...",
  "file_id": "a1b2c3d4...",
  "path": "src/datasets/invalid.py",
  "error_type": "SyntaxError",
  "error_message": "invalid syntax at line 15",
  "lineno": 15
}
```

---

## Idempotency Rules for Task 4 & Task 5
- **Neo4j Cypher Ingestion (Task 4)**:
  - Nodes: `MERGE (n:CPGNode {node_id: event.node_id}) SET n += event`
  - Edges: `MATCH (src:CPGNode {node_id: event.source_id}), (dst:CPGNode {node_id: event.target_id}) MERGE (src)-[r:REL {edge_id: event.edge_id, edge_type: event.edge_type}]->(dst)`
- **MongoDB Ingestion (Task 5)**:
  - Collection: `source_metadata`
  - Upsert key: `file_id`
