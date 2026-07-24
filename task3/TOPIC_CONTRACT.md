# Kafka Event Contract 1.0.0

Nguồn chuẩn của tên topic, event type và trường dùng chung là
[`task2/event_contract.py`](../task2/event_contract.py). JSON Schema Draft
2020-12 nằm trong [`schemas/`](schemas/).

## Topic layout

| Topic | Record key | Cleanup | Consumer |
|---|---|---|---|
| `cpg.nodes` | `node_id` | `compact` | Neo4j node sink |
| `cpg.edges` | `edge_id` | `compact` | Neo4j edge sink |
| `cpg.metadata` | `file_id` | `compact` | Spark → MongoDB |
| `cpg.errors` | `error_id` | `delete`, retention 7 ngày | debug/monitoring |

Môi trường lab dùng một partition và replication factor 1 cho mỗi topic.

## Trường chung

Tất cả event đều có:

| Trường | Kiểu | Ý nghĩa |
|---|---|---|
| `schema_version` | string, hằng `"1.0.0"` | version contract |
| `event_type` | string | loại thao tác |
| `event_time` | ISO-8601 UTC string | thời điểm phát |
| `repository` | string | `huggingface/datasets` |
| `file_id` | 64-char SHA-256 | hash của `repository:path` |
| `path` | string | đường dẫn POSIX tương đối |
| `content_sha256` | 64-char SHA-256 | revision nội dung |

Không dùng các tên cũ `file_path`, `content_hash`, `source_node_id`,
`target_node_id` hoặc `CALLS`.

## Event theo topic

### `cpg.nodes`

- `NODE_UPSERT`: thêm `node_id`, `structural_path`, `node_type`, `label`,
  vị trí, `code`, `properties`.
- `NODE_DELETE`: chỉ cần `node_id` ngoài trường chung.

### `cpg.edges`

- `EDGE_UPSERT`: thêm `edge_id`, `edge_type`, `source_id`, `target_id`,
  `properties`.
- `EDGE_DELETE`: thêm `edge_id`, `edge_type`, `source_id`, `target_id`.
- `edge_type` là một trong `AST`, `CFG`, `DFG`, `CALL`.

### `cpg.metadata`

`FILE_METADATA_UPSERT` thêm `language`, `size_bytes`, `line_count`,
`ast_node_count`, `ast_edge_count`, `cfg_edge_count`, `dfg_edge_count`,
`call_edge_count`, `status`.

### `cpg.errors`

`PARSER_ERROR` thêm `error_id`, `error_type`, `error_message`, `lineno`,
`col_offset`.

## Quy tắc idempotency

- `file_id` chỉ phụ thuộc repository + path nên giữ nguyên qua revision.
- `node_id` dùng file ID + structural path + node type.
- `edge_id` dùng file ID + type + endpoint IDs + properties.
- Parser lưu state revision trước và phát DELETE trước UPSERT để dọn phần stale.
- Neo4j `MERGE` theo ID; MongoDB replace/upsert với `_id=file_id`.
- Kafka compaction giảm bản ghi cũ về lâu dài nhưng không thay thế idempotency ở
  sink.

Sample trong [`samples/`](samples/) phải validate được bằng schema cùng tên.
