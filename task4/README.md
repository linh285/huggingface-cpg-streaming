# Task 4 — Kafka Connect → Neo4j

Task 4 nhận topology trực tiếp từ hai topic:

```text
cpg.nodes ─> neo4j-sink-cpg-nodes ─┐
cpg.edges ─> neo4j-sink-cpg-edges ─┴─> Neo4j
```

Spark không nằm trên hai nhánh này.

## Idempotency và cập nhật revision

- Node connector xử lý `NODE_UPSERT` bằng `MERGE (n {node_id})` và
  `NODE_DELETE` bằng `DETACH DELETE`.
- Edge connector xử lý `EDGE_UPSERT` bằng `MERGE` theo `edge_id` và
  `EDGE_DELETE` theo đúng `edge_id`.
- Contract dùng `source_id`, `target_id` và edge type
  `AST|CFG|DFG|CALL`.
- Constraint `CPGNode.node_id IS UNIQUE` được tạo trước khi connector chạy.
- Endpoint placeholder cho phép edge đến trước node; node UPSERT sau đó điền đủ
  thuộc tính.

## Chạy

Từ root:

```bash
docker compose -f compose.yml -f task4/docker-compose.yml up -d
bash task4/scripts/register_connectors.sh
```

Trên PowerShell có thể dùng:

```powershell
powershell -File task4/scripts/setup.ps1
```

Script PowerShell dùng Compose service name, không phụ thuộc container name.

## Kiểm tra

```bash
curl http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status
curl http://localhost:8083/connectors/neo4j-sink-cpg-edges/status
python task4/verify_neo4j.py
```

Neo4j Browser: <http://localhost:7474>, tài khoản local lab
`neo4j/cpgpassword`. Truy vấn minh chứng nằm trong
[`verify_queries.cypher`](verify_queries.cypher).

## File chính

- `docker-compose.yml`: Neo4j, schema init, Kafka Connect và plugin volume.
- `connectors/*.json`: Cypher UPSERT/DELETE.
- `neo4j/init.cypher`: constraint/index.
- `scripts/register_connectors.sh`: PUT connector idempotently.
- `verify_neo4j.py`: count, distinct ID, endpoint và duplicate checks.
