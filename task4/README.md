# Task 4: Nạp Graph Topology vào Neo4j

Kết nối **Neo4j Kafka Connector Sink** vào các topic chứa sự kiện node và edge,
để đồ thị CPG được ghi thẳng từ Kafka vào Neo4j **mà không đi qua tầng Spark
trung gian**. Việc nạp dữ liệu là **idempotent**: xử lý lại cùng một node hay
edge sẽ không tạo bản sao trùng lặp.

---

## Kiến trúc (phần thuộc Task 4)

```
  Parser Service (Task 2)      Kafka của Task 3        Kafka Connect (Task 4)     Neo4j
 ┌──────────────────────┐     ┌──────────────┐     ┌────────────────────┐      ┌──────────┐
 │ phát sự kiện JSON     │  →  │ topic cpg.nodes│ → │ neo4j-sink-cpg-nodes│ MERGE │ CPGNode  │
 │  id SHA-256 tất định  │     │ topic cpg.edges│ → │ neo4j-sink-cpg-edges│ MERGE │ CPG_EDGE │
 └──────────────────────┘     └──────────────┘     └────────────────────┘      └──────────┘
                                        KHÔNG có Spark giữa Kafka và Neo4j
```

- **Broker:** dùng chung Kafka của **Task 3** (`../compose.yml`, `apache/kafka:4.3.1`,
  KRaft). Task 4 **không** dựng lại Kafka — chỉ chồng thêm (overlay) 2 service.
- **Connect:** image `confluentinc/cp-kafka-connect`, tự cài plugin chính chủ
  `neo4j/neo4j-kafka-connector:5.1.0` khi khởi động; kết nối tới broker Task 3 ở
  listener nội bộ `kafka:19092`.
- **Chiến lược sink:** *Cypher* — mỗi bản ghi được gán vào biến `event` rồi chạy
  qua câu lệnh `MERGE` idempotent (xem bên dưới).

> **Lưu ý về tên field:** Cypher trong connector dùng `coalesce()` để chấp nhận
> **cả hai** kiểu đặt tên: field thực tế mà Task 2 phát ra (`source_id`/`target_id`,
> `label`, `lineno`) **và** tên trong contract của Task 3
> (`source_node_id`/`target_node_id`, `name`, `line_start`). Nhờ vậy connector
> chạy đúng bất kể sự kiện đến từ Parser Service hay từ file mẫu của Task 3.

## Vì sao idempotent (không tạo trùng)

| Tầng | Cơ chế bảo đảm |
|------|----------------|
| Parser (Task 2) | `node_id` / `edge_id` là **SHA-256 tất định** — cùng nội dung → cùng id. |
| Cypher cho node | `MERGE (n:CPGNode {node_id: event.node_id}) SET ...` — cập nhật tại chỗ. |
| Cypher cho edge | `MERGE (src) MERGE (dst) MERGE (src)-[r:CPG_EDGE {edge_id}]->(dst) SET ...` — MERGE cả hai đầu nên edge đến trước node vẫn tạo được; quan hệ MERGE theo `edge_id`. |
| Schema Neo4j | `CONSTRAINT ... n.node_id IS UNIQUE` — CSDL từ chối bản sao thứ hai. |

`edge_type` (AST/CFG/DFG/CALLS) được lưu thành **thuộc tính** trên một loại quan
hệ duy nhất `CPG_EDGE` — vì Cypher không cho tham số hóa tên loại quan hệ, dùng
một loại + thuộc tính giúp câu MERGE gọn và vẫn idempotent.

---

## Yêu cầu chuẩn bị

- Docker Desktop (có `docker compose`).
- Python 3.10+ và `pip install -r requirements.txt` (cho script publish/verify).
- Có sẵn output của Task 2 — hoặc sự kiện đã nằm trong Kafka, hoặc các file
  JSONL dump trong `../artifacts/task2/` để "phát lại" qua cầu nối.

## Cách chạy

Chạy từ **thư mục gốc repo** để gộp compose của Task 3 và overlay của Task 4.

### 1. Khởi động broker Task 3 + Neo4j + Kafka Connect (gộp 2 file compose)
```powershell
docker compose -f compose.yml -f task4/docker-compose.yml up -d
```
Lần đầu sẽ tải image và cài plugin Neo4j (mất một hai phút). Theo dõi tiến trình:
`docker compose -f compose.yml -f task4/docker-compose.yml logs -f kafka-connect`.

### 2. Tạo topic (Task 3) — nếu chưa tạo
```bash
bash task3/create_topics.sh          # Task 3 sở hữu 4 topic
```

### 3. Áp schema Neo4j + đăng ký connector (một lệnh, chạy trong task4/)
```powershell
cd task4
powershell -File scripts/setup.ps1
```
`setup.ps1` cũng tự bảo đảm 4 topic tồn tại (idempotent) nếu bỏ qua bước 2.
Trên macOS/Linux dùng script shell (chạy từ gốc repo):
```bash
cat task4/neo4j/init.cypher | docker exec -i cpg-neo4j cypher-shell -u neo4j -p cpgpassword
bash task4/scripts/register_connectors.sh
```

### 4. Đưa sự kiện vào Kafka

**Cách A — Parser Service gửi thẳng vào Kafka** (khuyến nghị):
```powershell
python ../task2/parser_service.py `
  --manifest ../artifacts/task1/python_manifest.jsonl `
  --repo-dir ../.work/repos/datasets `
  --kafka-bootstrap localhost:9092
```

**Cách B — phát lại các file JSONL dry-run của Task 2** (nếu đã chạy Task 2 ở
chế độ dry-run):
```powershell
python publish_jsonl_to_kafka.py --bootstrap localhost:9092 --topics nodes,edges
```

Sink Neo4j tiêu thụ liên tục; node/edge xuất hiện sau vài giây.

### 5. Kiểm chứng kết quả nạp
```powershell
python verify_neo4j.py
```
Phần cuối kỳ vọng như sau:
```
Duplicate node_ids : 0  (rows>1: 0)
Duplicate edge_ids : 0  (rows>1: 0)
IDEMPOTENCY CHECK  : PASS -- no duplicates
```
Hoặc mở **Neo4j Browser** tại http://localhost:7474
(user `neo4j`, mật khẩu `cpgpassword`) và chạy các truy vấn trong
[verify_queries.cypher](verify_queries.cypher) — chụp màn hình để đưa vào chương
Jupyter Book.

### 6. Kiểm tra nhanh tính idempotent tại sink (tùy chọn)
Chạy lại bước 4 lần thứ hai. Số node/edge trong `verify_neo4j.py` vẫn **y hệt**
— `MERGE` "nuốt" lần phát lại. (Task 6 làm chặt chẽ hơn với file đã **sửa**.)

---

## Danh sách file

| File | Vai trò |
|------|---------|
| `docker-compose.yml` | **Overlay**: Neo4j + Kafka Connect (plugin Neo4j). Dùng chung Kafka của Task 3. |
| `connectors/neo4j-sink-nodes.json` | Sink cho `cpg.nodes` → `MERGE` CPGNode. |
| `connectors/neo4j-sink-edges.json` | Sink cho `cpg.edges` → `MERGE` CPG_EDGE. |
| `neo4j/init.cypher` | Ràng buộc uniqueness + index cho edge. |
| `scripts/setup.ps1` | One-shot cho Windows: bảo đảm topic + áp schema + đăng ký connector. |
| `scripts/register_connectors.sh` | Bản POSIX đăng ký connector qua REST API. |
| `publish_jsonl_to_kafka.py` | Cầu nối phát JSONL của Task 2 → topic Kafka. |
| `verify_neo4j.py` | Đếm, breakdown, phát hiện trùng, kết luận PASS/FAIL. |
| `verify_queries.cypher` | Truy vấn kiểm chứng dán vào Neo4j Browser. |

## Dọn dẹp (chạy từ gốc repo)
```powershell
docker compose -f compose.yml -f task4/docker-compose.yml down       # giữ volume
docker compose -f compose.yml -f task4/docker-compose.yml down -v    # xóa luôn dữ liệu
```

## Xử lý sự cố

- **Connector báo `FAILED`** → xem chi tiết:
  `curl http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status`.
  Nguyên nhân hay gặp là sai mật khẩu Neo4j — kiểm tra lại khớp với
  `NEO4J_AUTH` trong `docker-compose.yml`.
- **Không thấy plugin** → `curl http://localhost:8083/connector-plugins` phải
  liệt kê `org.neo4j.connectors.kafka.sink.Neo4jConnector`. Nếu thiếu, lệnh
  `confluent-hub install` lúc boot đã lỗi — xem
  `docker compose -f compose.yml -f task4/docker-compose.yml logs kafka-connect`.
- **Có node nhưng thiếu edge** → kiểm tra topic `cpg.edges` có dữ liệu chưa:
  `docker exec huggingface-cpg-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic cpg.edges`.
