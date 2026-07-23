# Task 6: Kiểm chứng phát lại Idempotent (Idempotent Replay Verification)

Sửa một file mã nguồn Python, xử lý lại **đúng file đó** qua Parser Service, rồi
kiểm chứng rằng:

1. Số node/edge trong Neo4j phản ánh thay đổi **mà không tạo node/edge trùng**;
2. MongoDB chứa document metadata **đã cập nhật** của file đó (một document,
   được upsert — không phải bản sao thứ hai);
3. Checkpoint của Spark Structured Streaming **bỏ qua offset đã xử lý** cho tất
   cả các file không đổi.

Task này dùng lại stack của Task 4 (Kafka → Neo4j sink) và stack của Task 5
(Kafka → Spark → MongoDB). Cả hai giờ chạy trên **một Kafka chung** của Task 3.
Bật toàn bộ trước, từ **thư mục gốc repo**:

```powershell
docker compose -f compose.yml -f task4/docker-compose.yml -f task5/docker-compose.yml up -d
```

---

## Vì sao phát lại an toàn (tóm tắt)

Toàn bộ pipeline idempotent vì mỗi phần tử được gán **id SHA-256 tất định** từ
Parser Service, và mọi sink đều ghi bằng **upsert**:

| Sink | Cách ghi | Kết quả khi phát lại |
|------|----------|----------------------|
| Neo4j | `MERGE` theo `node_id` / `edge_id` | cùng id → cập nhật tại chỗ |
| MongoDB | upsert theo `file_id` | cùng file → một document, ghi đè |
| Kafka→Spark | offset đã checkpoint | file không đổi → offset đã commit → bỏ qua |

Vậy nên phát lại một file **không đổi** là thao tác vô hại (no-op), còn phát lại
file **đã sửa** chỉ thay đổi phần đồ thị của riêng file đó — không nhân bản gì.

---

## Cách chạy

Chạy trong thư mục `task6/` (stack của Task 4 đã `up`).

### A. Kiểm chứng idempotent thuần — phát lại file KHÔNG đổi
Chứng minh phát lại tạo ra **0** node/edge mới:
```powershell
python replay_single_file.py --file src/datasets/load.py
```
Kết luận kỳ vọng:
```
Δ global nodes : +0
Δ global edges : +0
Duplicate node_ids in graph : 0
Duplicate edge_ids in graph : 0
VERDICT: PASS -- unchanged replay produced ZERO new nodes/edges and no duplicates
```

### B. File đã sửa — phát lại sau khi chỉnh
Chèn một hàm đánh dấu nhỏ, xử lý lại, rồi khôi phục lại file như cũ:
```powershell
python replay_single_file.py --file src/datasets/load.py --apply-edit
```
Kỳ vọng: `Δ file nodes/edges` **dương và nhỏ** (AST + edge của hàm mới thêm),
số trùng toàn cục vẫn là `0`. Các file khác không bị đụng tới.

> Muốn giữ nguyên phần sửa (để xem lại) thì thêm `--keep-edit`. Muốn tự tay sửa
> file thì chỉnh trong `.work/repos/datasets/...` rồi chạy `replay_single_file.py`
> **không kèm** `--apply-edit`.

### C. Kiểm tra nhất quán chéo hai sink (Neo4j + MongoDB)
```powershell
python verify_idempotency.py --file src/datasets/load.py
```
Báo cáo việc phát hiện trùng ở Neo4j và — nếu MongoDB của Task 5 đang chạy —
xác nhận đúng một document metadata cho `file_id` đó, in ra `content_sha256` đã
lưu (giá trị này thay đổi sau khi phát lại file đã sửa). Nếu MongoDB chưa bật,
phần đó được **BỎ QUA** gọn gàng và phần Neo4j vẫn chạy.

### D. Kiểm chứng checkpoint Spark bỏ qua offset không đổi (Task 5)
Job Structured Streaming (Task 5) commit offset vào thư mục checkpoint. Sau khi
phát lại một file, khởi động lại job và xác nhận trong log rằng nó tiếp tục từ
offset đã commit lần cuối và chỉ xử lý đúng một bản ghi `cpg.metadata` mới:
```powershell
# trong log của job Task 5, tìm dòng:
#   "Committed offsets for batch N. Metadata ..."
#   numInputRows = 1        <-- chỉ file vừa phát lại, các file cũ bị bỏ qua
```
Chụp màn hình tiến trình batch (`numInputRows`) để đưa vào chương Jupyter Book.

---

## Cần chụp gì cho Jupyter Book

1. Output console của `replay_single_file.py` cho lần chạy **không đổi** (Δ = 0).
2. Tương tự cho lần chạy **đã sửa** (Δ nhỏ, số trùng = 0).
3. Ảnh Neo4j Browser của truy vấn #2 và #3 trong
   [verify_queries.cypher](verify_queries.cypher) trả về **0 dòng**.
4. Document MongoDB của file trước/sau (thay đổi `content_sha256`).
5. Spark streaming UI / log thể hiện `numInputRows = 1` khi phát lại.

---

## Danh sách file

| File | Vai trò |
|------|---------|
| `replay_single_file.py` | Điều phối: snapshot → (sửa) → reparse → publish → snapshot → kết luận. |
| `verify_idempotency.py` | Kiểm tra trùng/nhất quán độc lập ở Neo4j + (tùy chọn) MongoDB. |
| `verify_queries.cypher` | Truy vấn Neo4j Browser cho so sánh trước/sau + chứng minh không trùng. |

## Thư viện phụ thuộc
```powershell
pip install -r ../task4/requirements.txt   # neo4j, kafka-python
pip install pymongo                         # chỉ cần cho phần MongoDB (Task 5)
```

## Lưu ý về phụ thuộc Task 5
Phần Neo4j của task này hoàn toàn tự chạy trên nền Task 4. Phần kiểm tra
document MongoDB (mục 2) và checkpoint Spark (mục 3) dùng **Task 5** (đã kéo về
`task5/`, chạy chung Kafka của Task 3 qua overlay `task5/docker-compose.yml`):

- Task 5 lưu MongoDB db `cpg`, collection `source_metadata`, khóa `_id = file_id`
  (`operationType=replace`, `upsertDocument=true`) — nên phát lại một file chỉ
  ghi đè đúng một document.
- Task 5 đã có sẵn script kiểm chứng riêng cho phần này:
  `task5/verify_task6_mongodb.sh` — bật Kafka + MongoDB + Spark, phát lại sự kiện
  hai lần, xác nhận đúng 1 document và offset checkpoint không đổi sau khi restart.
- `verify_idempotency.py` ở đây cũng kiểm tra MongoDB nhưng sẽ **tự bỏ qua**
  (SKIPPED) khi MongoDB chưa chạy, nên phần Neo4j vẫn luôn chạy được độc lập.
