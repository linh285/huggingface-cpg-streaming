# Task 6 — Idempotent Replay Verification

`replay_single_file.py` là bài kiểm tra bắt buộc xuyên hai sink. Script dùng một
file Python thật và thực hiện:

1. Publish revision gốc.
2. Publish lại cùng file, cùng revision.
3. Ghi tạm một hàm nhỏ vào file và publish revision mới.
4. So sánh exact node/edge ID sets trong Neo4j.
5. Xác nhận MongoDB có đúng một document theo `_id=file_id`.
6. Restart Spark và xác nhận offset/`processed_at` không đổi.
7. Khôi phục source, publish revision gốc và chờ hai database trở về trạng thái
   ban đầu.

Contract hiện tại dùng `content_sha256` làm revision nội dung. `file_id` chỉ
phụ thuộc `repository:path`, nên không đổi khi nội dung thay đổi.

## Chạy

Bật stack và đăng ký connector theo README gốc, sau đó:

```bash
python task6/replay_single_file.py \
  --file src/datasets/utils/experimental.py
```

Phụ thuộc Python:

```bash
python -m pip install -r task4/requirements.txt
```

## Tiêu chí PASS

- Hai connector và toàn bộ connector task đều `RUNNING`.
- Mỗi pha có `missing_nodes`, `stale_nodes`, `missing_edges`, `stale_edges`
  bằng 0.
- `duplicate_node_ids` và `duplicate_edge_ids` bằng 0.
- Cùng revision vẫn có MongoDB count 1; revision mới giữ nguyên `file_id` nhưng
  đổi `content_sha256`.
- `mongo_kafka_offset` tăng sau mỗi metadata event mới.
- Restart Spark giữ nguyên offset, `processed_at` và có checkpoint files.
- Cleanup trả file và database về content hash gốc.

Kết quả được lưu ở
[`artifacts/task6/replay_result.json`](../artifacts/task6/replay_result.json).
MongoDB không phải phần tùy chọn: không kết nối được MongoDB thì Task 6 FAIL.

## Kiểm tra độc lập

```bash
python task6/verify_idempotency.py \
  --file src/datasets/utils/experimental.py
```

Các truy vấn để xem trong Neo4j Browser nằm ở
[`verify_queries.cypher`](verify_queries.cypher).
