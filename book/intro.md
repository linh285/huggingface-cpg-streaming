# Big Data Lab 04: Incremental CPG Streaming

Book này ghi lại pipeline thật đã chạy trên snapshot của
[`huggingface/datasets`](https://github.com/huggingface/datasets): parser Python
xử lý từng file, phát bốn loại sự kiện qua Kafka, ghi graph topology trực tiếp
vào Neo4j và ghi metadata qua Spark Structured Streaming vào MongoDB.

## Phạm vi bài nộp

Theo đề Lab 04, bài nộp là URL gốc của Jupyter Book công khai trên GitHub Pages.
Mỗi chương Task 1–6 bên trái có giải thích, lý do thiết kế, lệnh quan trọng, cell
đã thực thi và output thật được lưu trong notebook. Ảnh Neo4j Browser, Spark UI
và MongoDB UI được chụp từ stack đã chạy; các tệp JSON trong `artifacts/` là
bằng chứng máy đọc được.

## Kết quả chính

- Shallow clone một revision cố định và chọn 147/233 file Python.
- Parser xử lý đủ 147 file, không có parser error.
- Bốn topic: `cpg.nodes`, `cpg.edges`, `cpg.metadata`, `cpg.errors`.
- Node/edge đi thẳng từ Kafka Connect vào Neo4j, không qua Spark.
- Metadata đi từ Kafka qua Spark Structured Streaming vào MongoDB.
- Replay một file thật giữ nguyên `file_id`, cập nhật revision theo
  `content_sha256`, không để node/edge stale và không tạo document thứ hai.
- Restart Spark không ghi lại offset đã checkpoint.

Các số liệu cuối cùng nằm trong các notebook và được sinh lại bằng
`python book/make_notebooks.py` sau khi chạy regression.
