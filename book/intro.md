# Big Data Lab 04: Incremental CPG Streaming

Book trình bày hệ thống xử lý incremental CPG cho snapshot của
[`huggingface/datasets`](https://github.com/huggingface/datasets): parser Python
xử lý từng file, phát bốn loại sự kiện qua Kafka, ghi graph topology trực tiếp
vào Neo4j và ghi metadata qua Spark Structured Streaming vào MongoDB.

## Kết quả chính

- Shallow clone một revision cố định và chọn 147/233 file Python.
- Parser xử lý đủ 147 file, không có parser error.
- Bốn topic: `cpg.nodes`, `cpg.edges`, `cpg.metadata`, `cpg.errors`.
- Node/edge đi thẳng từ Kafka Connect vào Neo4j, không qua Spark.
- Metadata đi từ Kafka qua Spark Structured Streaming vào MongoDB.
- Neo4j có 208.003 node và 267.695 edge, bằng đúng số ID distinct.
- MongoDB có 147 document, mỗi document dùng `_id = file_id`.
- Replay một file giữ nguyên `file_id`, cập nhật revision theo
  `content_sha256`, không để node/edge stale và không tạo document thứ hai.
- Restart Spark giữ nguyên snapshot của đủ 147 document.

Các notebook đọc artifact từ lần chạy Kafka, Neo4j, MongoDB và Task 6 gần nhất.
`python book/make_notebooks.py` kiểm tra các count bắt buộc, tạo lại notebook và
execute cell; script không tự khởi động hoặc chạy lại pipeline.
