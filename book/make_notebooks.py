#!/usr/bin/env python3
"""Build and execute the six evidence notebooks from checked-in artifacts."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import nbformat
from nbclient import NotebookClient


BOOK = Path(__file__).resolve().parent
REPO_URL = "https://github.com/linh285/huggingface-cpg-streaming"
BRANCH = "fix/lab04-final-integration"
SOURCE = f"{REPO_URL}/blob/{BRANCH}"

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def markdown(text: str):
    return nbformat.v4.new_markdown_cell(text.strip() + "\n")


def code(source: str):
    return nbformat.v4.new_code_cell(source.strip() + "\n")


def write_and_execute(name: str, cells: list) -> None:
    notebook = nbformat.v4.new_notebook(
        cells=cells,
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3"},
        },
    )
    client = NotebookClient(
        notebook,
        timeout=120,
        kernel_name="python3",
        allow_errors=False,
    )
    client.execute(cwd=str(BOOK))
    destination = BOOK / f"{name}.ipynb"
    nbformat.write(notebook, destination)
    print(f"[OK] executed {destination.relative_to(BOOK.parent)}")


def task1_cells() -> list:
    return [
        markdown(
            f"""
# Task 1 — Repository Cloning and File Discovery

## Yêu cầu của đề

Shallow-clone repository được phân công, liệt kê toàn bộ file Python và ghi lại
số file. Đề cho phép loại test, setup và file sinh tự động.

## Cách triển khai và lý do

[`task1/discover_files.py`]({SOURCE}/task1/discover_files.py) clone với
`--depth 1`, chuẩn hóa đường dẫn POSIX, tính `file_id =
SHA256("huggingface/datasets:" + path)` và xuất manifest JSONL. Cách này giữ
download nhỏ, đồng thời trao cho Task 2 một khóa file không phụ thuộc nội dung.

Chính sách lọc nằm ngay trong artifact, vì vậy người chấm có thể tái lập đúng
tập 147 file thay vì dựa vào một con số viết tay.

## Output thật đã chạy

Cell dưới đọc [`artifacts/task1/summary.json`]({SOURCE}/artifacts/task1/summary.json)
được tạo bởi lần chạy discovery cuối.
"""
        ),
        code(
            """
import json
from pathlib import Path

summary = json.loads(Path("../artifacts/task1/summary.json").read_text(encoding="utf-8"))
for key in (
    "repository", "commit_sha", "is_shallow_repository",
    "all_python_files", "selected_python_files", "excluded_python_files"
):
    print(f"{key}: {summary[key]}")
"""
        ),
        markdown(
            """
## Bằng chứng và lỗi đã khắc phục

Manifest và danh sách selected/excluded nằm trong `artifacts/task1/`. Revision
upstream thay đổi giữa các lần làm bài nên nhóm không giữ số đếm cũ: lần
regression cuối chạy lại discovery và lưu cả commit SHA. `.work/repos/datasets`
là clone runtime và bị loại khỏi Git.

## Reflection

Tách `file_id` khỏi content hash là quyết định quan trọng: cùng đường dẫn vẫn là
cùng file ở revision mới, còn `content_sha256` thể hiện revision nội dung.

## Tái lập

```bash
python task1/discover_files.py
```
"""
        ),
    ]


def task2_cells() -> list:
    return [
        markdown(
            f"""
# Task 2 — Incremental CPG Parser Service

## Yêu cầu của đề

Parser xử lý từng file trong bounded memory, trích AST, CFG, DFG và call edge,
gán ID ổn định và phát structured event vào Kafka.

## Cách triển khai và lý do

[`task2/cpg_parser.py`]({SOURCE}/task2/cpg_parser.py) dùng standard-library
`ast`, structural path và SHA-256. [`task2/parser_service.py`]({SOURCE}/task2/parser_service.py)
đọc manifest theo dòng, chỉ giữ một file trong bộ nhớ, phát UPSERT/DELETE và chỉ
lưu state sau khi producer đã `flush`. State của revision trước cho phép dọn
node/edge stale khi nội dung thay đổi.

Hợp đồng dùng thống nhất `schema_version="1.0.0"`, `path`,
`content_sha256`, `source_id`/`target_id` và edge type `CALL`.

## Output thật đã chạy

Cell này đọc summary của parser và kết quả kiểm tra toàn corpus. Script kiểm tra
đếm distinct ID và toàn bộ endpoint edge.
"""
        ),
        code(
            """
import json
from pathlib import Path

summary = json.loads(Path("../artifacts/task2/summary.json").read_text(encoding="utf-8"))
proof = json.loads(Path("../artifacts/task2/corpus_verification.json").read_text(encoding="utf-8"))
schema = json.loads(Path("../artifacts/task2/schema_validation.json").read_text(encoding="utf-8"))
print(json.dumps({
    "targeted_files": summary["total_files_targeted"],
    "processed_files": summary["processed_files"],
    "parser_errors": summary["error_files"],
    "node_events": proof["node_events"],
    "distinct_node_ids": proof["distinct_node_ids"],
    "edge_events": proof["edge_events"],
    "distinct_edge_ids": proof["distinct_edge_ids"],
    "metadata_events": proof["metadata_events"],
    "distinct_metadata_file_ids": proof["distinct_metadata_file_ids"],
    "missing_edge_sources": proof["missing_edge_sources"],
    "missing_edge_targets": proof["missing_edge_targets"],
    "schema_validation": schema,
}, indent=2))
"""
        ),
        markdown(
            f"""
## Code/lệnh quan trọng

```bash
python task2/parser_service.py --manifest artifacts/task1/python_manifest.jsonl \\
  --repo-dir .work/repos/datasets --kafka-bootstrap localhost:9092
python task2/verify_corpus.py artifacts/task2 --expected-files 147
```

Source liên quan:
[`event_contract.py`]({SOURCE}/task2/event_contract.py),
[`parser_state.py`]({SOURCE}/task2/parser_state.py),
[`verify_corpus.py`]({SOURCE}/task2/verify_corpus.py).

## Lỗi đã gặp và cách khắc phục

Contract cũ từng lệch giữa `CALLS`/`CALL`, `file_path`/`path` và kiểu
`schema_version`. Nhóm bỏ lớp tương thích mơ hồ, dùng một contract duy nhất và
thêm test schema bằng event thật. Kiểm tra chỉ dựa vào tổng count cũng chưa bắt
được ID trùng hoặc endpoint thiếu, nên regression cuối so sánh total với
distinct và kiểm từng `source_id`, `target_id`.

## Reflection

Structural path ổn định hơn số dòng cho replay không đổi; state cũ cộng DELETE
event mới là phần cần thiết để file sửa không để topology stale.

## Tái lập

Có thể thêm `--dry-run --output-dir artifacts/task2` để sinh JSONL và chạy
corpus verifier mà không cần Kafka.
"""
        ),
    ]


def task3_cells() -> list:
    return [
        markdown(
            f"""
# Task 3 — Kafka Topic Design

## Yêu cầu của đề

Có bốn topic tách biệt cho node, edge, metadata và parser error. Mỗi event mang
schema version và event time.

## Thiết kế và lý do

Node/edge/metadata dùng cleanup `compact` với record key lần lượt là
`node_id`, `edge_id`, `file_id`; error dùng `delete` và retention bảy ngày.
Mỗi topic một partition để bảo toàn thứ tự trong môi trường lab một broker.
Contract và JSON Schema ở
[`task3/TOPIC_CONTRACT.md`]({SOURCE}/task3/TOPIC_CONTRACT.md) và
[`task3/schemas/`]({SOURCE}/task3/schemas).

## Output thật đã chạy

Hai artifact dưới được thu trực tiếp từ Kafka CLI sau khi stack healthy.
"""
        ),
        code(
            """
from pathlib import Path

print("TOPICS")
print(Path("../artifacts/task3/topics_list.txt").read_text(encoding="utf-8").strip())
print("\\nCONFIG SUMMARY")
for line in Path("../artifacts/task3/topics_describe.txt").read_text(encoding="utf-8").splitlines():
    if "PartitionCount:" in line:
        print(line.strip())
"""
        ),
        markdown(
            f"""
## Lệnh quan trọng và bằng chứng

```bash
bash task3/create_topics.sh
bash task3/list_topics.sh
bash task3/describe_topics.sh
bash task3/send_samples.sh
bash task3/consume_samples.sh
```

Compose tạo bốn topic idempotently trong service `kafka-init`; các script vẫn
được giữ để kiểm tra thủ công. Sample JSON đã được validate theo Draft 2020-12.

## Lỗi đã gặp và cách khắc phục

Shell script trên Windows từng có nguy cơ CRLF và Compose từng nội suy nhầm biến
shell. `.gitattributes` ép LF cho `*.sh`; biến chạy trong container được escape
đúng để `docker compose config` không làm mất giá trị.

## Reflection

Topic ownership rõ ràng giúp kiến trúc không vô tình cho node/edge đi qua Spark.
Log compaction là lớp hỗ trợ, còn idempotency cuối cùng vẫn do sink upsert.

## Tái lập

Chạy stack từ root, sau đó chạy năm script trên. JSON sample có thể kiểm bằng
`python -m json.tool` và test contract trong `tests/`.
"""
        ),
    ]


def task4_cells() -> list:
    return [
        markdown(
            f"""
# Task 4 — Graph Topology Ingestion into Neo4j

## Yêu cầu của đề

Kafka Connector Sink phải ghi node/edge trực tiếp từ Kafka vào Neo4j, không có
Spark trung gian, và replay không tạo trùng.

## Cách triển khai và lý do

Hai connector độc lập đọc `cpg.nodes` và `cpg.edges`. Cypher xử lý
`NODE_UPSERT`/`NODE_DELETE` và `EDGE_UPSERT`/`EDGE_DELETE`; UPSERT dùng `MERGE`
theo ID, còn DELETE dọn topology revision cũ. Constraint unique cho `node_id`
được tạo trước khi đăng ký connector.

Source:
[`neo4j-sink-nodes.json`]({SOURCE}/task4/connectors/neo4j-sink-nodes.json),
[`neo4j-sink-edges.json`]({SOURCE}/task4/connectors/neo4j-sink-edges.json),
[`register_connectors.sh`]({SOURCE}/task4/scripts/register_connectors.sh).

## Output thật đã chạy
"""
        ),
        code(
            """
import json
from pathlib import Path

proof = json.loads(Path("../artifacts/task4/neo4j_verification.json").read_text(encoding="utf-8"))
print(json.dumps(proof, indent=2))
"""
        ),
        markdown(
            """
## Bằng chứng Neo4j Browser

Ảnh dưới là truy vấn đúng `file_id` dùng trong Task 6. Tổng node/edge bằng số ID
distinct.

![Neo4j exact ID counts](images/task4/neo4j-exact-id-counts.png)

## Lỗi đã gặp và cách khắc phục

Worker ban đầu không ghi được plugin vào named volume; overlay chạy bước tải
connector với quyền phù hợp rồi Kafka Connect mới khởi động. Node và edge ở hai
topic nên có thể đến khác thời điểm; edge connector `MERGE` endpoint placeholder,
node connector điền đủ thuộc tính khi node event tới. Regression chờ exact ID
sets thay vì dựa vào sleep cố định.

## Reflection

`MERGE` và constraint giải quyết duplicate; DELETE event mới giải quyết stale
graph khi source thay đổi. Hai vấn đề này độc lập và đều phải được kiểm thử.

## Tái lập

```bash
bash task4/scripts/register_connectors.sh
python task4/verify_neo4j.py
curl http://localhost:8083/connectors/neo4j-sink-cpg-nodes/status
curl http://localhost:8083/connectors/neo4j-sink-cpg-edges/status
```
"""
        ),
    ]


def task5_cells() -> list:
    return [
        markdown(
            f"""
# Task 5 — Source Metadata Ingestion into MongoDB

## Yêu cầu của đề

Spark Structured Streaming đọc metadata từ Kafka, parse JSON bằng schema rõ
ràng, ghi MongoDB và dùng checkpoint để tiếp tục đúng offset sau restart.

## Cách triển khai và lý do

[`task5/metadata_stream.py`]({SOURCE}/task5/metadata_stream.py) dùng
`StructType`, chỉ nhận `FILE_METADATA_UPSERT` schema `1.0.0`, thêm Kafka
partition/offset và ghi từng micro-batch bằng replace/upsert. `_id=file_id` làm
khóa duy nhất; checkpoint ở `/opt/spark-checkpoints/cpg-metadata` được gắn vào
named Docker volume.

## Output thật đã chạy
"""
        ),
        code(
            """
import json
from pathlib import Path

proof = json.loads(Path("../artifacts/task5/mongodb_verification.json").read_text(encoding="utf-8"))
print(json.dumps(proof, indent=2))
"""
        ),
        markdown(
            """
## Bằng chứng Spark và MongoDB

![Spark Structured Streaming query](images/task5/spark-structured-streaming.png)

![MongoDB source_metadata document](images/task5/mongodb-source-metadata.png)

Mongo Express chỉ là profile UI tùy chọn để chụp bằng chứng; pipeline mặc định
không phụ thuộc vào service này.

## Lỗi đã gặp và cách khắc phục

Lần đầu Spark cần tải connector packages nên query khởi động chậm. Nhóm dùng
named Ivy cache và healthcheck thay vì coi container `Up` là stream đã sẵn sàng.
Pull Mongo Express từng lỗi DNS; service được chuyển sang profile `ui` để lỗi UI
không làm full stack thất bại.

## Reflection

Checkpoint bảo vệ offset, còn MongoDB upsert bảo vệ dữ liệu. Chỉ một trong hai
không đủ để chứng minh exactly-once effect ở collection.

## Tái lập

```bash
docker compose -f compose.yml -f task4/docker-compose.yml -f task5/docker-compose.yml up -d
docker compose -f compose.yml -f task4/docker-compose.yml -f task5/docker-compose.yml \\
  exec -T mongodb mongosh --quiet cpg --eval 'db.source_metadata.countDocuments()'
docker compose -f compose.yml -f task4/docker-compose.yml -f task5/docker-compose.yml \\
  exec -T metadata-stream sh -c 'find /opt/spark-checkpoints/cpg-metadata -type f | wc -l'
```
"""
        ),
    ]


def task6_cells() -> list:
    return [
        markdown(
            f"""
# Task 6 — Idempotent Replay Verification

## Yêu cầu của đề

Sửa một file Python thật, reprocess đúng file đó và chứng minh Neo4j cập nhật
không trùng, MongoDB có metadata mới trong đúng một document, Spark restart dùng
checkpoint.

## Cách triển khai và lý do

[`task6/replay_single_file.py`]({SOURCE}/task6/replay_single_file.py) tự động:
publish baseline, publish lại cùng revision, thêm tạm một hàm vào file thật,
publish revision mới, restart Spark, rồi khôi phục file và hai database về
revision gốc. Mỗi pha chờ đến khi Neo4j có exact node/edge ID sets và MongoDB có
đúng một `_id=file_id`; không dùng count gần đúng.

`content_sha256` là revision nội dung của contract hiện tại. `file_id` chỉ phụ
thuộc repository + path nên không đổi qua hai revision.

## Output thật đã chạy
"""
        ),
        code(
            """
import json
from pathlib import Path

report = json.loads(Path("../artifacts/task6/replay_result.json").read_text(encoding="utf-8"))
print(json.dumps(report, indent=2))
"""
        ),
        markdown(
            """
## Bằng chứng chéo

Các ảnh Neo4j, Spark và MongoDB trong Task 4–5 được chụp trong cùng stack và
cùng `file_id` của báo cáo trên. File JSON ghi offset trước/sau restart và kết
quả cleanup.

## Lỗi đã gặp và cách khắc phục

Kiểm tra count toàn cục có thể PASS dù còn ID stale. Script được đổi sang so
sánh tập ID exact, bắt buộc cả connector và connector task `RUNNING`, bắt buộc
MongoDB count theo `_id` và `file_id` đều bằng 1, đồng thời kiểm
`processed_at`/offset không đổi qua restart. `finally` luôn phục hồi file thật
và phát revision gốc để database không bị để lại ở trạng thái demo.

## Reflection

Idempotency cần kiểm ba trạng thái: cùng revision, revision mới và restart.
Cleanup cũng là một phần của test, không phải thao tác thủ công sau cùng.

## Tái lập

```bash
python task6/replay_single_file.py \
  --file src/datasets/utils/experimental.py
```

Kết thúc hợp lệ phải có `status: PASS`, zero missing/stale/duplicate, MongoDB
count 1, hash cleanup bằng hash gốc và offset restart không đổi.
"""
        ),
    ]


def main() -> None:
    notebooks = {
        "task1": task1_cells(),
        "task2": task2_cells(),
        "task3": task3_cells(),
        "task4": task4_cells(),
        "task5": task5_cells(),
        "task6": task6_cells(),
    }
    for name, cells in notebooks.items():
        write_and_execute(name, cells)


if __name__ == "__main__":
    main()
