#!/usr/bin/env python3
"""Generate and execute the six Jupyter Book task notebooks from artifacts."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import nbformat
from nbclient import NotebookClient


BOOK = Path(__file__).resolve().parent
ROOT = BOOK.parent
REPO_URL = "https://github.com/linh285/huggingface-cpg-streaming"
SOURCE = f"{REPO_URL}/blob/main"

EXPECTED_FILES = 147
EXPECTED_NODES = 208003
EXPECTED_EDGES = 267695
EXPECTED_EDGE_BREAKDOWN = {
    "AST": 207856,
    "CFG": 18549,
    "DFG": 29557,
    "CALL": 11733,
}

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def markdown(text: str):
    return nbformat.v4.new_markdown_cell(text.strip() + "\n")


def code(source: str):
    return nbformat.v4.new_code_cell(source.strip() + "\n")


def load_json(relative_path: str) -> dict:
    return json.loads((ROOT / relative_path).read_text(encoding="utf-8"))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def validate_artifacts() -> None:
    """Stop notebook generation when a required live result is missing."""
    dry = load_json("artifacts/task2/summary.json")
    live = load_json("artifacts/task2/kafka_publish_summary.json")
    corpus = load_json("artifacts/task2/corpus_verification.json")
    neo4j = load_json("artifacts/task4/neo4j_corpus_verification.json")
    mongo = load_json("artifacts/task5/mongodb_corpus_verification.json")
    replay = load_json("artifacts/task6/replay_result.json")

    require(dry["dry_run"] is True, "Task 2 dry-run summary is not a dry-run")
    require(live["dry_run"] is False, "Task 2 Kafka summary is not live mode")
    for name, result in (("dry-run", dry), ("Kafka", live)):
        require(
            result["total_files_targeted"] == EXPECTED_FILES
            and result["processed_files"] == EXPECTED_FILES
            and result["error_files"] == 0,
            f"Task 2 {name} file counts are incomplete",
        )
        require(
            result["total_nodes_emitted"] == EXPECTED_NODES
            and result["total_edges_emitted"] == EXPECTED_EDGES,
            f"Task 2 {name} graph counts do not match the corpus",
        )
    require(
        corpus["distinct_node_ids"] == EXPECTED_NODES
        and corpus["distinct_edge_ids"] == EXPECTED_EDGES
        and corpus["missing_edge_sources"] == 0
        and corpus["missing_edge_targets"] == 0,
        "Task 2 corpus verification failed",
    )

    counts = neo4j["counts"]
    require(
        neo4j["status"] == "PASS"
        and counts["total_nodes"] == EXPECTED_NODES
        and counts["distinct_node_ids"] == EXPECTED_NODES
        and counts["total_edges"] == EXPECTED_EDGES
        and counts["distinct_edge_ids"] == EXPECTED_EDGES
        and counts["placeholder_nodes"] == 0
        and neo4j["edge_breakdown"] == EXPECTED_EDGE_BREAKDOWN,
        "Task 4 full-corpus verification failed",
    )
    for name, status in neo4j["connectors"].items():
        require(
            status["connector"] == "RUNNING"
            and status["tasks"]
            and all(task == "RUNNING" for task in status["tasks"]),
            f"Task 4 connector is not running: {name}",
        )

    require(
        mongo["status"] == "PASS"
        and mongo["total_documents"] == EXPECTED_FILES
        and mongo["distinct_file_ids"] == EXPECTED_FILES
        and mongo["distinct_document_ids"] == EXPECTED_FILES
        and mongo["id_file_id_mismatches"] == 0
        and not mongo["missing_manifest_file_ids"]
        and not mongo["unexpected_document_ids"]
        and mongo["spark_query"]["status"] == "RUNNING"
        and mongo["checkpoint_file_count"] > 0,
        "Task 5 full-corpus verification failed",
    )

    phases = replay["phase_comparison"]
    require(
        replay["status"] == "PASS"
        and phases["baseline"]["global_nodes"] == EXPECTED_NODES
        and phases["baseline"]["global_edges"] == EXPECTED_EDGES
        and phases["modified_replay"]["global_nodes"] == 208014
        and phases["modified_replay"]["global_edges"] == 267709
        and phases["restart"]["mongo_documents_before"] == EXPECTED_FILES
        and phases["restart"]["mongo_documents_after"] == EXPECTED_FILES
        and phases["restart"]["snapshots_equal"] is True
        and phases["cleanup"]["global_nodes"] == EXPECTED_NODES
        and phases["cleanup"]["global_edges"] == EXPECTED_EDGES,
        "Task 6 replay verification failed",
    )


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
    NotebookClient(
        notebook,
        timeout=120,
        kernel_name="python3",
        allow_errors=False,
    ).execute(cwd=str(BOOK))
    destination = BOOK / f"{name}.ipynb"
    nbformat.write(notebook, destination)
    print(f"[OK] executed {destination.relative_to(ROOT)}")


def task1_cells() -> list:
    return [
        markdown(
            f"""
# Task 1 — Repository Cloning and File Discovery

## Mục tiêu

Shallow-clone repository được phân công, liệt kê file Python và lưu tập file đầu
vào cho Parser Service.

## Thiết kế và triển khai

[`task1/discover_files.py`]({SOURCE}/task1/discover_files.py) dùng `--depth 1`,
chuẩn hóa đường dẫn POSIX và tạo `file_id` bằng SHA-256 của
`huggingface/datasets:<path>`. Khóa này không phụ thuộc nội dung, nên một file
giữ nguyên định danh khi revision thay đổi.

## Lệnh thực thi

```bash
python task1/discover_files.py
```

## Kết quả thực nghiệm

Cell sau đọc summary do Task 1 sinh ra.
"""
        ),
        code(
            """
import json
from pathlib import Path

summary = json.loads(Path("../artifacts/task1/summary.json").read_text(encoding="utf-8"))
assert summary["selected_python_files"] == 147
for key in (
    "repository", "commit_sha", "is_shallow_repository",
    "all_python_files", "selected_python_files", "excluded_python_files",
):
    print(f"{key}: {summary[key]}")
"""
        ),
        markdown(
            """
## Vấn đề và cách xử lý

Revision upstream thay đổi giữa các lần chạy có thể làm số file biến động. Lần
discovery cuối lưu cả commit SHA, danh sách được chọn và danh sách bị loại;
clone runtime trong `.work/` không được đưa vào Git.

## Đánh giá

Task 1 tạo được manifest 147 file và khóa `file_id` ổn định. Điểm cần lưu ý là
kết quả phụ thuộc đúng commit upstream đã ghi trong artifact; khi đổi commit
phải chạy lại discovery và các task phía sau.
"""
        ),
    ]


def task2_cells() -> list:
    return [
        markdown(
            f"""
# Task 2 — Incremental CPG Parser Service

## Mục tiêu

Xử lý từng file trong bounded memory, trích AST, CFG, DFG và call edge, gán ID
ổn định rồi phát event theo hợp đồng chung.

## Thiết kế và triển khai

[`task2/cpg_parser.py`]({SOURCE}/task2/cpg_parser.py) dùng `ast`, structural path
và SHA-256. [`task2/parser_service.py`]({SOURCE}/task2/parser_service.py) đọc
manifest theo dòng, chỉ giữ một file trong bộ nhớ, phát UPSERT/DELETE và lưu
state sau khi Kafka xác nhận batch. Contract thống nhất các trường
`schema_version`, `path`, `content_sha256`, `source_id`, `target_id` và edge
type `CALL`.

Dry-run kiểm tra parser trên toàn bộ 147 file và tạo JSONL cục bộ. Kafka mode
phát cùng corpus vào bốn topic đang chạy; hai chế độ có summary riêng.

## Lệnh thực thi

```bash
python task2/parser_service.py --manifest artifacts/task1/python_manifest.jsonl \\
  --repo-dir .work/repos/datasets --dry-run --output-dir artifacts/task2

python task2/parser_service.py --manifest artifacts/task1/python_manifest.jsonl \\
  --repo-dir .work/repos/datasets --kafka-bootstrap "$KAFKA_BOOTSTRAP" \\
  --summary-output artifacts/task2/kafka_publish_summary.json
```

## Kết quả thực nghiệm

Cell sau đọc cả dry-run summary, Kafka publish summary và corpus verifier.
"""
        ),
        code(
            """
import json
from pathlib import Path

def read(name):
    return json.loads(Path(name).read_text(encoding="utf-8"))

dry = read("../artifacts/task2/summary.json")
live = read("../artifacts/task2/kafka_publish_summary.json")
corpus = read("../artifacts/task2/corpus_verification.json")
schema = read("../artifacts/task2/schema_validation.json")

assert dry["dry_run"] is True and live["dry_run"] is False
for result in (dry, live):
    assert result["processed_files"] == 147
    assert result["error_files"] == 0
    assert result["total_nodes_emitted"] == 208003
    assert result["total_edges_emitted"] == 267695
assert corpus["distinct_node_ids"] == 208003
assert corpus["distinct_edge_ids"] == 267695
assert corpus["missing_edge_sources"] == 0
assert corpus["missing_edge_targets"] == 0

print("mode     files errors nodes  edges  duration_sec")
for mode, result in (("dry-run", dry), ("Kafka", live)):
    print(
        f"{mode:<8} {result['processed_files']:>5} {result['error_files']:>6} "
        f"{result['total_nodes_emitted']:>6} {result['total_edges_emitted']:>6} "
        f"{result['execution_duration_sec']:>12}"
    )
print("\\ncorpus:", json.dumps(corpus, indent=2))
print("schema:", json.dumps(schema, indent=2))
"""
        ),
        markdown(
            """
## Vấn đề và cách xử lý

Contract cũ từng lệch giữa `CALLS`/`CALL`, `file_path`/`path` và kiểu
`schema_version`; các biến thể được thay bằng một contract và JSON Schema duy
nhất. Lần publish đầu dừng ở file lớn vì ACK timeout và Docker thiếu RAM. Producer
được đổi sang batch 1.000 event, gzip và timeout dài hơn; Compose giới hạn heap
cho Kafka, Kafka Connect, Neo4j và Spark. Lần chạy lại hoàn thành 147 file.

## Đánh giá

Cả dry-run và Kafka mode đều cho 208.003 node, 267.695 edge, 0 parser error và
không thiếu endpoint. Giới hạn còn lại là thời gian publish phụ thuộc tài nguyên
Docker; summary có `fatal_error` nếu hạ tầng dừng giữa chừng.
"""
        ),
    ]


def task3_cells() -> list:
    return [
        markdown(
            f"""
# Task 3 — Kafka Topic Design

## Mục tiêu

Tách node, edge, metadata và parser error thành bốn topic; mọi event mang schema
version và event time.

## Thiết kế và triển khai

Node, edge và metadata dùng cleanup `compact`, record key lần lượt là
`node_id`, `edge_id`, `file_id`. Error dùng cleanup `delete` với retention bảy
ngày. Contract và schema nằm tại
[`task3/TOPIC_CONTRACT.md`]({SOURCE}/task3/TOPIC_CONTRACT.md) và
[`task3/schemas/`]({SOURCE}/task3/schemas).

## Lệnh thực thi

```bash
bash task3/create_topics.sh
bash task3/list_topics.sh
bash task3/describe_topics.sh
bash task3/send_samples.sh
bash task3/consume_samples.sh
```

## Kết quả thực nghiệm
"""
        ),
        code(
            """
from pathlib import Path

topics = Path("../artifacts/task3/topics_list.txt").read_text(encoding="utf-8").split()
expected = {"cpg.nodes", "cpg.edges", "cpg.metadata", "cpg.errors"}
assert set(topics) == expected
print("topics:", ", ".join(sorted(topics)))

description = Path("../artifacts/task3/topics_describe.txt").read_text(encoding="utf-8")
for topic in sorted(expected):
    assert f"Topic: {topic}" in description
for line in description.splitlines():
    if "PartitionCount:" in line:
        print(line.strip())
"""
        ),
        markdown(
            """
## Vấn đề và cách xử lý

Script shell chạy trên Windows có nguy cơ CRLF và Compose từng nội suy biến
shell quá sớm. `.gitattributes` ép LF cho `*.sh`, còn biến chạy trong container
được escape để `docker compose config` giữ nguyên.

## Đánh giá

Bốn topic tồn tại với đúng cleanup policy và một partition cho môi trường lab.
Một partition đơn giản hóa thứ tự nhưng không đại diện cho cấu hình scale-out;
khi tăng partition phải giữ record key ổn định để cùng ID vào cùng partition.
"""
        ),
    ]


def task4_cells() -> list:
    return [
        markdown(
            f"""
# Task 4 — Graph Topology Ingestion into Neo4j

## Mục tiêu

Ghi node và edge trực tiếp từ Kafka Connect vào Neo4j, không qua Spark, đồng
thời giữ idempotency khi replay.

## Thiết kế và triển khai

Hai connector độc lập đọc `cpg.nodes` và `cpg.edges`. Cypher xử lý
UPSERT/DELETE; `MERGE` dùng `node_id` hoặc `edge_id`, còn DELETE dọn topology
của revision trước. Constraint unique được tạo trước khi đăng ký connector.
Source:
[`neo4j-sink-nodes.json`]({SOURCE}/task4/connectors/neo4j-sink-nodes.json),
[`neo4j-sink-edges.json`]({SOURCE}/task4/connectors/neo4j-sink-edges.json).

## Lệnh thực thi

```bash
bash task4/scripts/register_connectors.sh
python task4/verify_neo4j.py --connect-url "$KAFKA_CONNECT_URL" \\
  --expected-nodes 208003 --expected-edges 267695 \\
  --expected-ast-edges 207856 --expected-cfg-edges 18549 \\
  --expected-dfg-edges 29557 --expected-call-edges 11733 \\
  --require-zero-placeholders --timeout 300 \\
  --json artifacts/task4/neo4j_corpus_verification.json
```

## Kết quả thực nghiệm
"""
        ),
        code(
            """
import json
from pathlib import Path

proof = json.loads(Path("../artifacts/task4/neo4j_corpus_verification.json").read_text(encoding="utf-8"))
counts = proof["counts"]
assert proof["status"] == "PASS"
assert counts == {
    "total_nodes": 208003,
    "total_edges": 267695,
    "distinct_node_ids": 208003,
    "distinct_edge_ids": 267695,
    "placeholder_nodes": 0,
}
assert proof["edge_breakdown"] == {
    "AST": 207856, "CFG": 18549, "DFG": 29557, "CALL": 11733
}
for status in proof["connectors"].values():
    assert status["connector"] == "RUNNING"
    assert status["tasks"] == ["RUNNING"]
print(json.dumps({
    "counts": counts,
    "edge_breakdown": proof["edge_breakdown"],
    "duplicate_node_ids": len(proof["duplicate_node_ids"]),
    "duplicate_edge_ids": len(proof["duplicate_edge_ids"]),
    "connectors": proof["connectors"],
}, indent=2))
"""
        ),
        markdown(
            """
![Neo4j Browser — full corpus counts](images/task4/neo4j-full-corpus-counts.jpg)

## Vấn đề và cách xử lý

Node và edge nằm ở hai topic nên edge có thể đến trước node; edge connector tạo
placeholder endpoint và node connector hoàn thiện thuộc tính sau đó. Khi publish
file lớn, Docker thiếu RAM làm API treo; heap/page cache được giới hạn và
consumer lag được đưa về 0 trước khi truy vấn.

## Đánh giá

Neo4j chứa 208.003 node và 267.695 edge, bằng đúng số ID distinct; placeholder
và duplicate đều bằng 0. Hệ thống hiện dùng một connector task cho mỗi topic,
phù hợp máy lab nhưng thông lượng phụ thuộc tài nguyên Neo4j.
"""
        ),
    ]


def task5_cells() -> list:
    return [
        markdown(
            f"""
# Task 5 — Source Metadata Ingestion into MongoDB

## Mục tiêu

Đọc metadata từ Kafka bằng Spark Structured Streaming, parse JSON theo schema,
upsert vào MongoDB và tiếp tục đúng offset sau restart.

## Thiết kế và triển khai

[`task5/metadata_stream.py`]({SOURCE}/task5/metadata_stream.py) dùng
`StructType`, chỉ nhận `FILE_METADATA_UPSERT` schema `1.0.0`, bổ sung Kafka
partition/offset rồi ghi replace/upsert với `_id=file_id`. Checkpoint
`/opt/spark-checkpoints/cpg-metadata` nằm trên named Docker volume.

## Lệnh thực thi

```bash
python task5/verify_mongodb_corpus.py --timeout 300 \\
  --json-output artifacts/task5/mongodb_corpus_verification.json
```

## Kết quả thực nghiệm
"""
        ),
        code(
            """
import json
from pathlib import Path

proof = json.loads(Path("../artifacts/task5/mongodb_corpus_verification.json").read_text(encoding="utf-8"))
assert proof["status"] == "PASS"
assert proof["total_documents"] == 147
assert proof["distinct_file_ids"] == 147
assert proof["distinct_document_ids"] == 147
assert proof["id_file_id_mismatches"] == 0
assert not proof["missing_manifest_file_ids"]
assert not proof["unexpected_document_ids"]
assert all(value == 0 for value in proof["required_field_missing"].values())
assert proof["spark_query"]["status"] == "RUNNING"
assert proof["checkpoint_file_count"] > 0
print(json.dumps({
    "total_documents": proof["total_documents"],
    "distinct_file_ids": proof["distinct_file_ids"],
    "distinct_document_ids": proof["distinct_document_ids"],
    "id_file_id_mismatches": proof["id_file_id_mismatches"],
    "required_field_missing": proof["required_field_missing"],
    "spark_query": {
        "name": proof["spark_query"]["name"],
        "status": proof["spark_query"]["status"],
    },
    "checkpoint_location": proof["checkpoint_location"],
    "checkpoint_file_count": proof["checkpoint_file_count"],
}, indent=2))
"""
        ),
        markdown(
            """
![Spark Structured Streaming — query RUNNING](images/task5/spark-full-corpus-running.jpg)

![Mongo Express — source_metadata có 147 document](images/task5/mongodb-full-corpus.jpg)

## Vấn đề và cách xử lý

Spark cần tải connector packages ở lần khởi động đầu và từng cạnh tranh bộ nhớ
với Neo4j. Ivy cache, checkpoint volume và giới hạn driver memory giúp restart
nhanh hơn. Mongo Express chỉ được bật ở profile `ui`, không tham gia đường ghi
dữ liệu.

## Đánh giá

Collection có 147 document, 147 `_id`, 147 `file_id`, không thiếu file trong
manifest và đủ `path`, `content_sha256`, `kafka_partition`, `kafka_offset`.
Checkpoint có 76 file ở lần xác minh cuối. UI chỉ phục vụ quan sát; verifier
dùng truy vấn MongoDB và Spark UI để quyết định kết quả.
"""
        ),
    ]


def task6_cells() -> list:
    return [
        markdown(
            f"""
# Task 6 — Idempotent Replay Verification

## Mục tiêu

Reprocess một file không đổi, sửa file đó rồi reprocess, restart Spark và cuối
cùng phục hồi source/database về revision gốc mà không tạo dữ liệu trùng hoặc
stale.

## Thiết kế và triển khai

[`task6/replay_single_file.py`]({SOURCE}/task6/replay_single_file.py) so sánh
exact node/edge ID sets cho từng pha, kiểm count toàn graph, giữ
`file_id` ổn định và dùng `content_sha256` làm revision. Trước restart, script
chụp `(_id, content_sha256, kafka_offset, processed_at)` của đủ 147 document;
sau khi query Spark trở lại `RUNNING`, script poll collection liên tục trong
15 giây và toàn bộ snapshot phải giữ nguyên.

## Lệnh thực thi

```bash
python task6/replay_single_file.py \\
  --file src/datasets/utils/experimental.py --timeout 600
```

## Kết quả thực nghiệm
"""
        ),
        code(
            """
import json
from pathlib import Path

report = json.loads(Path("../artifacts/task6/replay_result.json").read_text(encoding="utf-8"))
phases = report["phase_comparison"]
assert report["status"] == "PASS"
assert report["file_id_stable_across_revisions"] is True
assert report["duplicate_node_ids"] == 0
assert report["duplicate_edge_ids"] == 0
assert phases["baseline"] == {
    "file_nodes": 57, "file_edges": 75,
    "global_nodes": 208003, "global_edges": 267695,
    "mongo_documents": 147,
}
assert phases["unchanged_replay"] == phases["baseline"]
assert phases["modified_replay"] == {
    "file_nodes": 68, "file_edges": 89,
    "global_nodes": 208014, "global_edges": 267709,
    "mongo_documents": 147,
}
assert phases["restart"] == {
    "mongo_documents_before": 147,
    "mongo_documents_after": 147,
    "snapshots_equal": True,
}
assert phases["cleanup"] == phases["baseline"]

print("phase              file N/E   global N/E       Mongo docs")
for name in ("baseline", "unchanged_replay", "modified_replay", "cleanup"):
    phase = phases[name]
    print(
        f"{name:<18} {phase['file_nodes']:>3}/{phase['file_edges']:<3} "
        f"{phase['global_nodes']:>6}/{phase['global_edges']:<6} "
        f"{phase['mongo_documents']:>10}"
    )
print("\\nrestart:", json.dumps(phases["restart"], indent=2))
print(
    "checkpoint files:",
    report["spark_restart"]["checkpoint_files_before"],
    "->",
    report["spark_restart"]["checkpoint_files_after"],
)
print(
    "restart observation:",
    report["spark_restart"]["observation_seconds"],
    "seconds /",
    report["spark_restart"]["observation_checks"],
    "snapshot checks",
)
print("original hash:", report["original_content_sha256"])
print("modified hash:", report["modified_content_sha256"])
print("cleanup hash:", report["cleanup_restore"]["mongo_content_sha256"])
"""
        ),
        markdown(
            """
![Neo4j sau cleanup](images/task4/neo4j-full-corpus-counts.jpg)

![Spark query sau restart](images/task5/spark-full-corpus-running.jpg)

![MongoDB sau cleanup](images/task5/mongodb-full-corpus.jpg)

## Vấn đề và cách xử lý

Chỉ kiểm count từng file có thể bỏ sót tác động lên phần còn lại của corpus.
Script được mở rộng để kiểm cả per-file và global count, bắt duplicate trên
toàn graph và so sánh 147 document qua restart. Khối `finally` phục hồi file và
publish revision gốc ngay cả khi một pha trước lỗi.

## Đánh giá

Baseline và cleanup đều là 57/75 cho file, 208.003/267.695 toàn graph; revision
tạm là 68/89 và 208.014/267.709. MongoDB giữ 147 document trong mọi pha, hai
snapshot restart giống nhau và hash cleanup trở về hash gốc. Test hiện phụ thuộc
stack local còn đủ RAM và các connector ở trạng thái `RUNNING`.
"""
        ),
    ]


def main() -> None:
    validate_artifacts()
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
