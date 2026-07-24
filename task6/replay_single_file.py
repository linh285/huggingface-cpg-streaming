#!/usr/bin/env python3
"""Run the mandatory Task 6 replay proof against both database sinks."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
TASK2 = PROJECT_ROOT / "task2"
sys.path.insert(0, str(TASK2))

from cpg_parser import CPGParseResult, parse_python_file  # noqa: E402
from event_contract import file_id_for  # noqa: E402


EDIT_MARKER = (
    "\n\n"
    "def __cpg_task6_revision_marker__(value):\n"
    "    \"\"\"Temporary real-file edit used by the Task 6 replay proof.\"\"\"\n"
    "    return value + 1\n"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify exact-set replay idempotency in Neo4j and MongoDB."
    )
    parser.add_argument(
        "--file",
        default="src/datasets/utils/experimental.py",
        help="Real Python file relative to --repo-dir.",
    )
    parser.add_argument(
        "--repo-dir",
        type=Path,
        default=PROJECT_ROOT / ".work" / "repos" / "datasets",
    )
    parser.add_argument("--repository-name", default="huggingface/datasets")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="cpgpassword")
    parser.add_argument("--neo4j-database", default="neo4j")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--mongo-database", default="cpg")
    parser.add_argument("--mongo-collection", default="source_metadata")
    parser.add_argument("--connect-url", default="http://localhost:8083")
    parser.add_argument("--spark-ui-url", default="http://localhost:4040")
    parser.add_argument("--expected-corpus-files", type=int, default=147)
    parser.add_argument("--expected-corpus-nodes", type=int, default=208003)
    parser.add_argument("--expected-corpus-edges", type=int, default=267695)
    parser.add_argument(
        "--restart-observation-seconds",
        type=int,
        default=15,
        help="Continuous no-event observation window after Spark becomes active.",
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=PROJECT_ROOT / ".runtime" / "parser-state",
    )
    parser.add_argument(
        "--compose-file",
        type=Path,
        action="append",
        default=None,
        help="Compose file; repeat to override the default full-stack set.",
    )
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--json-output",
        type=Path,
        default=PROJECT_ROOT / "artifacts" / "task6" / "replay_result.json",
    )
    return parser.parse_args()


def expected_graph(
    file_id: str,
    relative_path: str,
    content: bytes,
) -> tuple[CPGParseResult, set[str], set[str], str]:
    result = parse_python_file(file_id, relative_path, content)
    if result.error_event is not None:
        raise RuntimeError(f"Target file does not parse: {result.error_event}")
    return (
        result,
        {node.node_id for node in result.nodes},
        {edge.edge_id for edge in result.edges},
        hashlib.sha256(content).hexdigest(),
    )


def run_parser(args: argparse.Namespace, relative_path: str) -> None:
    command = [
        sys.executable,
        str(TASK2 / "parser_service.py"),
        "--repo-dir",
        str(args.repo_dir),
        "--repository-name",
        args.repository_name,
        "--single-file",
        relative_path,
        "--kafka-bootstrap",
        args.bootstrap,
        "--state-dir",
        str(args.state_dir),
    ]
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def connector_states(connect_url: str) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for name in ("neo4j-sink-cpg-nodes", "neo4j-sink-cpg-edges"):
        try:
            with urllib.request.urlopen(
                f"{connect_url}/connectors/{name}/status", timeout=5
            ) as response:
                status = json.load(response)
        except Exception as exc:
            raise RuntimeError(
                f"Cannot read Kafka Connect status for {name}: {exc}"
            ) from exc
        connector_state = status.get("connector", {}).get("state")
        tasks = status.get("tasks", [])
        task_states = [task.get("state") for task in tasks]
        if connector_state != "RUNNING" or not tasks or any(
            state != "RUNNING" for state in task_states
        ):
            raise RuntimeError(f"Connector is not RUNNING: {name}: {status}")
        result[name] = {
            "connector": connector_state,
            "tasks": ",".join(task_states),
        }
    return result


def neo4j_ids(driver: Any, database: str, file_id: str) -> tuple[set[str], set[str]]:
    with driver.session(database=database) as session:
        node_ids = {
            record["id"]
            for record in session.run(
                "MATCH (n:CPGNode {file_id: $file_id}) "
                "RETURN n.node_id AS id",
                file_id=file_id,
            )
        }
        edge_ids = {
            record["id"]
            for record in session.run(
                "MATCH ()-[r:CPG_EDGE {file_id: $file_id}]->() "
                "RETURN r.edge_id AS id",
                file_id=file_id,
            )
        }
    return node_ids, edge_ids


def global_graph_snapshot(driver: Any, database: str) -> dict[str, int]:
    with driver.session(database=database) as session:
        nodes = session.run(
            "MATCH (n:CPGNode) "
            "RETURN count(n) AS total, "
            "count(DISTINCT n.node_id) AS distinct_total, "
            "count(CASE WHEN n.node_type IS NULL THEN 1 END) AS placeholders"
        ).single()
        edges = session.run(
            "MATCH ()-[r:CPG_EDGE]->() "
            "RETURN count(r) AS total, "
            "count(DISTINCT r.edge_id) AS distinct_total"
        ).single()
    return {
        "total_nodes": int(nodes["total"]),
        "distinct_node_ids": int(nodes["distinct_total"]),
        "placeholder_nodes": int(nodes["placeholders"]),
        "total_edges": int(edges["total"]),
        "distinct_edge_ids": int(edges["distinct_total"]),
    }


def require_global_counts(
    snapshot: dict[str, int],
    expected_nodes: int,
    expected_edges: int,
    phase: str,
) -> None:
    expected = {
        "total_nodes": expected_nodes,
        "distinct_node_ids": expected_nodes,
        "placeholder_nodes": 0,
        "total_edges": expected_edges,
        "distinct_edge_ids": expected_edges,
    }
    differences = {
        key: {"expected": value, "actual": snapshot.get(key)}
        for key, value in expected.items()
        if snapshot.get(key) != value
    }
    if differences:
        raise RuntimeError(f"{phase} global graph mismatch: {differences}")


def normalized_collection_snapshot(collection: Any) -> list[dict[str, Any]]:
    def normalized(value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, int):
            return int(value)
        return value

    documents: list[dict[str, Any]] = []
    projection = {
        "_id": 1,
        "content_sha256": 1,
        "kafka_offset": 1,
        "processed_at": 1,
    }
    for document in collection.find({}, projection).sort("_id", 1):
        documents.append(
            {
                key: normalized(document.get(key))
                for key in (
                    "_id",
                    "content_sha256",
                    "kafka_offset",
                    "processed_at",
                )
            }
        )
    return documents


def wait_for_spark_query(
    spark_ui_url: str,
    query_name: str,
    timeout: int,
) -> dict[str, str]:
    url = f"{spark_ui_url.rstrip('/')}/StreamingQuery/"
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=10) as response:
                html = response.read().decode("utf-8", errors="replace")
            if (
                f"<td>{query_name}</td>" in html
                and "<td>RUNNING</td>" in html
                and "Active Streaming Queries (1)" in html
            ):
                return {"name": query_name, "status": "RUNNING", "ui_url": url}
            last_error = "query is not listed as RUNNING"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(2)
    raise RuntimeError(
        f"Spark query {query_name} did not become RUNNING: {last_error}"
    )


def observe_unchanged_collection(
    collection: Any,
    expected: list[dict[str, Any]],
    observation_seconds: int,
) -> tuple[list[dict[str, Any]], int]:
    deadline = time.monotonic() + observation_seconds
    checks = 0
    latest = normalized_collection_snapshot(collection)
    while True:
        checks += 1
        if latest != expected:
            raise RuntimeError(
                "MongoDB collection changed after Spark restart without a new event"
            )
        if time.monotonic() >= deadline:
            return latest, checks
        time.sleep(min(2, max(0, deadline - time.monotonic())))
        latest = normalized_collection_snapshot(collection)


def mongo_document(
    collection: Any,
    file_id: str,
) -> tuple[int, int, dict[str, Any] | None]:
    by_id = collection.count_documents({"_id": file_id})
    by_file_id = collection.count_documents({"file_id": file_id})
    return by_id, by_file_id, collection.find_one({"_id": file_id})


def wait_for_exact_state(
    *,
    driver: Any,
    database: str,
    collection: Any,
    file_id: str,
    expected_nodes: set[str],
    expected_edges: set[str],
    expected_hash: str,
    timeout: int,
    expected_collection_documents: int,
    minimum_mongo_offset: int | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        actual_nodes, actual_edges = neo4j_ids(driver, database, file_id)
        mongo_id_count, mongo_file_count, document = mongo_document(
            collection, file_id
        )
        actual_hash = document.get("content_sha256") if document else None
        mongo_offset = document.get("kafka_offset") if document else None
        collection_documents = collection.count_documents({})
        distinct_file_ids = len(collection.distinct("file_id"))
        last = {
            "expected_node_count": len(expected_nodes),
            "actual_node_count": len(actual_nodes),
            "missing_nodes": len(expected_nodes - actual_nodes),
            "stale_nodes": len(actual_nodes - expected_nodes),
            "expected_edge_count": len(expected_edges),
            "actual_edge_count": len(actual_edges),
            "missing_edges": len(expected_edges - actual_edges),
            "stale_edges": len(actual_edges - expected_edges),
            "mongo_id_count": mongo_id_count,
            "mongo_file_id_count": mongo_file_count,
            "mongo_content_sha256": actual_hash,
            "mongo_kafka_offset": mongo_offset,
            "mongo_collection_documents": collection_documents,
            "mongo_distinct_file_ids": distinct_file_ids,
        }
        if (
            actual_nodes == expected_nodes
            and actual_edges == expected_edges
            and mongo_id_count == 1
            and mongo_file_count == 1
            and actual_hash == expected_hash
            and isinstance(mongo_offset, int)
            and collection_documents == expected_collection_documents
            and distinct_file_ids == expected_collection_documents
            and (
                minimum_mongo_offset is None
                or mongo_offset >= minimum_mongo_offset
            )
        ):
            return last
        time.sleep(2)
    raise RuntimeError(
        "Timed out waiting for exact Neo4j ID sets and one matching MongoDB "
        f"document: {last}"
    )


def compose_command(args: argparse.Namespace, *parts: str) -> list[str]:
    files = args.compose_file or [
        PROJECT_ROOT / "compose.yml",
        PROJECT_ROOT / "task4" / "docker-compose.yml",
        PROJECT_ROOT / "task5" / "docker-compose.yml",
    ]
    command = ["docker", "compose"]
    for file in files:
        command.extend(["-f", str(file)])
    command.extend(parts)
    return command


def checkpoint_file_count(args: argparse.Namespace) -> int:
    result = subprocess.run(
        compose_command(
            args,
            "exec",
            "-T",
            "metadata-stream",
            "sh",
            "-c",
            "find /opt/spark-checkpoints/cpg-metadata -type f | wc -l",
        ),
        cwd=PROJECT_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return int(result.stdout.strip())


def restart_spark_and_verify_checkpoint(
    args: argparse.Namespace,
    collection: Any,
) -> dict[str, Any]:
    before = normalized_collection_snapshot(collection)
    if len(before) != args.expected_corpus_files:
        raise RuntimeError(
            "MongoDB corpus size before restart is "
            f"{len(before)}, expected {args.expected_corpus_files}"
        )
    files_before = checkpoint_file_count(args)
    if files_before <= 0:
        raise RuntimeError("Spark checkpoint directory contains no files")

    subprocess.run(
        compose_command(args, "restart", "metadata-stream"),
        cwd=PROJECT_ROOT,
        check=True,
    )
    query = wait_for_spark_query(
        args.spark_ui_url,
        "cpg_metadata_to_mongodb",
        args.timeout,
    )
    after, observation_checks = observe_unchanged_collection(
        collection,
        before,
        args.restart_observation_seconds,
    )
    files_after = checkpoint_file_count(args)
    return {
        "document_count_before": len(before),
        "document_count_after": len(after),
        "snapshots_equal": before == after,
        "documents_before": before,
        "documents_after": after,
        "checkpoint_files_before": files_before,
        "checkpoint_files_after": files_after,
        "spark_query": query,
        "observation_seconds": args.restart_observation_seconds,
        "observation_checks": observation_checks,
    }


def main() -> int:
    args = parse_args()
    args.repo_dir = args.repo_dir.resolve()
    relative_path = args.file.replace("\\", "/")
    absolute_path = (args.repo_dir / relative_path).resolve()
    try:
        absolute_path.relative_to(args.repo_dir)
    except ValueError:
        print("[FAIL] --file escapes --repo-dir", file=sys.stderr)
        return 2
    if not absolute_path.is_file():
        print(f"[FAIL] Target file not found: {absolute_path}", file=sys.stderr)
        return 2

    try:
        from neo4j import GraphDatabase
        from pymongo import MongoClient
    except ImportError:
        print(
            "[FAIL] Task 6 requires neo4j and pymongo: "
            "python -m pip install -r task4/requirements.txt",
            file=sys.stderr,
        )
        return 2

    file_id = file_id_for(args.repository_name, relative_path)
    original_bytes = absolute_path.read_bytes()
    original = expected_graph(file_id, relative_path, original_bytes)
    modified_bytes = original_bytes + EDIT_MARKER.encode("utf-8")
    modified = expected_graph(file_id, relative_path, modified_bytes)
    if original[3] == modified[3]:
        print("[FAIL] Modified revision has the same hash", file=sys.stderr)
        return 2

    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password),
    )
    mongo = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    collection = mongo[args.mongo_database][args.mongo_collection]
    modified_on_disk = False
    baseline_published = False
    report: dict[str, Any] = {
        "file": relative_path,
        "file_id": file_id,
        "file_id_stable_across_revisions": True,
        "original_content_sha256": original[3],
        "modified_content_sha256": modified[3],
        "expected_file_counts": {
            "baseline_nodes": len(original[1]),
            "baseline_edges": len(original[2]),
            "modified_nodes": len(modified[1]),
            "modified_edges": len(modified[2]),
        },
        "expected_global_counts": {
            "baseline_nodes": args.expected_corpus_nodes,
            "baseline_edges": args.expected_corpus_edges,
            "modified_nodes": (
                args.expected_corpus_nodes + len(modified[1]) - len(original[1])
            ),
            "modified_edges": (
                args.expected_corpus_edges + len(modified[2]) - len(original[2])
            ),
        },
        "global_counts": {},
    }
    required_file_counts = (57, 75, 68, 89)
    actual_file_counts = (
        len(original[1]),
        len(original[2]),
        len(modified[1]),
        len(modified[2]),
    )
    if actual_file_counts != required_file_counts:
        print(
            "[FAIL] Unexpected Task 6 per-file counts: "
            f"expected {required_file_counts}, got {actual_file_counts}",
            file=sys.stderr,
        )
        driver.close()
        mongo.close()
        return 2

    try:
        driver.verify_connectivity()
        mongo.admin.command("ping")
        report["connectors"] = connector_states(args.connect_url)
        prior_document = collection.find_one({"_id": file_id})
        prior_offset = (
            prior_document.get("kafka_offset")
            if prior_document is not None
            else None
        )

        print("[1/5] Publish the real file for the first time")
        run_parser(args, relative_path)
        baseline_published = True
        report["first_publish"] = wait_for_exact_state(
            driver=driver,
            database=args.neo4j_database,
            collection=collection,
            file_id=file_id,
            expected_nodes=original[1],
            expected_edges=original[2],
            expected_hash=original[3],
            timeout=args.timeout,
            expected_collection_documents=args.expected_corpus_files,
            minimum_mongo_offset=(
                prior_offset + 1 if prior_offset is not None else None
            ),
        )
        baseline_global = global_graph_snapshot(driver, args.neo4j_database)
        require_global_counts(
            baseline_global,
            args.expected_corpus_nodes,
            args.expected_corpus_edges,
            "baseline",
        )
        report["global_counts"]["baseline"] = baseline_global
        first_offset = report["first_publish"]["mongo_kafka_offset"]

        print("[2/5] Replay the unchanged file and same revision")
        run_parser(args, relative_path)
        report["unchanged_replay"] = wait_for_exact_state(
            driver=driver,
            database=args.neo4j_database,
            collection=collection,
            file_id=file_id,
            expected_nodes=original[1],
            expected_edges=original[2],
            expected_hash=original[3],
            timeout=args.timeout,
            expected_collection_documents=args.expected_corpus_files,
            minimum_mongo_offset=first_offset + 1,
        )
        unchanged_global = global_graph_snapshot(driver, args.neo4j_database)
        require_global_counts(
            unchanged_global,
            args.expected_corpus_nodes,
            args.expected_corpus_edges,
            "unchanged replay",
        )
        report["global_counts"]["unchanged_replay"] = unchanged_global
        unchanged_offset = report["unchanged_replay"]["mongo_kafka_offset"]

        print("[3/5] Publish a modified revision of the same real file")
        absolute_path.write_bytes(modified_bytes)
        modified_on_disk = True
        run_parser(args, relative_path)
        report["modified_replay"] = wait_for_exact_state(
            driver=driver,
            database=args.neo4j_database,
            collection=collection,
            file_id=file_id,
            expected_nodes=modified[1],
            expected_edges=modified[2],
            expected_hash=modified[3],
            timeout=args.timeout,
            expected_collection_documents=args.expected_corpus_files,
            minimum_mongo_offset=unchanged_offset + 1,
        )
        modified_global = global_graph_snapshot(driver, args.neo4j_database)
        require_global_counts(
            modified_global,
            report["expected_global_counts"]["modified_nodes"],
            report["expected_global_counts"]["modified_edges"],
            "modified replay",
        )
        report["global_counts"]["modified_replay"] = modified_global

        node_duplicates = (
            modified_global["total_nodes"]
            - modified_global["distinct_node_ids"]
        )
        edge_duplicates = (
            modified_global["total_edges"]
            - modified_global["distinct_edge_ids"]
        )
        report["duplicate_node_ids"] = node_duplicates
        report["duplicate_edge_ids"] = edge_duplicates
        if node_duplicates or edge_duplicates:
            raise RuntimeError(
                f"Duplicate IDs found: nodes={node_duplicates}, "
                f"edges={edge_duplicates}"
            )

        print("[4/5] Restart Spark and verify its persistent checkpoint")
        report["spark_restart"] = restart_spark_and_verify_checkpoint(
            args, collection
        )
    except Exception as exc:
        report["status"] = "FAIL"
        report["error"] = str(exc)
        print(f"[FAIL] {exc}", file=sys.stderr)
    finally:
        if modified_on_disk:
            absolute_path.write_bytes(original_bytes)
            print("[cleanup] Restored the real source file")
            if baseline_published:
                try:
                    run_parser(args, relative_path)
                    report["cleanup_restore"] = wait_for_exact_state(
                        driver=driver,
                        database=args.neo4j_database,
                        collection=collection,
                        file_id=file_id,
                        expected_nodes=original[1],
                        expected_edges=original[2],
                        expected_hash=original[3],
                        timeout=args.timeout,
                        expected_collection_documents=args.expected_corpus_files,
                        minimum_mongo_offset=(
                            report.get("modified_replay", {}).get(
                                "mongo_kafka_offset"
                            )
                            + 1
                            if report.get("modified_replay", {}).get(
                                "mongo_kafka_offset"
                            )
                            is not None
                            else None
                        ),
                    )
                    cleanup_global = global_graph_snapshot(
                        driver, args.neo4j_database
                    )
                    require_global_counts(
                        cleanup_global,
                        args.expected_corpus_nodes,
                        args.expected_corpus_edges,
                        "cleanup",
                    )
                    report["global_counts"]["cleanup"] = cleanup_global
                except Exception as exc:
                    report["status"] = "FAIL"
                    report["cleanup_error"] = str(exc)
                    print(f"[FAIL] Cleanup replay failed: {exc}", file=sys.stderr)
        driver.close()
        mongo.close()

    if report.get("status") != "FAIL":
        report["status"] = "PASS"
        report["phase_comparison"] = {
            "baseline": {
                "file_nodes": report["first_publish"]["actual_node_count"],
                "file_edges": report["first_publish"]["actual_edge_count"],
                "global_nodes": report["global_counts"]["baseline"]["total_nodes"],
                "global_edges": report["global_counts"]["baseline"]["total_edges"],
                "mongo_documents": report["first_publish"][
                    "mongo_collection_documents"
                ],
            },
            "unchanged_replay": {
                "file_nodes": report["unchanged_replay"]["actual_node_count"],
                "file_edges": report["unchanged_replay"]["actual_edge_count"],
                "global_nodes": report["global_counts"]["unchanged_replay"][
                    "total_nodes"
                ],
                "global_edges": report["global_counts"]["unchanged_replay"][
                    "total_edges"
                ],
                "mongo_documents": report["unchanged_replay"][
                    "mongo_collection_documents"
                ],
            },
            "modified_replay": {
                "file_nodes": report["modified_replay"]["actual_node_count"],
                "file_edges": report["modified_replay"]["actual_edge_count"],
                "global_nodes": report["global_counts"]["modified_replay"][
                    "total_nodes"
                ],
                "global_edges": report["global_counts"]["modified_replay"][
                    "total_edges"
                ],
                "mongo_documents": report["modified_replay"][
                    "mongo_collection_documents"
                ],
            },
            "restart": {
                "mongo_documents_before": report["spark_restart"][
                    "document_count_before"
                ],
                "mongo_documents_after": report["spark_restart"][
                    "document_count_after"
                ],
                "snapshots_equal": report["spark_restart"]["snapshots_equal"],
            },
            "cleanup": {
                "file_nodes": report["cleanup_restore"]["actual_node_count"],
                "file_edges": report["cleanup_restore"]["actual_edge_count"],
                "global_nodes": report["global_counts"]["cleanup"]["total_nodes"],
                "global_edges": report["global_counts"]["cleanup"]["total_edges"],
                "mongo_documents": report["cleanup_restore"][
                    "mongo_collection_documents"
                ],
            },
        }
        print("[5/5] PASS: exact IDs, MongoDB upsert, and checkpoint verified")

    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(
        json.dumps(report, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
