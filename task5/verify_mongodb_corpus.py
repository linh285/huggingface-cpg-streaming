#!/usr/bin/env python3
"""Verify the full Task 5 metadata corpus directly in MongoDB."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "task2"))

from event_contract import file_id_for  # noqa: E402


REQUIRED_FIELDS = (
    "path",
    "content_sha256",
    "kafka_partition",
    "kafka_offset",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify all manifest files in MongoDB and the Spark checkpoint."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=PROJECT_ROOT / "artifacts" / "task1" / "python_manifest.jsonl",
    )
    parser.add_argument("--repository-name", default="huggingface/datasets")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--database", default="cpg")
    parser.add_argument("--collection", default="source_metadata")
    parser.add_argument("--expected-documents", type=int, default=147)
    parser.add_argument("--spark-ui-url", default="http://localhost:4040")
    parser.add_argument("--query-name", default="cpg_metadata_to_mongodb")
    parser.add_argument("--timeout", type=int, default=0)
    parser.add_argument(
        "--json-output",
        type=Path,
        default=(
            PROJECT_ROOT
            / "artifacts"
            / "task5"
            / "mongodb_corpus_verification.json"
        ),
    )
    return parser.parse_args()


def manifest_ids(
    manifest: Path,
    repository_name: str,
) -> tuple[set[str], dict[str, str]]:
    ids: set[str] = set()
    paths: dict[str, str] = {}
    with manifest.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            if not raw_line.strip():
                continue
            item = json.loads(raw_line)
            path = item["path"].replace("\\", "/")
            expected_id = file_id_for(repository_name, path)
            if item.get("file_id", expected_id) != expected_id:
                raise RuntimeError(
                    f"{manifest}:{line_number}: file_id does not match path"
                )
            ids.add(expected_id)
            paths[expected_id] = path
    return ids, paths


def compose_command(*parts: str) -> list[str]:
    command = ["docker", "compose"]
    for file in (
        PROJECT_ROOT / "compose.yml",
        PROJECT_ROOT / "task4" / "docker-compose.yml",
        PROJECT_ROOT / "task5" / "docker-compose.yml",
    ):
        command.extend(["-f", str(file)])
    command.extend(parts)
    return command


def checkpoint_file_count() -> int:
    result = subprocess.run(
        compose_command(
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


def stream_container_state() -> str:
    container = subprocess.run(
        compose_command("ps", "-q", "metadata-stream"),
        cwd=PROJECT_ROOT,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    if not container:
        return "missing"
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Status}}", container],
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.strip()


def spark_query_status(base_url: str, query_name: str) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/StreamingQuery/"
    with urllib.request.urlopen(url, timeout=10) as response:
        html = response.read().decode("utf-8", errors="replace")
    active = (
        "Active Streaming Queries (1)" in html
        and f"<td>{query_name}</td>" in html
        and "<td>RUNNING</td>" in html
    )
    return {
        "name": query_name,
        "status": "RUNNING" if active else "NOT_RUNNING",
        "ui_url": url,
    }


def database_snapshot(
    collection: Any,
    expected_ids: set[str],
) -> dict[str, Any]:
    total = collection.count_documents({})
    file_ids = set(collection.distinct("file_id"))
    document_ids = set(collection.distinct("_id"))
    duplicate_file_ids = list(
        collection.aggregate(
            [
                {"$group": {"_id": "$file_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}},
                {"$sort": {"count": -1}},
            ]
        )
    )
    required_field_missing = {
        field: collection.count_documents({field: {"$exists": False}})
        for field in REQUIRED_FIELDS
    }
    return {
        "total_documents": total,
        "distinct_file_ids": len(file_ids),
        "distinct_document_ids": len(document_ids),
        "id_file_id_mismatches": collection.count_documents(
            {"$expr": {"$ne": ["$_id", "$file_id"]}}
        ),
        "duplicate_file_ids": duplicate_file_ids,
        "missing_manifest_file_ids": sorted(expected_ids - document_ids),
        "unexpected_document_ids": sorted(document_ids - expected_ids),
        "required_field_missing": required_field_missing,
    }


def mismatches(
    snapshot: dict[str, Any],
    expected_documents: int,
) -> list[str]:
    problems: list[str] = []
    for key in (
        "total_documents",
        "distinct_file_ids",
        "distinct_document_ids",
    ):
        if snapshot[key] != expected_documents:
            problems.append(
                f"{key}: expected {expected_documents}, got {snapshot[key]}"
            )
    if snapshot["id_file_id_mismatches"]:
        problems.append(
            f"_id != file_id for {snapshot['id_file_id_mismatches']} documents"
        )
    if snapshot["duplicate_file_ids"]:
        problems.append("duplicate file_id values detected")
    if snapshot["missing_manifest_file_ids"]:
        problems.append(
            f"{len(snapshot['missing_manifest_file_ids'])} manifest files are missing"
        )
    if snapshot["unexpected_document_ids"]:
        problems.append(
            f"{len(snapshot['unexpected_document_ids'])} unexpected documents exist"
        )
    for field, missing in snapshot["required_field_missing"].items():
        if missing:
            problems.append(f"{field} is absent in {missing} documents")
    return problems


def main() -> int:
    args = parse_args()
    try:
        from pymongo import MongoClient
    except ImportError:
        print("[ERROR] pymongo is required", file=sys.stderr)
        return 2

    expected_ids, _ = manifest_ids(args.manifest, args.repository_name)
    if len(expected_ids) != args.expected_documents:
        print(
            f"[ERROR] manifest has {len(expected_ids)} distinct IDs, "
            f"expected {args.expected_documents}",
            file=sys.stderr,
        )
        return 2

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    collection = client[args.database][args.collection]
    deadline = time.monotonic() + args.timeout
    while True:
        snapshot = database_snapshot(collection, expected_ids)
        problems = mismatches(snapshot, args.expected_documents)
        try:
            query = spark_query_status(args.spark_ui_url, args.query_name)
            container_state = stream_container_state()
            checkpoint_files = checkpoint_file_count()
        except Exception as exc:
            query = {
                "name": args.query_name,
                "status": "UNKNOWN",
                "ui_url": f"{args.spark_ui_url.rstrip('/')}/StreamingQuery/",
            }
            container_state = "unknown"
            checkpoint_files = 0
            problems.append(f"cannot verify Spark/checkpoint: {exc}")
        if query["status"] != "RUNNING":
            problems.append(f"Spark query status is {query['status']}")
        if container_state != "running":
            problems.append(f"metadata-stream container is {container_state}")
        if checkpoint_files <= 0:
            problems.append("checkpoint directory contains no files")
        problems = list(dict.fromkeys(problems))
        if not problems or time.monotonic() >= deadline:
            break
        time.sleep(2)
    client.close()

    report = {
        "status": "PASS" if not problems else "FAIL",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "manifest": str(args.manifest.relative_to(PROJECT_ROOT)),
        "expected_documents": args.expected_documents,
        **snapshot,
        "mongodb_database": args.database,
        "mongodb_collection": args.collection,
        "spark_query": query,
        "stream_container_state": container_state,
        "checkpoint_location": "/opt/spark-checkpoints/cpg-metadata",
        "checkpoint_file_count": checkpoint_files,
        "mismatches": problems,
    }
    args.json_output.parent.mkdir(parents=True, exist_ok=True)
    args.json_output.write_text(
        json.dumps(report, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, default=str))
    return 0 if report["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
