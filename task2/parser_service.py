#!/usr/bin/env python3
"""
Task 2 - Incremental CPG Parser Service CLI.

Reads Python source files incrementally, parses AST/CFG/DFG/Call edges,
and emits structured JSON events to Kafka or dry-run JSONL output files.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cpg_parser import parse_python_file
from kafka_producer import KafkaEventProducer


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def count_lines(data: bytes) -> int:
    if not data:
        return 0
    cnt = data.count(b"\n")
    if not data.endswith(b"\n"):
        cnt += 1
    return cnt


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incremental CPG Parser Service for Python source files."
    )

    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("artifacts/task1/python_manifest.jsonl"),
        help="Path to Python manifest file from Task 1.",
    )

    parser.add_argument(
        "--repo-dir",
        type=Path,
        default=Path(".work/repos/datasets"),
        help="Directory containing cloned repository files.",
    )

    parser.add_argument(
        "--repository-name",
        default="huggingface/datasets",
        help="Repository name used for deterministic file_id calculation.",
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/task2"),
        help="Output directory for Task 2 JSONL and summary artifacts.",
    )

    parser.add_argument(
        "--kafka-bootstrap",
        default=None,
        help="Kafka bootstrap server (e.g., localhost:9092).",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Force writing to local JSONL files instead of sending to Kafka.",
    )

    parser.add_argument(
        "--single-file",
        type=str,
        default=None,
        help="Reprocess a single Python file path relative to repo-dir (used for Task 6 replay).",
    )

    return parser.parse_args()


def load_manifest(manifest_path: Path) -> list[dict[str, Any]]:
    if not manifest_path.exists():
        raise RuntimeError(f"Manifest file not found: {manifest_path}")

    items = []
    with manifest_path.open("r", encoding="utf-8") as f:
        for line in f:
            line_str = line.strip()
            if line_str:
                items.append(json.loads(line_str))
    return items


def main() -> int:
    args = parse_arguments()

    repo_dir: Path = args.repo_dir.resolve()
    output_dir: Path = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    producer = KafkaEventProducer(
        bootstrap_servers=args.kafka_bootstrap,
        dry_run=args.dry_run or (args.kafka_bootstrap is None),
        output_dir=output_dir,
    )

    # Determine files to process
    file_entries: list[dict[str, Any]] = []

    if args.single_file:
        relative_path_str = args.single_file.replace("\\", "/")
        file_id_source = f"{args.repository_name}:{relative_path_str}".encode("utf-8")
        file_id = sha256_bytes(file_id_source)
        file_entries = [{"file_id": file_id, "path": relative_path_str}]
    elif args.manifest.exists():
        file_entries = load_manifest(args.manifest)
    else:
        # Fallback to direct directory scan if manifest doesn't exist
        print(f"[INFO] Scanning directory directly: {repo_dir}")
        for path in sorted(repo_dir.rglob("*.py")):
            if path.is_file():
                rel_path = path.relative_to(repo_dir).as_posix()
                file_id_source = f"{args.repository_name}:{rel_path}".encode("utf-8")
                file_entries.append({"file_id": sha256_bytes(file_id_source), "path": rel_path})

    total_files = len(file_entries)
    processed_count = 0
    error_count = 0
    total_nodes = 0
    total_edges = 0
    total_ast_nodes = 0
    total_cfg_edges = 0
    total_dfg_edges = 0
    total_call_edges = 0

    start_time = datetime.now(timezone.utc)

    print(f"[INFO] Starting Incremental CPG Parser Service on {total_files} file(s)...")

    for entry in file_entries:
        file_id = entry["file_id"]
        rel_path = entry["path"]
        abs_path = repo_dir / rel_path

        event_time = datetime.now(timezone.utc).isoformat()

        if not abs_path.exists():
            error_event = {
                "schema_version": "1.0.0",
                "event_time": event_time,
                "event_type": "error",
                "error_id": sha256_bytes(f"{file_id}:FileNotFoundError".encode("utf-8")),
                "file_id": file_id,
                "path": rel_path,
                "error_type": "FileNotFoundError",
                "error_message": f"File not found on disk: {abs_path}",
                "lineno": 0,
            }
            producer.emit_event("error", error_event, key=file_id)
            error_count += 1
            continue

        try:
            code_bytes = abs_path.read_bytes()
        except Exception as exc:
            error_event = {
                "schema_version": "1.0.0",
                "event_time": event_time,
                "event_type": "error",
                "error_id": sha256_bytes(f"{file_id}:{type(exc).__name__}".encode("utf-8")),
                "file_id": file_id,
                "path": rel_path,
                "error_type": type(exc).__name__,
                "error_message": f"Failed to read file: {exc}",
                "lineno": 0,
            }
            producer.emit_event("error", error_event, key=file_id)
            error_count += 1
            continue

        # Parse file
        parse_result = parse_python_file(
            file_id=file_id,
            relative_path=rel_path,
            code_bytes=code_bytes,
        )

        if parse_result.error_event:
            producer.emit_event("error", parse_result.error_event, key=file_id)
            error_count += 1
        else:
            # Emit Node events
            for node in parse_result.nodes:
                producer.emit_event("node", node.to_event(event_time), key=node.node_id)
                total_nodes += 1

            # Emit Edge events
            for edge in parse_result.edges:
                producer.emit_event("edge", edge.to_event(event_time), key=edge.edge_id)
                total_edges += 1

            total_ast_nodes += parse_result.ast_node_count
            total_cfg_edges += parse_result.cfg_edge_count
            total_dfg_edges += parse_result.dfg_edge_count
            total_call_edges += parse_result.call_edge_count

        # Emit Metadata event
        metadata_event = parse_result.to_metadata_event(
            repository=args.repository_name,
            size_bytes=len(code_bytes),
            line_count=count_lines(code_bytes),
            content_sha256=sha256_bytes(code_bytes),
            event_time=event_time,
        )
        producer.emit_event("metadata", metadata_event, key=file_id)
        processed_count += 1

    producer.flush()
    producer.close()

    end_time = datetime.now(timezone.utc)
    duration_sec = (end_time - start_time).total_seconds()

    summary = {
        "repository": args.repository_name,
        "total_files_targeted": total_files,
        "processed_files": processed_count,
        "error_files": error_count,
        "total_nodes_emitted": total_nodes,
        "total_edges_emitted": total_edges,
        "cpg_breakdown": {
            "ast_nodes": total_ast_nodes,
            "cfg_edges": total_cfg_edges,
            "dfg_edges": total_dfg_edges,
            "call_edges": total_call_edges,
        },
        "execution_duration_sec": round(duration_sec, 3),
        "executed_at": end_time.isoformat(),
        "dry_run": args.dry_run or (args.kafka_bootstrap is None),
    }

    # Save summary.json
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    print("\n========== TASK 2 SUMMARY ==========")
    print(f"Target Files      : {total_files}")
    print(f"Successfully Parsed: {processed_count}")
    print(f"Errors Encountered: {error_count}")
    print(f"Total Nodes Emitted: {total_nodes} (AST: {total_ast_nodes})")
    print(f"Total Edges Emitted: {total_edges} (CFG: {total_cfg_edges}, DFG: {total_dfg_edges}, CALLS: {total_call_edges})")
    print(f"Duration           : {duration_sec:.2f} seconds")
    print(f"Summary Saved To   : {summary_path}")
    print("====================================")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
