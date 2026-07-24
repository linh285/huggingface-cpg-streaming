#!/usr/bin/env python3
"""Incrementally parse Python files and publish CPG change events."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from cpg_parser import CPGParseResult, parse_python_file
from event_contract import (
    EDGE_DELETE,
    EDGE_UPSERT,
    FILE_METADATA_UPSERT,
    NODE_DELETE,
    NODE_UPSERT,
    PARSER_ERROR,
    common_fields,
    file_id_for,
    sha256_bytes,
    sha256_text,
    utc_now,
)
from kafka_producer import KafkaEventProducer
from parser_state import ParserStateStore


def count_lines(data: bytes) -> int:
    if not data:
        return 0
    return data.count(b"\n") + (0 if data.endswith(b"\n") else 1)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incremental CPG Parser Service for Python source files."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("artifacts/task1/python_manifest.jsonl"),
        help="Task 1 JSONL manifest.",
    )
    parser.add_argument(
        "--repo-dir",
        type=Path,
        default=Path(".work/repos/datasets"),
        help="Cloned repository directory.",
    )
    parser.add_argument(
        "--repository-name",
        default="huggingface/datasets",
        help="Stable repository name used to derive file_id.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/task2"),
        help="JSONL output directory; used only with --dry-run.",
    )
    parser.add_argument(
        "--summary-output",
        type=Path,
        help=(
            "Optional JSON summary path for either Kafka or dry-run mode. "
            "Dry-run still defaults to <output-dir>/summary.json."
        ),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(".runtime/parser-state"),
        help="Persistent state used to calculate delete events.",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        help="Kafka bootstrap server, for example localhost:9092.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write JSONL files instead of connecting to Kafka.",
    )
    parser.add_argument(
        "--single-file",
        help="One path relative to --repo-dir, used for replay.",
    )
    args = parser.parse_args()
    if args.dry_run and args.kafka_bootstrap:
        parser.error("--dry-run and --kafka-bootstrap are mutually exclusive")
    if not args.dry_run and not args.kafka_bootstrap:
        parser.error("Kafka mode requires --kafka-bootstrap (or use --dry-run)")
    return args


def iter_manifest_entries(
    manifest_path: Path,
    repository_name: str,
) -> Iterator[dict[str, str]]:
    if not manifest_path.is_file():
        raise RuntimeError(f"Manifest file not found: {manifest_path}")
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(
                    f"{manifest_path}:{line_number}: invalid JSON: {exc}"
                ) from exc
            if not isinstance(item, dict) or not isinstance(item.get("path"), str):
                raise RuntimeError(
                    f"{manifest_path}:{line_number}: missing string field 'path'"
                )
            relative_path = item["path"].replace("\\", "/")
            expected_id = file_id_for(repository_name, relative_path)
            manifest_id = item.get("file_id")
            if manifest_id is not None and manifest_id != expected_id:
                raise RuntimeError(
                    f"{manifest_path}:{line_number}: file_id does not match "
                    f"{repository_name}:{relative_path}"
                )
            yield {"file_id": expected_id, "path": relative_path}


def iter_file_entries(args: argparse.Namespace) -> Iterator[dict[str, str]]:
    if args.single_file:
        relative_path = args.single_file.replace("\\", "/")
        yield {
            "file_id": file_id_for(args.repository_name, relative_path),
            "path": relative_path,
        }
        return
    yield from iter_manifest_entries(args.manifest, args.repository_name)


def event_common(
    *,
    event_type: str,
    event_time: str,
    repository: str,
    file_id: str,
    path: str,
    content_sha256: str,
) -> dict[str, Any]:
    return common_fields(
        event_type=event_type,
        event_time=event_time,
        repository=repository,
        file_id=file_id,
        path=path,
        content_sha256=content_sha256,
    )


def state_for_result(
    result: CPGParseResult,
    content_sha256: str,
) -> dict[str, Any]:
    return {
        "content_sha256": content_sha256,
        "node_ids": sorted(node.node_id for node in result.nodes),
        "edges": sorted(
            (
                {
                    "edge_id": edge.edge_id,
                    "edge_type": edge.edge_type,
                    "source_id": edge.source_id,
                    "target_id": edge.target_id,
                }
                for edge in result.edges
            ),
            key=lambda edge: edge["edge_id"],
        ),
    }


def publish_success(
    *,
    result: CPGParseResult,
    code_bytes: bytes,
    repository_name: str,
    producer: KafkaEventProducer,
    state_store: ParserStateStore,
) -> dict[str, int]:
    content_hash = sha256_bytes(code_bytes)
    event_time = utc_now()
    previous = state_store.load(result.file_id) or {}
    previous_nodes = set(previous.get("node_ids", []))
    previous_edges = {
        edge["edge_id"]: edge for edge in previous.get("edges", [])
    }
    current_nodes = {node.node_id for node in result.nodes}
    current_edges = {edge.edge_id for edge in result.edges}

    deleted_edge_ids = sorted(previous_edges.keys() - current_edges)
    for edge_id in deleted_edge_ids:
        old_edge = previous_edges[edge_id]
        event = {
            **event_common(
                event_type=EDGE_DELETE,
                event_time=event_time,
                repository=repository_name,
                file_id=result.file_id,
                path=result.relative_path,
                content_sha256=content_hash,
            ),
            **old_edge,
        }
        producer.emit_event("edge", event, key=edge_id)

    deleted_node_ids = sorted(previous_nodes - current_nodes)
    for node_id in deleted_node_ids:
        event = {
            **event_common(
                event_type=NODE_DELETE,
                event_time=event_time,
                repository=repository_name,
                file_id=result.file_id,
                path=result.relative_path,
                content_sha256=content_hash,
            ),
            "node_id": node_id,
        }
        producer.emit_event("node", event, key=node_id)

    node_common = event_common(
        event_type=NODE_UPSERT,
        event_time=event_time,
        repository=repository_name,
        file_id=result.file_id,
        path=result.relative_path,
        content_sha256=content_hash,
    )
    for node in result.nodes:
        producer.emit_event(
            "node", node.to_event(node_common), key=node.node_id
        )

    edge_common = event_common(
        event_type=EDGE_UPSERT,
        event_time=event_time,
        repository=repository_name,
        file_id=result.file_id,
        path=result.relative_path,
        content_sha256=content_hash,
    )
    for edge in result.edges:
        producer.emit_event(
            "edge", edge.to_event(edge_common), key=edge.edge_id
        )

    metadata_common = event_common(
        event_type=FILE_METADATA_UPSERT,
        event_time=event_time,
        repository=repository_name,
        file_id=result.file_id,
        path=result.relative_path,
        content_sha256=content_hash,
    )
    metadata = result.to_metadata_event(
        metadata_common,
        size_bytes=len(code_bytes),
        line_count=count_lines(code_bytes),
    )
    producer.emit_event("metadata", metadata, key=result.file_id)

    # State advances only after Kafka acknowledges every event for this file.
    producer.flush()
    state_store.save(
        result.file_id, state_for_result(result, content_hash)
    )
    return {
        "nodes": len(result.nodes),
        "edges": len(result.edges),
        "deleted_nodes": len(deleted_node_ids),
        "deleted_edges": len(deleted_edge_ids),
    }


def publish_failure(
    *,
    file_id: str,
    relative_path: str,
    repository_name: str,
    code_bytes: bytes,
    error: dict[str, Any],
    producer: KafkaEventProducer,
) -> None:
    event_time = utc_now()
    content_hash = sha256_bytes(code_bytes)
    error_type = str(error.get("error_type", "ParserError"))
    error_message = str(error.get("error_message", "Unknown parser error"))
    lineno = int(error.get("lineno", 0) or 0)
    col_offset = int(error.get("col_offset", 0) or 0)
    common = event_common(
        event_type=PARSER_ERROR,
        event_time=event_time,
        repository=repository_name,
        file_id=file_id,
        path=relative_path,
        content_sha256=content_hash,
    )
    error_event = {
        **common,
        "error_id": sha256_text(
            f"{file_id}:{error_type}:{lineno}:{col_offset}:{error_message}"
        ),
        "error_type": error_type,
        "error_message": error_message,
        "lineno": lineno,
        "col_offset": col_offset,
    }
    producer.emit_event("error", error_event, key=error_event["error_id"])
    metadata = {
        **event_common(
            event_type=FILE_METADATA_UPSERT,
            event_time=event_time,
            repository=repository_name,
            file_id=file_id,
            path=relative_path,
            content_sha256=content_hash,
        ),
        "language": "python",
        "size_bytes": len(code_bytes),
        "line_count": count_lines(code_bytes),
        "ast_node_count": 0,
        "ast_edge_count": 0,
        "cfg_edge_count": 0,
        "dfg_edge_count": 0,
        "call_edge_count": 0,
        "status": "FAILED",
    }
    producer.emit_event("metadata", metadata, key=file_id)
    producer.flush()


def main() -> int:
    args = parse_arguments()
    repo_dir = args.repo_dir.resolve()
    if not repo_dir.is_dir():
        print(f"[ERROR] Repository directory not found: {repo_dir}", file=sys.stderr)
        return 1

    producer: KafkaEventProducer | None = None
    started_at = datetime.now(timezone.utc)
    summary = {
        "repository": args.repository_name,
        "total_files_targeted": 0,
        "processed_files": 0,
        "error_files": 0,
        "total_nodes_emitted": 0,
        "total_edges_emitted": 0,
        "deleted_nodes_emitted": 0,
        "deleted_edges_emitted": 0,
        "cpg_breakdown": {
            "ast_nodes": 0,
            "ast_edges": 0,
            "cfg_edges": 0,
            "dfg_edges": 0,
            "call_edges": 0,
        },
        "dry_run": args.dry_run,
    }
    fatal_error: str | None = None

    try:
        producer = KafkaEventProducer(
            bootstrap_servers=args.kafka_bootstrap,
            dry_run=args.dry_run,
            output_dir=args.output_dir.resolve(),
        )
        state_store = ParserStateStore(args.state_dir.resolve())

        for entry in iter_file_entries(args):
            summary["total_files_targeted"] += 1
            file_id = entry["file_id"]
            relative_path = entry["path"]
            absolute_path = (repo_dir / relative_path).resolve()
            try:
                absolute_path.relative_to(repo_dir)
            except ValueError:
                raise RuntimeError(
                    f"Path escapes repository root: {relative_path}"
                )

            try:
                code_bytes = absolute_path.read_bytes()
            except OSError as exc:
                publish_failure(
                    file_id=file_id,
                    relative_path=relative_path,
                    repository_name=args.repository_name,
                    code_bytes=b"",
                    error={
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    },
                    producer=producer,
                )
                summary["error_files"] += 1
                continue

            result = parse_python_file(file_id, relative_path, code_bytes)
            if result.error_event is not None:
                publish_failure(
                    file_id=file_id,
                    relative_path=relative_path,
                    repository_name=args.repository_name,
                    code_bytes=code_bytes,
                    error=result.error_event,
                    producer=producer,
                )
                summary["error_files"] += 1
                continue

            counts = publish_success(
                result=result,
                code_bytes=code_bytes,
                repository_name=args.repository_name,
                producer=producer,
                state_store=state_store,
            )
            summary["processed_files"] += 1
            summary["total_nodes_emitted"] += counts["nodes"]
            summary["total_edges_emitted"] += counts["edges"]
            summary["deleted_nodes_emitted"] += counts["deleted_nodes"]
            summary["deleted_edges_emitted"] += counts["deleted_edges"]
            breakdown = summary["cpg_breakdown"]
            breakdown["ast_nodes"] += result.ast_node_count
            breakdown["ast_edges"] += result.ast_edge_count
            breakdown["cfg_edges"] += result.cfg_edge_count
            breakdown["dfg_edges"] += result.dfg_edge_count
            breakdown["call_edges"] += result.call_edge_count

        producer.flush()
    except Exception as exc:
        fatal_error = str(exc)
        summary["fatal_error"] = fatal_error
        print(f"[ERROR] {exc}", file=sys.stderr)
    finally:
        if producer is not None:
            try:
                producer.close()
            except Exception as exc:
                if fatal_error is None:
                    fatal_error = f"Failed to close producer: {exc}"
                    summary["fatal_error"] = fatal_error
                print(f"[ERROR] Failed to close producer: {exc}", file=sys.stderr)

    finished_at = datetime.now(timezone.utc)
    summary["execution_duration_sec"] = round(
        (finished_at - started_at).total_seconds(), 3
    )
    summary["executed_at"] = finished_at.isoformat()

    summary_output = args.summary_output
    if summary_output is None and args.dry_run:
        summary_output = args.output_dir / "summary.json"
    if summary_output is not None:
        summary_output.parent.mkdir(parents=True, exist_ok=True)
        summary_output.write_text(
            json.dumps(summary, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 1 if summary["error_files"] or fatal_error is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
