#!/usr/bin/env python3
"""Verify ID uniqueness and referential integrity in a Task 2 JSONL dump."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def read_jsonl(path: Path):
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"{path}:{line_number}: invalid JSON: {exc}"
                    ) from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--expected-files", type=int, default=147)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()

    node_ids = [
        event["node_id"]
        for event in read_jsonl(args.directory / "nodes.jsonl")
    ]
    edge_ids: list[str] = []
    edge_endpoints: list[tuple[str, str]] = []
    for event in read_jsonl(args.directory / "edges.jsonl"):
        edge_ids.append(event["edge_id"])
        edge_endpoints.append((event["source_id"], event["target_id"]))
    metadata_file_ids = [
        event["file_id"]
        for event in read_jsonl(args.directory / "metadata.jsonl")
    ]
    error_count = sum(
        1 for _ in read_jsonl(args.directory / "errors.jsonl")
    )
    node_id_set = set(node_ids)
    missing_sources = {
        source_id
        for source_id, _ in edge_endpoints
        if source_id not in node_id_set
    }
    missing_targets = {
        target_id
        for _, target_id in edge_endpoints
        if target_id not in node_id_set
    }
    result = {
        "node_events": len(node_ids),
        "distinct_node_ids": len(set(node_ids)),
        "edge_events": len(edge_ids),
        "distinct_edge_ids": len(set(edge_ids)),
        "metadata_events": len(metadata_file_ids),
        "distinct_metadata_file_ids": len(set(metadata_file_ids)),
        "error_events": error_count,
        "missing_edge_sources": len(missing_sources),
        "missing_edge_targets": len(missing_targets),
    }
    ok = (
        result["node_events"] == result["distinct_node_ids"]
        and result["edge_events"] == result["distinct_edge_ids"]
        and result["metadata_events"] == args.expected_files
        and result["distinct_metadata_file_ids"] == args.expected_files
        and result["error_events"] == 0
        and result["missing_edge_sources"] == 0
        and result["missing_edge_targets"] == 0
    )
    result["status"] = "PASS" if ok else "FAIL"
    if args.json_output:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(result, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(result, indent=2))
    print(f"VERDICT: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
