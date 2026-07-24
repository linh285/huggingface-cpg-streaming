#!/usr/bin/env python3
"""
Task 4 - Neo4j ingestion verification.

Connects to Neo4j and reports the ingested graph topology:
  * total CPGNode / CPG_EDGE counts
  * breakdown by node_type and edge_type (AST / CFG / DFG / CALL)
  * duplicate detection on node_id and edge_id (idempotency proof: both 0)
  * count of placeholder nodes still missing a node_type (edges seen before
    their node event -- should trend to 0 once the node topic is drained)

Prints a human-readable report and, with --json, a machine-readable snapshot
(reused by Task 6 for before/after comparison).

Usage:
    python verify_neo4j.py
    python verify_neo4j.py --json snapshot.json
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

COUNT_QUERIES = {
    "total_nodes": "MATCH (n:CPGNode) RETURN count(n) AS value",
    "total_edges": "MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS value",
    "distinct_node_ids": "MATCH (n:CPGNode) RETURN count(DISTINCT n.node_id) AS value",
    "distinct_edge_ids": "MATCH ()-[r:CPG_EDGE]->() RETURN count(DISTINCT r.edge_id) AS value",
    "placeholder_nodes": "MATCH (n:CPGNode) WHERE n.node_type IS NULL RETURN count(n) AS value",
}

NODE_BREAKDOWN = (
    "MATCH (n:CPGNode) "
    "RETURN coalesce(n.node_type, '<placeholder>') AS k, count(*) AS c "
    "ORDER BY c DESC"
)
EDGE_BREAKDOWN = (
    "MATCH ()-[r:CPG_EDGE]->() "
    "RETURN coalesce(r.edge_type, '<unknown>') AS k, count(*) AS c "
    "ORDER BY c DESC"
)
DUP_NODES = (
    "MATCH (n:CPGNode) WITH n.node_id AS id, count(*) AS c WHERE c > 1 "
    "RETURN id, c ORDER BY c DESC LIMIT 10"
)
DUP_EDGES = (
    "MATCH ()-[r:CPG_EDGE]->() WITH r.edge_id AS id, count(*) AS c WHERE c > 1 "
    "RETURN id, c ORDER BY c DESC LIMIT 10"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Verify CPG ingestion into Neo4j.")
    p.add_argument("--uri", default="bolt://localhost:7687")
    p.add_argument("--user", default="neo4j")
    p.add_argument("--password", default="cpgpassword")
    p.add_argument("--database", default="neo4j")
    p.add_argument("--json", default=None, help="Optional path to write a JSON snapshot.")
    p.add_argument("--connect-url", default=None, help="Kafka Connect REST base URL.")
    p.add_argument("--expected-nodes", type=int, default=None)
    p.add_argument("--expected-edges", type=int, default=None)
    p.add_argument("--expected-ast-edges", type=int, default=None)
    p.add_argument("--expected-cfg-edges", type=int, default=None)
    p.add_argument("--expected-dfg-edges", type=int, default=None)
    p.add_argument("--expected-call-edges", type=int, default=None)
    p.add_argument(
        "--require-zero-placeholders",
        action="store_true",
        help="Fail while any edge-created placeholder node remains.",
    )
    p.add_argument(
        "--timeout",
        type=int,
        default=0,
        help="Poll expected live counts for at most this many seconds.",
    )
    return p.parse_args()


def scalar(session, query: str) -> int:
    return session.run(query).single()["value"]


def rows(session, query: str) -> list[dict]:
    return [dict(r) for r in session.run(query)]


def connector_states(connect_url: str) -> dict[str, dict]:
    result: dict[str, dict] = {}
    for name in ("neo4j-sink-cpg-nodes", "neo4j-sink-cpg-edges"):
        with urllib.request.urlopen(
            f"{connect_url.rstrip('/')}/connectors/{name}/status", timeout=5
        ) as response:
            status = json.load(response)
        result[name] = {
            "connector": status.get("connector", {}).get("state"),
            "tasks": [task.get("state") for task in status.get("tasks", [])],
        }
    return result


def connector_mismatches(connectors: dict[str, dict]) -> list[str]:
    mismatches: list[str] = []
    for name, status in connectors.items():
        tasks = status.get("tasks", [])
        if status.get("connector") != "RUNNING":
            mismatches.append(f"{name} connector is {status.get('connector')}")
        if not tasks or any(task != "RUNNING" for task in tasks):
            mismatches.append(f"{name} tasks are {tasks}")
    return mismatches


def expected_values(args: argparse.Namespace) -> dict[str, int | bool]:
    values: dict[str, int | bool] = {}
    for argument, key in (
        ("expected_nodes", "total_nodes"),
        ("expected_edges", "total_edges"),
        ("expected_ast_edges", "AST"),
        ("expected_cfg_edges", "CFG"),
        ("expected_dfg_edges", "DFG"),
        ("expected_call_edges", "CALL"),
    ):
        value = getattr(args, argument)
        if value is not None:
            values[key] = value
    if args.require_zero_placeholders:
        values["placeholder_nodes"] = 0
    return values


def verify_snapshot(
    args: argparse.Namespace,
    counts: dict[str, int],
    edge_breakdown: dict[str, int],
    duplicate_nodes: list[dict],
    duplicate_edges: list[dict],
    connectors: dict[str, dict],
) -> list[str]:
    mismatches: list[str] = []
    expected = expected_values(args)
    for key in ("total_nodes", "total_edges", "placeholder_nodes"):
        if key in expected and counts[key] != expected[key]:
            mismatches.append(f"{key}: expected {expected[key]}, got {counts[key]}")
    for edge_type in ("AST", "CFG", "DFG", "CALL"):
        if edge_type in expected and edge_breakdown.get(edge_type, 0) != expected[edge_type]:
            mismatches.append(
                f"{edge_type} edges: expected {expected[edge_type]}, "
                f"got {edge_breakdown.get(edge_type, 0)}"
            )
    if counts["total_nodes"] != counts["distinct_node_ids"] or duplicate_nodes:
        mismatches.append("duplicate node_id values detected")
    if counts["total_edges"] != counts["distinct_edge_ids"] or duplicate_edges:
        mismatches.append("duplicate edge_id values detected")
    if args.connect_url:
        mismatches.extend(connector_mismatches(connectors))
    return mismatches


def main() -> int:
    args = parse_args()
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("[ERROR] neo4j driver required. Install with: pip install neo4j", file=sys.stderr)
        return 1

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        print(f"[ERROR] Cannot connect to Neo4j at {args.uri}: {exc}", file=sys.stderr)
        return 1

    deadline = time.monotonic() + args.timeout
    while True:
        with driver.session(database=args.database) as session:
            counts = {name: scalar(session, q) for name, q in COUNT_QUERIES.items()}
            node_breakdown = {r["k"]: r["c"] for r in rows(session, NODE_BREAKDOWN)}
            edge_breakdown = {r["k"]: r["c"] for r in rows(session, EDGE_BREAKDOWN)}
            dup_nodes = rows(session, DUP_NODES)
            dup_edges = rows(session, DUP_EDGES)
        try:
            connectors = connector_states(args.connect_url) if args.connect_url else {}
            mismatches = verify_snapshot(
                args,
                counts,
                edge_breakdown,
                dup_nodes,
                dup_edges,
                connectors,
            )
        except Exception as exc:
            connectors = {}
            mismatches = [f"cannot read Kafka Connect status: {exc}"]
        if not mismatches or time.monotonic() >= deadline:
            break
        time.sleep(2)
    driver.close()

    snapshot = {
        "status": "PASS" if not mismatches else "FAIL",
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "counts": counts,
        "node_breakdown": node_breakdown,
        "edge_breakdown": edge_breakdown,
        "duplicate_node_ids": dup_nodes,
        "duplicate_edge_ids": dup_edges,
        "connectors": connectors,
        "expected": expected_values(args),
        "mismatches": mismatches,
    }

    # --- report ---
    print("=" * 60)
    print("  NEO4J CPG INGESTION VERIFICATION")
    print("=" * 60)
    print(f"Total CPGNode      : {counts['total_nodes']:>10}")
    print(f"Total CPG_EDGE     : {counts['total_edges']:>10}")
    print(f"Distinct node_id   : {counts['distinct_node_ids']:>10}")
    print(f"Distinct edge_id   : {counts['distinct_edge_ids']:>10}")
    print(f"Placeholder nodes  : {counts['placeholder_nodes']:>10}  (edges seen before node event)")
    print("-" * 60)
    print("Node breakdown by node_type:")
    for k, c in node_breakdown.items():
        print(f"    {k:<28} {c:>10}")
    print("Edge breakdown by edge_type:")
    for k, c in edge_breakdown.items():
        print(f"    {k:<28} {c:>10}")
    print("-" * 60)

    # --- idempotency verdict ---
    node_dupes = counts["total_nodes"] - counts["distinct_node_ids"]
    edge_dupes = counts["total_edges"] - counts["distinct_edge_ids"]
    ok = not mismatches
    print(f"Duplicate node_ids : {node_dupes}  (rows>1: {len(dup_nodes)})")
    print(f"Duplicate edge_ids : {edge_dupes}  (rows>1: {len(dup_edges)})")
    print("-" * 60)
    print(f"VERIFICATION       : {'PASS' if ok else 'FAIL'}")
    for mismatch in mismatches:
        print(f"  - {mismatch}")
    print("=" * 60)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        print(f"[snapshot] written to {args.json}")

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
