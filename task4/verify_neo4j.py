#!/usr/bin/env python3
"""
Task 4 - Neo4j ingestion verification.

Connects to Neo4j and reports the ingested graph topology:
  * total CPGNode / CPG_EDGE counts
  * breakdown by node_type and edge_type (AST / CFG / DFG / CALLS)
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
    return p.parse_args()


def scalar(session, query: str) -> int:
    return session.run(query).single()["value"]


def rows(session, query: str) -> list[dict]:
    return [dict(r) for r in session.run(query)]


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

    with driver.session(database=args.database) as session:
        counts = {name: scalar(session, q) for name, q in COUNT_QUERIES.items()}
        node_breakdown = {r["k"]: r["c"] for r in rows(session, NODE_BREAKDOWN)}
        edge_breakdown = {r["k"]: r["c"] for r in rows(session, EDGE_BREAKDOWN)}
        dup_nodes = rows(session, DUP_NODES)
        dup_edges = rows(session, DUP_EDGES)
    driver.close()

    snapshot = {
        "counts": counts,
        "node_breakdown": node_breakdown,
        "edge_breakdown": edge_breakdown,
        "duplicate_node_ids": dup_nodes,
        "duplicate_edge_ids": dup_edges,
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
    ok = (node_dupes == 0 and edge_dupes == 0 and not dup_nodes and not dup_edges)
    print(f"Duplicate node_ids : {node_dupes}  (rows>1: {len(dup_nodes)})")
    print(f"Duplicate edge_ids : {edge_dupes}  (rows>1: {len(dup_edges)})")
    print("-" * 60)
    print(f"IDEMPOTENCY CHECK  : {'PASS -- no duplicates' if ok else 'FAIL -- duplicates found!'}")
    print("=" * 60)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        print(f"[snapshot] written to {args.json}")

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
