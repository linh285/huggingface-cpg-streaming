#!/usr/bin/env python3
"""
Task 6 - Standalone idempotency / consistency verifier.

Checks that the pipeline's persisted state is duplicate-free across BOTH sinks:

  Neo4j   (Task 4): no node_id or edge_id appears more than once.
  MongoDB (Task 5): the source_metadata collection holds exactly one document
                    per file_id (upsert key), and -- if --file is given -- shows
                    that file's stored content_sha256 so a replay's updated hash
                    can be confirmed.

The MongoDB half is OPTIONAL: if pymongo is missing or the server is
unreachable (e.g. Task 5 not yet running) it is skipped with a clear notice,
and the Neo4j half still runs. Exit code is non-zero only on a real duplicate.

Usage:
    python verify_idempotency.py
    python verify_idempotency.py --file src/datasets/load.py
"""
from __future__ import annotations

import argparse
import hashlib
import sys


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Verify pipeline idempotency in Neo4j and MongoDB.")
    # Neo4j
    p.add_argument("--uri", default="bolt://localhost:7687")
    p.add_argument("--user", default="neo4j")
    p.add_argument("--password", default="cpgpassword")
    p.add_argument("--database", default="neo4j")
    # MongoDB (Task 5)
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    p.add_argument("--mongo-db", default="cpg")
    p.add_argument("--mongo-collection", default="source_metadata")
    # optional single-file focus
    p.add_argument("--file", default=None, help="Repo-relative path to inspect for that file's metadata.")
    p.add_argument("--repository-name", default="huggingface/datasets")
    return p.parse_args()


def verify_neo4j(args) -> bool:
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("[neo4j] ERROR: neo4j driver required: pip install neo4j", file=sys.stderr)
        return False

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        print(f"[neo4j] ERROR: cannot connect at {args.uri}: {exc}", file=sys.stderr)
        return False

    with driver.session(database=args.database) as s:
        total_nodes = s.run("MATCH (n:CPGNode) RETURN count(n) AS v").single()["v"]
        total_edges = s.run("MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS v").single()["v"]
        dist_nodes = s.run("MATCH (n:CPGNode) RETURN count(DISTINCT n.node_id) AS v").single()["v"]
        dist_edges = s.run("MATCH ()-[r:CPG_EDGE]->() RETURN count(DISTINCT r.edge_id) AS v").single()["v"]
        dup_nodes = [dict(r) for r in s.run(
            "MATCH (n:CPGNode) WITH n.node_id AS id, count(*) AS c WHERE c>1 RETURN id,c LIMIT 5")]
        dup_edges = [dict(r) for r in s.run(
            "MATCH ()-[r:CPG_EDGE]->() WITH r.edge_id AS id, count(*) AS c WHERE c>1 RETURN id,c LIMIT 5")]
    driver.close()

    print("--- Neo4j (Task 4) ---")
    print(f"  nodes total={total_nodes} distinct={dist_nodes} duplicates={total_nodes - dist_nodes}")
    print(f"  edges total={total_edges} distinct={dist_edges} duplicates={total_edges - dist_edges}")
    ok = (total_nodes == dist_nodes and total_edges == dist_edges and not dup_nodes and not dup_edges)
    print(f"  Neo4j idempotency: {'PASS' if ok else 'FAIL'}")
    if dup_nodes:
        print(f"  offending node_ids: {dup_nodes}")
    if dup_edges:
        print(f"  offending edge_ids: {dup_edges}")
    return ok


def verify_mongodb(args) -> bool | None:
    """Returns True/False if checked, or None if skipped (Task 5 not available)."""
    try:
        from pymongo import MongoClient
    except ImportError:
        print("--- MongoDB (Task 5) --- SKIPPED (pymongo not installed)")
        return None

    try:
        client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
    except Exception as exc:
        print(f"--- MongoDB (Task 5) --- SKIPPED (not reachable at {args.mongo_uri}: {exc})")
        return None

    coll = client[args.mongo_db][args.mongo_collection]
    total_docs = coll.count_documents({})
    # Duplicate file_id detection via aggregation.
    dupes = list(coll.aggregate([
        {"$group": {"_id": "$file_id", "c": {"$sum": 1}}},
        {"$match": {"c": {"$gt": 1}}},
        {"$limit": 5},
    ]))
    distinct_files = len(coll.distinct("file_id"))

    print("--- MongoDB (Task 5) ---")
    print(f"  documents={total_docs} distinct file_id={distinct_files} duplicate file_id groups={len(dupes)}")
    ok = (len(dupes) == 0)

    if args.file:
        fid = sha256_str(f"{args.repository_name}:{args.file.replace(chr(92), '/')}")
        # Task 5 stores the file_id as the document _id (and also keeps a file_id
        # field), so accept either as the lookup key.
        selector = {"$or": [{"_id": fid}, {"file_id": fid}]}
        doc = coll.find_one(selector)
        if doc:
            print(f"  [file] {args.file}")
            print(f"         file_id        = {fid}")
            print(f"         content_sha256 = {doc.get('content_sha256')}")
            print(f"         line_count     = {doc.get('line_count')}  size={doc.get('size_bytes')}")
            same_fid_docs = coll.count_documents(selector)
            print(f"         docs with this file_id = {same_fid_docs}  (expect 1 -> upsert worked)")
            ok = ok and (same_fid_docs == 1)
        else:
            print(f"  [file] {args.file}: no metadata document found for file_id={fid}")
    client.close()
    print(f"  MongoDB idempotency: {'PASS' if ok else 'FAIL'}")
    return ok


def main() -> int:
    args = parse_args()
    print("=" * 60)
    print("  TASK 6 - PIPELINE IDEMPOTENCY VERIFICATION")
    print("=" * 60)

    neo_ok = verify_neo4j(args)
    mongo_ok = verify_mongodb(args)

    print("-" * 60)
    print(f"Neo4j   : {'PASS' if neo_ok else 'FAIL'}")
    if mongo_ok is None:
        print("MongoDB : SKIPPED (Task 5 sink not available)")
    else:
        print(f"MongoDB : {'PASS' if mongo_ok else 'FAIL'}")
    print("=" * 60)

    # Fail only on a genuine duplicate; a skipped Mongo check is not a failure.
    failed = (not neo_ok) or (mongo_ok is False)
    return 2 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
