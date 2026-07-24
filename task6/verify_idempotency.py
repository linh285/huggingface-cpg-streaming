#!/usr/bin/env python3
"""Fail-fast cross-sink duplicate verification for Task 6."""

from __future__ import annotations

import argparse
import hashlib
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--neo4j-uri", default="bolt://localhost:7687")
    parser.add_argument("--neo4j-user", default="neo4j")
    parser.add_argument("--neo4j-password", default="cpgpassword")
    parser.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    parser.add_argument("--mongo-database", default="cpg")
    parser.add_argument("--mongo-collection", default="source_metadata")
    parser.add_argument("--file", required=True)
    parser.add_argument("--repository-name", default="huggingface/datasets")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        from neo4j import GraphDatabase
        from pymongo import MongoClient
    except ImportError as exc:
        print(f"[FAIL] Missing mandatory dependency: {exc}", file=sys.stderr)
        return 2

    relative_path = args.file.replace("\\", "/")
    file_id = hashlib.sha256(
        f"{args.repository_name}:{relative_path}".encode()
    ).hexdigest()
    driver = GraphDatabase.driver(
        args.neo4j_uri,
        auth=(args.neo4j_user, args.neo4j_password),
    )
    mongo = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        driver.verify_connectivity()
        mongo.admin.command("ping")
        with driver.session() as session:
            node_duplicates = session.run(
                "MATCH (n:CPGNode) WITH n.node_id AS id, count(*) AS c "
                "WHERE c > 1 RETURN count(*) AS value"
            ).single()["value"]
            edge_duplicates = session.run(
                "MATCH ()-[r:CPG_EDGE]->() "
                "WITH r.edge_id AS id, count(*) AS c "
                "WHERE c > 1 RETURN count(*) AS value"
            ).single()["value"]
        collection = mongo[args.mongo_database][args.mongo_collection]
        mongo_count = collection.count_documents({"_id": file_id})
        file_id_count = collection.count_documents({"file_id": file_id})
        document = collection.find_one({"_id": file_id})
    except Exception as exc:
        print(f"[FAIL] Mandatory sink check failed: {exc}", file=sys.stderr)
        return 2
    finally:
        driver.close()
        mongo.close()

    ok = (
        node_duplicates == 0
        and edge_duplicates == 0
        and mongo_count == 1
        and file_id_count == 1
        and document is not None
    )
    print(f"file_id: {file_id}")
    print(f"Neo4j duplicate node IDs: {node_duplicates}")
    print(f"Neo4j duplicate edge IDs: {edge_duplicates}")
    print(f"MongoDB documents by _id: {mongo_count}")
    print(f"MongoDB documents by file_id: {file_id_count}")
    if document:
        print(f"MongoDB content_sha256: {document.get('content_sha256')}")
    print(f"VERDICT: {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
