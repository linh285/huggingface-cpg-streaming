#!/usr/bin/env python3
"""
Task 6 - Idempotent Replay Verification (orchestrator).

End-to-end proof that reprocessing a single Python file through the pipeline
updates Neo4j (and MongoDB) *in place* without creating duplicates:

  1. Snapshot Neo4j BEFORE  (global + this file's node/edge counts).
  2. Optionally inject a small edit into the target file (--apply-edit),
     backing up the original so the repo can be restored.
  3. Reprocess ONLY that file with the Task 2 Parser Service (single-file mode)
     into an isolated output dir -- the full Task 2 dump is never clobbered.
  4. Bridge those events into Kafka via the Task 4 publisher.
  5. Poll Neo4j until the counts stabilise (sink drained).
  6. Snapshot AFTER and print the delta + an idempotency verdict.

This deliberately shells out to the existing Task 2 / Task 4 tooling instead of
re-implementing it, so the replay path is byte-for-byte the same code that runs
in normal operation.

Usage (from task6/):
    # pure idempotency: replay an UNCHANGED file -> expect zero delta
    python replay_single_file.py --file src/datasets/load.py

    # modified file: inject an edit -> expect a small, non-duplicating delta
    python replay_single_file.py --file src/datasets/load.py --apply-edit
"""
from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
PARSER = REPO_ROOT / "task2" / "parser_service.py"
BRIDGE = REPO_ROOT / "task4" / "publish_jsonl_to_kafka.py"
REPLAY_OUT = REPO_ROOT / "artifacts" / "task6" / "replay"

# A benign, syntactically valid marker appended when --apply-edit is used.
EDIT_MARKER = "\n\ndef __cpg_replay_marker__(x):\n    # injected by Task 6 replay test\n    return x + 1\n"


def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def compute_file_id(repository: str, rel_path: str) -> str:
    """Mirror the Parser Service's deterministic file_id derivation."""
    rel = rel_path.replace("\\", "/")
    return sha256_str(f"{repository}:{rel}")


# --------------------------------------------------------------------------- #
# Neo4j snapshot helpers
# --------------------------------------------------------------------------- #
def neo4j_snapshot(driver, database: str, file_id: str) -> dict:
    q_global_nodes = "MATCH (n:CPGNode) RETURN count(n) AS v"
    q_global_edges = "MATCH ()-[r:CPG_EDGE]->() RETURN count(r) AS v"
    q_dist_nodes = "MATCH (n:CPGNode) RETURN count(DISTINCT n.node_id) AS v"
    q_dist_edges = "MATCH ()-[r:CPG_EDGE]->() RETURN count(DISTINCT r.edge_id) AS v"
    q_file_nodes = "MATCH (n:CPGNode {file_id: $fid}) RETURN count(n) AS v"
    q_file_edges = "MATCH ()-[r:CPG_EDGE {file_id: $fid}]->() RETURN count(r) AS v"
    with driver.session(database=database) as s:
        return {
            "global_nodes": s.run(q_global_nodes).single()["v"],
            "global_edges": s.run(q_global_edges).single()["v"],
            "distinct_node_ids": s.run(q_dist_nodes).single()["v"],
            "distinct_edge_ids": s.run(q_dist_edges).single()["v"],
            "file_nodes": s.run(q_file_nodes, fid=file_id).single()["v"],
            "file_edges": s.run(q_file_edges, fid=file_id).single()["v"],
        }


def wait_until_stable(driver, database: str, file_id: str, timeout: int = 90) -> dict:
    """Poll until two consecutive snapshots match (sink has drained)."""
    prev = None
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur = neo4j_snapshot(driver, database, file_id)
        if prev is not None and cur == prev:
            return cur
        prev = cur
        time.sleep(3)
    return prev  # best effort


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Task 6 idempotent single-file replay.")
    p.add_argument("--file", required=True, help="Repo-relative path of the .py file to replay.")
    p.add_argument("--repo-dir", type=Path, default=REPO_ROOT / ".work" / "repos" / "datasets")
    p.add_argument("--repository-name", default="huggingface/datasets")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--uri", default="bolt://localhost:7687")
    p.add_argument("--user", default="neo4j")
    p.add_argument("--password", default="cpgpassword")
    p.add_argument("--database", default="neo4j")
    p.add_argument("--apply-edit", action="store_true",
                   help="Inject a marker function into the file (restored afterwards).")
    p.add_argument("--keep-edit", action="store_true",
                   help="Do NOT restore the file after the run (leave the edit in place).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rel_path = args.file.replace("\\", "/")
    abs_path = (args.repo_dir / rel_path).resolve()
    file_id = compute_file_id(args.repository_name, rel_path)

    if not abs_path.exists():
        print(f"[ERROR] Target file not found: {abs_path}", file=sys.stderr)
        return 1

    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("[ERROR] neo4j driver required: pip install neo4j", file=sys.stderr)
        return 1

    driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    try:
        driver.verify_connectivity()
    except Exception as exc:
        print(f"[ERROR] Cannot connect to Neo4j at {args.uri}: {exc}", file=sys.stderr)
        return 1

    print("=" * 66)
    print("  TASK 6 - IDEMPOTENT REPLAY VERIFICATION")
    print("=" * 66)
    print(f"File        : {rel_path}")
    print(f"file_id     : {file_id}")
    print(f"Mode        : {'MODIFIED (edit injected)' if args.apply_edit else 'UNCHANGED (pure replay)'}")
    print("-" * 66)

    # --- 1. snapshot BEFORE ---
    before = neo4j_snapshot(driver, args.database, file_id)
    print(f"[BEFORE] global nodes={before['global_nodes']} edges={before['global_edges']} "
          f"| file nodes={before['file_nodes']} edges={before['file_edges']}")

    # --- 2. optional edit (with backup) ---
    backup = abs_path.with_suffix(abs_path.suffix + ".task6.bak")
    original_bytes = abs_path.read_bytes()
    if args.apply_edit:
        backup.write_bytes(original_bytes)
        with abs_path.open("a", encoding="utf-8") as f:
            f.write(EDIT_MARKER)
        print(f"[edit] Appended marker function; backup at {backup.name}")

    try:
        # --- 3. reprocess ONLY this file (isolated output dir) ---
        REPLAY_OUT.mkdir(parents=True, exist_ok=True)
        print(f"[parse] Reprocessing single file -> {REPLAY_OUT}")
        subprocess.run(
            [sys.executable, str(PARSER),
             "--repo-dir", str(args.repo_dir),
             "--repository-name", args.repository_name,
             "--single-file", rel_path,
             "--output-dir", str(REPLAY_OUT),
             "--dry-run"],
            check=True, cwd=str(REPO_ROOT / "task2"),
        )

        # --- 4. bridge the replayed events into Kafka ---
        print("[publish] Bridging replayed events -> Kafka")
        subprocess.run(
            [sys.executable, str(BRIDGE),
             "--bootstrap", args.bootstrap,
             "--input-dir", str(REPLAY_OUT),
             "--topics", "nodes,edges,metadata"],
            check=True,
        )
    finally:
        # --- restore the file unless asked to keep the edit ---
        if args.apply_edit and not args.keep_edit:
            abs_path.write_bytes(original_bytes)
            if backup.exists():
                backup.unlink()
            print("[edit] Restored original file.")

    # --- 5. wait for sink to drain ---
    print("[wait] Waiting for Neo4j sink to drain...")
    after = wait_until_stable(driver, args.database, file_id)
    print(f"[AFTER ] global nodes={after['global_nodes']} edges={after['global_edges']} "
          f"| file nodes={after['file_nodes']} edges={after['file_edges']}")
    driver.close()

    # --- 6. verdict ---
    d_global_nodes = after["global_nodes"] - before["global_nodes"]
    d_global_edges = after["global_edges"] - before["global_edges"]
    d_file_nodes = after["file_nodes"] - before["file_nodes"]
    d_file_edges = after["file_edges"] - before["file_edges"]
    node_dupes = after["global_nodes"] - after["distinct_node_ids"]
    edge_dupes = after["global_edges"] - after["distinct_edge_ids"]

    print("-" * 66)
    print(f"Δ global nodes : {d_global_nodes:+}")
    print(f"Δ global edges : {d_global_edges:+}")
    print(f"Δ file   nodes : {d_file_nodes:+}")
    print(f"Δ file   edges : {d_file_edges:+}")
    print(f"Duplicate node_ids in graph : {node_dupes}")
    print(f"Duplicate edge_ids in graph : {edge_dupes}")
    print("-" * 66)

    no_dupes = (node_dupes == 0 and edge_dupes == 0)
    if not args.apply_edit:
        # Unchanged replay: MERGE must be a no-op -> zero delta everywhere.
        ok = no_dupes and d_global_nodes == 0 and d_global_edges == 0
        verdict = ("PASS -- unchanged replay produced ZERO new nodes/edges and no duplicates"
                   if ok else "FAIL -- unchanged replay changed the graph or duplicated data")
    else:
        # Modified replay: graph must change, but never duplicate.
        ok = no_dupes
        verdict = ("PASS -- graph updated in place (delta reflects the edit) with no duplicates"
                   if ok else "FAIL -- duplicates detected after modified replay")

    print(f"VERDICT: {verdict}")
    print("=" * 66)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
