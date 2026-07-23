#!/usr/bin/env python3
"""
Task 4 - JSONL -> Kafka publisher bridge.

The Parser Service (Task 2) can either publish directly to Kafka
(`--kafka-bootstrap`) or, in dry-run mode, dump events to JSONL files under
`artifacts/task2/`. This bridge streams those JSONL dumps into the Kafka
topics so the Neo4j sink connector can ingest them.

Publishing is idempotent by construction: every record keeps its deterministic
key (node_id / edge_id / file_id) and the downstream Cypher MERGE dedupes, so
re-running this script never creates duplicates in Neo4j.

Usage:
    python publish_jsonl_to_kafka.py \
        --bootstrap localhost:9092 \
        --input-dir ../artifacts/task2 \
        --topics nodes,edges          # subset; default: nodes,edges,metadata,errors
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Maps the JSONL file (event category) to its Kafka topic and key field.
STREAMS = {
    "nodes":    {"file": "nodes.jsonl",    "topic": "cpg.nodes",    "key": "node_id"},
    "edges":    {"file": "edges.jsonl",    "topic": "cpg.edges",    "key": "edge_id"},
    "metadata": {"file": "metadata.jsonl", "topic": "cpg.metadata", "key": "file_id"},
    "errors":   {"file": "errors.jsonl",   "topic": "cpg.errors",   "key": "error_id"},
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Publish Task 2 JSONL dumps into Kafka topics.")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server.")
    p.add_argument(
        "--input-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "artifacts" / "task2",
        help="Directory holding the Task 2 *.jsonl dumps.",
    )
    p.add_argument(
        "--topics",
        default="nodes,edges,metadata,errors",
        help="Comma-separated subset of streams to publish (nodes,edges,metadata,errors).",
    )
    p.add_argument("--batch-report", type=int, default=5000, help="Progress print interval.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("[ERROR] kafka-python is required. Install with: pip install kafka-python", file=sys.stderr)
        return 1

    selected = [s.strip() for s in args.topics.split(",") if s.strip()]
    unknown = [s for s in selected if s not in STREAMS]
    if unknown:
        print(f"[ERROR] Unknown stream(s): {unknown}. Valid: {list(STREAMS)}", file=sys.stderr)
        return 1

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",          # wait for the broker to persist -> at-least-once
        linger_ms=20,
        retries=5,
    )

    grand_total = 0
    for stream in selected:
        cfg = STREAMS[stream]
        path = args.input_dir / cfg["file"]
        if not path.exists():
            print(f"[WARN] {path} not found, skipping '{stream}'.")
            continue

        topic, key_field = cfg["topic"], cfg["key"]
        count = 0
        print(f"[publish] {path.name} -> topic '{topic}' (key={key_field})")
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                event = json.loads(line)
                producer.send(topic, value=event, key=event.get(key_field))
                count += 1
                if count % args.batch_report == 0:
                    print(f"    ... {count} records queued")
        producer.flush()
        print(f"[publish] '{stream}': {count} records published to '{topic}'.")
        grand_total += count

    producer.close()
    print(f"\n[publish] DONE. Total records published: {grand_total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
