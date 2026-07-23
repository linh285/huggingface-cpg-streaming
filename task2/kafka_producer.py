"""
Task 2 - Kafka Producer & Event Sink Wrapper.

Handles sending CPG event JSON payloads to Kafka topics or dumping to JSONL files (dry-run mode).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_TOPICS = {
    "node": "cpg.nodes",
    "edge": "cpg.edges",
    "metadata": "cpg.metadata",
    "error": "cpg.errors",
}


class KafkaEventProducer:
    """Producer for Kafka or JSONL file fallback (dry-run)."""

    def __init__(
        self,
        bootstrap_servers: str | None = None,
        dry_run: bool = True,
        output_dir: Path | None = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.dry_run = dry_run
        self.output_dir = output_dir or Path("artifacts/task2")
        self.kafka_producer = None

        if self.output_dir:
            self.output_dir.mkdir(parents=True, exist_ok=True)

        self._files: dict[str, Any] = {}

        if not self.dry_run and self.bootstrap_servers:
            try:
                from kafka import KafkaProducer
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                )
                print(f"[INFO] Initialized KafkaProducer connecting to {self.bootstrap_servers}")
            except ImportError:
                print("[WARNING] kafka-python not installed. Falling back to dry-run mode.")
                self.dry_run = True
            except Exception as err:
                print(f"[WARNING] Failed to connect to Kafka ({err}). Falling back to dry-run mode.")
                self.dry_run = True

        if self.dry_run:
            self._files = {
                "node": (self.output_dir / "nodes.jsonl").open("w", encoding="utf-8", newline="\n"),
                "edge": (self.output_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n"),
                "metadata": (self.output_dir / "metadata.jsonl").open("w", encoding="utf-8", newline="\n"),
                "error": (self.output_dir / "errors.jsonl").open("w", encoding="utf-8", newline="\n"),
            }

    def emit_event(self, event_type: str, event_data: dict[str, Any], key: str | None = None) -> None:
        """Publish event to Kafka or dry-run JSONL file."""
        topic_name = DEFAULT_TOPICS.get(event_type, f"cpg.{event_type}s")

        if self.kafka_producer and not self.dry_run:
            event_key = key or event_data.get("node_id") or event_data.get("edge_id") or event_data.get("file_id")
            self.kafka_producer.send(topic_name, value=event_data, key=event_key)
        else:
            file_handle = self._files.get(event_type)
            if file_handle:
                file_handle.write(json.dumps(event_data, ensure_ascii=False, sort_keys=True) + "\n")

    def flush(self) -> None:
        """Flush Kafka producer buffers."""
        if self.kafka_producer:
            self.kafka_producer.flush()

    def close(self) -> None:
        """Close Kafka producer or JSONL files."""
        if self.kafka_producer:
            self.kafka_producer.close()
        for f in self._files.values():
            if not f.closed:
                f.close()
