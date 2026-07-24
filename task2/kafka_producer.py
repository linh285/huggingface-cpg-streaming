"""Kafka producer and explicit dry-run JSONL sink."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from event_contract import TOPICS


class KafkaEventProducer:
    """Publish events to Kafka, or to JSONL only when ``dry_run`` is true."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | None,
        dry_run: bool,
        output_dir: Path,
    ) -> None:
        self.dry_run = dry_run
        self.output_dir = output_dir
        self.kafka_producer = None
        self._files: dict[str, Any] = {}
        self._pending_futures: list[Any] = []

        if dry_run:
            output_dir.mkdir(parents=True, exist_ok=True)
            self._files = {
                "node": (output_dir / "nodes.jsonl").open("w", encoding="utf-8", newline="\n"),
                "edge": (output_dir / "edges.jsonl").open("w", encoding="utf-8", newline="\n"),
                "metadata": (output_dir / "metadata.jsonl").open("w", encoding="utf-8", newline="\n"),
                "error": (output_dir / "errors.jsonl").open("w", encoding="utf-8", newline="\n"),
            }
            return

        if not bootstrap_servers:
            raise RuntimeError("Kafka mode requires --kafka-bootstrap")

        try:
            from kafka import KafkaProducer
        except ImportError as exc:
            raise RuntimeError(
                "Kafka mode requires kafka-python; install it with: python -m pip install kafka-python"
            ) from exc

        try:
            self.kafka_producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                retries=5,
                max_block_ms=15_000,
                request_timeout_ms=15_000,
            )
        except Exception as exc:
            raise RuntimeError(
                f"Cannot connect Kafka producer to {bootstrap_servers}: {exc}"
            ) from exc

        print(f"[INFO] Kafka producer connected to {bootstrap_servers}")

    def emit_event(self, category: str, event: dict, *, key: str) -> None:
        if category not in TOPICS:
            raise RuntimeError(f"Unknown event category: {category}")
        if not key:
            raise RuntimeError(f"Missing Kafka key for {category} event")

        if self.dry_run:
            self._files[category].write(
                json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n"
            )
            return

        try:
            value_bytes = json.dumps(
                event, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
            future = self.kafka_producer.send(
                TOPICS[category],
                value=value_bytes,
                key=key.encode("utf-8"),
            )
        except Exception as exc:
            raise RuntimeError(f"Kafka send failed for {TOPICS[category]}: {exc}") from exc
        self._pending_futures.append(future)

    def flush(self) -> None:
        if self.dry_run:
            for handle in self._files.values():
                handle.flush()
            return

        try:
            self.kafka_producer.flush(timeout=30)
            for future in self._pending_futures:
                future.get(timeout=30)
        except Exception as exc:
            raise RuntimeError(f"Kafka delivery failed: {exc}") from exc
        finally:
            self._pending_futures.clear()

    def close(self) -> None:
        if self.kafka_producer is not None:
            self.kafka_producer.close(timeout=30)
        for handle in self._files.values():
            if not handle.closed:
                handle.close()
