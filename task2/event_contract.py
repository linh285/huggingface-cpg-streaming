"""Shared Kafka event contract for Tasks 2 through 5."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone


SCHEMA_VERSION = "1.0.0"

TOPICS = {
    "node": "cpg.nodes",
    "edge": "cpg.edges",
    "metadata": "cpg.metadata",
    "error": "cpg.errors",
}

NODE_UPSERT = "NODE_UPSERT"
NODE_DELETE = "NODE_DELETE"
EDGE_UPSERT = "EDGE_UPSERT"
EDGE_DELETE = "EDGE_DELETE"
FILE_METADATA_UPSERT = "FILE_METADATA_UPSERT"
PARSER_ERROR = "PARSER_ERROR"

EDGE_TYPES = {"AST", "CFG", "DFG", "CALL"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_path(path: str) -> str:
    return path.replace("\\", "/")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def file_id_for(repository: str, path: str) -> str:
    return sha256_text(f"{repository}:{normalize_path(path)}")


def common_fields(
    *,
    event_type: str,
    event_time: str,
    repository: str,
    file_id: str,
    path: str,
    content_sha256: str,
) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": event_type,
        "event_time": event_time,
        "repository": repository,
        "file_id": file_id,
        "path": normalize_path(path),
        "content_sha256": content_sha256,
    }
