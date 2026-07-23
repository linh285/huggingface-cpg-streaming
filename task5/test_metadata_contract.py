#!/usr/bin/env python3
"""Small dependency-free checks for the Task 5 handoff contract."""

from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT / "task2"))

from cpg_parser import CPGParseResult


REQUIRED_FIELDS = {
    "schema_version",
    "event_time",
    "event_type",
    "file_id",
    "repository",
    "path",
    "language",
    "size_bytes",
    "line_count",
    "content_sha256",
    "ast_node_count",
    "cfg_edge_count",
    "dfg_edge_count",
    "call_edge_count",
    "status",
}


def load_sample(name: str) -> dict:
    with (ROOT / "samples" / name).open(encoding="utf-8") as file:
        return json.load(file)


class MetadataContractTest(unittest.TestCase):
    def test_task2_metadata_event_matches_task5_schema(self) -> None:
        """Keep Task 5 aligned with the metadata object emitted by Task 2."""
        event = CPGParseResult(
            file_id="contract-file-id", relative_path="src/example.py"
        ).to_metadata_event(
            repository="huggingface/datasets",
            size_bytes=0,
            line_count=0,
            content_sha256="a" * 64,
            event_time="2026-07-23T10:00:00+00:00",
        )
        schema = json.loads((ROOT / "metadata_event.schema.json").read_text())

        self.assertEqual(set(schema["required"]), REQUIRED_FIELDS)
        self.assertEqual(set(schema["properties"]), REQUIRED_FIELDS)
        self.assertEqual(set(event), REQUIRED_FIELDS)
        self.assertEqual(event["event_type"], "metadata")
        self.assertEqual(event["file_id"], "contract-file-id")

    def test_samples_follow_task2_contract(self) -> None:
        for name in ("original_metadata.json", "modified_metadata.json"):
            event = load_sample(name)
            self.assertEqual(set(event), REQUIRED_FIELDS)
            self.assertEqual(event["schema_version"], "1.0.0")
            self.assertEqual(event["event_type"], "metadata")
            self.assertEqual(event["language"], "python")
            self.assertEqual(len(event["content_sha256"]), 64)

    def test_modified_event_reuses_file_id_but_changes_content(self) -> None:
        original = load_sample("original_metadata.json")
        modified = load_sample("modified_metadata.json")

        self.assertEqual(original["file_id"], modified["file_id"])
        self.assertEqual(original["path"], modified["path"])
        self.assertNotEqual(
            original["content_sha256"], modified["content_sha256"]
        )
        self.assertGreater(modified["line_count"], original["line_count"])


if __name__ == "__main__":
    unittest.main()
