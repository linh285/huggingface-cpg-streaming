#!/usr/bin/env python3
"""Contract checks shared by Task 2, Task 3, and Task 5."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent
sys.path.insert(0, str(PROJECT_ROOT / "task2"))

from cpg_parser import parse_python_file  # noqa: E402
from event_contract import file_id_for  # noqa: E402
from parser_service import publish_success  # noqa: E402
from parser_state import ParserStateStore  # noqa: E402


class RecordingProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def emit_event(self, category: str, event: dict, *, key: str) -> None:
        self.events.append((category, event))

    def flush(self) -> None:
        return


class MetadataContractTest(unittest.TestCase):
    def setUp(self) -> None:
        schema = json.loads(
            (ROOT / "metadata_event.schema.json").read_text(encoding="utf-8")
        )
        self.validator = Draft202012Validator(
            schema, format_checker=FormatChecker()
        )

    def assert_valid(self, event: dict) -> None:
        errors = list(self.validator.iter_errors(event))
        self.assertEqual(
            errors,
            [],
            "\n".join(error.message for error in errors),
        )

    def test_task2_metadata_event_matches_task5_schema(self) -> None:
        source = b"def answer():\n    return 42\n"
        relative_path = "src/example.py"
        result = parse_python_file(
            file_id_for("huggingface/datasets", relative_path),
            relative_path,
            source,
        )
        producer = RecordingProducer()
        with tempfile.TemporaryDirectory() as directory:
            publish_success(
                result=result,
                code_bytes=source,
                repository_name="huggingface/datasets",
                producer=producer,
                state_store=ParserStateStore(Path(directory)),
            )
        metadata = [
            event for category, event in producer.events if category == "metadata"
        ]
        self.assertEqual(len(metadata), 1)
        self.assert_valid(metadata[0])

    def test_samples_follow_the_same_contract(self) -> None:
        samples = []
        for name in ("original_metadata.json", "modified_metadata.json"):
            event = json.loads(
                (ROOT / "samples" / name).read_text(encoding="utf-8")
            )
            self.assert_valid(event)
            samples.append(event)
        original, modified = samples
        self.assertEqual(original["file_id"], modified["file_id"])
        self.assertEqual(original["path"], modified["path"])
        self.assertNotEqual(
            original["content_sha256"], modified["content_sha256"]
        )

    def test_task3_and_task5_metadata_schemas_have_same_payload_contract(self) -> None:
        task3 = json.loads(
            (
                PROJECT_ROOT
                / "task3"
                / "schemas"
                / "metadata-event.schema.json"
            ).read_text(encoding="utf-8")
        )
        task5 = json.loads(
            (ROOT / "metadata_event.schema.json").read_text(encoding="utf-8")
        )
        for schema in (task3, task5):
            schema.pop("$id", None)
        self.assertEqual(task3, task5)


if __name__ == "__main__":
    unittest.main()
