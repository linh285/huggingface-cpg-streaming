from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "task2"))

from cpg_parser import parse_python_file  # noqa: E402
from event_contract import file_id_for  # noqa: E402
from parser_service import publish_failure, publish_success  # noqa: E402
from parser_state import ParserStateStore  # noqa: E402


SCHEMAS = {
    "node": "node-event.schema.json",
    "edge": "edge-event.schema.json",
    "metadata": "metadata-event.schema.json",
    "error": "error-event.schema.json",
}


class RecordingProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    def emit_event(self, category: str, event: dict, *, key: str) -> None:
        self.events.append((category, event))

    def flush(self) -> None:
        return


class EventContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.validators = {
            category: Draft202012Validator(
                json.loads(
                    (ROOT / "task3" / "schemas" / filename).read_text(
                        encoding="utf-8"
                    )
                ),
                format_checker=FormatChecker(),
            )
            for category, filename in SCHEMAS.items()
        }

    def assert_valid(self, category: str, event: dict) -> None:
        errors = sorted(
            self.validators[category].iter_errors(event),
            key=lambda error: list(error.path),
        )
        self.assertEqual(
            errors,
            [],
            "\n".join(
                f"{'.'.join(map(str, error.path))}: {error.message}"
                for error in errors
            ),
        )

    def test_real_task2_success_events_match_task3_schemas(self) -> None:
        source = b"def add(a, b):\n    return a + b\n\nvalue = add(1, 2)\n"
        relative_path = "src/fixture.py"
        identifier = file_id_for("test/repository", relative_path)
        result = parse_python_file(identifier, relative_path, source)
        producer = RecordingProducer()
        with tempfile.TemporaryDirectory() as directory:
            publish_success(
                result=result,
                code_bytes=source,
                repository_name="test/repository",
                producer=producer,
                state_store=ParserStateStore(Path(directory)),
            )
        self.assertGreater(len(producer.events), 1)
        for category, event in producer.events:
            self.assert_valid(category, event)

    def test_delete_and_error_events_match_task3_schemas(self) -> None:
        relative_path = "src/fixture.py"
        identifier = file_id_for("test/repository", relative_path)
        first_source = b"x = 1\ny = x\n"
        second_source = b"x = 1\n"
        first = parse_python_file(identifier, relative_path, first_source)
        second = parse_python_file(identifier, relative_path, second_source)
        producer = RecordingProducer()
        with tempfile.TemporaryDirectory() as directory:
            store = ParserStateStore(Path(directory))
            publish_success(
                result=first,
                code_bytes=first_source,
                repository_name="test/repository",
                producer=producer,
                state_store=store,
            )
            producer.events.clear()
            publish_success(
                result=second,
                code_bytes=second_source,
                repository_name="test/repository",
                producer=producer,
                state_store=store,
            )
        for category, event in producer.events:
            self.assert_valid(category, event)

        producer.events.clear()
        publish_failure(
            file_id=identifier,
            relative_path=relative_path,
            repository_name="test/repository",
            code_bytes=b"def broken(:\n",
            error={
                "error_type": "SyntaxError",
                "error_message": "invalid syntax",
                "lineno": 1,
                "col_offset": 12,
            },
            producer=producer,
        )
        for category, event in producer.events:
            self.assert_valid(category, event)


if __name__ == "__main__":
    unittest.main()
