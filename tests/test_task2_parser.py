from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "task2"))

from cpg_parser import CPGParseResult, parse_python_file  # noqa: E402
from event_contract import file_id_for  # noqa: E402
from parser_service import publish_success  # noqa: E402
from parser_state import ParserStateStore  # noqa: E402


def parse_source(source: str) -> CPGParseResult:
    path = "fixture.py"
    return parse_python_file(
        file_id_for("test/repository", path),
        path,
        source.encode("utf-8"),
    )


def edge_paths(
    result: CPGParseResult,
    edge_type: str,
) -> list[tuple[str, str, dict]]:
    nodes = {node.node_id: node for node in result.nodes}
    return [
        (
            nodes[edge.source_id].structural_path,
            nodes[edge.target_id].structural_path,
            edge.properties,
        )
        for edge in result.edges
        if edge.edge_type == edge_type
    ]


class RecordingProducer:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict]] = []
        self.flushes = 0

    def emit_event(self, category: str, event: dict, *, key: str) -> None:
        self.events.append((category, key, event))

    def flush(self) -> None:
        self.flushes += 1


class CPGParserTests(unittest.TestCase):
    def test_structural_ids_are_unique_and_stable(self) -> None:
        source = """
def f(a, b):
    left = a + b
    right = a + b
    return left + right
"""
        first = parse_source(source)
        second = parse_source(source)
        self.assertIsNone(first.error_event)
        ids = [node.node_id for node in first.nodes]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(
            [(node.structural_path, node.node_id) for node in first.nodes],
            [(node.structural_path, node.node_id) for node in second.nodes],
        )

    def test_edge_ids_are_deduplicated(self) -> None:
        result = parse_source("x = 1\ny = x\n")
        edge_ids = [edge.edge_id for edge in result.edges]
        self.assertEqual(len(edge_ids), len(set(edge_ids)))
        self.assertEqual(
            result.ast_edge_count
            + result.cfg_edge_count
            + result.dfg_edge_count
            + result.call_edge_count,
            len(result.edges),
        )

    def test_dfg_uses_nearest_prior_definition_and_nested_shadow(self) -> None:
        result = parse_source(
            """
x = 1
before = x
x = 2
after = x
def outer(a):
    x = a
    def inner():
        x = 3
        return x
    return x
"""
        )
        dfg = edge_paths(result, "DFG")
        self.assertIn(
            (
                "root.body[0].targets[0]",
                "root.body[1].value",
                {"variable": "x"},
            ),
            dfg,
        )
        self.assertIn(
            (
                "root.body[2].targets[0]",
                "root.body[3].value",
                {"variable": "x"},
            ),
            dfg,
        )
        self.assertIn(
            (
                "root.body[4].body[1].body[0].targets[0]",
                "root.body[4].body[1].body[1].value",
                {"variable": "x"},
            ),
            dfg,
        )
        self.assertIn(
            (
                "root.body[4].body[0].targets[0]",
                "root.body[4].body[2].value",
                {"variable": "x"},
            ),
            dfg,
        )

    def test_dfg_honours_global_and_nonlocal(self) -> None:
        result = parse_source(
            """
x = 0
def set_global():
    global x
    x = 1
    return x
def outer():
    y = 1
    def inner():
        nonlocal y
        y = 2
        return y
    return inner
"""
        )
        dfg = edge_paths(result, "DFG")
        self.assertIn(
            (
                "root.body[1].body[1].targets[0]",
                "root.body[1].body[2].value",
                {"variable": "x"},
            ),
            dfg,
        )
        self.assertIn(
            (
                "root.body[2].body[1].body[1].targets[0]",
                "root.body[2].body[1].body[2].value",
                {"variable": "y"},
            ),
            dfg,
        )

    def test_calls_resolve_internal_function_definition(self) -> None:
        result = parse_source(
            """
def local():
    return 1
value = local()
print(value)
"""
        )
        calls = edge_paths(result, "CALL")
        self.assertIn(
            (
                "root.body[1].value",
                "root.body[0]",
                {"callee": "local", "resolution": "internal"},
            ),
            calls,
        )
        self.assertIn(
            (
                "root.body[2].value",
                "root.body[2].value.func",
                {"callee": "print", "resolution": "unresolved"},
            ),
            calls,
        )


class ParserStateTests(unittest.TestCase):
    def test_replay_emits_deletes_for_stale_ids_and_advances_state(self) -> None:
        first_source = b"x = 1\ny = x\n"
        second_source = b"x = 1\n"
        first = parse_source(first_source.decode())
        second = parse_source(second_source.decode())

        with tempfile.TemporaryDirectory() as directory:
            store = ParserStateStore(Path(directory))
            producer = RecordingProducer()
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

            delete_nodes = {
                event["node_id"]
                for category, _, event in producer.events
                if category == "node" and event["event_type"] == "NODE_DELETE"
            }
            delete_edges = {
                event["edge_id"]
                for category, _, event in producer.events
                if category == "edge" and event["event_type"] == "EDGE_DELETE"
            }
            self.assertEqual(
                delete_nodes,
                {node.node_id for node in first.nodes}
                - {node.node_id for node in second.nodes},
            )
            self.assertEqual(
                delete_edges,
                {edge.edge_id for edge in first.edges}
                - {edge.edge_id for edge in second.edges},
            )
            saved = store.load(second.file_id)
            self.assertEqual(
                set(saved["node_ids"]), {node.node_id for node in second.nodes}
            )


if __name__ == "__main__":
    unittest.main()
