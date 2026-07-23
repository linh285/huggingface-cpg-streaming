"""
Task 2 - CPG Parser Engine for Python Source Code.

This module provides AST, CFG, DFG, and Call graph extraction from Python source code
using Python's built-in `ast` module. Emits deterministic IDs (SHA-256) for idempotency.
"""

from __future__ import annotations

import ast
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "1.0.0"


def sha256_text(text: str) -> str:
    """Returns SHA-256 hash string of text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class CPGNode:
    node_id: str
    file_id: str
    node_type: str
    label: str
    lineno: int
    col_offset: int
    end_lineno: int
    end_col_offset: int
    code: str
    properties: dict[str, Any] = field(default_factory=dict)

    def to_event(self, event_time: str) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "event_time": event_time,
            "event_type": "node",
            "node_id": self.node_id,
            "file_id": self.file_id,
            "node_type": self.node_type,
            "label": self.label,
            "lineno": self.lineno,
            "col_offset": self.col_offset,
            "end_lineno": self.end_lineno,
            "end_col_offset": self.end_col_offset,
            "code": self.code,
            "properties": self.properties,
        }


@dataclass
class CPGEdge:
    edge_id: str
    file_id: str
    source_id: str
    target_id: str
    edge_type: str  # "AST", "CFG", "DFG", "CALLS"
    properties: dict[str, Any] = field(default_factory=dict)

    def to_event(self, event_time: str) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "event_time": event_time,
            "event_type": "edge",
            "edge_id": self.edge_id,
            "file_id": self.file_id,
            "source_id": self.source_id,
            "target_id": self.target_id,
            "edge_type": self.edge_type,
            "properties": self.properties,
        }


@dataclass
class CPGParseResult:
    file_id: str
    relative_path: str
    nodes: list[CPGNode] = field(default_factory=list)
    edges: list[CPGEdge] = field(default_factory=list)
    error_event: dict[str, Any] | None = None
    ast_node_count: int = 0
    cfg_edge_count: int = 0
    dfg_edge_count: int = 0
    call_edge_count: int = 0

    def to_metadata_event(
        self,
        repository: str,
        size_bytes: int,
        line_count: int,
        content_sha256: str,
        event_time: str,
    ) -> dict[str, Any]:
        status = "FAILED" if self.error_event else "PARSED"
        return {
            "schema_version": SCHEMA_VERSION,
            "event_time": event_time,
            "event_type": "metadata",
            "file_id": self.file_id,
            "repository": repository,
            "path": self.relative_path,
            "language": "python",
            "size_bytes": size_bytes,
            "line_count": line_count,
            "content_sha256": content_sha256,
            "ast_node_count": self.ast_node_count,
            "cfg_edge_count": self.cfg_edge_count,
            "dfg_edge_count": self.dfg_edge_count,
            "call_edge_count": self.call_edge_count,
            "status": status,
        }


class CPGParser:
    """CPG Extractor for a single Python file."""

    def __init__(self, file_id: str, relative_path: str, code_bytes: bytes):
        self.file_id = file_id
        self.relative_path = relative_path
        self.code_bytes = code_bytes
        self.code_str = ""
        self.nodes: list[CPGNode] = []
        self.edges: list[CPGEdge] = []
        self._node_map: dict[ast.AST, CPGNode] = {}

    def parse(self) -> CPGParseResult:
        result = CPGParseResult(
            file_id=self.file_id,
            relative_path=self.relative_path,
        )

        try:
            self.code_str = self.code_bytes.decode("utf-8", errors="replace")
            tree = ast.parse(self.code_str, filename=self.relative_path)
        except Exception as exc:
            event_time = datetime.now(timezone.utc).isoformat()
            error_id = sha256_text(
                f"{self.file_id}:{type(exc).__name__}:{str(exc)}"
            )
            lineno = getattr(exc, "lineno", 1) or 1
            result.error_event = {
                "schema_version": SCHEMA_VERSION,
                "event_time": event_time,
                "event_type": "error",
                "error_id": error_id,
                "file_id": self.file_id,
                "path": self.relative_path,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "lineno": lineno,
            }
            return result

        # 1. Build AST Nodes & AST Edges
        self._build_ast(tree)

        # 2. Build CFG Edges
        self._build_cfg(tree)

        # 3. Build DFG Edges
        self._build_dfg(tree)

        # 4. Build Call Edges
        self._build_calls(tree)

        result.nodes = self.nodes
        result.edges = self.edges
        result.ast_node_count = len(self.nodes)
        result.cfg_edge_count = sum(1 for e in self.edges if e.edge_type == "CFG")
        result.dfg_edge_count = sum(1 for e in self.edges if e.edge_type == "DFG")
        result.call_edge_count = sum(1 for e in self.edges if e.edge_type == "CALLS")

        return result

    def _generate_node_id(self, node: ast.AST, node_type: str, label: str) -> str:
        lineno = getattr(node, "lineno", 0)
        col_offset = getattr(node, "col_offset", 0)
        end_lineno = getattr(node, "end_lineno", lineno)
        end_col_offset = getattr(node, "end_col_offset", col_offset)
        id_str = f"{self.file_id}:{node_type}:{lineno}:{col_offset}:{end_lineno}:{end_col_offset}:{label}"
        return sha256_text(id_str)

    def _generate_edge_id(self, source_id: str, target_id: str, edge_type: str) -> str:
        id_str = f"{self.file_id}:{source_id}:{target_id}:{edge_type}"
        return sha256_text(id_str)

    def _add_edge(self, source_id: str, target_id: str, edge_type: str, props: dict[str, Any] | None = None) -> None:
        if not source_id or not target_id or source_id == target_id:
            return
        edge_id = self._generate_edge_id(source_id, target_id, edge_type)
        edge = CPGEdge(
            edge_id=edge_id,
            file_id=self.file_id,
            source_id=source_id,
            target_id=target_id,
            edge_type=edge_type,
            properties=props or {},
        )
        self.edges.append(edge)

    def _build_ast(self, tree: ast.AST) -> None:
        """Traverse AST, construct CPGNode for each ast.AST, and AST edges."""
        
        for parent in ast.walk(tree):
            parent_cpg = self._get_or_create_cpg_node(parent)

            for child_field, child in ast.iter_fields(parent):
                if isinstance(child, ast.AST):
                    child_cpg = self._get_or_create_cpg_node(child)
                    self._add_edge(parent_cpg.node_id, child_cpg.node_id, "AST", {"field": child_field})
                elif isinstance(child, list):
                    for item in child:
                        if isinstance(item, ast.AST):
                            child_cpg = self._get_or_create_cpg_node(item)
                            self._add_edge(parent_cpg.node_id, child_cpg.node_id, "AST", {"field": child_field})

    def _get_or_create_cpg_node(self, node: ast.AST) -> CPGNode:
        if node in self._node_map:
            return self._node_map[node]

        node_type = type(node).__name__
        label = self._make_label(node, node_type)
        
        lineno = getattr(node, "lineno", 1)
        col_offset = getattr(node, "col_offset", 0)
        end_lineno = getattr(node, "end_lineno", lineno)
        end_col_offset = getattr(node, "end_col_offset", col_offset)

        try:
            code_snippet = ast.unparse(node)
            if len(code_snippet) > 300:
                code_snippet = code_snippet[:297] + "..."
        except Exception:
            code_snippet = label

        props: dict[str, Any] = {}
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            props["name"] = node.name
            props["args"] = [arg.arg for arg in node.args.args]
        elif isinstance(node, ast.ClassDef):
            props["name"] = node.name
        elif isinstance(node, ast.Name):
            props["id"] = node.id
            props["ctx"] = type(node.ctx).__name__
        elif isinstance(node, ast.Attribute):
            props["attr"] = node.attr
        elif isinstance(node, ast.Constant):
            props["value"] = str(node.value)[:100]

        node_id = self._generate_node_id(node, node_type, label)
        cpg_node = CPGNode(
            node_id=node_id,
            file_id=self.file_id,
            node_type=node_type,
            label=label,
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            code=code_snippet,
            properties=props,
        )

        self.nodes.append(cpg_node)
        self._node_map[node] = cpg_node
        return cpg_node

    def _make_label(self, node: ast.AST, node_type: str) -> str:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return f"{node_type}:{node.name}"
        elif isinstance(node, ast.Name):
            return f"Name:{node.id}"
        elif isinstance(node, ast.Attribute):
            return f"Attribute:{node.attr}"
        elif isinstance(node, ast.Call):
            func_name = self._extract_call_name(node.func)
            return f"Call:{func_name}"
        elif isinstance(node, ast.Constant):
            return f"Constant:{str(node.value)[:20]}"
        return node_type

    def _extract_call_name(self, func_node: ast.AST) -> str:
        if isinstance(func_node, ast.Name):
            return func_node.id
        elif isinstance(func_node, ast.Attribute):
            base = self._extract_call_name(func_node.value)
            return f"{base}.{func_node.attr}" if base else func_node.attr
        return "anonymous_call"

    def _build_cfg(self, tree: ast.AST) -> None:
        """Construct Control Flow Graph edges for statement blocks."""
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                body = getattr(node, "body", [])
                self._link_statement_sequence(body)
            elif isinstance(node, ast.If):
                self._link_statement_sequence(node.body)
                if node.orelse:
                    self._link_statement_sequence(node.orelse)
                # CFG branch edges: If test -> body[0] and test -> orelse[0]
                if node.body:
                    if_cpg = self._node_map.get(node)
                    body_first_cpg = self._node_map.get(node.body[0])
                    if if_cpg and body_first_cpg:
                        self._add_edge(if_cpg.node_id, body_first_cpg.node_id, "CFG", {"branch": "true"})
                if node.orelse:
                    if_cpg = self._node_map.get(node)
                    else_first_cpg = self._node_map.get(node.orelse[0])
                    if if_cpg and else_first_cpg:
                        self._add_edge(if_cpg.node_id, else_first_cpg.node_id, "CFG", {"branch": "false"})
            elif isinstance(node, (ast.For, ast.AsyncFor, ast.While)):
                self._link_statement_sequence(node.body)
                # Loop back edge: body[-1] -> loop_header
                if node.body:
                    loop_cpg = self._node_map.get(node)
                    last_cpg = self._node_map.get(node.body[-1])
                    first_cpg = self._node_map.get(node.body[0])
                    if loop_cpg and first_cpg:
                        self._add_edge(loop_cpg.node_id, first_cpg.node_id, "CFG", {"label": "loop_enter"})
                    if last_cpg and loop_cpg:
                        self._add_edge(last_cpg.node_id, loop_cpg.node_id, "CFG", {"label": "loop_back"})

    def _link_statement_sequence(self, stmts: list[ast.stmt]) -> None:
        stmts_ast = [s for s in stmts if isinstance(s, ast.AST)]
        for i in range(len(stmts_ast) - 1):
            s1 = self._node_map.get(stmts_ast[i])
            s2 = self._node_map.get(stmts_ast[i + 1])
            if s1 and s2:
                self._add_edge(s1.node_id, s2.node_id, "CFG", {"label": "FLOWS_TO"})

    def _build_dfg(self, tree: ast.AST) -> None:
        """Construct Data Flow Graph (variable definition to usage) per scope."""
        for scope_node in ast.walk(tree):
            if isinstance(scope_node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef)):
                # Map variable_name -> list of CPGNodes where it was defined
                definitions: dict[str, list[CPGNode]] = {}

                for node in ast.walk(scope_node):
                    if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                        assign_cpg = self._node_map.get(node)
                        if not assign_cpg:
                            continue
                        targets = []
                        if isinstance(node, ast.Assign):
                            targets = node.targets
                        elif isinstance(node, (ast.AnnAssign, ast.AugAssign)):
                            targets = [node.target]
                        
                        for t in targets:
                            for var_node in ast.walk(t):
                                if isinstance(var_node, ast.Name) and isinstance(var_node.ctx, ast.Store):
                                    definitions.setdefault(var_node.id, []).append(assign_cpg)

                    elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        use_cpg = self._node_map.get(node)
                        if not use_cpg:
                            continue
                        var_id = node.id
                        if var_id in definitions:
                            for def_cpg in definitions[var_id]:
                                self._add_edge(
                                    def_cpg.node_id,
                                    use_cpg.node_id,
                                    "DFG",
                                    {"variable": var_id, "label": "REACHES"},
                                )

    def _build_calls(self, tree: ast.AST) -> None:
        """Construct Call Graph edges connecting ast.Call nodes to functions/methods."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                call_cpg = self._node_map.get(node)
                if not call_cpg:
                    continue

                callee_name = self._extract_call_name(node.func)
                func_cpg = self._node_map.get(node.func)
                if func_cpg:
                    self._add_edge(
                        call_cpg.node_id,
                        func_cpg.node_id,
                        "CALLS",
                        {"callee": callee_name},
                    )


def parse_python_file(file_id: str, relative_path: str, code_bytes: bytes) -> CPGParseResult:
    """Helper function to parse a single Python source file into CPG elements."""
    parser = CPGParser(file_id=file_id, relative_path=relative_path, code_bytes=code_bytes)
    return parser.parse()
