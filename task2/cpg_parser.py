"""Incremental Python CPG extraction with deterministic structural identifiers."""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass, field
from typing import Any

from event_contract import EDGE_TYPES, sha256_text


@dataclass
class CPGNode:
    node_id: str
    file_id: str
    structural_path: str
    node_type: str
    label: str
    lineno: int
    col_offset: int
    end_lineno: int
    end_col_offset: int
    code: str
    properties: dict[str, Any] = field(default_factory=dict)

    def to_event(self, common: dict) -> dict:
        return {
            **common,
            "node_id": self.node_id,
            "structural_path": self.structural_path,
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
    edge_type: str
    properties: dict[str, Any] = field(default_factory=dict)

    def to_event(self, common: dict) -> dict:
        return {
            **common,
            "edge_id": self.edge_id,
            "edge_type": self.edge_type,
            "source_id": self.source_id,
            "target_id": self.target_id,
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
    ast_edge_count: int = 0
    cfg_edge_count: int = 0
    dfg_edge_count: int = 0
    call_edge_count: int = 0

    def to_metadata_event(
        self,
        common: dict,
        *,
        size_bytes: int,
        line_count: int,
    ) -> dict:
        return {
            **common,
            "language": "python",
            "size_bytes": size_bytes,
            "line_count": line_count,
            "ast_node_count": self.ast_node_count,
            "ast_edge_count": self.ast_edge_count,
            "cfg_edge_count": self.cfg_edge_count,
            "dfg_edge_count": self.dfg_edge_count,
            "call_edge_count": self.call_edge_count,
            "status": "FAILED" if self.error_event else "PARSED",
        }


@dataclass
class _Scope:
    root: ast.AST
    parent: "_Scope | None"
    name: str
    local_names: set[str]
    global_names: set[str]
    nonlocal_names: set[str]
    function_nodes: dict[str, ast.AST]
    definitions: dict[str, CPGNode] = field(default_factory=dict)


class _BindingCollector(ast.NodeVisitor):
    """Collect bindings in one lexical scope without entering nested scopes."""

    def __init__(self) -> None:
        self.local_names: set[str] = set()
        self.global_names: set[str] = set()
        self.nonlocal_names: set[str] = set()
        self.function_nodes: dict[str, ast.AST] = {}

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.local_names.add(node.id)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self.local_names.add(node.name)
        self.function_nodes[node.name] = node

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self.local_names.add(node.name)
        self.function_nodes[node.name] = node

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        self.local_names.add(node.name)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            self.local_names.add(alias.asname or alias.name.split(".")[0])

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        for alias in node.names:
            if alias.name != "*":
                self.local_names.add(alias.asname or alias.name)

    def visit_Global(self, node: ast.Global) -> None:
        self.global_names.update(node.names)

    def visit_Nonlocal(self, node: ast.Nonlocal) -> None:
        self.nonlocal_names.update(node.names)


class CPGParser:
    """Extract AST, CFG, lexical DFG, and CALL edges for one source file."""

    def __init__(self, file_id: str, relative_path: str, code_bytes: bytes):
        self.file_id = file_id
        self.relative_path = relative_path
        self.code_bytes = code_bytes
        self.code_str = ""
        self.nodes: list[CPGNode] = []
        self._nodes_by_path: dict[str, CPGNode] = {}
        self._primary_node_by_object: dict[int, CPGNode] = {}
        self._edges_by_id: dict[str, CPGEdge] = {}
        self._scope_by_object: dict[int, _Scope] = {}

    def parse(self) -> CPGParseResult:
        result = CPGParseResult(self.file_id, self.relative_path)
        try:
            self.code_str = self.code_bytes.decode("utf-8", errors="replace")
            tree = ast.parse(self.code_str, filename=self.relative_path)
        except Exception as exc:
            result.error_event = {
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "lineno": int(getattr(exc, "lineno", 0) or 0),
                "col_offset": int(getattr(exc, "offset", 0) or 0),
            }
            return result

        self._visit_ast(tree, "root")
        self._build_cfg(tree)
        self._analyze_scope(tree, None)

        result.nodes = self.nodes
        result.edges = list(self._edges_by_id.values())
        result.ast_node_count = len(result.nodes)
        result.ast_edge_count = self._count_edges("AST")
        result.cfg_edge_count = self._count_edges("CFG")
        result.dfg_edge_count = self._count_edges("DFG")
        result.call_edge_count = self._count_edges("CALL")
        return result

    def _count_edges(self, edge_type: str) -> int:
        return sum(edge.edge_type == edge_type for edge in self._edges_by_id.values())

    # ------------------------------------------------------------------ AST --
    def _visit_ast(self, node: ast.AST, structural_path: str) -> CPGNode:
        parent = self._create_node(node, structural_path)
        for field_name, value in ast.iter_fields(node):
            if isinstance(value, ast.AST):
                child_path = f"{structural_path}.{field_name}"
                child = self._visit_ast(value, child_path)
                self._add_edge(
                    parent.node_id,
                    child.node_id,
                    "AST",
                    {"field": field_name},
                )
            elif isinstance(value, list):
                for index, item in enumerate(value):
                    if not isinstance(item, ast.AST):
                        continue
                    child_path = f"{structural_path}.{field_name}[{index}]"
                    child = self._visit_ast(item, child_path)
                    self._add_edge(
                        parent.node_id,
                        child.node_id,
                        "AST",
                        {"field": field_name, "index": index},
                    )
        return parent

    def _create_node(self, node: ast.AST, structural_path: str) -> CPGNode:
        node_type = type(node).__name__
        node_id = sha256_text(f"{self.file_id}:{structural_path}:{node_type}")
        lineno = int(getattr(node, "lineno", 0) or 0)
        col_offset = int(getattr(node, "col_offset", 0) or 0)
        end_lineno = int(getattr(node, "end_lineno", lineno) or lineno)
        end_col_offset = int(getattr(node, "end_col_offset", col_offset) or col_offset)
        label = self._make_label(node)
        code = ast.get_source_segment(self.code_str, node)
        if code is None:
            try:
                code = ast.unparse(node)
            except Exception:
                code = label
        if len(code) > 300:
            code = code[:297] + "..."

        properties: dict[str, Any] = {}
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            properties = {
                "name": node.name,
                "args": [arg.arg for arg in node.args.args],
            }
        elif isinstance(node, ast.ClassDef):
            properties = {"name": node.name}
        elif isinstance(node, ast.Name):
            properties = {"id": node.id, "ctx": type(node.ctx).__name__}
        elif isinstance(node, ast.arg):
            properties = {"arg": node.arg}
        elif isinstance(node, ast.Attribute):
            properties = {"attr": node.attr}
        elif isinstance(node, ast.Constant):
            properties = {"value": str(node.value)[:100]}

        cpg_node = CPGNode(
            node_id=node_id,
            file_id=self.file_id,
            structural_path=structural_path,
            node_type=node_type,
            label=label,
            lineno=lineno,
            col_offset=col_offset,
            end_lineno=end_lineno,
            end_col_offset=end_col_offset,
            code=code,
            properties=properties,
        )
        self.nodes.append(cpg_node)
        self._nodes_by_path[structural_path] = cpg_node
        self._primary_node_by_object.setdefault(id(node), cpg_node)
        return cpg_node

    def _make_label(self, node: ast.AST) -> str:
        node_type = type(node).__name__
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return f"{node_type}:{node.name}"
        if isinstance(node, ast.Name):
            return f"Name:{node.id}"
        if isinstance(node, ast.arg):
            return f"arg:{node.arg}"
        if isinstance(node, ast.Attribute):
            return f"Attribute:{node.attr}"
        if isinstance(node, ast.Call):
            return f"Call:{self._extract_call_name(node.func)}"
        if isinstance(node, ast.Constant):
            return f"Constant:{str(node.value)[:20]}"
        return node_type

    def _node_for(self, node: ast.AST) -> CPGNode:
        return self._primary_node_by_object[id(node)]

    def _add_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        if edge_type not in EDGE_TYPES:
            raise RuntimeError(f"Unsupported edge type: {edge_type}")
        if not source_id or not target_id or source_id == target_id:
            return
        props = properties or {}
        identity = json.dumps(props, sort_keys=True, separators=(",", ":"))
        edge_id = sha256_text(
            f"{self.file_id}:{edge_type}:{source_id}:{target_id}:{identity}"
        )
        self._edges_by_id.setdefault(
            edge_id,
            CPGEdge(
                edge_id=edge_id,
                file_id=self.file_id,
                source_id=source_id,
                target_id=target_id,
                edge_type=edge_type,
                properties=props,
            ),
        )

    # ------------------------------------------------------------------ CFG --
    def _build_cfg(self, tree: ast.AST) -> None:
        self._build_cfg_block(getattr(tree, "body", []), None)
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                self._build_cfg_block(node.body, None)

    def _build_cfg_block(
        self,
        statements: list[ast.stmt],
        follow: ast.stmt | None,
    ) -> None:
        for index, statement in enumerate(statements):
            next_statement = statements[index + 1] if index + 1 < len(statements) else follow
            self._build_cfg_statement(statement, next_statement)

    def _cfg_edge(self, source: ast.AST, target: ast.AST | None, label: str) -> None:
        if target is not None:
            self._add_edge(
                self._node_for(source).node_id,
                self._node_for(target).node_id,
                "CFG",
                {"label": label},
            )

    def _build_cfg_statement(self, statement: ast.stmt, follow: ast.stmt | None) -> None:
        if isinstance(statement, ast.If):
            true_target = statement.body[0] if statement.body else follow
            false_target = statement.orelse[0] if statement.orelse else follow
            self._cfg_edge(statement, true_target, "branch_true")
            self._cfg_edge(statement, false_target, "branch_false")
            self._build_cfg_block(statement.body, follow)
            self._build_cfg_block(statement.orelse, follow)
            return

        if isinstance(statement, (ast.For, ast.AsyncFor, ast.While)):
            body_target = statement.body[0] if statement.body else statement
            exit_target = statement.orelse[0] if statement.orelse else follow
            self._cfg_edge(statement, body_target, "loop_enter")
            self._cfg_edge(statement, exit_target, "loop_exit")
            self._build_cfg_block(statement.body, statement)
            self._build_cfg_block(statement.orelse, follow)
            return

        if isinstance(statement, (ast.With, ast.AsyncWith)):
            body_target = statement.body[0] if statement.body else follow
            self._cfg_edge(statement, body_target, "with_enter")
            self._build_cfg_block(statement.body, follow)
            return

        if isinstance(statement, ast.Try):
            if statement.body:
                self._cfg_edge(statement, statement.body[0], "try_body")
            for index, handler in enumerate(statement.handlers):
                if handler.body:
                    self._cfg_edge(statement, handler.body[0], f"except_{index}")
                    self._build_cfg_block(handler.body, follow)
            self._build_cfg_block(statement.body, follow)
            self._build_cfg_block(statement.orelse, follow)
            self._build_cfg_block(statement.finalbody, follow)
            return

        if isinstance(statement, (ast.Return, ast.Raise, ast.Break, ast.Continue)):
            return
        self._cfg_edge(statement, follow, "next")

    # ------------------------------------------------------------ DFG/CALL --
    def _scope_bindings(self, root: ast.AST) -> _BindingCollector:
        collector = _BindingCollector()
        if isinstance(root, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            arguments = root.args
            for arg in [
                *arguments.posonlyargs,
                *arguments.args,
                *arguments.kwonlyargs,
            ]:
                collector.local_names.add(arg.arg)
            if arguments.vararg:
                collector.local_names.add(arguments.vararg.arg)
            if arguments.kwarg:
                collector.local_names.add(arguments.kwarg.arg)
        body = root.body if hasattr(root, "body") and isinstance(root.body, list) else []
        for statement in body:
            collector.visit(statement)
        collector.local_names.difference_update(collector.global_names)
        collector.local_names.difference_update(collector.nonlocal_names)
        return collector

    def _analyze_scope(self, root: ast.AST, parent: _Scope | None) -> _Scope:
        bindings = self._scope_bindings(root)
        scope = _Scope(
            root=root,
            parent=parent,
            name=self._node_for(root).structural_path,
            local_names=bindings.local_names,
            global_names=bindings.global_names,
            nonlocal_names=bindings.nonlocal_names,
            function_nodes=bindings.function_nodes,
        )
        self._scope_by_object[id(root)] = scope

        if isinstance(root, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            arguments = root.args
            for arg in [
                *arguments.posonlyargs,
                *arguments.args,
                *arguments.kwonlyargs,
            ]:
                self._bind_name(arg.arg, self._node_for(arg), scope)
            if arguments.vararg:
                self._bind_name(arguments.vararg.arg, self._node_for(arguments.vararg), scope)
            if arguments.kwarg:
                self._bind_name(arguments.kwarg.arg, self._node_for(arguments.kwarg), scope)

        body = root.body if hasattr(root, "body") and isinstance(root.body, list) else []
        for statement in body:
            self._analyze_statement(statement, scope)
        return scope

    def _analyze_statement(self, statement: ast.stmt, scope: _Scope) -> None:
        self._scope_by_object[id(statement)] = scope

        if isinstance(statement, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in statement.decorator_list:
                self._visit_value(decorator, scope)
            for default in [*statement.args.defaults, *statement.args.kw_defaults]:
                if default is not None:
                    self._visit_value(default, scope)
            if statement.returns:
                self._visit_value(statement.returns, scope)
            self._bind_name(statement.name, self._node_for(statement), scope)
            self._analyze_scope(statement, scope)
            return

        if isinstance(statement, ast.ClassDef):
            for value in [*statement.bases, *statement.decorator_list]:
                self._visit_value(value, scope)
            for keyword in statement.keywords:
                self._visit_value(keyword.value, scope)
            self._bind_name(statement.name, self._node_for(statement), scope)
            self._analyze_scope(statement, scope)
            return

        if isinstance(statement, ast.Assign):
            self._visit_value(statement.value, scope)
            for target in statement.targets:
                self._bind_target(target, scope)
            return

        if isinstance(statement, ast.AnnAssign):
            self._visit_value(statement.annotation, scope)
            if statement.value:
                self._visit_value(statement.value, scope)
            self._bind_target(statement.target, scope)
            return

        if isinstance(statement, ast.AugAssign):
            self._visit_target_as_load(statement.target, scope)
            self._visit_value(statement.value, scope)
            self._bind_target(statement.target, scope)
            return

        if isinstance(statement, (ast.For, ast.AsyncFor)):
            self._visit_value(statement.iter, scope)
            self._bind_target(statement.target, scope)
            for child in [*statement.body, *statement.orelse]:
                self._analyze_statement(child, scope)
            return

        if isinstance(statement, ast.While):
            self._visit_value(statement.test, scope)
            for child in [*statement.body, *statement.orelse]:
                self._analyze_statement(child, scope)
            return

        if isinstance(statement, ast.If):
            self._visit_value(statement.test, scope)
            for child in [*statement.body, *statement.orelse]:
                self._analyze_statement(child, scope)
            return

        if isinstance(statement, (ast.With, ast.AsyncWith)):
            for item in statement.items:
                self._visit_value(item.context_expr, scope)
                if item.optional_vars:
                    self._bind_target(item.optional_vars, scope)
            for child in statement.body:
                self._analyze_statement(child, scope)
            return

        if isinstance(statement, ast.Try):
            for child in statement.body:
                self._analyze_statement(child, scope)
            for handler in statement.handlers:
                if handler.type:
                    self._visit_value(handler.type, scope)
                if handler.name:
                    self._bind_name(handler.name, self._node_for(handler), scope)
                for child in handler.body:
                    self._analyze_statement(child, scope)
            for child in [*statement.orelse, *statement.finalbody]:
                self._analyze_statement(child, scope)
            return

        if isinstance(statement, (ast.Import, ast.ImportFrom)):
            for alias in statement.names:
                name = alias.asname or alias.name.split(".")[0]
                if name != "*":
                    self._bind_name(name, self._node_for(alias), scope)
            return

        for child in ast.iter_child_nodes(statement):
            if isinstance(child, ast.stmt):
                self._analyze_statement(child, scope)
            else:
                self._visit_value(child, scope)

    def _visit_value(self, node: ast.AST, scope: _Scope) -> None:
        self._scope_by_object[id(node)] = scope

        if isinstance(node, ast.Name):
            if isinstance(node.ctx, ast.Load):
                definition = self._resolve_definition(node.id, scope)
                if definition is not None:
                    self._add_edge(
                        definition.node_id,
                        self._node_for(node).node_id,
                        "DFG",
                        {"variable": node.id},
                    )
            return

        if isinstance(node, ast.NamedExpr):
            self._visit_value(node.value, scope)
            self._bind_target(node.target, scope)
            return

        if isinstance(node, ast.Lambda):
            self._analyze_scope(node, scope)
            return

        if isinstance(node, ast.Call):
            self._visit_value(node.func, scope)
            for arg in node.args:
                self._visit_value(arg, scope)
            for keyword in node.keywords:
                self._visit_value(keyword.value, scope)
            self._add_call_edge(node, scope)
            return

        if isinstance(node, ast.comprehension):
            self._visit_value(node.iter, scope)
            self._bind_target(node.target, scope)
            for condition in node.ifs:
                self._visit_value(condition, scope)
            return

        for child in ast.iter_child_nodes(node):
            self._visit_value(child, scope)

    def _visit_target_as_load(self, target: ast.AST, scope: _Scope) -> None:
        if isinstance(target, ast.Name):
            definition = self._resolve_definition(target.id, scope)
            if definition is not None:
                self._add_edge(
                    definition.node_id,
                    self._node_for(target).node_id,
                    "DFG",
                    {"variable": target.id},
                )
            return
        for child in ast.iter_child_nodes(target):
            self._visit_value(child, scope)

    def _bind_target(self, target: ast.AST, scope: _Scope) -> None:
        self._scope_by_object[id(target)] = scope
        if isinstance(target, ast.Name):
            self._bind_name(target.id, self._node_for(target), scope)
            return
        if isinstance(target, (ast.Tuple, ast.List)):
            for element in target.elts:
                self._bind_target(element, scope)
            return
        for child in ast.iter_child_nodes(target):
            self._visit_value(child, scope)

    def _bind_name(self, name: str, node: CPGNode, scope: _Scope) -> None:
        if name in scope.global_names:
            self._root_scope(scope).definitions[name] = node
            return
        if name in scope.nonlocal_names:
            current = scope.parent
            while current is not None:
                if name in current.definitions or name in current.local_names:
                    current.definitions[name] = node
                    return
                current = current.parent
        scope.definitions[name] = node

    def _root_scope(self, scope: _Scope) -> _Scope:
        while scope.parent is not None:
            scope = scope.parent
        return scope

    def _resolve_definition(self, name: str, scope: _Scope) -> CPGNode | None:
        if name in scope.global_names:
            return self._root_scope(scope).definitions.get(name)
        if name in scope.nonlocal_names:
            current = scope.parent
            while current is not None:
                if name in current.definitions:
                    return current.definitions[name]
                current = current.parent
            return None
        if name in scope.definitions:
            return scope.definitions[name]
        if name in scope.local_names:
            return None
        if scope.parent is not None:
            return self._resolve_definition(name, scope.parent)
        return None

    def _resolve_function(self, name: str, scope: _Scope) -> ast.AST | None:
        if name in scope.function_nodes:
            return scope.function_nodes[name]
        if name in scope.local_names:
            return None
        if scope.parent is not None:
            return self._resolve_function(name, scope.parent)
        return None

    def _add_call_edge(self, call: ast.Call, scope: _Scope) -> None:
        callee_name = self._extract_call_name(call.func)
        target_node: CPGNode
        resolution = "unresolved"
        if isinstance(call.func, ast.Name):
            function = self._resolve_function(call.func.id, scope)
            if function is not None:
                target_node = self._node_for(function)
                resolution = "internal"
            else:
                target_node = self._node_for(call.func)
        else:
            target_node = self._node_for(call.func)
        self._add_edge(
            self._node_for(call).node_id,
            target_node.node_id,
            "CALL",
            {"callee": callee_name, "resolution": resolution},
        )

    def _extract_call_name(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = self._extract_call_name(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        return "<dynamic>"


def parse_python_file(
    file_id: str,
    relative_path: str,
    code_bytes: bytes,
) -> CPGParseResult:
    return CPGParser(file_id, relative_path, code_bytes).parse()
