#!/usr/bin/env python3
"""Ratchet legacy ingestion-source wiring toward the declarative catalog.

The scanner is intentionally dependency-free and never imports production
modules. It parses production Python and SQL and reports these rule IDs:

``SRCARCH001``
    Mutable planner, fetcher, or reconciler dispatch registration.
``SRCARCH002``
    Handler registration performed as a module-import side effect.
``SRCARCH003``
    A Python literal that duplicates most or all canonical source IDs.
``SRCARCH004``
    A SQL ``CHECK (source IN (...))`` source registry introduced after the
    contract-catalog cutover. Earlier migration checks were dropped by 0193
    and no longer describe active architecture.
``SRCARCH005``
    Provider/source branching that constructs a provider client.
``SRCARCH006``
    A top-level source-keyed mapping that duplicates contract-owned source
    metadata, routes, policies, or runtime bindings.
``SRCARCH007``
    A retired synthetic source harness or single-host spammer compatibility
    surface still exists after Provider Lab became the only simulator.
``SRCARCH008``
    An installation query selects an arbitrary first/latest row instead of an
    exact installation UUID or an explicit collection.
``SRCARCH009``
    A provider integration performs a raw HTTP request outside the callback
    governed by ``ProviderRequestBinding`` / ``ProviderTransport``.
``SRCARCH010``
    Shared ingress or control-plane code branches on a canonical source to
    select provider behavior instead of resolving a contract-owned callable.
``SRCARCH011``
    A live runtime is represented by a fabricated ``ingest.live.*`` label
    instead of an executable launcher or explicit managed ingress boundary.

Normal CI mode subtracts the exact, reviewed entries in the baseline file and
fails only for new findings. ``--no-baseline`` ignores that debt allowance and
is the strict P9 gate once the legacy findings have been removed.
"""

from __future__ import annotations

import argparse
import ast
import json
import math
import re
import sys
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BASELINE_PATH = Path("scripts/source_architecture_baseline.json")
DEFAULT_CATALOG_PATH = Path("services/ingest/source_contract/catalog.py")

RULE_MUTABLE_DISPATCH = "SRCARCH001"
RULE_HANDLER_IMPORT_REGISTRATION = "SRCARCH002"
RULE_DUPLICATE_SOURCE_IDS = "SRCARCH003"
RULE_SQL_SOURCE_CHECK = "SRCARCH004"
RULE_SOURCE_CLIENT_SWITCH = "SRCARCH005"
RULE_PARALLEL_SOURCE_MAP = "SRCARCH006"
RULE_LEGACY_PROVIDER_HARNESS = "SRCARCH007"
RULE_ARBITRARY_INSTALLATION_SELECTION = "SRCARCH008"
RULE_PROVIDER_TRANSPORT_BYPASS = "SRCARCH009"
RULE_SHARED_SOURCE_BEHAVIOR_SWITCH = "SRCARCH010"
RULE_FABRICATED_LIVE_BINDING = "SRCARCH011"

RULE_DESCRIPTIONS: dict[str, str] = {
    RULE_MUTABLE_DISPATCH: ("mutable planner/fetcher/reconciler dispatch registration"),
    RULE_HANDLER_IMPORT_REGISTRATION: (
        "handler registration executed as an import side effect"
    ),
    RULE_DUPLICATE_SOURCE_IDS: "duplicated canonical source-ID literal",
    RULE_SQL_SOURCE_CHECK: "duplicated SQL source CHECK constraint",
    RULE_SOURCE_CLIENT_SWITCH: "source-based provider-client construction switch",
    RULE_PARALLEL_SOURCE_MAP: "parallel top-level source-keyed mapping",
    RULE_LEGACY_PROVIDER_HARNESS: "legacy provider simulator or spammer binding",
    RULE_ARBITRARY_INSTALLATION_SELECTION: (
        "arbitrary first/latest installation selection"
    ),
    RULE_PROVIDER_TRANSPORT_BYPASS: (
        "provider HTTP request bypasses ProviderTransport"
    ),
    RULE_SHARED_SOURCE_BEHAVIOR_SWITCH: (
        "shared ingress/control-plane source behavior switch"
    ),
    RULE_FABRICATED_LIVE_BINDING: "fabricated non-executable live binding",
}

_DISPATCH_NAMES = frozenset(
    {
        "PLANNER_DISPATCH",
        "FETCHER_DISPATCH",
        "RECONCILER_DISPATCH",
    }
)
_MUTATING_METHODS = frozenset(
    {
        "__setitem__",
        "clear",
        "pop",
        "popitem",
        "setdefault",
        "update",
    }
)
_SOURCE_SELECTOR_NAMES = frozenset(
    {
        "normalized_provider",
        "normalized_source",
        "provider",
        "provider_id",
        "source",
        "source_id",
    }
)
_SCAN_ROOT_NAMES = ("lib", "services", "scripts", "db")
_EXCLUDED_DIRECTORY_NAMES = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "docs",
        "generated",
        "graphify-out",
        "node_modules",
        "site",
        "test",
        "tests",
        "venv",
    }
)
_SQL_STRING_RE = re.compile(r"'(?:''|[^'])*'")
_SQL_CHECK_RE = re.compile(
    r"""
    (?:
        (?:ADD\s+)?
        CONSTRAINT\s+
        (?P<constraint>[A-Za-z_][A-Za-z0-9_$]*)
        \s+
    )?
    CHECK
    \s*\(\s*
    (?:(?:[A-Za-z_][A-Za-z0-9_$]*|"(?:[^"]|"")+")\s*\.\s*)?
    "?source(?:_id)?"?
    (?:\s*::\s*[A-Za-z_][A-Za-z0-9_$]*)?
    \s+IN\s*
    \((?P<values>[^()]*)\)
    \s*\)
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)
_SQL_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_SQL_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_MIGRATION_FILENAME_RE = re.compile(r"^(?P<number>\d+)_.*\.sql$")
_SOURCE_CATALOG_CUTOVER_MIGRATION = 193
_SOURCE_MAP_EXEMPT_PATHS = frozenset(
    {
        Path("services/ingest/source_certification/catalog.py"),
    }
)
# Shared source-classification modules must not recreate even a small
# membership list.  Their grouping decision comes from SourceDefinition
# metadata; the repository-wide 80% threshold below remains appropriate for
# ordinary domain fixtures and scenario definitions.
_STRICT_SOURCE_LIST_PATHS = frozenset(
    {
        Path("scripts/webhook_install.py"),
        Path("services/platform/runtime/source_browser_agent_setup.py"),
    }
)
_LEGACY_PROVIDER_HARNESS_PATHS: tuple[Path, ...] = (
    Path("services/ingest/synthetic/mock_servers"),
    Path("services/ingest/synthetic/spammer"),
    Path("services/ingest/synthetic/validation_runs/run_all_sources.py"),
)
_LEGACY_SPAMMER_ENDPOINT_PATH = Path("lib/integrations/endpoints.py")
_LEGACY_SPAMMER_ENDPOINT_MARKERS = (
    "_SPAMMER_BASE_ENV",
    "_SPAMMER_SUBPATH",
    "SYNTHETIC_SOURCE_API_BASE",
)
_SYNTHETIC_ROOT = Path("services/ingest/synthetic")
_PROVIDER_INTEGRATION_ROOT = Path("services/ingest/integrations")
_PROVIDER_TRANSPORT_EXEMPT_PATHS = frozenset(
    {
        Path("services/ingest/integrations/provider_transport.py"),
        Path("services/ingest/integrations/provider_transport_runtime.py"),
    }
)
# These are deliberately shared, provider-agnostic orchestration surfaces.
# Provider-specific algorithms belong in ``services/ingest/integrations`` and
# must be referenced by SourceDefinition/ProviderDefinition callables. Keep
# this list exact: source comparisons in provider-owned modules and ordinary
# application data filtering are not architecture dispatch.
_SHARED_SOURCE_BEHAVIOR_PATHS = frozenset(
    {
        Path("scripts/manage_dedicated_source_installations.py"),
        Path("scripts/webhook_install.py"),
        Path("services/app/gateway/finance_router.py"),
        Path("services/app/gateway/route_mounts.py"),
        Path("services/app/gateway/byoc_onboarding_router.py"),
        Path("services/app/webhooks/secrets.py"),
        Path("services/app/webhooks/signatures/__init__.py"),
        Path("services/app/webhooks/router.py"),
        Path("services/app/webhooks/tenant_resolver.py"),
        Path("services/ingest/ingestion/reconcilers/__init__.py"),
        Path("services/ingest/integrations/oauth_refresh.py"),
        Path("services/ingest/integrations/router.py"),
        Path("services/platform/runtime/source_browser_agent_recipes.py"),
        Path("services/platform/runtime/source_browser_agent_runner.py"),
        Path("services/platform/runtime/source_browser_agent_workflow.py"),
        Path("services/ingest/synthetic/validation_runs/composition.py"),
        Path("services/ingest/synthetic/validation_runs/preflight.py"),
    }
)
_RAW_HTTP_CLIENT_TYPES = frozenset(
    {
        "aiohttp.ClientSession",
        "httpx.AsyncClient",
        "httpx.Client",
        "requests.Session",
    }
)
_PROVIDER_TRANSPORT_TYPES = frozenset(
    {
        "ProviderExecutor",
        "ProviderRequestBinding",
        "ProviderTransport",
        "lib.shared.provider_transport.ProviderTransport",
        "services.ingest.integrations.provider_transport.ProviderExecutor",
        "services.ingest.integrations.provider_transport.ProviderRequestBinding",
    }
)
_RAW_HTTP_METHODS = frozenset(
    {
        "delete",
        "get",
        "head",
        "options",
        "patch",
        "post",
        "put",
        "request",
        "send",
        "stream",
        "ws_connect",
    }
)
_RAW_HTTP_MODULE_CALLS = frozenset(
    {
        *(f"httpx.{method}" for method in _RAW_HTTP_METHODS),
        *(f"requests.{method}" for method in _RAW_HTTP_METHODS),
        "aiohttp.request",
        "urllib.request.urlopen",
    }
)
_PROVIDER_SDK_METHODS_BY_ROOT: tuple[tuple[Path, frozenset[str]], ...] = (
    (
        Path("services/ingest/integrations/aws"),
        frozenset(
            {
                "assume_role",
                "get_caller_identity",
                "lookup_events",
            }
        ),
    ),
)
_INSTALLATION_TABLE_RE = re.compile(
    r"\b(?:FROM|JOIN)\s+(?P<table>[a-z_][a-z0-9_]*installations)\b",
    re.IGNORECASE,
)
_LIMIT_ONE_RE = re.compile(r"\bLIMIT\s+1\b", re.IGNORECASE)
_EXACT_INSTALLATION_ID_RE = re.compile(
    r"""
    \b(?:[a-z_][a-z0-9_]*\s*\.\s*)?id
    (?:\s*::\s*(?:text|uuid))?
    \s*=\s*
    (?:\$\d+|:[a-z_][a-z0-9_]*|%s|\?|\()
    """,
    re.IGNORECASE | re.VERBOSE,
)
_INSTALLATION_EXISTENCE_PROBE_RE = re.compile(
    r"""
    ^\s*SELECT\s+1\s+
    FROM\s+[a-z_][a-z0-9_]*installations
    (?:\s+(?:AS\s+)?[a-z_][a-z0-9_]*)?
    \s+LIMIT\s+1\s*;?\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


class SourceArchitectureCheckError(RuntimeError):
    """The scanner or its baseline is malformed and cannot fail safely."""


@dataclass(frozen=True, order=True)
class BaselineEntry:
    """One exact legacy allowance.

    Line numbers are intentionally absent: harmless formatting can move a
    finding, while changing its registration key, source IDs, or switch shape
    produces a new signature and therefore a CI failure.
    """

    rule_id: str
    path: str
    signature: str


@dataclass(frozen=True)
class Finding:
    rule_id: str
    path: Path
    line_number: int
    signature: str
    message: str

    @property
    def baseline_entry(self) -> BaselineEntry:
        return BaselineEntry(
            rule_id=self.rule_id,
            path=self.path.as_posix(),
            signature=self.signature,
        )


@dataclass(frozen=True)
class RatchetResult:
    findings: tuple[Finding, ...]
    new_findings: tuple[Finding, ...]
    baselined_findings: tuple[Finding, ...]
    resolved_entries: tuple[BaselineEntry, ...]


def _resolve_under_repo(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _is_excluded(relative_path: Path) -> bool:
    parts = relative_path.parts
    if any(part in _EXCLUDED_DIRECTORY_NAMES for part in parts):
        return True
    # Synthetic harnesses are test infrastructure, not production source
    # wiring. In particular, Provider Lab owns a deliberately independent
    # source list and must not become architectural baseline debt.
    if parts[:3] == ("services", "ingest", "synthetic"):
        return True
    name = relative_path.name
    return name.startswith("test_") or name.endswith("_test.py")


def iter_production_files(repo_root: Path) -> Iterator[Path]:
    """Yield deterministic production ``.py``/``.sql`` paths."""
    candidates: list[Path] = []
    for root_name in _SCAN_ROOT_NAMES:
        root = repo_root / root_name
        if not root.exists():
            continue
        for path in root.rglob("*"):
            if not path.is_file() or path.suffix not in {".py", ".sql"}:
                continue
            relative = path.relative_to(repo_root)
            if not _is_excluded(relative):
                candidates.append(path)
    yield from sorted(
        candidates, key=lambda item: item.relative_to(repo_root).as_posix()
    )


def iter_synthetic_binding_files(repo_root: Path) -> Iterator[Path]:
    """Yield executable synthetic Python that can bind real installation rows.

    Provider Lab deliberately owns independent provider fixtures and source
    metadata, so the general production rules do not apply under
    ``services/ingest/synthetic``. Exact installation identity does apply:
    certification code must not make a broken runtime look correct by quietly
    selecting an arbitrary tenant installation.
    """

    root = repo_root / _SYNTHETIC_ROOT
    if not root.exists():
        return
    candidates: list[Path] = []
    for path in root.rglob("*.py"):
        if not path.is_file():
            continue
        relative = path.relative_to(repo_root)
        if any(part in _EXCLUDED_DIRECTORY_NAMES for part in relative.parts):
            continue
        if relative.name.startswith("test_") or relative.name.endswith("_test.py"):
            continue
        candidates.append(path)
    yield from sorted(
        candidates, key=lambda item: item.relative_to(repo_root).as_posix()
    )


def _literal_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _dotted_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def _subscript_root_name(node: ast.Subscript) -> str | None:
    value: ast.AST = node.value
    while isinstance(value, ast.Subscript):
        value = value.value
    return _dotted_name(value)


def _display_node(node: ast.AST) -> str:
    try:
        return ast.unparse(node)
    except (AttributeError, ValueError):
        return ast.dump(node, annotate_fields=False, include_attributes=False)


def _collection_strings(node: ast.AST) -> tuple[str, ...] | None:
    elements: Sequence[ast.AST] | None = None
    if isinstance(node, (ast.List, ast.Set, ast.Tuple)):
        elements = node.elts
    elif isinstance(node, ast.Subscript):
        name = _dotted_name(node.value)
        if name is not None and name.rsplit(".", 1)[-1] == "Literal":
            elements = (
                node.slice.elts if isinstance(node.slice, ast.Tuple) else (node.slice,)
            )
    elif isinstance(node, ast.Call):
        name = _dotted_name(node.func)
        if (
            name is not None
            and name.rsplit(".", 1)[-1] in {"frozenset", "list", "set", "tuple"}
            and len(node.args) == 1
            and not node.keywords
        ):
            return _collection_strings(node.args[0])
    if elements is None or not elements:
        return None
    values: list[str] = []
    for element in elements:
        value = _literal_string(element)
        if value is None:
            return None
        values.append(value)
    return tuple(values)


def _source_definition_ids(node: ast.AST) -> tuple[str, ...] | None:
    """Extract source IDs from the authoritative ``_source(...)`` entries."""

    if not isinstance(node, (ast.List, ast.Tuple)) or not node.elts:
        return None
    source_ids: list[str] = []
    for element in node.elts:
        if (
            not isinstance(element, ast.Call)
            or _dotted_name(element.func) != "_source"
            or not element.args
        ):
            return None
        source_id = _literal_string(element.args[0])
        if source_id is None:
            return None
        source_ids.append(source_id)
    return tuple(source_ids)


def _assignment_target_names(targets: Iterable[ast.AST]) -> tuple[str, ...]:
    names: list[str] = []

    def collect(target: ast.AST) -> None:
        if isinstance(target, ast.Name):
            names.append(target.id)
        elif isinstance(target, (ast.List, ast.Tuple)):
            for element in target.elts:
                collect(element)

    for target in targets:
        collect(target)
    return tuple(names)


def _source_selector(node: ast.AST) -> bool:
    name = _dotted_name(node)
    if name is None:
        return False
    return name.rsplit(".", 1)[-1].casefold() in _SOURCE_SELECTOR_NAMES


def _canonical_literals(
    node: ast.AST,
    canonical_source_ids: frozenset[str],
    aliases: Mapping[str, frozenset[str]] | None = None,
) -> frozenset[str]:
    value = _literal_string(node)
    if value is not None:
        return frozenset({value}) if value in canonical_source_ids else frozenset()
    if isinstance(node, ast.Name) and aliases is not None:
        return aliases.get(node.id, frozenset())
    if isinstance(node, (ast.List, ast.Set, ast.Tuple)):
        return frozenset(
            value
            for element in node.elts
            if (value := _literal_string(element)) in canonical_source_ids
        )
    strings = _collection_strings(node)
    if strings is not None:
        return frozenset(strings) & canonical_source_ids
    return frozenset()


def _selected_source_ids(
    node: ast.AST,
    canonical_source_ids: frozenset[str],
    aliases: Mapping[str, frozenset[str]] | None = None,
) -> frozenset[str]:
    if isinstance(node, ast.BoolOp):
        selected: set[str] = set()
        for value in node.values:
            selected.update(
                _selected_source_ids(
                    value,
                    canonical_source_ids,
                    aliases,
                )
            )
        return frozenset(selected)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return _selected_source_ids(
            node.operand,
            canonical_source_ids,
            aliases,
        )
    if not isinstance(node, ast.Compare) or len(node.ops) != 1:
        return frozenset()
    left, right = node.left, node.comparators[0]
    operation = node.ops[0]
    if isinstance(operation, (ast.Eq, ast.NotEq)):
        if _source_selector(left):
            return _canonical_literals(right, canonical_source_ids, aliases)
        if _source_selector(right):
            return _canonical_literals(left, canonical_source_ids, aliases)
    if isinstance(operation, (ast.In, ast.NotIn)) and _source_selector(left):
        return _canonical_literals(right, canonical_source_ids, aliases)
    return frozenset()


def _canonical_literal_aliases(
    tree: ast.Module,
    canonical_source_ids: frozenset[str],
) -> dict[str, frozenset[str]]:
    """Resolve module-level names that contain literal canonical source IDs."""

    assignments: list[tuple[tuple[str, ...], ast.AST]] = []
    for node in tree.body:
        targets: tuple[ast.AST, ...]
        value: ast.AST | None
        if isinstance(node, ast.Assign):
            targets = tuple(node.targets)
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            targets = (node.target,)
            value = node.value
        else:
            continue
        if value is None:
            continue
        names = _assignment_target_names(targets)
        if names:
            assignments.append((names, value))

    aliases: dict[str, frozenset[str]] = {}
    # A small fixed point also handles ``PROVIDER = SOURCE`` aliases without
    # importing the module or evaluating arbitrary expressions.
    for _ in range(len(assignments) + 1):
        changed = False
        for names, value in assignments:
            selected = _canonical_literals(
                value,
                canonical_source_ids,
                aliases,
            )
            if not selected:
                continue
            for name in names:
                if aliases.get(name) != selected:
                    aliases[name] = selected
                    changed = True
        if not changed:
            break
    return aliases


def _is_client_constructor(call: ast.Call) -> bool:
    dotted = _dotted_name(call.func)
    if dotted is None:
        return False
    final = dotted.rsplit(".", 1)[-1]
    lower = final.casefold()
    if final.endswith(("API", "Api", "Client")):
        return True
    return "client" in lower and lower.startswith(
        ("build_", "construct_", "create_", "get_", "make_", "open_", "resolve_")
    )


def _client_calls(statements: Sequence[ast.stmt]) -> tuple[str, ...]:
    calls: set[str] = set()

    def walk(node: ast.AST) -> None:
        if isinstance(
            node,
            (ast.AsyncFunctionDef, ast.ClassDef, ast.FunctionDef, ast.Lambda),
        ):
            return
        if isinstance(node, ast.Call) and _is_client_constructor(node):
            dotted = _dotted_name(node.func)
            if dotted is not None:
                calls.add(dotted)
        for child in ast.iter_child_nodes(node):
            walk(child)

    for statement in statements:
        walk(statement)
    return tuple(sorted(calls))


def _match_pattern_source_ids(
    pattern: ast.pattern,
    canonical_source_ids: frozenset[str],
    aliases: Mapping[str, frozenset[str]] | None = None,
) -> frozenset[str]:
    if isinstance(pattern, ast.MatchValue):
        return _canonical_literals(
            pattern.value,
            canonical_source_ids,
            aliases,
        )
    if isinstance(pattern, ast.MatchOr):
        selected: set[str] = set()
        for child in pattern.patterns:
            selected.update(
                _match_pattern_source_ids(
                    child,
                    canonical_source_ids,
                    aliases,
                )
            )
        return frozenset(selected)
    return frozenset()


def _handler_registration_call(node: ast.AST) -> ast.Call | None:
    if not isinstance(node, ast.Call):
        return None
    candidate: ast.AST = node.func
    if isinstance(candidate, ast.Call):
        candidate = candidate.func
    name = _dotted_name(candidate)
    if name is None or name.rsplit(".", 1)[-1] != "register":
        return None
    return node


def _arbitrary_installation_selection(
    sql: str,
) -> tuple[str, str] | None:
    stripped = _strip_sql_comments(sql)
    outer_query = _without_lateral_subqueries(stripped)
    table_match = _INSTALLATION_TABLE_RE.search(outer_query)
    if table_match is None or _LIMIT_ONE_RE.search(outer_query) is None:
        return None
    if _INSTALLATION_EXISTENCE_PROBE_RE.fullmatch(outer_query):
        return None
    if _EXACT_INSTALLATION_ID_RE.search(outer_query):
        return None
    normalized = re.sub(r"\s+", " ", outer_query).strip()
    return table_match.group("table").casefold(), normalized[:180]


def _without_lateral_subqueries(sql: str) -> str:
    """Blank nested LATERAL bodies so their row limits are scoped correctly."""

    marker = re.compile(r"\bJOIN\s+LATERAL\s*\(", re.IGNORECASE)
    output = list(sql)
    search_from = 0
    while (match := marker.search(sql, search_from)) is not None:
        open_index = match.end() - 1
        depth = 1
        index = open_index + 1
        quote: str | None = None
        while index < len(sql) and depth:
            character = sql[index]
            if quote is not None:
                if character == quote:
                    if index + 1 < len(sql) and sql[index + 1] == quote:
                        index += 2
                        continue
                    quote = None
            elif character in {"'", '"'}:
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            index += 1
        if depth:
            break
        for offset in range(open_index + 1, index - 1):
            if output[offset] != "\n":
                output[offset] = " "
        search_from = index
    return "".join(output)


def _path_is_under(path: Path, root: Path) -> bool:
    return path == root or path.parts[: len(root.parts)] == root.parts


class _OutboundScopeIndex(ast.NodeVisitor):
    """Index lexical owners without importing provider integrations."""

    def __init__(self) -> None:
        self.parents: dict[ast.AST, ast.AST] = {}
        self.function_parent: dict[
            ast.AsyncFunctionDef | ast.FunctionDef,
            ast.AsyncFunctionDef | ast.FunctionDef | None,
        ] = {}
        self.function_class: dict[
            ast.AsyncFunctionDef | ast.FunctionDef,
            ast.ClassDef | None,
        ] = {}
        self.node_function: dict[
            ast.AST,
            ast.AsyncFunctionDef | ast.FunctionDef | None,
        ] = {}
        self.node_class: dict[ast.AST, ast.ClassDef | None] = {}
        self.functions_by_owner: dict[
            tuple[
                ast.ClassDef | None, ast.AsyncFunctionDef | ast.FunctionDef | None, str
            ],
            ast.AsyncFunctionDef | ast.FunctionDef,
        ] = {}
        self._functions: list[ast.AsyncFunctionDef | ast.FunctionDef] = []
        self._classes: list[ast.ClassDef] = []

    def generic_visit(self, node: ast.AST) -> None:
        current_function = self._functions[-1] if self._functions else None
        current_class = self._classes[-1] if self._classes else None
        self.node_function[node] = current_function
        self.node_class[node] = current_class
        for child in ast.iter_child_nodes(node):
            self.parents[child] = node
            self.visit(child)

    def _visit_function(
        self,
        node: ast.AsyncFunctionDef | ast.FunctionDef,
    ) -> None:
        parent_function = self._functions[-1] if self._functions else None
        owner_class = self._classes[-1] if self._classes else None
        self.node_function[node] = parent_function
        self.node_class[node] = owner_class
        self.function_parent[node] = parent_function
        self.function_class[node] = owner_class
        self.functions_by_owner[(owner_class, parent_function, node.name)] = node
        self._functions.append(node)
        for child in ast.iter_child_nodes(node):
            self.parents[child] = node
            self.visit(child)
        self._functions.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._visit_function(node)

    def visit_AsyncFunctionDef(  # noqa: N802
        self,
        node: ast.AsyncFunctionDef,
    ) -> None:
        self._visit_function(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        current_function = self._functions[-1] if self._functions else None
        current_class = self._classes[-1] if self._classes else None
        self.node_function[node] = current_function
        self.node_class[node] = current_class
        self._classes.append(node)
        for child in ast.iter_child_nodes(node):
            self.parents[child] = node
            self.visit(child)
        self._classes.pop()


def _module_import_aliases(tree: ast.Module) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for item in node.names:
                if item.asname is not None:
                    aliases[item.asname] = item.name
                else:
                    root = item.name.split(".", 1)[0]
                    aliases[root] = root
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            for item in node.names:
                if item.name == "*":
                    continue
                aliases[item.asname or item.name] = f"{node.module}.{item.name}"
    return aliases


def _resolved_dotted_name(
    node: ast.AST,
    aliases: Mapping[str, str],
) -> str | None:
    dotted = _dotted_name(node)
    if dotted is None:
        return None
    root, separator, suffix = dotted.partition(".")
    resolved_root = aliases.get(root, root)
    return f"{resolved_root}.{suffix}" if separator else resolved_root


def _annotation_mentions(
    annotation: ast.AST | None,
    *,
    aliases: Mapping[str, str],
    types: frozenset[str],
) -> bool:
    if annotation is None:
        return False
    for child in ast.walk(annotation):
        dotted = _resolved_dotted_name(child, aliases)
        if dotted in types:
            return True
    return False


def _function_arguments(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
) -> tuple[ast.arg, ...]:
    positional = (*node.args.posonlyargs, *node.args.args)
    optional = (*node.args.kwonlyargs,)
    variadic = tuple(
        item for item in (node.args.vararg, node.args.kwarg) if item is not None
    )
    return (*positional, *optional, *variadic)


@dataclass
class _OutboundFacts:
    aliases: dict[str, str]
    scopes: _OutboundScopeIndex
    raw_names: dict[ast.AsyncFunctionDef | ast.FunctionDef | None, set[str]]
    binding_names: dict[ast.AsyncFunctionDef | ast.FunctionDef | None, set[str]]
    raw_attributes: dict[ast.ClassDef, set[str]]
    binding_attributes: dict[ast.ClassDef, set[str]]
    raw_returners: set[
        tuple[ast.ClassDef | None, ast.AsyncFunctionDef | ast.FunctionDef | None, str]
    ]
    binding_returners: set[
        tuple[ast.ClassDef | None, ast.AsyncFunctionDef | ast.FunctionDef | None, str]
    ]

    @classmethod
    def build(cls, tree: ast.Module) -> _OutboundFacts:
        aliases = _module_import_aliases(tree)
        scopes = _OutboundScopeIndex()
        scopes.visit(tree)
        functions = tuple(scopes.function_parent)
        facts = cls(
            aliases=aliases,
            scopes=scopes,
            raw_names={None: set()},
            binding_names={None: set()},
            raw_attributes={},
            binding_attributes={},
            raw_returners=set(),
            binding_returners=set(),
        )
        for function in functions:
            facts.raw_names[function] = set()
            facts.binding_names[function] = set()
            owner = (
                scopes.function_class[function],
                scopes.function_parent[function],
                function.name,
            )
            if _annotation_mentions(
                function.returns,
                aliases=aliases,
                types=_RAW_HTTP_CLIENT_TYPES,
            ):
                facts.raw_returners.add(owner)
            if _annotation_mentions(
                function.returns,
                aliases=aliases,
                types=_PROVIDER_TRANSPORT_TYPES,
            ):
                facts.binding_returners.add(owner)
            for argument in _function_arguments(function):
                if _annotation_mentions(
                    argument.annotation,
                    aliases=aliases,
                    types=_RAW_HTTP_CLIENT_TYPES,
                ):
                    facts.raw_names[function].add(argument.arg)
                if _annotation_mentions(
                    argument.annotation,
                    aliases=aliases,
                    types=_PROVIDER_TRANSPORT_TYPES,
                ):
                    facts.binding_names[function].add(argument.arg)

        assignments = tuple(
            node
            for node in ast.walk(tree)
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr))
        )
        changed = True
        while changed:
            changed = False
            for assignment in assignments:
                scope = scopes.node_function.get(assignment)
                owner_class = (
                    scopes.function_class.get(scope)
                    if scope is not None
                    else scopes.node_class.get(assignment)
                )
                if isinstance(assignment, ast.Assign):
                    targets = assignment.targets
                    value = assignment.value
                    annotation = None
                else:
                    targets = (assignment.target,)
                    value = assignment.value
                    annotation = (
                        assignment.annotation
                        if isinstance(assignment, ast.AnnAssign)
                        else None
                    )
                raw = _annotation_mentions(
                    annotation,
                    aliases=aliases,
                    types=_RAW_HTTP_CLIENT_TYPES,
                ) or facts.expression_is_raw(
                    value,
                    scope=scope,
                    owner_class=owner_class,
                )
                binding = _annotation_mentions(
                    annotation,
                    aliases=aliases,
                    types=_PROVIDER_TRANSPORT_TYPES,
                ) or facts.expression_is_binding(
                    value,
                    scope=scope,
                    owner_class=owner_class,
                )
                if raw:
                    changed |= facts._mark_targets(
                        targets,
                        scope=scope,
                        owner_class=owner_class,
                        raw=True,
                    )
                if binding:
                    changed |= facts._mark_targets(
                        targets,
                        scope=scope,
                        owner_class=owner_class,
                        raw=False,
                    )
        return facts

    def _scope_has_name(
        self,
        name: str,
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        raw: bool,
    ) -> bool:
        names = self.raw_names if raw else self.binding_names
        current = scope
        while True:
            if name in names.get(current, set()):
                return True
            if current is None:
                return False
            current = self.scopes.function_parent[current]

    def _returner_matches(
        self,
        call: ast.Call,
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        owner_class: ast.ClassDef | None,
        raw: bool,
    ) -> bool:
        returners = self.raw_returners if raw else self.binding_returners
        if isinstance(call.func, ast.Name):
            current = scope
            while True:
                if (owner_class, current, call.func.id) in returners:
                    return True
                if current is None:
                    return (None, None, call.func.id) in returners
                current = self.scopes.function_parent[current]
        if (
            isinstance(call.func, ast.Attribute)
            and isinstance(call.func.value, ast.Name)
            and call.func.value.id in {"cls", "self"}
            and owner_class is not None
        ):
            return (owner_class, None, call.func.attr) in returners
        return False

    def _expression_has_kind(
        self,
        node: ast.AST | None,
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        owner_class: ast.ClassDef | None,
        raw: bool,
    ) -> bool:
        if node is None:
            return False
        expected_types = _RAW_HTTP_CLIENT_TYPES if raw else _PROVIDER_TRANSPORT_TYPES
        names = self.raw_attributes if raw else self.binding_attributes
        if isinstance(node, ast.Name):
            return self._scope_has_name(node.id, scope=scope, raw=raw)
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in {"cls", "self"}
            and owner_class is not None
        ):
            return node.attr in names.get(owner_class, set())
        if isinstance(node, ast.Call):
            dotted = _resolved_dotted_name(node.func, self.aliases)
            return dotted in expected_types or self._returner_matches(
                node,
                scope=scope,
                owner_class=owner_class,
                raw=raw,
            )
        if isinstance(node, ast.BoolOp):
            return any(
                self._expression_has_kind(
                    child,
                    scope=scope,
                    owner_class=owner_class,
                    raw=raw,
                )
                for child in node.values
            )
        if isinstance(node, ast.IfExp):
            return any(
                self._expression_has_kind(
                    child,
                    scope=scope,
                    owner_class=owner_class,
                    raw=raw,
                )
                for child in (node.body, node.orelse)
            )
        if isinstance(node, (ast.Await, ast.NamedExpr)):
            return self._expression_has_kind(
                node.value,
                scope=scope,
                owner_class=owner_class,
                raw=raw,
            )
        return False

    def expression_is_raw(
        self,
        node: ast.AST | None,
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        owner_class: ast.ClassDef | None,
    ) -> bool:
        return self._expression_has_kind(
            node,
            scope=scope,
            owner_class=owner_class,
            raw=True,
        )

    def expression_is_binding(
        self,
        node: ast.AST | None,
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        owner_class: ast.ClassDef | None,
    ) -> bool:
        return self._expression_has_kind(
            node,
            scope=scope,
            owner_class=owner_class,
            raw=False,
        )

    def _mark_targets(
        self,
        targets: Sequence[ast.AST],
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        owner_class: ast.ClassDef | None,
        raw: bool,
    ) -> bool:
        scoped_names = self.raw_names if raw else self.binding_names
        attributes = self.raw_attributes if raw else self.binding_attributes
        changed = False

        def mark(target: ast.AST) -> None:
            nonlocal changed
            if isinstance(target, ast.Name):
                before = len(scoped_names.setdefault(scope, set()))
                scoped_names[scope].add(target.id)
                changed |= len(scoped_names[scope]) != before
            elif (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id in {"cls", "self"}
                and owner_class is not None
            ):
                before = len(attributes.setdefault(owner_class, set()))
                attributes[owner_class].add(target.attr)
                changed |= len(attributes[owner_class]) != before
            elif isinstance(target, (ast.List, ast.Tuple)):
                for child in target.elts:
                    mark(child)

        for target in targets:
            mark(target)
        return changed

    def resolve_callable(
        self,
        node: ast.AST,
        *,
        scope: ast.AsyncFunctionDef | ast.FunctionDef | None,
        owner_class: ast.ClassDef | None,
    ) -> ast.AsyncFunctionDef | ast.FunctionDef | ast.Lambda | None:
        if isinstance(node, ast.Lambda):
            return node
        if isinstance(node, ast.Name):
            current = scope
            while True:
                resolved = self.scopes.functions_by_owner.get(
                    (owner_class, current, node.id)
                )
                if resolved is not None:
                    return resolved
                if current is None:
                    return self.scopes.functions_by_owner.get((None, None, node.id))
                current = self.scopes.function_parent[current]
        if (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id in {"cls", "self"}
            and owner_class is not None
        ):
            return self.scopes.functions_by_owner.get((owner_class, None, node.attr))
        return None


class _ProviderOutboundScanner(ast.NodeVisitor):
    """Find raw provider HTTP attempts not governed by the request contract."""

    def __init__(self, *, path: Path, tree: ast.Module) -> None:
        self.path = path
        self.facts = _OutboundFacts.build(tree)
        self.findings: list[Finding] = []
        self._guarded_callables: set[
            ast.AsyncFunctionDef | ast.FunctionDef | ast.Lambda
        ] = set()
        self._index_guarded_callbacks(tree)

    def _index_guarded_callbacks(self, tree: ast.Module) -> None:
        guarded_parameters: dict[
            ast.AsyncFunctionDef | ast.FunctionDef,
            set[str],
        ] = {}
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(
                node.func,
                ast.Attribute,
            ):
                continue
            scope = self.facts.scopes.node_function.get(node)
            owner_class = (
                self.facts.scopes.function_class.get(scope)
                if scope is not None
                else self.facts.scopes.node_class.get(node)
            )
            if node.func.attr != "execute" or not self.facts.expression_is_binding(
                node.func.value,
                scope=scope,
                owner_class=owner_class,
            ):
                continue
            candidates = list(node.args)
            candidates.extend(
                keyword.value
                for keyword in node.keywords
                if keyword.arg in {"call", "request"}
            )
            for candidate in candidates:
                resolved = self.facts.resolve_callable(
                    candidate,
                    scope=scope,
                    owner_class=owner_class,
                )
                if resolved is not None:
                    self._guarded_callables.add(resolved)
                elif (
                    isinstance(candidate, ast.Name)
                    and scope is not None
                    and candidate.id
                    in {argument.arg for argument in _function_arguments(scope)}
                ):
                    guarded_parameters.setdefault(scope, set()).add(candidate.id)

        # Carry the guarantee through small typed helpers such as
        # ``AwsClient._execute(operation, call)``. The helper is trustworthy
        # only because its callback parameter is itself passed to the exact
        # ProviderRequestBinding owned by that class.
        changed = True
        while changed:
            changed = False
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                scope = self.facts.scopes.node_function.get(node)
                owner_class = (
                    self.facts.scopes.function_class.get(scope)
                    if scope is not None
                    else self.facts.scopes.node_class.get(node)
                )
                helper = self.facts.resolve_callable(
                    node.func,
                    scope=scope,
                    owner_class=owner_class,
                )
                parameter_names = guarded_parameters.get(helper) if helper else None
                if helper is None or not parameter_names:
                    continue
                ordered = _function_arguments(helper)
                if (
                    isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id in {"cls", "self"}
                    and ordered
                    and ordered[0].arg in {"cls", "self"}
                ):
                    ordered = ordered[1:]
                supplied: dict[str, ast.AST] = {
                    argument.arg: value
                    for argument, value in zip(ordered, node.args, strict=False)
                }
                supplied.update(
                    {
                        keyword.arg: keyword.value
                        for keyword in node.keywords
                        if keyword.arg is not None
                    }
                )
                caller_parameters = (
                    {argument.arg for argument in _function_arguments(scope)}
                    if scope is not None
                    else set()
                )
                for parameter_name in parameter_names:
                    candidate = supplied.get(parameter_name)
                    if candidate is None:
                        continue
                    resolved = self.facts.resolve_callable(
                        candidate,
                        scope=scope,
                        owner_class=owner_class,
                    )
                    if resolved is not None and resolved not in self._guarded_callables:
                        self._guarded_callables.add(resolved)
                        changed = True
                    elif (
                        isinstance(candidate, ast.Name)
                        and scope is not None
                        and candidate.id in caller_parameters
                        and candidate.id
                        not in guarded_parameters.setdefault(scope, set())
                    ):
                        guarded_parameters[scope].add(candidate.id)
                        changed = True

        # A retry callback is safe only when ProviderTransport is its sole
        # entrypoint. An optional ``binding`` branch such as
        # ``binding.execute(..., _once) if binding else await _once()`` still
        # leaves a real unmetered request path and must not receive credit.
        directly_invoked: set[ast.AsyncFunctionDef | ast.FunctionDef | ast.Lambda] = (
            set()
        )
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            scope = self.facts.scopes.node_function.get(node)
            owner_class = (
                self.facts.scopes.function_class.get(scope)
                if scope is not None
                else self.facts.scopes.node_class.get(node)
            )
            resolved = self.facts.resolve_callable(
                node.func,
                scope=scope,
                owner_class=owner_class,
            )
            if resolved not in self._guarded_callables:
                continue
            current = self.facts.scopes.parents.get(node)
            while current is not None and current not in self._guarded_callables:
                current = self.facts.scopes.parents.get(current)
            if current is None:
                directly_invoked.add(resolved)
        self._guarded_callables.difference_update(directly_invoked)

    def _is_guarded(self, node: ast.AST) -> bool:
        current: ast.AST | None = node
        while current is not None:
            if current in self._guarded_callables:
                return True
            current = self.facts.scopes.parents.get(current)
        return False

    def _raw_provider_call(self, node: ast.Call) -> str | None:
        dotted = _resolved_dotted_name(node.func, self.facts.aliases)
        if dotted in _RAW_HTTP_MODULE_CALLS:
            return dotted
        if not isinstance(node.func, ast.Attribute):
            return None
        for root, methods in _PROVIDER_SDK_METHODS_BY_ROOT:
            if _path_is_under(self.path, root) and node.func.attr in methods:
                return dotted or _display_node(node.func)
        if node.func.attr not in _RAW_HTTP_METHODS:
            return None
        scope = self.facts.scopes.node_function.get(node)
        owner_class = (
            self.facts.scopes.function_class.get(scope)
            if scope is not None
            else self.facts.scopes.node_class.get(node)
        )
        if not self.facts.expression_is_raw(
            node.func.value,
            scope=scope,
            owner_class=owner_class,
        ):
            return None
        return dotted or _display_node(node.func)

    def _owner(self, node: ast.AST) -> str:
        scope = self.facts.scopes.node_function.get(node)
        names: list[str] = []
        current = scope
        while current is not None:
            names.append(current.name)
            current = self.facts.scopes.function_parent[current]
        names.reverse()
        owner_class = (
            self.facts.scopes.function_class.get(scope)
            if scope is not None
            else self.facts.scopes.node_class.get(node)
        )
        if owner_class is not None:
            names.insert(0, owner_class.name)
        return ".".join(names) or "<module>"

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        outbound = self._raw_provider_call(node)
        if outbound is not None and not self._is_guarded(node):
            owner = self._owner(node)
            rendered = _display_node(node)
            self.findings.append(
                Finding(
                    rule_id=RULE_PROVIDER_TRANSPORT_BYPASS,
                    path=self.path,
                    line_number=node.lineno,
                    signature=f"owner={owner}:call={rendered}",
                    message=(
                        f"{outbound} sends a provider request outside a "
                        "ProviderRequestBinding.execute() callback"
                    ),
                )
            )
        self.generic_visit(node)


class _PythonScanner(ast.NodeVisitor):
    def __init__(
        self,
        *,
        path: Path,
        canonical_source_ids: tuple[str, ...],
        authoritative_catalog_path: Path,
        behavior_selector_ids: Sequence[str] | None = None,
        canonical_literal_aliases: Mapping[str, frozenset[str]] | None = None,
    ) -> None:
        self.path = path
        self.canonical_source_ids = frozenset(canonical_source_ids)
        self.behavior_selector_ids = frozenset(
            behavior_selector_ids or canonical_source_ids
        )
        self.authoritative_catalog_path = authoritative_catalog_path
        self.canonical_literal_aliases = dict(canonical_literal_aliases or {})
        self.findings: list[Finding] = []
        self._definition_depth = 0
        self._qualname: list[str] = []

    def _add(
        self,
        *,
        rule_id: str,
        node: ast.AST,
        signature: str,
        message: str,
    ) -> None:
        self.findings.append(
            Finding(
                rule_id=rule_id,
                path=self.path,
                line_number=getattr(node, "lineno", 1),
                signature=signature,
                message=message,
            )
        )

    def _scan_dispatch_target(
        self,
        *,
        target: ast.AST,
        value: ast.AST | None,
        operation: str,
        node: ast.AST,
    ) -> None:
        if isinstance(target, ast.Name) and target.id in _DISPATCH_NAMES:
            registry = target.id
            keys: list[str] = []
            dynamic_keys = 0
            if isinstance(value, ast.Dict):
                for key in value.keys:
                    if key is None:
                        dynamic_keys += 1
                        continue
                    literal = _literal_string(key)
                    if literal is None:
                        dynamic_keys += 1
                    else:
                        keys.append(literal)
            shape = ",".join(sorted(keys))
            if dynamic_keys:
                shape += f"|dynamic={dynamic_keys}"
            signature = f"{operation}:{registry}:keys={shape or '<none>'}"
            self._add(
                rule_id=RULE_MUTABLE_DISPATCH,
                node=node,
                signature=signature,
                message=(
                    f"{registry} is a mutable dispatch registry; bind source "
                    "implementations through the declarative source contract"
                ),
            )
            return
        if isinstance(target, ast.Subscript):
            registry = _subscript_root_name(target)
            if registry not in _DISPATCH_NAMES:
                return
            key = _literal_string(target.slice) or _display_node(target.slice)
            self._add(
                rule_id=RULE_MUTABLE_DISPATCH,
                node=node,
                signature=f"{operation}:{registry}:key={key}",
                message=(f"{operation} mutates {registry}[{key!r}] for registration"),
            )
            return
        if isinstance(target, (ast.List, ast.Tuple)):
            for child in target.elts:
                self._scan_dispatch_target(
                    target=child,
                    value=value,
                    operation=operation,
                    node=node,
                )

    def _scan_source_list(
        self,
        *,
        targets: Sequence[ast.AST],
        value: ast.AST,
        node: ast.AST,
    ) -> None:
        if self.path == self.authoritative_catalog_path:
            return
        names = _assignment_target_names(targets)
        if not names:
            return
        strings = _collection_strings(value)
        if strings is None:
            return
        unique_strings = frozenset(strings)
        canonical_overlap = unique_strings & self.canonical_source_ids
        minimum_overlap = (
            2
            if self.path in _STRICT_SOURCE_LIST_PATHS
            else max(
                4,
                math.ceil(len(self.canonical_source_ids) * 0.8),
            )
        )
        noncanonical = unique_strings - self.canonical_source_ids
        if len(canonical_overlap) < minimum_overlap or (
            self.path not in _STRICT_SOURCE_LIST_PATHS
            and len(noncanonical) > 1
        ):
            return
        coverage = len(canonical_overlap) / len(self.canonical_source_ids)
        self._add(
            rule_id=RULE_DUPLICATE_SOURCE_IDS,
            node=node,
            signature=(
                f"targets={','.join(sorted(names))}:"
                f"ids={','.join(sorted(unique_strings))}"
            ),
            message=(
                f"{', '.join(names)} duplicates {len(canonical_overlap)}/"
                f"{len(self.canonical_source_ids)} canonical source IDs "
                f"({coverage:.0%}); derive it from the source catalog"
            ),
        )

    def _scan_source_map(
        self,
        *,
        targets: Sequence[ast.AST],
        value: ast.AST,
        node: ast.AST,
    ) -> None:
        if (
            self._definition_depth != 0
            or self.path == self.authoritative_catalog_path
            or self.path in _SOURCE_MAP_EXEMPT_PATHS
            or not isinstance(value, ast.Dict)
        ):
            return
        names = _assignment_target_names(targets)
        if not names:
            return
        literal_keys = tuple(
            key
            for item in value.keys
            if item is not None and (key := _literal_string(item)) is not None
        )
        canonical_keys = frozenset(literal_keys) & self.canonical_source_ids
        if len(canonical_keys) < 4:
            return
        noncanonical_keys = frozenset(literal_keys) - self.canonical_source_ids
        if len(noncanonical_keys) > 2:
            return
        self._add(
            rule_id=RULE_PARALLEL_SOURCE_MAP,
            node=node,
            signature=(
                f"targets={','.join(sorted(names))}:"
                f"ids={','.join(sorted(canonical_keys))}"
            ),
            message=(
                f"{', '.join(names)} duplicates contract-owned metadata for "
                f"{len(canonical_keys)} canonical sources; add the metadata "
                "to SourceDefinition and derive the consumer view"
            ),
        )

    def visit_Assign(self, node: ast.Assign) -> None:  # noqa: N802
        for target in node.targets:
            self._scan_dispatch_target(
                target=target,
                value=node.value,
                operation="assign",
                node=node,
            )
        self._scan_source_list(
            targets=node.targets,
            value=node.value,
            node=node,
        )
        self._scan_source_map(
            targets=node.targets,
            value=node.value,
            node=node,
        )
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:  # noqa: N802
        self._scan_dispatch_target(
            target=node.target,
            value=node.value,
            operation="assign",
            node=node,
        )
        if node.value is not None:
            self._scan_source_list(
                targets=(node.target,),
                value=node.value,
                node=node,
            )
            self._scan_source_map(
                targets=(node.target,),
                value=node.value,
                node=node,
            )
        self.generic_visit(node)

    def visit_AugAssign(self, node: ast.AugAssign) -> None:  # noqa: N802
        self._scan_dispatch_target(
            target=node.target,
            value=node.value,
            operation=f"augassign:{type(node.op).__name__}",
            node=node,
        )
        self.generic_visit(node)

    def visit_Delete(self, node: ast.Delete) -> None:  # noqa: N802
        for target in node.targets:
            self._scan_dispatch_target(
                target=target,
                value=None,
                operation="delete",
                node=node,
            )
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        if isinstance(node.func, ast.Attribute):
            registry = _dotted_name(node.func.value)
            if registry in _DISPATCH_NAMES and node.func.attr in _MUTATING_METHODS:
                arguments = ",".join(_display_node(item) for item in node.args)
                self._add(
                    rule_id=RULE_MUTABLE_DISPATCH,
                    node=node,
                    signature=(f"call:{registry}.{node.func.attr}:args={arguments}"),
                    message=(
                        f"{registry}.{node.func.attr}() mutates a source "
                        "dispatch registry"
                    ),
                )
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:  # noqa: N802
        if not isinstance(node.value, str):
            return
        if (
            self.path != Path("scripts/check_source_architecture_ratchet.py")
            and node.value.startswith("ingest.live.")
        ):
            self._add(
                rule_id=RULE_FABRICATED_LIVE_BINDING,
                node=node,
                signature=f"binding={node.value}",
                message=(
                    f"{node.value!r} is not executable; declare a real live "
                    "launcher or managed ingress boundary in SourceDefinition"
                ),
            )
        arbitrary = _arbitrary_installation_selection(node.value)
        if arbitrary is None:
            return
        table, query_shape = arbitrary
        owner = ".".join(self._qualname) or "<module>"
        self._add(
            rule_id=RULE_ARBITRARY_INSTALLATION_SELECTION,
            node=node,
            signature=f"owner={owner}:table={table}:query={query_shape}",
            message=(
                f"{table} is reduced to an arbitrary LIMIT 1 row; require the "
                "tenant/source installation UUID or return the collection"
            ),
        )

    def visit_Expr(self, node: ast.Expr) -> None:  # noqa: N802
        in_handler_package = self.path.parts[:4] == (
            "services",
            "ingest",
            "ingestion",
            "handlers",
        )
        registration = (
            _handler_registration_call(node.value)
            if self._definition_depth == 0 and in_handler_package
            else None
        )
        if registration is not None:
            rendered = _display_node(registration)
            self._add(
                rule_id=RULE_HANDLER_IMPORT_REGISTRATION,
                node=registration,
                signature=f"call:{rendered}",
                message=(f"{rendered} registers a handler while importing the module"),
            )
        self.generic_visit(node)

    def _scan_handler_decorators(
        self,
        node: ast.AsyncFunctionDef | ast.FunctionDef,
    ) -> None:
        if self._definition_depth != 0:
            return
        in_handler_package = self.path.parts[:4] == (
            "services",
            "ingest",
            "ingestion",
            "handlers",
        )
        if not in_handler_package:
            return
        for decorator in node.decorator_list:
            call = decorator if isinstance(decorator, ast.Call) else None
            name = _dotted_name(call.func) if call is not None else None
            if name is None or name.rsplit(".", 1)[-1] != "register":
                continue
            rendered = _display_node(decorator)
            self._add(
                rule_id=RULE_HANDLER_IMPORT_REGISTRATION,
                node=decorator,
                signature=f"decorator:{node.name}:{rendered}",
                message=(
                    f"{node.name} is registered by {rendered} while importing "
                    "the handler module"
                ),
            )

    def _visit_definition(
        self,
        node: ast.AsyncFunctionDef | ast.FunctionDef,
    ) -> None:
        self._scan_handler_decorators(node)
        self._definition_depth += 1
        self._qualname.append(node.name)
        self.generic_visit(node)
        self._qualname.pop()
        self._definition_depth -= 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:  # noqa: N802
        self._visit_definition(node)

    def visit_AsyncFunctionDef(  # noqa: N802
        self,
        node: ast.AsyncFunctionDef,
    ) -> None:
        self._visit_definition(node)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:  # noqa: N802
        self._definition_depth += 1
        self._qualname.append(node.name)
        self.generic_visit(node)
        self._qualname.pop()
        self._definition_depth -= 1

    def visit_If(self, node: ast.If) -> None:  # noqa: N802
        selected = _selected_source_ids(
            node.test,
            self.behavior_selector_ids,
            self.canonical_literal_aliases,
        )
        if selected and self.path in _SHARED_SOURCE_BEHAVIOR_PATHS:
            function = ".".join(self._qualname) or "<module>"
            self._add(
                rule_id=RULE_SHARED_SOURCE_BEHAVIOR_SWITCH,
                node=node,
                signature=(
                    f"if:{function}:condition={_display_node(node.test)}:"
                    f"sources={','.join(sorted(selected))}"
                ),
                message=(
                    f"shared orchestration branches on "
                    f"{', '.join(sorted(selected))}; resolve provider behavior "
                    "from the source/provider contract"
                ),
            )
        calls = _client_calls(node.body)
        if selected and calls:
            function = ".".join(self._qualname) or "<module>"
            self._add(
                rule_id=RULE_SOURCE_CLIENT_SWITCH,
                node=node,
                signature=(
                    f"if:{function}:condition={_display_node(node.test)}:"
                    f"sources={','.join(sorted(selected))}:"
                    f"calls={','.join(calls)}"
                ),
                message=(
                    f"source switch for {', '.join(sorted(selected))} "
                    f"constructs {', '.join(calls)}; resolve a catalog binding"
                ),
            )
        self.generic_visit(node)

    def visit_IfExp(self, node: ast.IfExp) -> None:  # noqa: N802
        selected = _selected_source_ids(
            node.test,
            self.behavior_selector_ids,
            self.canonical_literal_aliases,
        )
        if selected and self.path in _SHARED_SOURCE_BEHAVIOR_PATHS:
            function = ".".join(self._qualname) or "<module>"
            self._add(
                rule_id=RULE_SHARED_SOURCE_BEHAVIOR_SWITCH,
                node=node,
                signature=(
                    f"ifexp:{function}:condition={_display_node(node.test)}:"
                    f"sources={','.join(sorted(selected))}"
                ),
                message=(
                    f"shared orchestration conditionally selects behavior for "
                    f"{', '.join(sorted(selected))}; resolve it from the "
                    "source/provider contract"
                ),
            )
        self.generic_visit(node)

    def visit_Match(self, node: ast.Match) -> None:  # noqa: N802
        if _source_selector(node.subject):
            function = ".".join(self._qualname) or "<module>"
            for case in node.cases:
                selected = _match_pattern_source_ids(
                    case.pattern,
                    self.behavior_selector_ids,
                    self.canonical_literal_aliases,
                )
                if selected and self.path in _SHARED_SOURCE_BEHAVIOR_PATHS:
                    self._add(
                        rule_id=RULE_SHARED_SOURCE_BEHAVIOR_SWITCH,
                        node=case.pattern,
                        signature=(
                            f"match:{function}:"
                            f"subject={_display_node(node.subject)}:"
                            f"sources={','.join(sorted(selected))}"
                        ),
                        message=(
                            f"shared orchestration match-selects behavior for "
                            f"{', '.join(sorted(selected))}; resolve it from "
                            "the source/provider contract"
                        ),
                    )
                calls = _client_calls(case.body)
                if not selected or not calls:
                    continue
                self._add(
                    rule_id=RULE_SOURCE_CLIENT_SWITCH,
                    node=case.pattern,
                    signature=(
                        f"match:{function}:subject={_display_node(node.subject)}:"
                        f"sources={','.join(sorted(selected))}:"
                        f"calls={','.join(calls)}"
                    ),
                    message=(
                        f"source match case for {', '.join(sorted(selected))} "
                        f"constructs {', '.join(calls)}; resolve a catalog binding"
                    ),
                )
        self.generic_visit(node)


def _preserve_newlines_as_spaces(match: re.Match[str]) -> str:
    return "".join("\n" if char == "\n" else " " for char in match.group(0))


def _strip_sql_comments(text: str) -> str:
    without_blocks = _SQL_BLOCK_COMMENT_RE.sub(_preserve_newlines_as_spaces, text)
    return _SQL_LINE_COMMENT_RE.sub(_preserve_newlines_as_spaces, without_blocks)


def _parse_sql_string_list(raw: str) -> tuple[str, ...] | None:
    values = tuple(
        match.group(0)[1:-1].replace("''", "'")
        for match in _SQL_STRING_RE.finditer(raw)
    )
    if not values:
        return None
    residual = _SQL_STRING_RE.sub("", raw)
    if re.fullmatch(r"[\s,]*", residual) is None:
        return None
    return values


def _scan_sql(
    *,
    relative_path: Path,
    text: str,
    canonical_source_ids: tuple[str, ...],
) -> list[Finding]:
    # Migration 0193 replaces every copied source-membership CHECK with a
    # foreign key to ingestion_source_catalog. Historical migration text is
    # immutable replay history, not active architecture; only a later migration
    # can reintroduce this anti-pattern. SQL outside the ordered migration
    # directory is still scanned because it may represent current schema.
    if relative_path.parts[:2] == ("db", "migrations"):
        migration_match = _MIGRATION_FILENAME_RE.fullmatch(relative_path.name)
        if (
            migration_match is not None
            and int(migration_match.group("number"))
            <= _SOURCE_CATALOG_CUTOVER_MIGRATION
        ):
            return []

    canonical = frozenset(canonical_source_ids)
    stripped = _strip_sql_comments(text)
    anonymous_occurrences: Counter[str] = Counter()
    findings: list[Finding] = []
    for match in _SQL_CHECK_RE.finditer(stripped):
        values = _parse_sql_string_list(match.group("values"))
        if values is None:
            continue
        unique_values = frozenset(values)
        if len(unique_values) < 2 or not unique_values <= canonical:
            continue
        constraint = match.group("constraint")
        value_signature = ",".join(sorted(unique_values))
        if constraint is None:
            anonymous_occurrences[value_signature] += 1
            identity = f"anonymous-{anonymous_occurrences[value_signature]}"
        else:
            identity = constraint.casefold()
        line_number = stripped.count("\n", 0, match.start()) + 1
        findings.append(
            Finding(
                rule_id=RULE_SQL_SOURCE_CHECK,
                path=relative_path,
                line_number=line_number,
                signature=f"constraint={identity}:ids={value_signature}",
                message=(
                    f"SQL source CHECK {identity!r} duplicates "
                    f"{len(unique_values)} catalog source IDs"
                ),
            )
        )
    return findings


def load_canonical_source_ids(
    *,
    repo_root: Path,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> tuple[str, ...]:
    """Read source IDs from ``SOURCE_DEFINITIONS`` without importing services."""
    resolved = _resolve_under_repo(repo_root, catalog_path)
    try:
        tree = ast.parse(resolved.read_text(encoding="utf-8"), filename=str(resolved))
    except (OSError, SyntaxError) as exc:
        raise SourceArchitectureCheckError(
            f"cannot parse canonical source catalog {resolved}: {exc}"
        ) from exc
    for node in tree.body:
        target: ast.AST | None = None
        value: ast.AST | None = None
        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target, value = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            target, value = node.target, node.value
        if (
            isinstance(target, ast.Name)
            and target.id == "SOURCE_DEFINITIONS"
            and value is not None
        ):
            source_ids = _source_definition_ids(value)
            if source_ids is None or len(source_ids) != len(set(source_ids)):
                break
            if not source_ids:
                break
            return source_ids
    raise SourceArchitectureCheckError(
        f"{resolved} must declare a unique literal SOURCE_DEFINITIONS "
        "tuple of _source(...) entries"
    )


def load_contract_provider_ids(
    *,
    repo_root: Path,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> tuple[str, ...]:
    """Read provider identities without importing the production catalog.

    ``PROVIDER_DEFINITIONS`` is assembled from a filtered base tuple, so its
    final assignment is not a literal that an AST-only ratchet can evaluate.
    Each base entry is nevertheless a literal ``ProviderDefinition`` call whose
    first argument (or ``provider_id=`` keyword) names the provider.  Those
    identities matter to shared behavior switches even when they are ingress-only
    providers (Linear and Stripe) or vendor groups whose ID differs from the
    observation source (Google, Atlassian, Intuit, and Meta).
    """

    resolved = _resolve_under_repo(repo_root, catalog_path)
    try:
        tree = ast.parse(resolved.read_text(encoding="utf-8"), filename=str(resolved))
    except (OSError, SyntaxError) as exc:
        raise SourceArchitectureCheckError(
            f"cannot parse canonical source catalog {resolved}: {exc}"
        ) from exc

    provider_ids: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callable_name = _dotted_name(node.func)
        if callable_name is None or callable_name.rsplit(".", 1)[-1] != (
            "ProviderDefinition"
        ):
            continue
        provider_keyword = next(
            (keyword for keyword in node.keywords if keyword.arg == "provider_id"),
            None,
        )
        provider_node = (
            provider_keyword.value
            if provider_keyword is not None
            else node.args[0]
            if node.args
            else None
        )
        provider_id = (
            _literal_string(provider_node) if provider_node is not None else None
        )
        if provider_id is None:
            raise SourceArchitectureCheckError(
                f"{resolved} ProviderDefinition at line {node.lineno} must "
                "declare a literal provider identity"
            )
        provider_ids.append(provider_id)
    if len(provider_ids) != len(set(provider_ids)):
        raise SourceArchitectureCheckError(
            f"{resolved} contains duplicate ProviderDefinition provider IDs"
        )
    return tuple(provider_ids)


def scan_repository(
    *,
    repo_root: Path,
    canonical_source_ids: Sequence[str] | None = None,
    catalog_path: Path = DEFAULT_CATALOG_PATH,
) -> tuple[Finding, ...]:
    """Scan production source without importing it."""
    root = repo_root.resolve()
    authoritative_catalog = _resolve_under_repo(root, catalog_path)
    canonical = tuple(
        canonical_source_ids
        if canonical_source_ids is not None
        else load_canonical_source_ids(repo_root=root, catalog_path=catalog_path)
    )
    if len(canonical) != len(set(canonical)) or not canonical:
        raise SourceArchitectureCheckError(
            "canonical_source_ids must be non-empty and unique"
        )
    provider_ids = (
        load_contract_provider_ids(
            repo_root=root,
            catalog_path=catalog_path,
        )
        if authoritative_catalog.is_file()
        else ()
    )
    behavior_selector_ids = tuple(dict.fromkeys((*canonical, *provider_ids)))
    try:
        authoritative_relative = authoritative_catalog.resolve().relative_to(root)
    except (OSError, ValueError) as exc:
        raise SourceArchitectureCheckError(
            "catalog path must resolve inside repo_root"
        ) from exc

    findings: list[Finding] = []
    for relative in _LEGACY_PROVIDER_HARNESS_PATHS:
        target = root / relative
        if target.exists():
            findings.append(
                Finding(
                    rule_id=RULE_LEGACY_PROVIDER_HARNESS,
                    path=relative,
                    line_number=1,
                    signature=f"legacy-path={relative.as_posix()}",
                    message=(
                        f"{relative.as_posix()} is a retired provider simulator "
                        "surface; port its callers to Provider Lab and delete it"
                    ),
                )
            )

    spammer_endpoint = root / _LEGACY_SPAMMER_ENDPOINT_PATH
    if spammer_endpoint.is_file():
        try:
            endpoint_text = spammer_endpoint.read_text(encoding="utf-8")
        except OSError as exc:
            raise SourceArchitectureCheckError(
                f"cannot read {_LEGACY_SPAMMER_ENDPOINT_PATH}: {exc}"
            ) from exc
        present_markers = tuple(
            marker
            for marker in _LEGACY_SPAMMER_ENDPOINT_MARKERS
            if marker in endpoint_text
        )
        if present_markers:
            findings.append(
                Finding(
                    rule_id=RULE_LEGACY_PROVIDER_HARNESS,
                    path=_LEGACY_SPAMMER_ENDPOINT_PATH,
                    line_number=1,
                    signature=(
                        "single-host-spammer-markers=" + ",".join(present_markers)
                    ),
                    message=(
                        "the single-host spammer endpoint fallback remains; "
                        "use explicit Provider Lab source base URLs"
                    ),
                )
            )

    for path in iter_production_files(root):
        relative = path.relative_to(root)
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise SourceArchitectureCheckError(
                f"cannot read {relative}: {exc}"
            ) from exc
        if path.suffix == ".sql":
            findings.extend(
                _scan_sql(
                    relative_path=relative,
                    text=text,
                    canonical_source_ids=canonical,
                )
            )
            continue
        try:
            tree = ast.parse(text, filename=str(relative))
        except SyntaxError as exc:
            raise SourceArchitectureCheckError(
                f"cannot parse production Python {relative}:{exc.lineno}: {exc.msg}"
            ) from exc
        scanner = _PythonScanner(
            path=relative,
            canonical_source_ids=canonical,
            authoritative_catalog_path=authoritative_relative,
            behavior_selector_ids=behavior_selector_ids,
            canonical_literal_aliases=_canonical_literal_aliases(
                tree,
                frozenset(behavior_selector_ids),
            ),
        )
        scanner.visit(tree)
        findings.extend(scanner.findings)
        if (
            _path_is_under(relative, _PROVIDER_INTEGRATION_ROOT)
            and relative not in _PROVIDER_TRANSPORT_EXEMPT_PATHS
        ):
            outbound_scanner = _ProviderOutboundScanner(
                path=relative,
                tree=tree,
            )
            outbound_scanner.visit(tree)
            findings.extend(outbound_scanner.findings)

    for path in iter_synthetic_binding_files(root):
        relative = path.relative_to(root)
        try:
            tree = ast.parse(
                path.read_text(encoding="utf-8"),
                filename=str(relative),
            )
        except (OSError, SyntaxError) as exc:
            raise SourceArchitectureCheckError(
                f"cannot parse synthetic Python {relative}: {exc}"
            ) from exc
        scanner = _PythonScanner(
            path=relative,
            canonical_source_ids=canonical,
            authoritative_catalog_path=authoritative_relative,
            behavior_selector_ids=behavior_selector_ids,
        )
        scanner.visit(tree)
        findings.extend(
            finding
            for finding in scanner.findings
            if (
                finding.rule_id == RULE_ARBITRARY_INSTALLATION_SELECTION
                or (
                    finding.rule_id == RULE_SHARED_SOURCE_BEHAVIOR_SWITCH
                    and relative in _SHARED_SOURCE_BEHAVIOR_PATHS
                )
            )
        )

    findings.sort(
        key=lambda finding: (
            finding.path.as_posix(),
            finding.line_number,
            finding.rule_id,
            finding.signature,
        )
    )
    keys = [finding.baseline_entry for finding in findings]
    duplicates = [entry for entry, count in Counter(keys).items() if count > 1]
    if duplicates:
        rendered = ", ".join(
            f"{entry.rule_id}:{entry.path}:{entry.signature}" for entry in duplicates
        )
        raise SourceArchitectureCheckError(
            f"scanner produced ambiguous duplicate finding keys: {rendered}"
        )
    return tuple(findings)


def load_baseline(path: Path) -> tuple[BaselineEntry, ...]:
    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceArchitectureCheckError(
            f"cannot load baseline {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise SourceArchitectureCheckError(
            f"{path} must be a schema_version=1 JSON object"
        )
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise SourceArchitectureCheckError(f"{path} entries must be a JSON list")
    entries: list[BaselineEntry] = []
    for index, raw in enumerate(raw_entries):
        if not isinstance(raw, dict):
            raise SourceArchitectureCheckError(
                f"{path} entry {index} must be an object"
            )
        rule_id = raw.get("rule_id")
        entry_path = raw.get("path")
        signature = raw.get("signature")
        if not isinstance(rule_id, str) or rule_id not in RULE_DESCRIPTIONS:
            raise SourceArchitectureCheckError(
                f"{path} entry {index} has unknown rule_id {rule_id!r}"
            )
        if (
            not isinstance(entry_path, str)
            or not entry_path
            or Path(entry_path).is_absolute()
            or ".." in Path(entry_path).parts
        ):
            raise SourceArchitectureCheckError(
                f"{path} entry {index} has an invalid repository-relative path"
            )
        if not isinstance(signature, str) or not signature:
            raise SourceArchitectureCheckError(
                f"{path} entry {index} has an empty signature"
            )
        entries.append(
            BaselineEntry(
                rule_id=rule_id,
                path=entry_path,
                signature=signature,
            )
        )
    if len(entries) != len(set(entries)):
        raise SourceArchitectureCheckError(f"{path} contains duplicate entries")
    return tuple(sorted(entries))


def write_baseline(path: Path, findings: Sequence[Finding]) -> None:
    entries = sorted(finding.baseline_entry for finding in findings)
    payload = {
        "schema_version": 1,
        "description": (
            "Exact legacy source-architecture findings. Remove entries as debt "
            "is retired; do not add entries without architectural review."
        ),
        "entries": [
            {
                "rule_id": entry.rule_id,
                "path": entry.path,
                "signature": entry.signature,
            }
            for entry in entries
        ],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def apply_baseline(
    findings: Sequence[Finding],
    baseline_entries: Sequence[BaselineEntry],
) -> RatchetResult:
    baseline = frozenset(baseline_entries)
    current = {finding.baseline_entry for finding in findings}
    new_findings = tuple(
        finding for finding in findings if finding.baseline_entry not in baseline
    )
    baselined = tuple(
        finding for finding in findings if finding.baseline_entry in baseline
    )
    return RatchetResult(
        findings=tuple(findings),
        new_findings=new_findings,
        baselined_findings=baselined,
        resolved_entries=tuple(sorted(baseline - current)),
    )


def _format_finding(finding: Finding) -> str:
    return (
        f"{finding.rule_id} {finding.path}:{finding.line_number}: "
        f"{finding.message} [signature: {finding.signature}]"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    parser.add_argument(
        "--no-baseline",
        action="store_true",
        help="strict P9 mode: treat every current finding as a violation",
    )
    parser.add_argument(
        "--write-baseline",
        action="store_true",
        help="replace --baseline with the exact current findings",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.no_baseline and args.write_baseline:
        parser.error("--no-baseline and --write-baseline are mutually exclusive")

    repo_root = args.repo_root.resolve()
    baseline_path = _resolve_under_repo(repo_root, args.baseline)
    try:
        findings = scan_repository(
            repo_root=repo_root,
            catalog_path=args.catalog,
        )
        if args.write_baseline:
            write_baseline(baseline_path, findings)
            print(f"Wrote {len(findings)} exact findings to {baseline_path}.")
            return 0
        baseline_entries = () if args.no_baseline else load_baseline(baseline_path)
    except SourceArchitectureCheckError as exc:
        print(
            f"source architecture checker configuration error: {exc}", file=sys.stderr
        )
        return 2

    result = apply_baseline(findings, baseline_entries)
    mode = "strict/no-baseline" if args.no_baseline else "ratchet"
    if result.new_findings:
        print(
            f"Source architecture {mode} violations "
            f"({len(result.new_findings)} new):",
            file=sys.stderr,
        )
        for finding in result.new_findings:
            print(f"  - {_format_finding(finding)}", file=sys.stderr)
        return 1

    print(
        f"Source architecture {mode} passed: "
        f"{len(result.baselined_findings)} baselined, "
        f"{len(result.resolved_entries)} resolved."
    )
    if result.resolved_entries:
        print(
            "Baseline cleanup available: remove resolved entries with "
            "--write-baseline after review."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
