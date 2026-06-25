#!/usr/bin/env python3
"""Fail product HTTP routes that expose raw exception text to callers."""

from __future__ import annotations

import ast
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
PRODUCT_ROOT = REPO_ROOT / "services" / "product"
ERROR_CODE_RE = re.compile(r"^[a-z][a-z0-9_]*(?:\.[a-z0-9_]+)*$")
IMPLEMENTATION_DETAIL_TERMS = (
    "asyncpg",
    "bypassrls",
    "column",
    "database",
    "db_",
    "gateway deps",
    "gateway_deps",
    "migration",
    "pool",
    "postgres",
    "provider_installations",
    "rls",
    "sql",
    "stack",
    "superuser",
    "table",
    "tenant_flags",
    "traceback",
)


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    message: str


def iter_product_files(root: Path = PRODUCT_ROOT) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.py")
        if "tests" not in path.parts and "__pycache__" not in path.parts
    )


def validate_product_error_contract(paths: Iterable[Path]) -> list[Violation]:
    violations: list[Violation] = []
    for path in paths:
        try:
            tree = ast.parse(path.read_text(), filename=str(path))
        except SyntaxError as exc:
            violations.append(
                Violation(path, exc.lineno or 1, f"could not parse Python: {exc.msg}")
            )
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and _call_name(node.func) == "HTTPException":
                detail = _keyword(node, "detail")
                if detail is not None:
                    violations.extend(_validate_http_detail(path, detail))
            if isinstance(node, ast.Call) and _call_name(node.func).endswith("JSONResponse"):
                content = _json_response_content(node)
                if content is not None:
                    violations.extend(_validate_json_response_content(path, content))
    return violations


def _validate_http_detail(path: Path, node: ast.AST) -> list[Violation]:
    implementation_detail = _implementation_detail_violation(path, node)
    if implementation_detail is not None:
        return [implementation_detail]
    if _is_bounded_error_code(node):
        return []
    if isinstance(node, ast.Dict):
        if _contains_raw_text_boundary(node):
            return [
                Violation(
                    path,
                    getattr(node, "lineno", 1),
                    "structured HTTPException detail contains raw exception text",
                )
            ]
        return []
    if _contains_raw_text_boundary(node):
        return [
            Violation(
                path,
                getattr(node, "lineno", 1),
                "HTTPException detail must be a bounded error code",
            )
        ]
    if isinstance(node, (ast.Name, ast.Attribute)):
        return []
    return [
        Violation(
            path,
            getattr(node, "lineno", 1),
            "HTTPException detail must be a bounded error code or structured object",
        )
    ]


def _validate_json_response_content(path: Path, node: ast.AST) -> list[Violation]:
    if not isinstance(node, ast.Dict):
        return []
    violations: list[Violation] = []
    for key, value in zip(node.keys, node.values, strict=False):
        key_value = key.value if isinstance(key, ast.Constant) else None
        if key_value not in {"error", "detail"}:
            continue
        implementation_detail = _implementation_detail_violation(path, value)
        if implementation_detail is not None:
            violations.append(implementation_detail)
            continue
        if _is_bounded_error_code(value):
            continue
        if _contains_raw_text_boundary(value):
            violations.append(
                Violation(
                    path,
                    getattr(value, "lineno", getattr(node, "lineno", 1)),
                    f"JSONResponse {key_value!r} must not expose raw exception text",
                )
            )
        elif isinstance(value, ast.Constant) and isinstance(value.value, str):
            violations.append(
                Violation(
                    path,
                    getattr(value, "lineno", getattr(node, "lineno", 1)),
                    f"JSONResponse {key_value!r} must be a bounded error code",
                )
            )
    return violations


def _implementation_detail_violation(
    path: Path,
    node: ast.AST,
) -> Violation | None:
    for literal in _iter_string_literals(node):
        if _contains_implementation_detail(literal.value):
            return Violation(
                path,
                literal.lineno,
                "response content must not expose implementation details",
            )
    return None


def _contains_raw_text_boundary(node: ast.AST) -> bool:
    if isinstance(node, ast.JoinedStr):
        return not _is_invalid_field_code(node)
    if isinstance(node, ast.BinOp):
        return True
    if isinstance(node, ast.Call):
        name = _call_name(node.func)
        if name == "str" or name.endswith(".format"):
            return True
    return any(_contains_raw_text_boundary(child) for child in ast.iter_child_nodes(node))


def _is_bounded_error_code(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return bool(ERROR_CODE_RE.fullmatch(node.value))
    return _is_invalid_field_code(node)


def _is_invalid_field_code(node: ast.AST) -> bool:
    if not isinstance(node, ast.JoinedStr) or len(node.values) != 2:
        return False
    literal, value = node.values
    if not (
        isinstance(literal, ast.Constant)
        and literal.value == "invalid_"
        and isinstance(value, ast.FormattedValue)
        and value.format_spec is None
    ):
        return False
    return isinstance(value.value, ast.Name) and value.value.id == "field"


def _iter_string_literals(node: ast.AST) -> Iterable[ast.Constant]:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            yield child


def _contains_implementation_detail(value: str) -> bool:
    normalized = value.strip().lower()
    if not normalized:
        return False
    return any(term in normalized for term in IMPLEMENTATION_DETAIL_TERMS)


def _json_response_content(node: ast.Call) -> ast.AST | None:
    if node.args:
        return node.args[0]
    return _keyword(node, "content")


def _keyword(node: ast.Call, name: str) -> ast.AST | None:
    for keyword in node.keywords:
        if keyword.arg == name:
            return keyword.value
    return None


def _call_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = _call_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return ""


def format_violations(violations: Sequence[Violation]) -> str:
    return "\n".join(
        f"{violation.path.relative_to(REPO_ROOT)}:{violation.line}: {violation.message}"
        for violation in violations
    )


def main() -> int:
    violations = validate_product_error_contract(iter_product_files())
    if violations:
        print(format_violations(violations))
        return 1
    print("Product error contract passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
