#!/usr/bin/env python3
"""Fail gateway HTTP routes that expose raw exception text to callers."""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
GATEWAY_ROOT = REPO_ROOT / "services" / "app" / "gateway"
EXCEPTION_NAMES = {"e", "exc", "err", "error"}


@dataclass(frozen=True)
class Violation:
    path: Path
    line: int
    message: str


def iter_gateway_files(root: Path = GATEWAY_ROOT) -> list[Path]:
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.py")
        if "tests" not in path.parts and "__pycache__" not in path.parts
    )


def validate_gateway_error_contract(paths: Iterable[Path]) -> list[Violation]:
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
                if detail is not None and _contains_raw_exception_text(detail):
                    violations.append(
                        Violation(
                            path,
                            getattr(detail, "lineno", getattr(node, "lineno", 1)),
                            "HTTPException detail must not expose raw exception text",
                        )
                    )
            if isinstance(node, ast.Call) and _call_name(node.func).endswith(
                "JSONResponse"
            ):
                content = _json_response_content(node)
                if content is not None and _contains_raw_exception_text(content):
                    violations.append(
                        Violation(
                            path,
                            getattr(content, "lineno", getattr(node, "lineno", 1)),
                            "JSONResponse content must not expose raw exception text",
                        )
                    )
    return violations


def _contains_raw_exception_text(node: ast.AST) -> bool:
    if isinstance(node, ast.Call):
        name = _call_name(node.func)
        if name == "str" and node.args and _is_exception_name(node.args[0]):
            return True
        if name.endswith(".format") and any(
            _is_exception_name(arg) for arg in node.args
        ):
            return True
    if isinstance(node, ast.JoinedStr):
        return any(
            isinstance(value, ast.FormattedValue)
            and _is_exception_name(value.value)
            for value in node.values
        )
    return any(_contains_raw_exception_text(child) for child in ast.iter_child_nodes(node))


def _is_exception_name(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id in EXCEPTION_NAMES


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
    violations = validate_gateway_error_contract(iter_gateway_files())
    if violations:
        print(format_violations(violations))
        return 1
    print("Gateway error contract passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
