from __future__ import annotations

import ast
from pathlib import Path


_INTEGRATIONS_ROOT = Path(__file__).resolve().parents[1]


def _called_name(node: ast.Call) -> str | None:
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _production_modules() -> tuple[Path, ...]:
    return tuple(
        path
        for path in _INTEGRATIONS_ROOT.rglob("*.py")
        if "tests" not in path.parts
    )


def test_provider_clients_do_not_construct_local_request_policies() -> None:
    violations: list[str] = []
    for path in _production_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and _called_name(node) == "RequestPolicy"
            ):
                violations.append(
                    f"{path.relative_to(_INTEGRATIONS_ROOT)}:{node.lineno}"
                )

    assert violations == [], (
        "provider clients must resolve source-owned operation policies: "
        + ", ".join(violations)
    )


def test_typed_rate_limits_declare_the_parser_identity() -> None:
    violations: list[str] = []
    for path in _production_modules():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and _called_name(node) == "ProviderRateLimited"
            ):
                continue
            keyword_names = {keyword.arg for keyword in node.keywords}
            if "header_parser_id" not in keyword_names:
                violations.append(
                    f"{path.relative_to(_INTEGRATIONS_ROOT)}:{node.lineno}"
                )

    assert violations == [], (
        "ProviderRateLimited must identify its parser: "
        + ", ".join(violations)
    )
