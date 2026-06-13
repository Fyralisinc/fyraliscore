"""Repository hygiene checks for the layered services layout."""
from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

PYTHON_ROOTS = (
    "services",
    "lib",
    "scripts",
    "tests",
    "simulation",
    "demo",
)

OLD_FLAT_SERVICE_MODULES = {
    "services.actors",
    "services.acts",
    "services.calibration",
    "services.contestability",
    "services.conversations",
    "services.decision_deltas",
    "services.demo",
    "services.dynamics",
    "services.entity_aliases",
    "services.execution",
    "services.forecasts",
    "services.gateway",
    "services.greeting",
    "services.history",
    "services.ingestion",
    "services.integrations",
    "services.judgment",
    "services.models",
    "services.observations",
    "services.query",
    "services.realtime",
    "services.recommendations",
    "services.relationships",
    "services.rendering",
    "services.resources",
    "services.retrieval",
    "services.sage",
    "services.synthetic",
    "services.think",
    "services.today",
    "services.topology",
    "services.webhooks",
}


def _python_files() -> list[Path]:
    files: list[Path] = []
    for root in PYTHON_ROOTS:
        base = REPO_ROOT / root
        if base.is_file() and base.suffix == ".py":
            files.append(base)
            continue
        if not base.exists():
            continue
        files.extend(path for path in base.rglob("*.py") if path.is_file())
    files.append(REPO_ROOT / "conftest.py")
    return sorted(set(files))


def _is_forbidden(module: str | None) -> bool:
    if not module:
        return False
    return any(
        module == old or module.startswith(old + ".")
        for old in OLD_FLAT_SERVICE_MODULES
    )


def test_no_old_flat_service_imports_remain() -> None:
    violations: list[str] = []

    for path in _python_files():
        tree = ast.parse(path.read_text(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if _is_forbidden(alias.name):
                        rel = path.relative_to(REPO_ROOT)
                        violations.append(f"{rel}:{node.lineno}: import {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if _is_forbidden(node.module):
                    rel = path.relative_to(REPO_ROOT)
                    violations.append(
                        f"{rel}:{node.lineno}: from {node.module} import ..."
                    )
            elif isinstance(node, ast.Assign):
                names = [
                    target.id
                    for target in node.targets
                    if isinstance(target, ast.Name)
                ]
                if "pytest_plugins" not in names:
                    continue
                for value in ast.walk(node.value):
                    if isinstance(value, ast.Constant) and isinstance(value.value, str):
                        if _is_forbidden(value.value):
                            rel = path.relative_to(REPO_ROOT)
                            violations.append(
                                f"{rel}:{node.lineno}: pytest_plugins {value.value}"
                            )

    assert not violations, "old flat services imports remain:\n" + "\n".join(
        violations
    )
