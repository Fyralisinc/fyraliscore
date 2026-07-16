"""Static import boundary for the active autonomous-company-learning slice."""
from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

ACTIVE_PACKAGE_ROOTS = (
    Path("services/domain/conversation_context"),
    Path("services/domain/entity_grounding"),
    Path("services/domain/source_semantics"),
    Path("services/workers/entity_resolver"),
    Path("services/workers/source_semantic_worker"),
)

ACTIVE_FILES = (
    Path("lib/contracts/conversation_context.py"),
    Path("lib/contracts/entity_mentions.py"),
    Path("lib/contracts/perception.py"),
    Path("lib/contracts/source_semantics.py"),
    Path("lib/evaluation/company_learning.py"),
    Path("services/app/gateway/clarifications_router.py"),
    Path("services/domain/models/epistemic_applier.py"),
)

FORBIDDEN_IMPORT_PREFIXES = (
    "lib.contracts.agency",
    "lib.contracts.execution",
    "lib.contracts.failure",
    "lib.contracts.runtime",
    "services.domain.agency_activation",
    "services.domain.concerns",
    "services.domain.execution",
    "services.domain.intent",
    "services.domain.intervention_runtime",
    "services.domain.outcomes",
    "services.domain.work_scheduling",
    "services.workers.agency_activation_worker",
    "services.workers.intervention_episode_coordinator",
    "services.workers.work_scheduler_worker",
)


def _active_python_files() -> tuple[Path, ...]:
    files = {REPO_ROOT / relative for relative in ACTIVE_FILES}
    for relative_root in ACTIVE_PACKAGE_ROOTS:
        root = REPO_ROOT / relative_root
        files.update(
            path
            for path in root.rglob("*.py")
            if "tests" not in path.relative_to(root).parts
        )
    missing = sorted(
        str(path.relative_to(REPO_ROOT)) for path in files if not path.is_file()
    )
    assert not missing, "active boundary paths are missing:\n" + "\n".join(missing)
    return tuple(sorted(files))


def _module_name(path: Path) -> str:
    relative = path.relative_to(REPO_ROOT).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imported_modules(path: Path, node: ast.Import | ast.ImportFrom) -> set[str]:
    if isinstance(node, ast.Import):
        return {alias.name for alias in node.names}

    current = _module_name(path).split(".")
    current_package = (
        current
        if path.name == "__init__.py"
        else current[:-1]
    )
    if node.level:
        ascend = node.level - 1
        base_parts = current_package[: len(current_package) - ascend]
        if node.module:
            base_parts.extend(node.module.split("."))
        base = ".".join(base_parts)
    else:
        base = node.module or ""

    imported = {base} if base else set()
    for alias in node.names:
        if alias.name == "*":
            continue
        imported.add(f"{base}.{alias.name}" if base else alias.name)
    return imported


def _is_forbidden(module: str) -> bool:
    if module == "lib.contracts":
        return True
    return any(
        module == prefix or module.startswith(prefix + ".")
        for prefix in FORBIDDEN_IMPORT_PREFIXES
    )


def test_active_company_learning_slice_excludes_task_autonomy_imports() -> None:
    violations: list[str] = []
    for path in _active_python_files():
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue
            for module in sorted(_imported_modules(path, node)):
                if _is_forbidden(module):
                    relative = path.relative_to(REPO_ROOT)
                    violations.append(f"{relative}:{node.lineno}: {module}")

    assert not violations, (
        "active autonomous-company-learning code imports dormant task-autonomy "
        "or legacy agency contracts:\n" + "\n".join(violations)
    )
