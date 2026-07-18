#!/usr/bin/env python3
"""Mechanical architecture ratchets for Fyralis Core.

These checks intentionally start small. They encode contracts that are already
mostly true so future cleanup can remove allowlist entries instead of rediscovering
the same drift.
"""

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ROOTS = ("services", "lib", "scripts", "tests", "benchmarks")
IGNORED_PARTS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "site",
    "truss_run",
    "truss_run_2",
}

RAW_THINK_TRIGGER_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+think_trigger_queue\b",
    re.IGNORECASE,
)
RAW_MODEL_REEVAL_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+model_reeval_queue\b",
    re.IGNORECASE,
)
RAW_PENDING_POST_COMMIT_ACTION_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+pending_post_commit_actions\b",
    re.IGNORECASE,
)
RAW_THINK_OBLIGATION_INSERT_RE = re.compile(
    r"\bINSERT\s+INTO\s+think_obligations\b",
    re.IGNORECASE,
)
VALIDATE_ONLY_POLICY_ISSUER_RE = re.compile(
    r"\bissue_evaluation_validate_only_policy\b",
)

RAW_THINK_TRIGGER_INSERT_ALLOWED_FILES = {
    Path("services/domain/triggers.py"),
}
RAW_MODEL_REEVAL_INSERT_ALLOWED_FILES = {
    Path("services/domain/triggers.py"),
    # Registry callbacks live in lib/shared to avoid lib -> services imports.
    Path("lib/shared/edge_registry.py"),
}
RAW_PENDING_POST_COMMIT_ACTION_INSERT_ALLOWED_FILES = {
    Path("services/reasoning/think/post_commit.py"),
}
RAW_THINK_OBLIGATION_INSERT_ALLOWED_FILES = {
    Path("services/domain/obligations.py"),
}
VALIDATE_ONLY_POLICY_ISSUER_ALLOWED_FILES = {
    Path("services/evaluation/epistemic_repair/p7_production_runner.py"),
    Path("services/reasoning/think/execution_policy.py"),
}
IMPORT_LINTER_IGNORE_IMPORT_LIMITS = {
    "core never imports the demo / simulation overlays": 0,
    "lib is independent of services (shared libraries never import app code)": 8,
    "reasoning core does not directly import the app, product, or ingest layers": 0,
    "domain does not add new imports of reasoning internals": 15,
    "domain does not add new imports of product code": 1,
    "ingest does not add new imports of app code": 47,
}


@dataclass(frozen=True)
class Violation:
    check: str
    path: Path
    line_number: int
    message: str

    def render(self) -> str:
        return f"{self.path}:{self.line_number}: {self.check}: {self.message}"


def _is_test_path(path: Path) -> bool:
    return (
        "tests" in path.parts
        or path.name.startswith("test_")
        or path.name.endswith("_test.py")
    )


def _iter_python_files(
    *,
    repo_root: Path,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> Iterable[Path]:
    for root_name in roots:
        root = repo_root / root_name
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            rel = path.relative_to(repo_root)
            if any(part in IGNORED_PARTS for part in rel.parts):
                continue
            yield rel


def _find_raw_insert_violations(
    *,
    repo_root: Path,
    roots: Sequence[str],
    pattern: re.Pattern[str],
    allowed_files: set[Path],
    check: str,
    message: str,
) -> list[Violation]:
    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if rel in allowed_files or _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if pattern.search(line):
                violations.append(
                    Violation(
                        check=check,
                        path=rel,
                        line_number=line_number,
                        message=message,
                    )
                )
    return violations


def find_raw_think_trigger_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into think_trigger_queue.

    Tests may still seed queue rows directly. Production code should use
    services.domain.triggers.enqueue_trigger so the queue contract has one
    owning module.
    """

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_THINK_TRIGGER_INSERT_RE,
        allowed_files=RAW_THINK_TRIGGER_INSERT_ALLOWED_FILES,
        check="raw-think-trigger-insert",
        message=("use services.domain.triggers.enqueue_trigger instead of raw SQL"),
    )


def find_raw_model_reeval_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into model_reeval_queue.

    Most producers should use services.domain.triggers.enqueue_model_reeval.
    The edge registry is explicitly allowlisted because it is shared library
    code and cannot import the services layer without breaking architecture
    contracts.
    """

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_MODEL_REEVAL_INSERT_RE,
        allowed_files=RAW_MODEL_REEVAL_INSERT_ALLOWED_FILES,
        check="raw-model-reeval-insert",
        message=(
            "use services.domain.triggers.enqueue_model_reeval instead of raw SQL"
        ),
    )


def find_raw_pending_post_commit_action_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into pending_post_commit_actions."""

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_PENDING_POST_COMMIT_ACTION_INSERT_RE,
        allowed_files=RAW_PENDING_POST_COMMIT_ACTION_INSERT_ALLOWED_FILES,
        check="raw-pending-post-commit-action-insert",
        message=(
            "use services.reasoning.think.post_commit.enqueue_post_commit_actions "
            "instead of raw SQL"
        ),
    )


def find_raw_think_obligation_insert_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Return production-code raw INSERTs into think_obligations."""

    return _find_raw_insert_violations(
        repo_root=repo_root,
        roots=roots,
        pattern=RAW_THINK_OBLIGATION_INSERT_RE,
        allowed_files=RAW_THINK_OBLIGATION_INSERT_ALLOWED_FILES,
        check="raw-think-obligation-insert",
        message="use services.domain.obligations.open_obligation instead of raw SQL",
    )


def find_validate_only_policy_issuer_violations(
    *,
    repo_root: Path = REPO_ROOT,
    roots: Sequence[str] = DEFAULT_ROOTS,
) -> list[Violation]:
    """Keep non-applying Think authority inside the sealed evaluator boundary."""

    violations: list[Violation] = []
    for rel in _iter_python_files(repo_root=repo_root, roots=roots):
        if rel in VALIDATE_ONLY_POLICY_ISSUER_ALLOWED_FILES or _is_test_path(rel):
            continue
        text = (repo_root / rel).read_text(encoding="utf-8", errors="ignore")
        for line_number, line in enumerate(text.splitlines(), start=1):
            if VALIDATE_ONLY_POLICY_ISSUER_RE.search(line):
                violations.append(Violation(
                    check="validate-only-policy-issuer-boundary",
                    path=rel,
                    line_number=line_number,
                    message="validate-only Think policy may only be issued by the P7 evaluator",
                ))
    return violations


def _import_linter_ignore_counts(repo_root: Path) -> dict[str, int]:
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return {}
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    contracts = data.get("tool", {}).get("importlinter", {}).get("contracts", [])
    counts: dict[str, int] = {}
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        name = contract.get("name")
        if not isinstance(name, str):
            continue
        ignored = contract.get("ignore_imports", [])
        counts[name] = len(ignored) if isinstance(ignored, list) else 0
    return counts


def find_import_linter_allowlist_violations(
    *,
    repo_root: Path = REPO_ROOT,
    limits: Mapping[str, int] = IMPORT_LINTER_IGNORE_IMPORT_LIMITS,
) -> list[Violation]:
    """Return import-linter allowlist counts that grew beyond the baseline."""

    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return []

    counts = _import_linter_ignore_counts(repo_root)
    violations: list[Violation] = []
    for contract_name, limit in sorted(limits.items()):
        if contract_name not in counts:
            violations.append(
                Violation(
                    check="import-linter-allowlist-ratchet",
                    path=Path("pyproject.toml"),
                    line_number=1,
                    message=f"missing tracked contract {contract_name!r}",
                )
            )
            continue
        count = counts[contract_name]
        if count > limit:
            violations.append(
                Violation(
                    check="import-linter-allowlist-ratchet",
                    path=Path("pyproject.toml"),
                    line_number=1,
                    message=(
                        f"{contract_name!r} has {count} ignored imports; "
                        f"limit is {limit}"
                    ),
                )
            )
    return violations


def run_checks(repo_root: Path = REPO_ROOT) -> list[Violation]:
    violations: list[Violation] = []
    violations.extend(find_raw_think_trigger_insert_violations(repo_root=repo_root))
    violations.extend(find_raw_model_reeval_insert_violations(repo_root=repo_root))
    violations.extend(
        find_raw_pending_post_commit_action_insert_violations(repo_root=repo_root)
    )
    violations.extend(find_raw_think_obligation_insert_violations(repo_root=repo_root))
    violations.extend(find_validate_only_policy_issuer_violations(repo_root=repo_root))
    violations.extend(find_import_linter_allowlist_violations(repo_root=repo_root))
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=REPO_ROOT,
        help="Repository root to scan.",
    )
    args = parser.parse_args(argv)

    violations = run_checks(args.repo_root.resolve())
    if violations:
        print("Architecture ratchet violations:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation.render()}", file=sys.stderr)
        return 1
    print("Architecture ratchets passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
