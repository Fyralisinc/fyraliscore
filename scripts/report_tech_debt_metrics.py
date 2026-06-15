#!/usr/bin/env python3
"""Report lightweight technical-debt metrics for the repository.

This script is intentionally static and infrastructure-free. It is a dashboard,
not a gate: use it to see whether refactor work is shrinking the hotspots that
make the codebase hard to maintain.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.check_architecture_ratchets import (  # noqa: E402
    find_raw_model_reeval_insert_violations,
    find_raw_pending_post_commit_action_insert_violations,
    find_raw_think_trigger_insert_violations,
    find_raw_think_obligation_insert_violations,
)


DEFAULT_ROOTS = ("services", "lib", "scripts", "benchmarks", "tests")
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


@dataclass(frozen=True)
class FileHotspot:
    path: str
    lines: int


@dataclass(frozen=True)
class FunctionHotspot:
    path: str
    line: int
    name: str
    lines: int


@dataclass(frozen=True)
class ClassHotspot:
    path: str
    line: int
    name: str
    lines: int
    methods: int


@dataclass(frozen=True)
class ImportLinterContractMetric:
    name: str
    ignored_imports: int


@dataclass(frozen=True)
class TechDebtReport:
    python_files: int
    python_lines: int
    test_files: int
    non_test_files: int
    files_over_threshold: list[FileHotspot]
    functions_over_threshold: list[FunctionHotspot]
    classes_over_threshold: list[ClassHotspot]
    import_linter_contracts: list[ImportLinterContractMetric]
    import_linter_ignored_imports_total: int
    raw_think_trigger_insert_violations: int
    raw_model_reeval_insert_violations: int
    raw_pending_post_commit_action_insert_violations: int
    raw_think_obligation_insert_violations: int
    parse_errors: dict[str, str]


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


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8", errors="ignore") as fh:
        return sum(1 for _ in fh)


def _node_lines(node: ast.AST) -> int | None:
    lineno = getattr(node, "lineno", None)
    end_lineno = getattr(node, "end_lineno", None)
    if not isinstance(lineno, int) or not isinstance(end_lineno, int):
        return None
    return end_lineno - lineno + 1


def _import_linter_metrics(repo_root: Path) -> list[ImportLinterContractMetric]:
    pyproject = repo_root / "pyproject.toml"
    if not pyproject.exists():
        return []
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    contracts = data.get("tool", {}).get("importlinter", {}).get("contracts", [])
    metrics: list[ImportLinterContractMetric] = []
    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        ignored = contract.get("ignore_imports", [])
        metrics.append(
            ImportLinterContractMetric(
                name=str(contract.get("name", "<unnamed>")),
                ignored_imports=len(ignored) if isinstance(ignored, list) else 0,
            )
        )
    return metrics


def build_report(
    *,
    repo_root: Path = REPO_ROOT,
    file_line_threshold: int = 1500,
    function_line_threshold: int = 200,
    class_line_threshold: int = 600,
    class_method_threshold: int = 15,
) -> TechDebtReport:
    files = sorted(_iter_python_files(repo_root=repo_root))
    file_hotspots: list[FileHotspot] = []
    function_hotspots: list[FunctionHotspot] = []
    class_hotspots: list[ClassHotspot] = []
    parse_errors: dict[str, str] = {}
    total_lines = 0

    for rel in files:
        path = repo_root / rel
        lines = _line_count(path)
        total_lines += lines
        if lines >= file_line_threshold:
            file_hotspots.append(FileHotspot(path=str(rel), lines=lines))
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError as exc:
            parse_errors[str(rel)] = f"{exc.__class__.__name__}: {exc}"
            continue
        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                node_lines = _node_lines(node)
                if node_lines is not None and node_lines >= function_line_threshold:
                    function_hotspots.append(
                        FunctionHotspot(
                            path=str(rel),
                            line=node.lineno,
                            name=node.name,
                            lines=node_lines,
                        )
                    )
            elif isinstance(node, ast.ClassDef):
                node_lines = _node_lines(node)
                method_count = sum(
                    isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                    for child in node.body
                )
                if node_lines is not None and (
                    node_lines >= class_line_threshold
                    or method_count >= class_method_threshold
                ):
                    class_hotspots.append(
                        ClassHotspot(
                            path=str(rel),
                            line=node.lineno,
                            name=node.name,
                            lines=node_lines,
                            methods=method_count,
                        )
                    )

    file_hotspots.sort(key=lambda item: item.lines, reverse=True)
    function_hotspots.sort(key=lambda item: item.lines, reverse=True)
    class_hotspots.sort(key=lambda item: (item.lines, item.methods), reverse=True)
    contract_metrics = _import_linter_metrics(repo_root)
    raw_trigger_violations = find_raw_think_trigger_insert_violations(
        repo_root=repo_root
    )
    raw_model_reeval_violations = find_raw_model_reeval_insert_violations(
        repo_root=repo_root
    )
    raw_pending_post_commit_action_violations = (
        find_raw_pending_post_commit_action_insert_violations(repo_root=repo_root)
    )
    raw_think_obligation_violations = find_raw_think_obligation_insert_violations(
        repo_root=repo_root
    )
    test_files = sum(1 for path in files if _is_test_path(path))

    return TechDebtReport(
        python_files=len(files),
        python_lines=total_lines,
        test_files=test_files,
        non_test_files=len(files) - test_files,
        files_over_threshold=file_hotspots,
        functions_over_threshold=function_hotspots,
        classes_over_threshold=class_hotspots,
        import_linter_contracts=contract_metrics,
        import_linter_ignored_imports_total=sum(
            metric.ignored_imports for metric in contract_metrics
        ),
        raw_think_trigger_insert_violations=len(raw_trigger_violations),
        raw_model_reeval_insert_violations=len(raw_model_reeval_violations),
        raw_pending_post_commit_action_insert_violations=len(
            raw_pending_post_commit_action_violations
        ),
        raw_think_obligation_insert_violations=len(raw_think_obligation_violations),
        parse_errors=parse_errors,
    )


def _top(items: Sequence[Any], limit: int) -> Sequence[Any]:
    return items[: max(0, limit)]


def render_markdown(report: TechDebtReport, *, top: int = 20) -> str:
    lines = [
        "# Technical Debt Metrics",
        "",
        "Static, infrastructure-free snapshot.",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "| --- | ---: |",
        f"| Python files | {report.python_files} |",
        f"| Python lines | {report.python_lines} |",
        f"| Test files | {report.test_files} |",
        f"| Non-test files | {report.non_test_files} |",
        f"| Files above threshold | {len(report.files_over_threshold)} |",
        f"| Functions above threshold | {len(report.functions_over_threshold)} |",
        f"| Classes above threshold | {len(report.classes_over_threshold)} |",
        f"| Import-linter ignored imports | {report.import_linter_ignored_imports_total} |",
        f"| Raw Think trigger insert violations | {report.raw_think_trigger_insert_violations} |",
        f"| Raw model re-eval insert violations | {report.raw_model_reeval_insert_violations} |",
        f"| Raw pending post-commit action insert violations | {report.raw_pending_post_commit_action_insert_violations} |",
        f"| Raw Think obligation insert violations | {report.raw_think_obligation_insert_violations} |",
        f"| Parse errors | {len(report.parse_errors)} |",
        "",
        "## Largest Files",
        "",
        "| Lines | Path |",
        "| ---: | --- |",
    ]
    for item in _top(report.files_over_threshold, top):
        lines.append(f"| {item.lines} | `{item.path}` |")

    lines.extend(
        [
            "",
            "## Longest Functions",
            "",
            "| Lines | Function | Location |",
            "| ---: | --- | --- |",
        ]
    )
    for item in _top(report.functions_over_threshold, top):
        lines.append(f"| {item.lines} | `{item.name}` | `{item.path}:{item.line}` |")

    lines.extend(
        [
            "",
            "## Largest Classes",
            "",
            "| Lines | Methods | Class | Location |",
            "| ---: | ---: | --- | --- |",
        ]
    )
    for item in _top(report.classes_over_threshold, top):
        lines.append(
            f"| {item.lines} | {item.methods} | `{item.name}` | `{item.path}:{item.line}` |"
        )

    lines.extend(
        [
            "",
            "## Import-Linter Allowlists",
            "",
            "| Ignored imports | Contract |",
            "| ---: | --- |",
        ]
    )
    for metric in report.import_linter_contracts:
        lines.append(f"| {metric.ignored_imports} | `{metric.name}` |")

    if report.parse_errors:
        lines.extend(["", "## Parse Errors", "", "| Path | Error |", "| --- | --- |"])
        for path, error in sorted(report.parse_errors.items()):
            lines.append(f"| `{path}` | `{error}` |")

    return "\n".join(lines) + "\n"


def _json_default(value: Any) -> Any:
    if hasattr(value, "__dict__"):
        return asdict(value)
    return str(value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--file-line-threshold", type=int, default=1500)
    parser.add_argument("--function-line-threshold", type=int, default=200)
    parser.add_argument("--class-line-threshold", type=int, default=600)
    parser.add_argument("--class-method-threshold", type=int, default=15)
    args = parser.parse_args(argv)

    report = build_report(
        repo_root=args.repo_root.resolve(),
        file_line_threshold=args.file_line_threshold,
        function_line_threshold=args.function_line_threshold,
        class_line_threshold=args.class_line_threshold,
        class_method_threshold=args.class_method_threshold,
    )
    if args.format == "json":
        json.dump(
            asdict(report), sys.stdout, indent=2, sort_keys=True, default=_json_default
        )
        sys.stdout.write("\n")
    else:
        sys.stdout.write(render_markdown(report, top=args.top))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
