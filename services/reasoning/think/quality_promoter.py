"""Promotion helpers for Think quality replay cases.

`/debug/think-quality/cases` produces inspectable cases from recent
production/debug Think runs. This module turns those cases into stable
JSON fixtures that can live under `tests/quality_replay/cases` and be
checked by a lightweight replay-contract test.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_CASE_DIR = Path("tests/quality_replay/cases")


def _slug(value: str) -> str:
    value = value.replace("think-quality:", "")
    value = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value).strip("-")
    return value[:120] or "case"


def promoted_case_document(
    case: dict[str, Any],
    *,
    source: dict[str, Any] | None = None,
    expectation_mode: str = "known_failure",
) -> dict[str, Any]:
    """Wrap one extracted case in a versioned replay fixture document."""
    if expectation_mode not in {"known_failure", "must_pass"}:
        raise ValueError("expectation_mode must be 'known_failure' or 'must_pass'")
    case_id = str(case.get("case_id") or "")
    if not case_id:
        raise ValueError("case is missing case_id")
    flags = [str(flag) for flag in case.get("flags") or []]
    return {
        "schema_version": SCHEMA_VERSION,
        "case_id": case_id,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
        "source": source or {},
        "expectation": {
            "mode": expectation_mode,
            "expected_flags": flags,
            "minimum_selected_context_reference_ratio": 0.20,
            "require_graph_context_when_selected": True,
            "require_edge_ops_when_graph_selected": False,
        },
        "case": case,
    }


def promote_quality_cases(
    cases: list[dict[str, Any]],
    *,
    output_dir: Path = DEFAULT_CASE_DIR,
    source: dict[str, Any] | None = None,
    expectation_mode: str = "known_failure",
    overwrite: bool = False,
) -> list[Path]:
    """Write extracted cases as JSON fixtures and return written paths."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for case in cases:
        doc = promoted_case_document(
            case,
            source=source,
            expectation_mode=expectation_mode,
        )
        path = output_dir / f"{_slug(doc['case_id'])}.json"
        if path.exists() and not overwrite:
            written.append(path)
            continue
        path.write_text(
            json.dumps(doc, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written.append(path)
    return written


def load_promoted_cases(case_dir: Path = DEFAULT_CASE_DIR) -> list[dict[str, Any]]:
    """Load promoted case fixture documents from a directory."""
    if not case_dir.exists():
        return []
    docs: list[dict[str, Any]] = []
    for path in sorted(case_dir.glob("*.json")):
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError(f"{path} is not a JSON object")
        raw["_path"] = str(path)
        docs.append(raw)
    return docs


def evaluate_promoted_case(doc: dict[str, Any]) -> dict[str, Any]:
    """Evaluate a promoted case against its replay expectation.

    This is intentionally deterministic and LLM-free. It validates that
    the fixture remains structurally useful, and supports two modes:

    - `known_failure`: the captured case must still carry the expected
      failure flags. Use this for freshly promoted misses.
    - `must_pass`: the captured/updated case must satisfy quality
      thresholds. Use this after fixing a failure and refreshing the
      fixture expectations.
    """
    failures: list[str] = []
    if int(doc.get("schema_version") or 0) != SCHEMA_VERSION:
        failures.append("schema_version_mismatch")
    case = doc.get("case")
    if not isinstance(case, dict):
        return {"status": "fail", "failures": ["missing_case"]}

    expectation = doc.get("expectation")
    if not isinstance(expectation, dict):
        return {"status": "fail", "failures": ["missing_expectation"]}

    mode = expectation.get("mode")
    flags = {str(flag) for flag in case.get("flags") or []}
    expected_flags = {str(flag) for flag in expectation.get("expected_flags") or []}
    context = case.get("context_use") if isinstance(case.get("context_use"), dict) else {}
    run = case.get("run") if isinstance(case.get("run"), dict) else {}

    if mode == "known_failure":
        missing = sorted(expected_flags - flags)
        if missing:
            failures.append(f"missing_expected_flags:{','.join(missing)}")
    elif mode == "must_pass":
        if flags:
            failures.append("must_pass_case_has_flags")
        ratio = float(
            context.get(
                "selected_context_reference_ratio",
                run.get("selected_context_reference_ratio", 0.0),
            )
            or 0.0
        )
        minimum = float(
            expectation.get("minimum_selected_context_reference_ratio") or 0.0
        )
        if ratio < minimum:
            failures.append("selected_context_reference_ratio_too_low")
        graph_selected = int(
            context.get(
                "graph_selected_model_count",
                run.get("graph_selected_model_count", 0),
            )
            or 0
        )
        graph_used = bool(context.get("graph_context_used"))
        if (
            expectation.get("require_graph_context_when_selected", True)
            and graph_selected > 0
            and not graph_used
        ):
            failures.append("graph_context_not_used")
        edge_ops = int(context.get("edge_ops_count", run.get("edge_ops_count", 0)) or 0)
        if (
            expectation.get("require_edge_ops_when_graph_selected", False)
            and graph_selected > 0
            and edge_ops == 0
        ):
            failures.append("graph_selected_without_edge_ops")
    else:
        failures.append("unknown_expectation_mode")

    return {
        "status": "fail" if failures else "pass",
        "failures": failures,
        "case_id": doc.get("case_id"),
        "mode": mode,
    }


__all__ = [
    "DEFAULT_CASE_DIR",
    "SCHEMA_VERSION",
    "evaluate_promoted_case",
    "load_promoted_cases",
    "promote_quality_cases",
    "promoted_case_document",
]
