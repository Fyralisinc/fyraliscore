#!/usr/bin/env python3
"""Verify cross-tenant boundary tests remain present across core layers."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class BoundaryTestRequirement:
    layer: str
    path: str
    test_name: str
    required_terms: tuple[str, ...] = ()

    @property
    def node_id(self) -> str:
        return f"{self.path}::{self.test_name}"


REQUIREMENTS: tuple[BoundaryTestRequirement, ...] = (
    BoundaryTestRequirement(
        layer="database",
        path="lib/shared/tests/test_rls_isolation.py",
        test_name="test_cross_tenant_select_blocked_via_rls",
        required_terms=("tenant_transaction", "leak-test-b-secret"),
    ),
    BoundaryTestRequirement(
        layer="gateway",
        path="services/app/gateway/tests/test_auth_and_rate_limit.py",
        test_name="test_tenant_a_cannot_see_tenant_b_observations",
        required_terms=("tenant_id_b", "/observations", "beta only"),
    ),
    BoundaryTestRequirement(
        layer="repository",
        path="services/domain/models/tests/test_repo.py",
        test_name="test_tenant_isolation",
        required_terms=("other_tenant", "search_by_scope", "theirs"),
    ),
    BoundaryTestRequirement(
        layer="worker",
        path="services/workers/deadline_resolver/tests/test_worker.py",
        test_name="test_tenant_isolation",
        required_terms=("other_tenant_id", "DeadlineResolver", "trig_b"),
    ),
    BoundaryTestRequirement(
        layer="realtime",
        path="services/app/realtime/tests/test_dispatcher.py",
        test_name="test_tenant_isolation",
        required_terms=("tenant_id_b", "initial_topics", "TimeoutError"),
    ),
)


@dataclass(frozen=True)
class BoundaryTestViolation:
    message: str


def _function_names(source: str) -> set[str]:
    tree = ast.parse(source)
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def validate_cross_tenant_boundary_tests(
    *,
    repo_root: Path = REPO_ROOT,
    requirements: Sequence[BoundaryTestRequirement] = REQUIREMENTS,
    enforce_required_layers: bool = True,
) -> list[BoundaryTestViolation]:
    violations: list[BoundaryTestViolation] = []
    seen_layers: set[str] = set()
    for requirement in requirements:
        seen_layers.add(requirement.layer)
        path = repo_root / requirement.path
        if not path.exists():
            violations.append(
                BoundaryTestViolation(
                    f"{requirement.layer} boundary test file is missing: "
                    f"{requirement.path}"
                )
            )
            continue
        source = path.read_text(encoding="utf-8")
        try:
            names = _function_names(source)
        except SyntaxError as exc:
            violations.append(
                BoundaryTestViolation(
                    f"{requirement.layer} boundary test file has invalid "
                    f"syntax: {requirement.path}: {exc}"
                )
            )
            continue
        if requirement.test_name not in names:
            violations.append(
                BoundaryTestViolation(
                    f"{requirement.layer} boundary test is missing: "
                    f"{requirement.node_id}"
                )
            )
        missing_terms = [
            term for term in requirement.required_terms if term not in source
        ]
        if missing_terms:
            violations.append(
                BoundaryTestViolation(
                    f"{requirement.layer} boundary test lost required "
                    f"cross-tenant evidence terms in {requirement.path}: "
                    + ", ".join(missing_terms)
                )
            )

    required_layers = {"database", "gateway", "repository", "worker", "realtime"}
    missing_layers = sorted(required_layers - seen_layers)
    if enforce_required_layers and missing_layers:
        violations.append(
            BoundaryTestViolation(
                "cross-tenant boundary contract missing layers: "
                + ", ".join(missing_layers)
            )
        )
    return violations


def main() -> int:
    violations = validate_cross_tenant_boundary_tests()
    if violations:
        for violation in violations:
            print(violation.message)
        return 1
    print("Cross-tenant boundary test contract passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
