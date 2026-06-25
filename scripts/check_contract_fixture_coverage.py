#!/usr/bin/env python3
"""Fail when a real-provider contract registry entry lacks its fixture."""

from __future__ import annotations

import sys
import importlib.util
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


_framework = _load_module(
    "fyralis_contract_framework",
    REPO_ROOT / "tests" / "contract" / "framework.py",
)
_registry = _load_module(
    "fyralis_contract_registry",
    REPO_ROOT / "tests" / "contract" / "registry.py",
)

fixture_path = _framework.fixture_path
has_fixture = _framework.has_fixture
ContractNeed = _registry.ContractNeed
REGISTRY = _registry.REGISTRY


@dataclass(frozen=True)
class ContractCoverageViolation:
    provider: str
    kind: str
    fixture: str
    path: Path
    finding: str
    message: str


def validate_contract_fixture_coverage(
    needs: Iterable[ContractNeed] = REGISTRY,
) -> list[ContractCoverageViolation]:
    violations: list[ContractCoverageViolation] = []
    for need in needs:
        path = fixture_path(need.provider, need.kind, need.fixture)
        if not has_fixture(need.provider, need.kind, need.fixture):
            violations.append(
                ContractCoverageViolation(
                    provider=need.provider,
                    kind=need.kind,
                    fixture=need.fixture,
                    path=path,
                    finding=need.finding,
                    message=(
                        "contract fixture missing for "
                        f"{need.provider}/{need.kind}/{need.fixture}"
                    ),
                )
            )
    return violations


def format_violations(violations: Sequence[ContractCoverageViolation]) -> str:
    lines = ["Real-provider contract fixture coverage violations:"]
    for violation in violations:
        rel_path = violation.path.relative_to(REPO_ROOT)
        lines.append(
            f"- {rel_path}: {violation.message} "
            f"({violation.finding})"
        )
    return "\n".join(lines)


def main() -> int:
    violations = validate_contract_fixture_coverage()
    if violations:
        print(format_violations(violations))
        return 1
    print("Contract fixture coverage passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
