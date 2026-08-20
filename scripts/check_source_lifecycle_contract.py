#!/usr/bin/env python3
"""Verify every manifest source uses the common operator lifecycle path."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.manage_source_installations import build_parser
from services.ingest.connector_platform.catalog import catalog_by_source
from services.ingest.ingestion.kafka.topics import INGESTION_SOURCES


REQUIRED_LIFECYCLE_COMMANDS = frozenset(
    {"status", "pause", "resume", "maintenance", "uninstall"}
)


@dataclass(frozen=True)
class SourceLifecycleViolation:
    message: str


def parser_commands(parser) -> set[str]:
    for action in parser._actions:  # noqa: SLF001 - argparse has no public API.
        choices = getattr(action, "choices", None)
        if choices:
            return set(choices)
    return set()


def validate_source_lifecycle_contract(
    *,
    canonical_sources: Sequence[str],
    manifest_sources: Sequence[str],
    commands: set[str],
) -> list[SourceLifecycleViolation]:
    violations: list[SourceLifecycleViolation] = []
    canonical = set(canonical_sources)
    manifests = set(manifest_sources)
    missing = sorted(canonical - manifests)
    if missing:
        violations.append(
            SourceLifecycleViolation(
                "canonical sources missing connector manifests: " + ", ".join(missing)
            )
        )
    unknown = sorted(manifests - canonical)
    if unknown:
        violations.append(
            SourceLifecycleViolation(
                "connector manifests reference unknown sources: " + ", ".join(unknown)
            )
        )
    missing_commands = sorted(REQUIRED_LIFECYCLE_COMMANDS - commands)
    if missing_commands:
        violations.append(
            SourceLifecycleViolation(
                "common lifecycle CLI missing commands: " + ", ".join(missing_commands)
            )
        )
    return violations


def main() -> int:
    violations = validate_source_lifecycle_contract(
        canonical_sources=INGESTION_SOURCES,
        manifest_sources=tuple(catalog_by_source()),
        commands=parser_commands(build_parser()),
    )
    if violations:
        for violation in violations:
            print(violation.message)
        return 1
    print(
        f"Source lifecycle contract passed for {len(INGESTION_SOURCES)} sources."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
