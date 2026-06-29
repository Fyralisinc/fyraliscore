#!/usr/bin/env python3
"""Verify every canonical ingestion source has an operator lifecycle path."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.manage_dedicated_source_installations import (  # noqa: E402
    SPECS as DEDICATED_SOURCE_SPECS,
)
from scripts.manage_dedicated_source_installations import (  # noqa: E402
    build_parser as build_dedicated_lifecycle_parser,
)
from scripts.manage_source_installations import (  # noqa: E402
    build_parser as build_generic_lifecycle_parser,
)
from services.ingest.ingestion.kafka.topics import INGESTION_SOURCES  # noqa: E402


GENERIC_PROVIDER_INSTALLATION_SOURCES: tuple[str, ...] = (
    "slack",
    "github",
    "discord",
    "notion",
)
REQUIRED_LIFECYCLE_COMMANDS: frozenset[str] = frozenset(
    {"status", "pause", "resume", "uninstall", "rotate-secret"}
)
WEBHOOK_SECRET_REF_COLUMN = "webhook_secret_ref"


@dataclass(frozen=True)
class SourceLifecycleViolation:
    message: str


def parser_commands(parser) -> set[str]:
    for action in parser._actions:  # noqa: SLF001 - argparse exposes no public API.
        choices = getattr(action, "choices", None)
        if choices:
            return set(choices)
    return set()


def validate_source_lifecycle_contract(
    *,
    canonical_sources: Sequence[str],
    dedicated_sources: Sequence[str],
    generic_sources: Sequence[str],
    generic_commands: set[str],
    dedicated_commands: set[str],
    dedicated_specs: Mapping[str, object],
) -> list[SourceLifecycleViolation]:
    violations: list[SourceLifecycleViolation] = []
    canonical = set(canonical_sources)
    dedicated = set(dedicated_sources)
    generic = set(generic_sources)
    covered = dedicated | generic

    missing = sorted(canonical - covered)
    if missing:
        violations.append(
            SourceLifecycleViolation(
                "canonical sources missing lifecycle coverage: " + ", ".join(missing)
            )
        )
    unknown = sorted(covered - canonical)
    if unknown:
        violations.append(
            SourceLifecycleViolation(
                "lifecycle coverage references unknown sources: " + ", ".join(unknown)
            )
        )
    overlap = sorted(dedicated & generic)
    if overlap:
        violations.append(
            SourceLifecycleViolation(
                "sources cannot be both generic and dedicated lifecycle sources: "
                + ", ".join(overlap)
            )
        )

    for label, commands in (
        ("generic", generic_commands),
        ("dedicated", dedicated_commands),
    ):
        missing_commands = sorted(REQUIRED_LIFECYCLE_COMMANDS - commands)
        if missing_commands:
            violations.append(
                SourceLifecycleViolation(
                    f"{label} lifecycle CLI missing commands: "
                    + ", ".join(missing_commands)
                )
            )

    for source, spec in dedicated_specs.items():
        for attr in ("table", "scope_column", "ref_columns"):
            if not getattr(spec, attr, None) and attr != "ref_columns":
                violations.append(
                    SourceLifecycleViolation(
                        f"dedicated source {source!r} missing {attr}"
                    )
                )
        if not isinstance(getattr(spec, "ref_columns", None), tuple):
            violations.append(
                SourceLifecycleViolation(
                    f"dedicated source {source!r} ref_columns must be a tuple"
                )
            )
        ref_columns = getattr(spec, "ref_columns", ())
        if isinstance(ref_columns, tuple) and WEBHOOK_SECRET_REF_COLUMN in ref_columns:
            if not getattr(spec, "webhook_installation_id_column", None):
                violations.append(
                    SourceLifecycleViolation(
                        f"dedicated source {source!r} has {WEBHOOK_SECRET_REF_COLUMN} "
                        "but no webhook_installation_id_column for local resolver cleanup"
                    )
                )
        if getattr(spec, "webhook_installation_id_transform", None) and not getattr(
            spec,
            "webhook_installation_id_column",
            None,
        ):
            violations.append(
                SourceLifecycleViolation(
                    f"dedicated source {source!r} has webhook_installation_id_transform "
                    "but no webhook_installation_id_column"
                )
            )
        if getattr(spec, "native_google_watch_table", False) and (
            not getattr(spec, "entity_table", None)
            or not getattr(spec, "entity_install_column", None)
        ):
            violations.append(
                SourceLifecycleViolation(
                    f"dedicated source {source!r} declares native_google_watch_table "
                    "without entity_table/entity_install_column"
                )
            )

    return violations


def main() -> int:
    violations = validate_source_lifecycle_contract(
        canonical_sources=INGESTION_SOURCES,
        dedicated_sources=tuple(DEDICATED_SOURCE_SPECS),
        generic_sources=GENERIC_PROVIDER_INSTALLATION_SOURCES,
        generic_commands=parser_commands(build_generic_lifecycle_parser()),
        dedicated_commands=parser_commands(build_dedicated_lifecycle_parser()),
        dedicated_specs=DEDICATED_SOURCE_SPECS,
    )
    if violations:
        for violation in violations:
            print(violation.message)
        return 1
    print("Source lifecycle contract passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
