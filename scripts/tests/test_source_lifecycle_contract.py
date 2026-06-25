from __future__ import annotations

from dataclasses import dataclass

from scripts.check_source_lifecycle_contract import (
    DEDICATED_SOURCE_SPECS,
    GENERIC_PROVIDER_INSTALLATION_SOURCES,
    INGESTION_SOURCES,
    REQUIRED_LIFECYCLE_COMMANDS,
    build_dedicated_lifecycle_parser,
    build_generic_lifecycle_parser,
    parser_commands,
    validate_source_lifecycle_contract,
)


@dataclass(frozen=True)
class _Spec:
    table: str
    scope_column: str
    ref_columns: tuple[str, ...]


def test_checked_in_source_lifecycle_contract_passes() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=INGESTION_SOURCES,
        dedicated_sources=tuple(DEDICATED_SOURCE_SPECS),
        generic_sources=GENERIC_PROVIDER_INSTALLATION_SOURCES,
        generic_commands=parser_commands(build_generic_lifecycle_parser()),
        dedicated_commands=parser_commands(build_dedicated_lifecycle_parser()),
        dedicated_specs=DEDICATED_SOURCE_SPECS,
    )

    assert violations == []


def test_detects_missing_source_coverage() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("slack", "new_source"),
        dedicated_sources=(),
        generic_sources=("slack",),
        generic_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_specs={},
    )

    assert [violation.message for violation in violations] == [
        "canonical sources missing lifecycle coverage: new_source"
    ]


def test_detects_missing_cli_command() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("slack",),
        dedicated_sources=(),
        generic_sources=("slack",),
        generic_commands={"status", "pause", "resume", "uninstall"},
        dedicated_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_specs={},
    )

    assert [violation.message for violation in violations] == [
        "generic lifecycle CLI missing commands: rotate-secret"
    ]


def test_accepts_dedicated_specs_without_secret_refs() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("gmail",),
        dedicated_sources=("gmail",),
        generic_sources=(),
        generic_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_specs={
            "gmail": _Spec(
                table="gmail_installations",
                scope_column="workspace_domain",
                ref_columns=(),
            )
        },
    )

    assert violations == []
