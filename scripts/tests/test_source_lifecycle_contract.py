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
    entity_table: str | None = None
    entity_install_column: str | None = None
    webhook_installation_id_column: str | None = None
    webhook_installation_id_transform: str | None = None
    native_google_watch_table: bool = False


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


def test_detects_webhook_source_without_local_resolver_cleanup_key() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("quickbooks",),
        dedicated_sources=("quickbooks",),
        generic_sources=(),
        generic_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_specs={
            "quickbooks": _Spec(
                table="quickbooks_installations",
                scope_column="realm_id",
                ref_columns=("secret_ref", "webhook_secret_ref"),
            )
        },
    )

    assert [violation.message for violation in violations] == [
        "dedicated source 'quickbooks' has webhook_secret_ref but no "
        "webhook_installation_id_column for local resolver cleanup"
    ]


def test_detects_native_watch_source_without_watch_entity_mapping() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("google_calendar",),
        dedicated_sources=("google_calendar",),
        generic_sources=(),
        generic_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_specs={
            "google_calendar": _Spec(
                table="google_calendar_installations",
                scope_column="workspace_domain",
                ref_columns=(),
                native_google_watch_table=True,
            )
        },
    )

    assert [violation.message for violation in violations] == [
        "dedicated source 'google_calendar' declares native_google_watch_table "
        "without entity_table/entity_install_column"
    ]


def test_accepts_webhook_source_with_local_resolver_cleanup_key() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("quickbooks",),
        dedicated_sources=("quickbooks",),
        generic_sources=(),
        generic_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_commands=set(REQUIRED_LIFECYCLE_COMMANDS),
        dedicated_specs={
            "quickbooks": _Spec(
                table="quickbooks_installations",
                scope_column="realm_id",
                ref_columns=("secret_ref", "webhook_secret_ref"),
                webhook_installation_id_column="realm_id",
            )
        },
    )

    assert violations == []
