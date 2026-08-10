from scripts.check_source_lifecycle_contract import (
    INGESTION_SOURCES,
    REQUIRED_LIFECYCLE_COMMANDS,
    build_parser,
    catalog_by_source,
    parser_commands,
    validate_source_lifecycle_contract,
)


def test_checked_in_source_lifecycle_contract_passes() -> None:
    assert validate_source_lifecycle_contract(
        canonical_sources=INGESTION_SOURCES,
        manifest_sources=tuple(catalog_by_source()),
        commands=parser_commands(build_parser()),
    ) == []


def test_detects_missing_manifest() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("slack", "new_source"),
        manifest_sources=("slack",),
        commands=set(REQUIRED_LIFECYCLE_COMMANDS),
    )
    assert [item.message for item in violations] == [
        "canonical sources missing connector manifests: new_source"
    ]


def test_detects_missing_common_command() -> None:
    violations = validate_source_lifecycle_contract(
        canonical_sources=("slack",),
        manifest_sources=("slack",),
        commands={"status", "pause", "resume", "uninstall"},
    )
    assert [item.message for item in violations] == [
        "common lifecycle CLI missing commands: maintenance"
    ]
