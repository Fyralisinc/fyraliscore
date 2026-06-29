from __future__ import annotations

from scripts.check_contract_fixture_coverage import (
    ContractNeed,
    REGISTRY,
    format_violations,
    validate_contract_fixture_coverage,
)


def test_checked_in_contract_registry_has_all_fixtures() -> None:
    assert validate_contract_fixture_coverage(REGISTRY) == []


def test_detects_missing_contract_fixture() -> None:
    violations = validate_contract_fixture_coverage(
        [
            ContractNeed(
                provider="missing_provider",
                kind="webhook",
                fixture="missing_event",
                finding="#missing",
                we_currently_read="nothing",
                must_confirm="fixture must exist",
            )
        ]
    )

    assert len(violations) == 1
    assert violations[0].provider == "missing_provider"
    assert "missing_provider/webhook/missing_event" in violations[0].message
    formatted = format_violations(violations)
    assert "tests/contract/fixtures/missing_provider/webhook/missing_event.json" in formatted
