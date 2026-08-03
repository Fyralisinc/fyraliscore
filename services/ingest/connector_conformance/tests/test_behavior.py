from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.ingest.connector_conformance.behavior import (
    REQUIRED_BEHAVIORS,
    BehavioralConformanceSuite,
    BehavioralFixture,
    PageEvidence,
    assert_cursor_monotonicity,
    assert_pagination,
    cleanup_idempotency_check,
    lifecycle_sequence_check,
    retry_classification_check,
    stable_operation_check,
    state_migration_check,
    webhook_verification_check,
)


@pytest.mark.asyncio
async def test_complete_behavior_fixture_produces_release_fingerprint() -> None:
    pages = (
        PageEvidence({"after": "1"}, ("one",)),
        PageEvidence(None, ("two",), end_of_data=True),
    )

    async def pagination() -> None:
        await assert_pagination(pages)

    async def cursors() -> None:
        await assert_cursor_monotonicity(pages)

    async def stable_value():
        return {"id": "stable", "at": datetime(2025, 1, 1, tzinfo=timezone.utc)}

    async def yes() -> bool:
        return True

    async def no() -> bool:
        return False

    async def phases():
        return ("Draft", "Authorizing", "Ready")

    async def migration():
        return 1, 2, {"cursor": "x"}, {"cursor": "x"}

    fixture = BehavioralFixture(
        {
            "pagination": pagination,
            "cursor_monotonicity": cursors,
            "identity_stability": stable_operation_check(
                stable_value, label="identity"
            ),
            "reconciliation": stable_operation_check(
                stable_value, label="reconciliation"
            ),
            "retry_classification": retry_classification_check(
                lambda exc: isinstance(exc, TimeoutError),
                transient=TimeoutError(),
                permanent=ValueError(),
            ),
            "webhook_verification": webhook_verification_check(yes, no),
            "normalization": stable_operation_check(
                stable_value, label="normalization"
            ),
            "cleanup": cleanup_idempotency_check(yes),
            "lifecycle": lifecycle_sequence_check(phases),
            "state_migration": state_migration_check(migration),
        }
    )

    report = await BehavioralConformanceSuite().run(
        connector_id="fyralis/test",
        connector_version="1.0.0",
        fixture=fixture,
    )

    assert report.passed
    assert len(report.checks) == len(REQUIRED_BEHAVIORS)
    assert len(report.fingerprint) == 64


@pytest.mark.asyncio
async def test_missing_behavior_is_a_failed_conformance_check() -> None:
    report = await BehavioralConformanceSuite().run(
        connector_id="fyralis/incomplete",
        connector_version="1.0.0",
        fixture=BehavioralFixture({}),
    )
    assert not report.passed
    assert {item.name for item in report.failures} == {
        f"behavior.{name}" for name in REQUIRED_BEHAVIORS
    }
