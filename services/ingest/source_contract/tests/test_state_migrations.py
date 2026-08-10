from __future__ import annotations

import pytest

from services.ingest.source_contract.models import VersionedState
from services.ingest.source_contract.state_migrations import (
    DowngradePolicy,
    StateMigration,
    StateMigrationRegistry,
    assert_mixed_worker_compatibility,
)


def _registry() -> StateMigrationRegistry:
    return StateMigrationRegistry(
        (
            StateMigration(
                kind="cursor",
                from_schema=1,
                to_schema=2,
                upgrade=lambda value: {**value, "checkpoint": value["cursor"]},
                downgrade=lambda value: {"cursor": value["cursor"]},
            ),
        )
    )


def _state(schema: int = 1) -> VersionedState:
    return VersionedState(
        kind="cursor",
        schema_version=schema,
        producing_connector_version="1.0.0",
        revision=1,
        payload={"cursor": "one", **({"checkpoint": "one"} if schema == 2 else {})},
    )


def test_state_upgrade_is_deterministic_and_reversible() -> None:
    first = _registry().migrate(
        _state(), target_schema=2, producing_connector_version="1.1.0"
    )
    second = _registry().migrate(
        _state(), target_schema=2, producing_connector_version="1.1.0"
    )
    assert first == second
    assert _registry().migrate(
        first,
        target_schema=1,
        producing_connector_version="1.0.0",
        downgrade_policy=DowngradePolicy.REQUIRE_REVERSIBLE,
    ).payload == {"cursor": "one"}


def test_mixed_worker_policy_rejects_major_or_schema_drift() -> None:
    assert_mixed_worker_compatibility(
        _state(2),
        worker_connector_version="1.4.0",
        accepted_state_schemas=frozenset({1, 2}),
    )
    with pytest.raises(ValueError, match="major-version"):
        assert_mixed_worker_compatibility(
            _state(2),
            worker_connector_version="2.0.0",
            accepted_state_schemas=frozenset({2}),
        )
