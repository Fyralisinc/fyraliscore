"""Deterministic connector-state upgrade and downgrade policy.

State is host-owned, so connectors propose schema changes through an explicit
registry.  Workers may read mixed connector patch/minor versions only when the
state schema is accepted; breaking connector-major or state-schema changes are
rejected before execution.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Any

from services.ingest.source_contract.models import VersionedState
from services.ingest.source_contract.versioning import SemanticVersion

StateTransform = Callable[[Mapping[str, Any]], Mapping[str, Any]]


class DowngradePolicy(StrEnum):
    FORBID = "forbid"
    REQUIRE_REVERSIBLE = "require_reversible"


@dataclass(frozen=True)
class StateMigration:
    kind: str
    from_schema: int
    to_schema: int
    upgrade: StateTransform
    downgrade: StateTransform | None = None

    def __post_init__(self) -> None:
        if self.from_schema < 1 or self.to_schema != self.from_schema + 1:
            raise ValueError("state migrations must advance exactly one schema")


class StateMigrationRegistry:
    def __init__(self, migrations: Sequence[StateMigration] = ()) -> None:
        by_edge: dict[tuple[str, int, int], StateMigration] = {}
        for migration in migrations:
            key = (migration.kind, migration.from_schema, migration.to_schema)
            if key in by_edge:
                raise ValueError(f"duplicate state migration: {key}")
            by_edge[key] = migration
        self._by_edge = MappingProxyType(by_edge)

    def migrate(
        self,
        state: VersionedState,
        *,
        target_schema: int,
        producing_connector_version: str,
        downgrade_policy: DowngradePolicy = DowngradePolicy.FORBID,
    ) -> VersionedState:
        if target_schema < 1:
            raise ValueError("target state schema must be positive")
        current = state.schema_version
        payload: Mapping[str, Any] = dict(state.payload)
        while current < target_schema:
            migration = self._by_edge.get((state.kind, current, current + 1))
            if migration is None:
                raise ValueError(
                    f"no upgrade path for {state.kind} schema {current}->{current + 1}"
                )
            payload = dict(migration.upgrade(payload))
            current += 1
        while current > target_schema:
            migration = self._by_edge.get((state.kind, current - 1, current))
            if downgrade_policy is DowngradePolicy.FORBID:
                raise ValueError("state downgrade is forbidden by policy")
            if migration is None or migration.downgrade is None:
                raise ValueError(
                    f"no reversible downgrade for {state.kind} schema {current}"
                )
            payload = dict(migration.downgrade(payload))
            current -= 1
        return VersionedState(
            kind=state.kind,
            schema_version=current,
            producing_connector_version=producing_connector_version,
            revision=state.revision + (0 if current == state.schema_version else 1),
            payload=dict(payload),
        )


def assert_mixed_worker_compatibility(
    state: VersionedState,
    *,
    worker_connector_version: str,
    accepted_state_schemas: frozenset[int],
) -> None:
    producer = SemanticVersion.parse(state.producing_connector_version)
    worker = SemanticVersion.parse(worker_connector_version)
    if producer.major != worker.major:
        raise ValueError(
            "mixed workers cannot cross a connector major-version boundary"
        )
    if state.schema_version not in accepted_state_schemas:
        raise ValueError(
            f"worker does not accept {state.kind} schema {state.schema_version}"
        )


__all__ = [
    "DowngradePolicy",
    "StateMigration",
    "StateMigrationRegistry",
    "StateTransform",
    "assert_mixed_worker_compatibility",
]
