"""Deterministic, generation-fenced connector state migrations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from services.ingest.source_contract.host_services import InstallationDataPatch


class StateMigration(Protocol):
    kind: str
    from_version: int
    to_version: int

    def migrate(self, value: Mapping[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True)
class StateMigrationResult:
    kind: str
    from_version: int
    to_version: int
    generation: int


class StateMigrationRunner:
    """Plan a contiguous migration chain and persist one recoverable CAS."""

    def __init__(self, migrations: Sequence[StateMigration]) -> None:
        self._migrations = tuple(migrations)

    def plan(self, kind: str, current: int, target: int) -> tuple[StateMigration, ...]:
        if target < current:
            raise ValueError("state downgrades require an explicit rollback migration")
        result: list[StateMigration] = []
        version = current
        while version < target:
            matches = [
                item
                for item in self._migrations
                if item.kind == kind and item.from_version == version
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"state migration chain for {kind!r} is not unique at v{version}"
                )
            migration = matches[0]
            if migration.to_version <= version or migration.to_version > target:
                raise ValueError("state migration chain is not contiguous")
            result.append(migration)
            version = migration.to_version
        return tuple(result)

    async def migrate(
        self,
        store: Any,
        *,
        kind: str,
        current_version: int,
        target_version: int,
    ) -> StateMigrationResult:
        namespace = f"connector_state:{kind}"
        current = await store.read(namespace)
        generation = current.generation if current is not None else 0
        values = dict(current.values) if current is not None else {}
        state = dict(values.get("state") or {})
        original = dict(state)
        for migration in self.plan(kind, current_version, target_version):
            state = dict(migration.migrate(state))
        next_generation = await store.compare_and_set(
            InstallationDataPatch(
                namespace=namespace,
                expected_generation=generation,
                values={
                    "schema_version": target_version,
                    "state": state,
                    "rollback": {
                        "schema_version": current_version,
                        "state": original,
                    },
                },
            )
        )
        return StateMigrationResult(
            kind=kind,
            from_version=current_version,
            to_version=target_version,
            generation=next_generation,
        )


__all__ = ["StateMigration", "StateMigrationResult", "StateMigrationRunner"]
