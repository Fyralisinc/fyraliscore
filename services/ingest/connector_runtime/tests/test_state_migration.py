from __future__ import annotations

from dataclasses import dataclass

import pytest

from services.ingest.connector_conformance.fakes import FakeInstallationStore
from services.ingest.connector_runtime.state_migration import StateMigrationRunner
from services.ingest.source_contract.host_services import InstallationDataPatch


@dataclass(frozen=True)
class _RenameCursor:
    kind: str = "poll"
    from_version: int = 1
    to_version: int = 2

    def migrate(self, value):
        return {"after": value.get("cursor")}


@pytest.mark.asyncio
async def test_state_migration_is_contiguous_cas_fenced_and_recoverable() -> None:
    store = FakeInstallationStore()
    await store.compare_and_set(
        InstallationDataPatch(
            namespace="connector_state:poll",
            expected_generation=0,
            values={"schema_version": 1, "state": {"cursor": "page-1"}},
        )
    )
    result = await StateMigrationRunner((_RenameCursor(),)).migrate(
        store,
        kind="poll",
        current_version=1,
        target_version=2,
    )
    saved = await store.read("connector_state:poll")

    assert result.generation == 2
    assert saved is not None
    assert saved.values["state"] == {"after": "page-1"}
    assert saved.values["rollback"] == {
        "schema_version": 1,
        "state": {"cursor": "page-1"},
    }
