"""Figma reconciliation keeps events and durable design snapshots aligned."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingest.source_contract.runtime import resolve_reconciler
from services.ingest.ingestion.reconcilers import figma as figma_reconciler
from services.ingest.ingestion.workflows.state import WorkflowState


pytestmark = pytest.mark.asyncio


class _Record:
    def __init__(self, **fields):
        self._fields = fields

    def __getitem__(self, key):
        return self._fields[key]


class _Pool:
    def __init__(self, install: _Record, files: list[_Record]):
        self.install = install
        self.files = files

    async def fetchrow(self, _query, *_args):
        return self.install

    async def fetch(self, _query, *_args):
        return self.files


class _Client:
    async def list_events(self, *_args, **_kwargs):
        return [{"createdAt": "2026-07-12T12:00:00Z"}], None, None


def _run(tenant_id):
    return _Record(tenant_id=tenant_id, source="figma")


def _event_shard(install_id):
    return _Record(
        id=uuid4(),
        state="done",
        shard_identifier={
            "shard_kind": "figma_file_events",
            "file_key": "CheckoutFile",
            "file_name": "Checkout redesign",
            "team_id": "oauth-user:designer",
            "installation_id": str(install_id),
        },
    )


async def test_new_figma_event_reshare_also_probes_snapshot_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    install_id = uuid4()
    install = _Record(id=install_id, tenant_id=tenant_id)
    pool = _Pool(
        install,
        [
            _Record(
                file_key="CheckoutFile",
                file_name="Checkout redesign",
                project_name="Growth",
                snapshot_version="version-41",
            ),
        ],
    )
    shard = _event_shard(install_id)

    async def load_cursor(_pool, _kind, workflow_id):
        assert workflow_id == str(shard["id"])
        return WorkflowState(
            workflow_kind="shard_fetch",
            workflow_id=workflow_id,
            tenant_id=None,
            state_data={
                "cursor": {"high_water_created": "2026-07-11T12:00:00Z"},
            },
            last_advanced_at=dt.datetime.now(tz=dt.timezone.utc),
        )

    async def open_client(_install):
        async def close():
            return None
        return _Client(), close

    monkeypatch.setattr(figma_reconciler, "load_state", load_cursor)
    monkeypatch.setattr(figma_reconciler, "_open_figma_client", open_client)
    monkeypatch.setattr(figma_reconciler, "_pool_provider", pool)

    decision = await figma_reconciler.reconcile_figma([shard], _run(tenant_id))

    assert decision.has_gaps is True
    assert [item.shard.shard_kind for item in decision.new_shards] == [
        "figma_file_events",
        "figma_file_snapshot",
    ]
    event, snapshot = decision.new_shards
    assert event.parent_shard_id == shard["id"]
    assert snapshot.parent_shard_id == shard["id"]
    assert snapshot.shard.shard_identifier["snapshot_version"] == "version-41"
    assert snapshot.shard.shard_identifier["project_name"] == "Growth"
    assert snapshot.shard.shard_identifier["file_key"] == "CheckoutFile"


async def test_figma_reconciler_is_wired() -> None:
    assert resolve_reconciler("figma") is figma_reconciler.reconcile_figma
