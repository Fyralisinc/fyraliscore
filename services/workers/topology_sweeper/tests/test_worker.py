from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.relationships.repo import RelationshipCandidateMetrics
from services.topology import TopologySweepReport
from services.workers.topology_sweeper.worker import run_once


pytestmark = pytest.mark.asyncio


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Conn:
    def __init__(self, tenants):
        self.tenants = tenants

    async def fetch(self, query: str):
        assert "GROUP BY tenant_id" in query
        return [{"tenant_id": tenant_id} for tenant_id in self.tenants]

    def transaction(self):
        return _Tx()


class _Acquire:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


class _Service:
    def __init__(self):
        self.calls: list[tuple[UUID, int, float, bool]] = []

    async def sweep_tenant(
        self,
        conn,
        *,
        tenant_id: UUID,
        limit: int,
        min_activation: float,
        enqueue_think: bool,
    ) -> TopologySweepReport:
        self.calls.append((tenant_id, limit, min_activation, enqueue_think))
        return TopologySweepReport(
            tenant_id=tenant_id,
            models_seen=1,
            candidates_inserted=2,
            think_triggers_enqueued=1,
        )


class _RelationshipRepo:
    async def metrics(self, conn, *, tenant_id: UUID | None = None, since=None):
        del conn, since
        return RelationshipCandidateMetrics(
            total=3,
            by_status={"candidate": 2, "accepted": 1},
            by_kind={"edge": 3},
            by_source={"latent_topology": 3},
        )


async def test_topology_sweeper_runs_bounded_sweep_for_each_tenant() -> None:
    tenants = [uuid7(), uuid7()]
    service = _Service()

    report = await run_once(
        _Pool(_Conn(tenants)),  # type: ignore[arg-type]
        limit_per_tenant=7,
        min_activation=0.42,
        enqueue_think=False,
        service=service,  # type: ignore[arg-type]
        relationship_repo=_RelationshipRepo(),  # type: ignore[arg-type]
    )

    assert [call[0] for call in service.calls] == tenants
    assert all(call[1:] == (7, 0.42, False) for call in service.calls)
    assert report.candidates_inserted == 4
    assert report.think_triggers_enqueued == 2
    assert report.candidate_metrics_after[tenants[0]].open_count == 2


async def test_topology_sweeper_can_target_one_tenant_without_listing() -> None:
    tenant = uuid7()
    service = _Service()

    report = await run_once(
        _Pool(_Conn([uuid7()])),  # type: ignore[arg-type]
        tenant_id=tenant,
        service=service,  # type: ignore[arg-type]
        relationship_repo=_RelationshipRepo(),  # type: ignore[arg-type]
    )

    assert [call[0] for call in service.calls] == [tenant]
    assert list(report.tenant_reports) == [tenant]
