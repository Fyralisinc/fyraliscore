from __future__ import annotations

from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from services.platform.access_control.authority import (
    AuthorityDecision,
    ObjectRef,
    Principal,
)
from services.product.today import aggregator


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    async def fetch(self, query: str, *args):
        return self.rows

    async def fetchrow(self, query: str, *args):
        return self.rows[0] if self.rows else None

    async def fetchval(self, query: str, *args):
        return len(self.rows)


async def test_fetch_evidence_filters_unauthorized_observations(monkeypatch) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    allowed_obs = uuid7()
    denied_obs = uuid7()
    conn = _FakeConn([
        {
            "id": allowed_obs,
            "occurred_at": datetime(2026, 6, 24, tzinfo=timezone.utc),
            "kind": "signal",
            "source_channel": "slack",
            "content_text": "Allowed signal",
        },
        {
            "id": denied_obs,
            "occurred_at": datetime(2026, 6, 24, tzinfo=timezone.utc),
            "kind": "signal",
            "source_channel": "finance",
            "content_text": "Restricted ARR signal",
        },
    ])

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        if object_ref.object_id == denied_obs:
            return AuthorityDecision(False, "label_denied:domain:financial")
        return AuthorityDecision(True, "ok")

    monkeypatch.setattr(aggregator, "authorize_read", fake_authorize_read)

    evidence = await aggregator._fetch_evidence(
        ids=[allowed_obs, denied_obs],
        tenant_id=tenant_id,
        conn=conn,  # type: ignore[arg-type]
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
    )

    assert [row["id"] for row in evidence] == [str(allowed_obs)]
    assert "Restricted ARR signal" not in str(evidence)


async def test_financial_metric_returns_none_when_resource_denied(monkeypatch) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    resource_id = uuid7()
    conn = _FakeConn([
        {
            "id": resource_id,
            "current_value": {"value": "123", "unit": "USD"},
            "last_updated_at": datetime(2026, 6, 24, tzinfo=timezone.utc),
        }
    ])

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        return AuthorityDecision(False, "label_denied:domain:financial")

    monkeypatch.setattr(aggregator, "authorize_read", fake_authorize_read)

    metric = await aggregator._financial_resource_metric(
        tenant_id=tenant_id,
        conn=conn,  # type: ignore[arg-type]
        label="ARR",
        identity_match="ARR",
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
    )

    assert metric is None


async def test_recent_signals_reports_only_authorized_rows(monkeypatch) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    allowed_obs = uuid7()
    denied_obs = uuid7()
    now = datetime(2026, 6, 24, 12, tzinfo=timezone.utc)
    conn = _FakeConn([
        {
            "id": denied_obs,
            "kind": "signal",
            "source_channel": "finance",
            "ingested_at": now,
            "content_text": "Restricted finance update",
        },
        {
            "id": allowed_obs,
            "kind": "signal",
            "source_channel": "slack",
            "ingested_at": now,
            "content_text": "Allowed customer update",
        },
    ])

    async def fake_authorize_read(
        principal: Principal,
        purpose: str,
        object_ref: ObjectRef,
        *,
        conn,
    ) -> AuthorityDecision:
        if object_ref.object_id == denied_obs:
            return AuthorityDecision(False, "restricted")
        return AuthorityDecision(True, "ok")

    monkeypatch.setattr(aggregator, "authorize_read", fake_authorize_read)

    feed = await aggregator._build_recent_signals(
        tenant_id=tenant_id,
        conn=conn,  # type: ignore[arg-type]
        now=now,
        principal=Principal(tenant_id=tenant_id, actor_id=actor_id),
    )

    assert feed is not None
    assert feed["total"] == 1
    assert [row["id"] for row in feed["signals"]] == [str(allowed_obs)]
    assert "Restricted finance update" not in str(feed)
