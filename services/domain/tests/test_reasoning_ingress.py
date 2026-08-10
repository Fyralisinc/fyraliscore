from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.domain.reasoning_ingress import (
    default_reasoning_ingress_mode,
    reasoning_ingress_mode,
)


def test_default_mode_is_direct_and_production_can_select_episode(monkeypatch) -> None:
    monkeypatch.delenv("REASONING_INGRESS_MODE", raising=False)
    assert default_reasoning_ingress_mode() == "direct"
    monkeypatch.setenv("REASONING_INGRESS_MODE", "episode")
    assert default_reasoning_ingress_mode() == "episode"
    monkeypatch.setenv("REASONING_INGRESS_MODE", "mixed")
    with pytest.raises(ValidationError, match="direct or episode"):
        default_reasoning_ingress_mode()


async def test_tenant_policy_overrides_deployment_default(monkeypatch) -> None:
    monkeypatch.setenv("REASONING_INGRESS_MODE", "episode")
    conn = AsyncMock()
    conn.fetchval.return_value = "direct"
    tenant_id = uuid7()
    assert await reasoning_ingress_mode(conn, tenant_id=tenant_id) == "direct"
    conn.fetchval.assert_awaited_once_with(
        "SELECT mode FROM reasoning_ingress_policies WHERE tenant_id=$1",
        tenant_id,
    )


async def test_deployment_default_applies_without_tenant_override(monkeypatch) -> None:
    monkeypatch.setenv("REASONING_INGRESS_MODE", "episode")
    conn = AsyncMock()
    conn.fetchval.return_value = None
    assert await reasoning_ingress_mode(conn, tenant_id=uuid7()) == "episode"
