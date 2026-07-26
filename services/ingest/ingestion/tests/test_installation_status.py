"""Unit tests for contract-owned installation status selection."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest

from services.ingest.ingestion.installation_status import (
    load_facebook_pages_installation_status_rows,
    load_managed_installation_status_rows,
    load_provider_installation_status_rows,
)


class _Executor:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.fetches: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetches.append((query, args))
        return self.rows


@pytest.mark.asyncio
async def test_managed_status_collection_returns_every_tenant_source_row() -> None:
    tenant_id = uuid4()
    first_id = uuid4()
    second_id = uuid4()
    installed_at = datetime(2026, 7, 26, tzinfo=timezone.utc)
    executor = _Executor(
        [
            {
                "id": first_id,
                "installed_at": installed_at,
                "enabled": True,
                "business_id": "business-one",
                "base_url": "https://api.ramp.com",
                "token_expires_at": None,
                "webhook_secret_ref": "secret://webhook-one",
                "secret_ref": "secret://access-one",
                "refresh_secret_ref": None,
            },
            {
                "id": second_id,
                "installed_at": installed_at,
                "enabled": False,
                "business_id": "business-two",
                "base_url": "https://api.ramp.com",
                "token_expires_at": None,
                "webhook_secret_ref": None,
                "secret_ref": None,
                "refresh_secret_ref": "secret://refresh-two",
            },
        ]
    )

    rows = await load_managed_installation_status_rows(
        executor,
        tenant_id=tenant_id,
        source="ramp",
    )

    query, arguments = executor.fetches[0]
    assert "FROM ramp_installations AS i" in query
    assert "i.id =" not in query
    assert arguments == (tenant_id,)
    assert [row["installation_id"] for row in rows] == [first_id, second_id]
    assert rows[0]["external_installation_id"] == "business-one"
    assert rows[0]["details"]["webhook_registered"] is True
    assert rows[1]["details"]["webhook_registered"] is False
    assert rows[0]["has_secret"] is True
    assert rows[1]["has_secret"] is True
    assert "secret_ref" not in rows[0]["details"]
    assert "refresh_secret_ref" not in rows[0]["details"]
    assert "webhook_secret_ref" not in rows[0]["details"]


@pytest.mark.asyncio
async def test_provider_status_exact_selection_uses_row_uuid_and_tenant() -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    installed_at = datetime(2026, 7, 26, tzinfo=timezone.utc)
    executor = _Executor(
        [
            {
                "id": installation_id,
                "external_installation_id": "workspace-provider-id",
                "enabled": True,
                "secret_ref": "secret://slack-token",
                "installed_at": installed_at,
            }
        ]
    )

    rows = await load_provider_installation_status_rows(
        executor,
        tenant_id=tenant_id,
        source="slack",
        installation_row_id=installation_id,
        include_disabled=False,
    )

    query, arguments = executor.fetches[0]
    assert "id = $3" in query
    assert "enabled = TRUE" in query
    assert arguments == (tenant_id, "slack", installation_id)
    assert rows == [
        {
            "id": installation_id,
            "installation_id": installation_id,
            "external_installation_id": "workspace-provider-id",
            "enabled": True,
            "has_secret": True,
            "installed_at": installed_at,
            "details": {
                "external_installation_id": "workspace-provider-id",
            },
        }
    ]


@pytest.mark.asyncio
async def test_status_loaders_fail_closed_for_the_wrong_contract_shape() -> None:
    executor = _Executor([])

    with pytest.raises(ValueError, match="has no managed install table"):
        await load_managed_installation_status_rows(
            executor,
            tenant_id=uuid4(),
            source="slack",
        )
    with pytest.raises(
        ValueError,
        match="Facebook Pages status loader received another source",
    ):
        await load_facebook_pages_installation_status_rows(
            executor,
            tenant_id=uuid4(),
            source="slack",
        )

    assert executor.fetches == []
