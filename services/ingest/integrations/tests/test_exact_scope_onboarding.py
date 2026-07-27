"""Production finalize-install gates for exact provider-scope identity."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass(frozen=True)
class _FinalizeCase:
    source: str
    table: str
    scope_column: str
    scope_argument: str
    collection_argument: str | None


_CASES = (
    _FinalizeCase(
        "mercury",
        "mercury_installations",
        "organization_id",
        "organization_id",
        "accounts",
    ),
    _FinalizeCase(
        "brex",
        "brex_installations",
        "organization_id",
        "organization_id",
        "accounts",
    ),
    _FinalizeCase(
        "deel",
        "deel_installations",
        "organization_id",
        "organization_id",
        "contracts",
    ),
    _FinalizeCase(
        "fireflies",
        "fireflies_installations",
        "workspace_id",
        "workspace_id",
        None,
    ),
    _FinalizeCase(
        "miro",
        "miro_installations",
        "org_id",
        "org_id",
        "boards",
    ),
    _FinalizeCase(
        "figma",
        "figma_installations",
        "team_id",
        "team_id",
        "files",
    ),
)


async def _seed_tenant(pool: asyncpg.Pool, source: str) -> UUID:
    tenant_id = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"exact-finalize-{source}-{tenant_id}",
    )
    return tenant_id


def _finalize_kwargs(case: _FinalizeCase, scope: str) -> dict[str, Any]:
    kwargs: dict[str, Any] = {case.scope_argument: scope}
    if case.collection_argument is not None:
        kwargs[case.collection_argument] = []
    return kwargs


@pytest.mark.parametrize("case", _CASES, ids=lambda item: item.source)
async def test_finalize_install_keys_resolved_rows_by_exact_provider_scope(
    fresh_db: asyncpg.Pool,
    case: _FinalizeCase,
) -> None:
    """Sibling scopes sharing one API host stay distinct; reconnect is stable."""

    finalize_install = getattr(
        import_module(
            f"services.ingest.integrations.{case.source}.onboarding",
        ),
        "finalize_install",
    )
    tenant_id = await _seed_tenant(fresh_db, case.source)
    canonical_base = "https://api.provider.test/"

    first_id = await finalize_install(
        fresh_db,
        tenant_id=tenant_id,
        base_url=canonical_base,
        **_finalize_kwargs(case, "scope-a"),
    )
    second_id = await finalize_install(
        fresh_db,
        tenant_id=tenant_id,
        base_url=canonical_base,
        **_finalize_kwargs(case, "scope-b"),
    )

    assert first_id != second_id
    assert await fresh_db.fetchval(
        f"SELECT count(*) FROM {case.table} WHERE tenant_id = $1",
        tenant_id,
    ) == 2

    # Provider scope remains the identity if the endpoint is reconfigured.
    reconnected_id = await finalize_install(
        fresh_db,
        tenant_id=tenant_id,
        base_url="https://regional.provider.test/",
        **_finalize_kwargs(case, " scope-a "),
    )
    assert reconnected_id == first_id

    row = await fresh_db.fetchrow(
        f"SELECT base_url, {case.scope_column} AS scope "
        f"FROM {case.table} WHERE id = $1",
        first_id,
    )
    assert row["scope"] == "scope-a"
    assert row["base_url"] == "https://regional.provider.test"
