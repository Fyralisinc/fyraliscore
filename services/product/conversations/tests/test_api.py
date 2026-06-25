"""Integration tests for card conversation route access."""
from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.app.gateway.main import build_app
from services.product.conversations.api import build_router
from services.product.conversations.handler import ProbeHandler
from services.product.conversations.repo import ConversationRepo
from services.product.recommendations.tests.conftest import (
    make_recommendation_proposition,
    seed_commitment,
    seed_observation,
    seed_recommendation_model,
)

from services.app.gateway.tests.conftest import (  # noqa: F401
    app_deps,
    gateway_pool,
    rate_limiter,
    seeded_actor,
    tenant_id,
    valid_session,
)


pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def conversations_client(
    app_deps,
) -> AsyncGenerator[httpx.AsyncClient, None]:
    app = build_app(
        pool=app_deps.pool,
        actor_repo=app_deps.actor_repo,
        alias_repo=app_deps.alias_repo,
        embedder=app_deps.embedder,
        rate_limiter=app_deps.rate_limiter,
        configure_logging=False,
    )
    repo = ConversationRepo(app_deps.pool)
    handler = ProbeHandler(repo=repo, pool=app_deps.pool, query_handler=None)
    app.include_router(build_router(repo=repo, handler=handler))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://test",
    ) as client:
        yield client


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def _seed_actor(pool: asyncpg.Pool, tenant: UUID, name: str) -> UUID:
    actor_id = uuid7()
    await pool.execute(
        """
        INSERT INTO actors (id, tenant_id, type, display_name, status)
        VALUES ($1, $2, 'human_internal', $3, 'active')
        """,
        actor_id,
        tenant,
        name,
    )
    return actor_id


async def _seed_recommendation_card(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    actor_id: UUID,
    title: str,
) -> UUID:
    obs_id = await seed_observation(pool, tenant=tenant, actor_id=actor_id)
    commitment_id = await seed_commitment(
        pool,
        tenant=tenant,
        owner_id=actor_id,
        born_from_event=obs_id,
        title=title,
    )
    model_id = await seed_recommendation_model(
        pool,
        tenant=tenant,
        target_actor_id=actor_id,
        born_from_event=obs_id,
        proposition=make_recommendation_proposition(
            target_actor_id=actor_id,
            target_type="commitment",
            target_id=commitment_id,
            qualitative_impact=f"{title} impact",
        ),
        natural=f"Recommendation for {title}",
    )
    await pool.execute(
        "UPDATE models SET visible_to_subjects = FALSE WHERE id = $1",
        model_id,
    )
    return model_id


@pytest.mark.asyncio
async def test_card_conversation_routes_gate_hidden_recommendation_cards(
    conversations_client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
) -> None:
    token, actor_id = valid_session
    visible_card_id = await _seed_recommendation_card(
        gateway_pool,
        tenant=tenant_id,
        actor_id=actor_id,
        title="Visible conversation card",
    )
    other_actor_id = await _seed_actor(
        gateway_pool,
        tenant_id,
        "Hidden Card Owner",
    )
    hidden_card_id = await _seed_recommendation_card(
        gateway_pool,
        tenant=tenant_id,
        actor_id=other_actor_id,
        title="Hidden conversation card",
    )

    visible_probe = await conversations_client.post(
        f"/v1/cards/{visible_card_id}/probe",
        headers=_auth(token),
        json={"kind": "ask", "query": "Why this?"},
    )
    visible_fetch = await conversations_client.get(
        f"/v1/cards/{visible_card_id}/conversation",
        headers=_auth(token),
    )
    hidden_probe = await conversations_client.post(
        f"/v1/cards/{hidden_card_id}/probe",
        headers=_auth(token),
        json={"kind": "ask", "query": "Why this?"},
    )
    hidden_fetch = await conversations_client.get(
        f"/v1/cards/{hidden_card_id}/conversation",
        headers=_auth(token),
    )
    hidden_delete = await conversations_client.delete(
        f"/v1/cards/{hidden_card_id}/conversation",
        headers=_auth(token),
    )
    missing_fetch = await conversations_client.get(
        f"/v1/cards/{uuid4()}/conversation",
        headers=_auth(token),
    )

    assert visible_probe.status_code == 200, visible_probe.text
    assert visible_fetch.status_code == 200, visible_fetch.text
    assert len(visible_fetch.json()["exchanges"]) == 1

    assert hidden_probe.status_code == 403, hidden_probe.text
    assert hidden_probe.json()["detail"] == "card_out_of_scope"
    assert hidden_fetch.status_code == 403, hidden_fetch.text
    assert hidden_delete.status_code == 403, hidden_delete.text
    assert missing_fetch.status_code == 404, missing_fetch.text
    assert missing_fetch.json()["detail"] == "card_not_found"
