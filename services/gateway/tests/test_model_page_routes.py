"""
services/gateway/tests/test_model_page_routes.py — integration tests
for the v2 Model page endpoints.

Coverage:
  - /api/model/overview returns 8 locked categories and at least the
    expected relationship bundles for the seeded edges
  - Tenant isolation on overview (B tenant's models never leak)
  - /api/model/categories/{id}/focus surfaces own items + related
    categories
  - /api/model/relationships/{bundleId} returns parsed source/target
  - /api/model/items/{id} returns wired-up neighbors via model_trace
  - /api/model/items/{id}/trace?direction=consequence returns a chain
  - Mode parameter is accepted and reflected in response
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import httpx
import pytest

# Reuse the seed helpers from the map_routes test module — they
# already handle FK + embedding requirements for direct model inserts.
from services.gateway.tests.test_map_routes import (  # type: ignore
    _seed_model,
    _seed_edge,
    _ensure_tenant,
    _auth,
)


@pytest.mark.asyncio
async def test_overview_returns_eight_categories(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    """Even with no models, the overview returns exactly 8 locked
    categories so the UI always has anchors to render. Sparse-tolerance
    is part of the contract — empty categories are fine."""
    token, _ = valid_session
    resp = await client.get("/model/overview", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    ids = {c["id"] for c in data["categories"]}
    expected = {
        "goals", "commitments", "decisions", "risks",
        "customers", "people", "systems", "finance",
    }
    assert ids == expected
    # Layout hints include positions for all 8.
    assert set(data["layoutHints"]["categoryPositions"].keys()) == expected
    # Mode echoes back as 'impact' by default.
    assert data["mode"] == "impact"


@pytest.mark.asyncio
async def test_overview_relationship_bundles_from_edges(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    """Seed a commitment + a decision + a 'contributes_to_resolution'
    edge between them. The overview should expose a relationship
    bundle whose source is `decisions` and target is `commitments` with
    the canonical verb `blocks`."""
    token, _ = valid_session
    commitment = await _seed_model(
        gateway_pool, tenant_id,
        natural="Commitment to ship Q3 roadmap",
        proposition_kind="state",
    )
    decision = await _seed_model(
        gateway_pool, tenant_id,
        natural="Decision D-12 packaging approach",
        proposition_kind="prediction",
    )
    await _seed_edge(
        gateway_pool, tenant_id,
        source=decision, target=commitment, kind="contributes_to_resolution",
    )

    resp = await client.get("/model/overview", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    bundles = data["relationshipBundles"]
    matching = [
        b for b in bundles
        if b["sourceCategoryId"] == "decisions"
        and b["targetCategoryId"] == "commitments"
    ]
    assert matching, (
        f"expected decisions→commitments bundle, got bundles={bundles}"
    )
    bundle = matching[0]
    assert bundle["verb"] == "blocks"
    assert bundle["instanceCount"] >= 1
    assert bundle["id"] == "decisions__blocks__commitments"


@pytest.mark.asyncio
async def test_overview_tenant_isolation(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    tenant_id_b: UUID,
    valid_session,
    valid_session_b,
):
    """Tenant B's models never appear in tenant A's overview."""
    token_a, _ = valid_session
    token_b, _ = valid_session_b
    a1 = await _seed_model(
        gateway_pool, tenant_id, natural="A commitment ship feature",
    )
    b1 = await _seed_model(
        gateway_pool, tenant_id_b, natural="B commitment unrelated",
    )
    resp_a = await client.get("/model/overview", headers=_auth(token_a))
    resp_b = await client.get("/model/overview", headers=_auth(token_b))
    assert resp_a.status_code == 200
    assert resp_b.status_code == 200
    a_items = {
        i["id"]
        for cat in resp_a.json()["categories"]
        for i in cat["topItems"]
    }
    b_items = {
        i["id"]
        for cat in resp_b.json()["categories"]
        for i in cat["topItems"]
    }
    assert str(a1) in a_items
    assert str(b1) not in a_items
    assert str(b1) in b_items
    assert str(a1) not in b_items


@pytest.mark.asyncio
async def test_overview_unauthorized(
    client: httpx.AsyncClient,
):
    """Without a bearer token, the endpoint returns 401."""
    resp = await client.get("/model/overview")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_overview_mode_param_is_accepted(
    client: httpx.AsyncClient,
    valid_session,
):
    """The mode query param is accepted and echoes back in the
    response; an unknown mode falls back to 'impact'."""
    token, _ = valid_session
    for m in ("impact", "dependencies", "ownership", "evidence"):
        r = await client.get(
            f"/model/overview?mode={m}", headers=_auth(token),
        )
        assert r.status_code == 200, r.text
        assert r.json()["mode"] == m
    r = await client.get(
        "/model/overview?mode=invalid", headers=_auth(token),
    )
    assert r.status_code == 200
    assert r.json()["mode"] == "impact"


@pytest.mark.asyncio
async def test_category_focus_returns_category_and_related(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    """Category focus returns the requested category, its own items,
    plus a list of all other 7 categories (related flag set per
    edges)."""
    token, _ = valid_session
    commitment = await _seed_model(
        gateway_pool, tenant_id,
        natural="Commitment ship Q3 roadmap",
        proposition_kind="state",
    )
    decision = await _seed_model(
        gateway_pool, tenant_id,
        natural="Decision D-1 unresolved",
        proposition_kind="prediction",
    )
    await _seed_edge(
        gateway_pool, tenant_id,
        source=decision, target=commitment, kind="contributes_to_resolution",
    )

    resp = await client.get(
        "/model/categories/commitments/focus", headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["category"]["id"] == "commitments"
    # 7 sibling categories returned (everything except commitments).
    related_ids = {c["id"] for c in body["relatedCategories"]}
    assert "commitments" not in related_ids
    assert len(related_ids) == 7
    # decisions is flagged as related because an edge crosses.
    decisions_row = next(
        c for c in body["relatedCategories"] if c["id"] == "decisions"
    )
    assert decisions_row["isRelated"] is True
    # Bundles surface includes the decisions→commitments link.
    bundle_ids = {b["id"] for b in body["relationshipBundles"]}
    assert "decisions__blocks__commitments" in bundle_ids
    # Own items include the seeded commitment.
    assert str(commitment) in {i["id"] for i in body["topItems"]}


@pytest.mark.asyncio
async def test_category_focus_invalid_category_returns_400(
    client: httpx.AsyncClient,
    valid_session,
):
    """An unknown category id is a client error, not a 500."""
    token, _ = valid_session
    resp = await client.get(
        "/model/categories/unknownthing/focus", headers=_auth(token),
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_relationship_focus_returns_endpoints(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    """The relationship-focus endpoint resolves the bundle id back to
    its source/target categories and (when matching edges exist)
    returns concrete instances."""
    token, _ = valid_session
    commitment = await _seed_model(
        gateway_pool, tenant_id,
        natural="Commitment ship feature X",
        proposition_kind="state",
    )
    decision = await _seed_model(
        gateway_pool, tenant_id,
        natural="Decision D-5 packaging",
        proposition_kind="prediction",
    )
    await _seed_edge(
        gateway_pool, tenant_id,
        source=decision, target=commitment, kind="contributes_to_resolution",
    )
    resp = await client.get(
        "/model/relationships/decisions__blocks__commitments",
        headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["bundle"]["verb"] == "blocks"
    assert body["sourceCategory"]["id"] == "decisions"
    assert body["targetCategory"]["id"] == "commitments"
    assert len(body["instances"]) >= 1


@pytest.mark.asyncio
async def test_relationship_focus_invalid_bundle_id_returns_400(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.get(
        "/model/relationships/not-a-valid-id", headers=_auth(token),
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_item_detail_returns_item_and_neighbors(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    """Item detail wraps the model_trace adjacency for the UI."""
    token, _ = valid_session
    commitment = await _seed_model(
        gateway_pool, tenant_id,
        natural="Commitment to stabilize sync",
        proposition_kind="state",
    )
    risk = await _seed_model(
        gateway_pool, tenant_id,
        natural="Risk R-99 sync instability",
        proposition_kind="concern",
    )
    # The risk supports the commitment (evidence relationship).
    await _seed_edge(
        gateway_pool, tenant_id,
        source=risk, target=commitment, kind="supports",
    )
    resp = await client.get(
        f"/model/items/{commitment}", headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["item"]["id"] == str(commitment)
    # The commitment's incoming neighbors are RelationshipInstance
    # objects: { sourceItem, targetItem, verb, ... }. The supporting
    # risk should appear on the source side (it supports the
    # commitment → target).
    incoming_source_ids = {
        n["sourceItem"]["id"] for n in body["neighbors"]["incoming"]
    }
    assert str(risk) in incoming_source_ids


@pytest.mark.asyncio
async def test_item_detail_not_found_returns_404(
    client: httpx.AsyncClient,
    valid_session,
):
    """An item id that doesn't exist (or belongs to another tenant)
    returns 404 — no leak."""
    token, _ = valid_session
    # Made-up but well-formed UUID.
    resp = await client.get(
        "/model/items/00000000-0000-0000-0000-000000000000",
        headers=_auth(token),
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_item_trace_consequence_walks_edges(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    """A consequence trace from the seed item walks outbound
    `supports` / `contributes_to_resolution` edges."""
    token, _ = valid_session
    seed = await _seed_model(
        gateway_pool, tenant_id,
        natural="Seed commitment",
        proposition_kind="state",
    )
    downstream = await _seed_model(
        gateway_pool, tenant_id,
        natural="Downstream customer outcome",
        proposition_kind="market_assessment",
    )
    await _seed_edge(
        gateway_pool, tenant_id,
        source=seed, target=downstream, kind="supports",
    )
    resp = await client.get(
        f"/model/items/{seed}/trace?direction=consequence&depth=3",
        headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["direction"] == "consequence"
    chain_ids = [n["id"] for n in body["nodes"]]
    assert str(seed) in chain_ids
    assert str(downstream) in chain_ids


@pytest.mark.asyncio
async def test_item_trace_invalid_direction_returns_400(
    client: httpx.AsyncClient,
    valid_session,
):
    token, _ = valid_session
    resp = await client.get(
        "/model/items/00000000-0000-0000-0000-000000000000/trace?direction=sideways",
        headers=_auth(token),
    )
    assert resp.status_code == 400
