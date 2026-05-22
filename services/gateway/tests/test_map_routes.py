"""
services/gateway/tests/test_map_routes.py — integration tests for the
CEO Map endpoints.

Coverage:
  - tenant isolation on /api/map/snapshot
  - the six MapNode.health enum values
  - change_summary headlines (high activity vs stable)
  - MapEdge.crosses_neighborhood flag
  - /api/map/topology_events since + limit
  - model story: supporting edges + signatures, 404 cross-tenant
  - PCA projection: normalised to [-1, 1], small-tenant fallback
  - /api/map/refresh_projection invalidates cache and refits
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import pytest

from lib.shared.ids import uuid7
from services.topology.umap_projector import (
    CACHE_KEY,
    MIN_MODELS_FOR_UMAP,
    UMAPProjector,
)


# ---------------------------------------------------------------------
# Local helpers — tenant + actor seed (with tenants registry row),
# direct INSERTs into models / model_edges / model_neighborhoods, etc.
# ---------------------------------------------------------------------


def _content_embedding(text: str, dim: int = 768) -> list[float]:
    """Deterministic 768-d unit vector for `models.embedding` inserts."""
    seed = int.from_bytes(
        hashlib.sha256(text.encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v))
    if norm == 0:
        return v
    return [x / norm for x in v]


def _topo_embedding(text: str, dim: int = 128) -> list[float]:
    """Deterministic 128-d unit vector for `models.topo_embedding`."""
    seed = int.from_bytes(
        hashlib.sha256(("topo:" + text).encode()).digest()[:8], "big"
    )
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v))
    if norm == 0:
        return v
    return [x / norm for x in v]


async def _ensure_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2) "
        "ON CONFLICT (id) DO NOTHING",
        tenant_id, f"map-test-{tenant_id}",
    )


async def _seed_observation(
    pool: asyncpg.Pool, tenant_id: UUID
) -> UUID:
    oid = uuid7()
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'test:signal',
            NULL, '{}'::jsonb, 'seed obs',
            NULL, TRUE, 'authoritative',
            $3, '[]'::jsonb
        )
        """,
        oid, tenant_id, f"map-test-obs-{oid}",
    )
    return oid


async def _seed_model(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    natural: str,
    confidence: float = 0.6,
    activation: float = 1.0,
    status: str = "active",
    created_at: datetime | None = None,
    contested: int = 0,
    confirmed: int = 0,
    last_confirmed_at: datetime | None = None,
    archived_at: datetime | None = None,
    archive_reason: str | None = None,
    falsifier: dict | None = None,
    signal_readings: list | None = None,
    proposition_kind: str = "state",
    born_from_event: UUID | None = None,
    set_topo: bool = True,
    topo_seed: str | None = None,
) -> UUID:
    """Insert a Model row directly (bypasses the 9-step pipeline)."""
    if born_from_event is None:
        born_from_event = await _seed_observation(pool, tenant_id)
    mid = uuid7()
    emb = _content_embedding(natural)
    topo = _topo_embedding(topo_seed or natural) if set_topo else None
    proposition = {
        "kind": proposition_kind,
        "subject": "x",
        "assertion": natural,
    }
    if created_at is None:
        created_at = datetime.now(timezone.utc)
    await pool.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status,
            confidence_at_assertion, confirmed_count, contested_count,
            last_confirmed_at, archived_at, archive_reason,
            activation, created_at, topo_embedding, topo_updated_at
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            '{}'::uuid[], '[]'::jsonb,
            '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
            $7, $8::jsonb, $9::jsonb,
            '{}'::uuid[], '{}'::uuid[],
            '{}'::uuid[], $10,
            $7, $11, $12,
            $13, $14, $15,
            $16, $17, $18::vector, $19
        )
        """,
        mid, tenant_id, born_from_event,
        json.dumps(proposition), natural, emb,
        confidence,
        json.dumps(falsifier) if falsifier is not None else None,
        json.dumps(signal_readings or []),
        status,
        confirmed, contested,
        last_confirmed_at,
        archived_at,
        archive_reason,
        activation,
        created_at,
        topo,
        datetime.now(timezone.utc) if topo is not None else None,
    )
    return mid


async def _seed_edge(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    source: UUID,
    target: UUID,
    kind: str,
    weight: float | None = None,
    detected_by: str = "test",
    created_at: datetime | None = None,
) -> UUID:
    eid = uuid7()
    await pool.execute(
        """
        INSERT INTO model_edges (
            id, tenant_id, source_model_id, target_model_id,
            edge_kind, weight, metadata, status, detected_by,
            created_at
        ) VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, 'active', $7,
                  COALESCE($8, now()))
        """,
        eid, tenant_id, source, target, kind, weight, detected_by,
        created_at,
    )
    return eid


async def _seed_neighborhood(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    members: list[UUID],
    named_signature: str | None = None,
    density: float | None = 0.5,
) -> UUID:
    nid = uuid7()
    centroid = _topo_embedding(f"centroid:{nid}")
    await pool.execute(
        """
        INSERT INTO model_neighborhoods (
            id, tenant_id, centroid_topo_embedding, member_model_ids,
            named_signature, density, status
        ) VALUES ($1, $2, $3::vector, $4, $5, $6, 'active')
        """,
        nid, tenant_id, centroid, members, named_signature, density,
    )
    for mid in members:
        await pool.execute(
            """
            INSERT INTO model_neighborhood_membership
                (tenant_id, model_id, neighborhood_id, centrality)
            VALUES ($1, $2, $3, 0.5)
            ON CONFLICT DO NOTHING
            """,
            tenant_id, mid, nid,
        )
    return nid


async def _seed_topology_event(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    kind: str = "emergence",
    neighborhood_id: UUID | None = None,
    members: list[UUID] | None = None,
    occurred_at: datetime | None = None,
    named_signature: str | None = None,
    magnitude: float | None = None,
) -> UUID:
    eid = uuid7()
    await pool.execute(
        """
        INSERT INTO topology_events (
            id, tenant_id, kind, neighborhood_id, member_model_ids,
            occurred_at, named_signature, magnitude, payload
        ) VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()), $7, $8,
                  '{}'::jsonb)
        """,
        eid, tenant_id, kind, neighborhood_id, members or [],
        occurred_at, named_signature, magnitude,
    )
    return eid


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------
# Common fixture: register the test tenants in the tenants registry
# so the FK on tenant-scoped INSERTs (added in migration 0037) doesn't
# blow up when we seed Models/Edges directly.
# ---------------------------------------------------------------------


import pytest_asyncio


@pytest_asyncio.fixture(autouse=True)
async def _ensure_tenants_seeded(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    tenant_id_b: UUID,
):
    await _ensure_tenant(gateway_pool, tenant_id)
    await _ensure_tenant(gateway_pool, tenant_id_b)
    return None


# ---------------------------------------------------------------------
# /api/map/snapshot
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_snapshot_returns_only_tenant_models(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    tenant_id_b: UUID,
    valid_session,
    valid_session_b,
):
    token_a, _ = valid_session
    token_b, _ = valid_session_b
    # Seed 2 models per tenant.
    a1 = await _seed_model(gateway_pool, tenant_id, natural="A1")
    a2 = await _seed_model(gateway_pool, tenant_id, natural="A2")
    b1 = await _seed_model(gateway_pool, tenant_id_b, natural="B1")
    b2 = await _seed_model(gateway_pool, tenant_id_b, natural="B2")

    resp = await client.get("/map/snapshot", headers=_auth(token_a))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    ids = {n["id"] for n in data["nodes"]}
    assert str(a1) in ids
    assert str(a2) in ids
    assert str(b1) not in ids
    assert str(b2) not in ids


@pytest.mark.asyncio
async def test_snapshot_unauthorized_without_token(
    client: httpx.AsyncClient,
):
    resp = await client.get("/map/snapshot")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_snapshot_health_enum_for_each_case(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    now = datetime.now(timezone.utc)

    # archived: status != 'active' (other gates ignored).
    arch = await _seed_model(
        gateway_pool, tenant_id,
        natural="arch", status="archived",
        created_at=now - timedelta(days=2),
        archived_at=now - timedelta(days=1),
        archive_reason="superseded",
    )
    # fresh: created within 7 days, status active.
    fresh = await _seed_model(
        gateway_pool, tenant_id,
        natural="fresh", created_at=now - timedelta(days=3),
    )
    # contested: contested > confirmed, both > 0; older than 7d.
    contested = await _seed_model(
        gateway_pool, tenant_id,
        natural="contested",
        created_at=now - timedelta(days=20),
        contested=3, confirmed=1,
    )
    # solid: confidence ≥ 0.7, confirmed ≥ contested, older than 7d.
    solid = await _seed_model(
        gateway_pool, tenant_id,
        natural="solid",
        confidence=0.8,
        created_at=now - timedelta(days=20),
        confirmed=5, contested=0,
        last_confirmed_at=now - timedelta(days=5),
    )
    # fading: activation < 0.3, older than 7d.
    fading = await _seed_model(
        gateway_pool, tenant_id,
        natural="fading", activation=0.1,
        created_at=now - timedelta(days=20),
    )
    # stable: middle-of-road; confidence 0.5, activation 1.0, no
    # contestation, no recent confirm pressure.
    stable = await _seed_model(
        gateway_pool, tenant_id,
        natural="stable",
        confidence=0.5,
        activation=1.0,
        created_at=now - timedelta(days=20),
    )

    resp = await client.get(
        "/map/snapshot?include_archived=true", headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    by_id = {n["id"]: n for n in resp.json()["nodes"]}
    assert by_id[str(arch)]["health"] == "archived"
    assert by_id[str(fresh)]["health"] == "fresh"
    assert by_id[str(contested)]["health"] == "contested"
    assert by_id[str(solid)]["health"] == "solid"
    assert by_id[str(fading)]["health"] == "fading"
    assert by_id[str(stable)]["health"] == "stable"


@pytest.mark.asyncio
async def test_snapshot_change_summary_headline_high_activity(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    # Seed 12 fresh models (all within the default 7d window).
    for i in range(12):
        await _seed_model(
            gateway_pool, tenant_id, natural=f"recent-{i}",
        )
    resp = await client.get("/map/snapshot", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    headline = resp.json()["change_summary"]["headline"]
    # Starts with a digit → "12 changes since …"
    assert headline[0].isdigit(), headline
    assert "changes" in headline


@pytest.mark.asyncio
async def test_snapshot_change_summary_headline_stable(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    # Seed only old data (created 60 days ago) so the 7-day window
    # is empty.
    old_ts = datetime.now(timezone.utc) - timedelta(days=60)
    await _seed_model(
        gateway_pool, tenant_id, natural="ancient", created_at=old_ts,
    )
    resp = await client.get("/map/snapshot", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    headline = resp.json()["change_summary"]["headline"]
    assert "stable" in headline.lower(), headline


@pytest.mark.asyncio
async def test_snapshot_crosses_neighborhood_flag(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    # Two models in distinct neighborhoods, with a supports edge
    # crossing the boundary.
    m1 = await _seed_model(gateway_pool, tenant_id, natural="m1")
    m2 = await _seed_model(gateway_pool, tenant_id, natural="m2")
    n1 = await _seed_neighborhood(
        gateway_pool, tenant_id, members=[m1], named_signature="N1",
    )
    n2 = await _seed_neighborhood(
        gateway_pool, tenant_id, members=[m2], named_signature="N2",
    )
    await _seed_edge(
        gateway_pool, tenant_id, source=m1, target=m2, kind="supports",
    )

    resp = await client.get("/map/snapshot", headers=_auth(token))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["edges"]) == 1
    assert data["edges"][0]["crosses_neighborhood"] is True


# ---------------------------------------------------------------------
# /api/map/topology_events
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_topology_events_respects_since_and_limit(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    now = datetime.now(timezone.utc)
    # 3 events: 2 inside the since window, 1 outside.
    nbh = await _seed_neighborhood(
        gateway_pool, tenant_id, members=[],
        named_signature="hot zone",
    )
    inside_a = await _seed_topology_event(
        gateway_pool, tenant_id,
        kind="emergence", neighborhood_id=nbh,
        occurred_at=now - timedelta(hours=2),
        magnitude=0.5,
    )
    inside_b = await _seed_topology_event(
        gateway_pool, tenant_id,
        kind="drift", neighborhood_id=nbh,
        occurred_at=now - timedelta(hours=1),
        magnitude=0.7,
    )
    outside = await _seed_topology_event(
        gateway_pool, tenant_id,
        kind="merge", neighborhood_id=nbh,
        occurred_at=now - timedelta(days=10),
        magnitude=0.2,
    )

    # since = 1 day ago -> includes inside_a + inside_b only.
    since = (now - timedelta(days=1)).isoformat()
    resp = await client.get(
        f"/map/topology_events?since={since}",
        headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    ids = {e["id"] for e in resp.json()["events"]}
    assert str(inside_a) in ids
    assert str(inside_b) in ids
    assert str(outside) not in ids
    # named_signature is sourced from the joined neighborhood when not
    # snapshotted on the event row.
    for e in resp.json()["events"]:
        assert e["named_signature"] == "hot zone"

    # limit clamps to 1 result.
    resp_lim = await client.get(
        f"/map/topology_events?since={since}&limit=1",
        headers=_auth(token),
    )
    assert resp_lim.status_code == 200, resp_lim.text
    assert len(resp_lim.json()["events"]) == 1


# ---------------------------------------------------------------------
# /api/map/models/{id}
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_model_story_includes_supporting_edges_with_signatures(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    target = await _seed_model(gateway_pool, tenant_id, natural="target")
    supporter = await _seed_model(gateway_pool, tenant_id, natural="supporter")
    nbh = await _seed_neighborhood(
        gateway_pool, tenant_id, members=[supporter],
        named_signature="engineering velocity",
    )
    await _seed_edge(
        gateway_pool, tenant_id, source=supporter, target=target,
        kind="supports", weight=0.9,
    )

    resp = await client.get(
        f"/map/models/{target}",
        headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["id"] == str(target)
    assert len(body["supporting"]) == 1
    sup = body["supporting"][0]
    assert sup["neighbor_id"] == str(supporter)
    assert sup["edge_kind"] == "supports"
    assert sup["neighbor_neighborhood_signature"] == "engineering velocity"


@pytest.mark.asyncio
async def test_model_story_404_for_other_tenant_model(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id_b: UUID,
    valid_session,
):
    token, _ = valid_session  # tenant A's token
    foreign = await _seed_model(
        gateway_pool, tenant_id_b, natural="foreign",
    )
    resp = await client.get(
        f"/map/models/{foreign}",
        headers=_auth(token),
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------
# PCA projector
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pca_projection_normalized_to_unit_box(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    # Seed 8 models with topo_embedding so UMAP fits comfortably above
    # MIN_MODELS_FOR_UMAP=6.
    for i in range(8):
        await _seed_model(
            gateway_pool, tenant_id, natural=f"pca-{i}",
            topo_seed=f"vec-{i}",
        )
    proj = UMAPProjector(gateway_pool)
    coords = await proj.project(tenant_id)
    assert len(coords) == 8
    for mid, (x, y) in coords.items():
        assert -1.0 <= x <= 1.0, (mid, x)
        assert -1.0 <= y <= 1.0, (mid, y)
    # At least one coord should reach the boundary (max-abs normalized).
    xs = [abs(c[0]) for c in coords.values()]
    ys = [abs(c[1]) for c in coords.values()]
    assert max(xs) == pytest.approx(1.0, abs=1e-9)
    assert max(ys) == pytest.approx(1.0, abs=1e-9)


@pytest.mark.asyncio
async def test_pca_projection_falls_back_for_tiny_tenant(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
):
    # 0 models with topo_embedding -> empty.
    proj = UMAPProjector(gateway_pool)
    coords = await proj.project(tenant_id)
    assert coords == {}

    # 5 models with topo_embedding -> still empty (< MIN_MODELS_FOR_UMAP=6).
    for i in range(5):
        await _seed_model(gateway_pool, tenant_id, natural=f"solo-{i}")
    coords2 = await proj.project(tenant_id)
    assert MIN_MODELS_FOR_UMAP == 6
    assert coords2 == {}


@pytest.mark.asyncio
async def test_refresh_projection_invalidates_cache_and_refits(
    client: httpx.AsyncClient,
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    valid_session,
):
    token, _ = valid_session
    for i in range(8):
        await _seed_model(
            gateway_pool, tenant_id, natural=f"r-{i}",
            topo_seed=f"vec-{i}",
        )
    proj = UMAPProjector(gateway_pool)
    # Fit once.
    first = await proj.project(tenant_id)
    assert len(first) == 8
    cache_row = await gateway_pool.fetchrow(
        "SELECT cached_at, cached_content FROM view_ceo_cache "
        "WHERE tenant_id = $1 AND cache_key = $2",
        tenant_id, CACHE_KEY,
    )
    assert cache_row is not None
    fitted_first = cache_row["cached_at"]

    # Hit the refresh endpoint.
    await asyncio.sleep(0.01)  # ensure cached_at strictly advances
    resp = await client.post(
        "/map/refresh_projection", headers=_auth(token),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["model_count"] == 8
    # Trustworthiness is in [0, 1]; with strongly-clustered synthetic
    # vectors UMAP should get most local structure right.
    assert 0.0 <= body["trustworthiness"] <= 1.0
    assert body["n_neighbors"] >= 1
    assert body["min_dist"] >= 0.0
    cache_row2 = await gateway_pool.fetchrow(
        "SELECT cached_at FROM view_ceo_cache "
        "WHERE tenant_id = $1 AND cache_key = $2",
        tenant_id, CACHE_KEY,
    )
    assert cache_row2 is not None
    assert cache_row2["cached_at"] > fitted_first
