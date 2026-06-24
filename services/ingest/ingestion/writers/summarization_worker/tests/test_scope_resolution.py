"""Unit tests for the shared scope resolvers (factored out of core.py).

These lock the behavior that `core.py` previously had inline, so the
document-memory worker can rely on the same helpers (§4.3). A fake asyncpg pool
drives the real EntityAliasRepo / ActorRepo without a DB.
"""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from services.ingest.ingestion.scope_resolution import (
    candidate_phrases,
    looks_like_entity,
    resolve_actor_ref,
    resolve_entities_in_text,
)
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo


_TENANT = UUID("33333333-3333-3333-3333-333333333333")


class _FakePool:
    def __init__(self, alias_map, actor_map):
        self._alias_map = alias_map
        self._actor_map = actor_map

    async def fetch(self, query, *args):
        norms = args[1]
        return [
            {"normalized": n, "resolved_entity_ref": json.dumps(self._alias_map[n])}
            for n in norms
            if n in self._alias_map
        ]

    async def fetchval(self, query, *args):
        return self._actor_map.get((args[0], args[1]))


def test_candidate_phrases_and_entity_heuristic():
    assert candidate_phrases("") == []
    phrases = candidate_phrases("Acme renews the SOC2 contract")
    assert any(p == "Acme" for p in phrases)
    assert looks_like_entity("Acme") is True
    assert looks_like_entity("multi-word") is True
    assert looks_like_entity("lowercase") is False


@pytest.mark.asyncio
async def test_resolve_entities_in_text_resolves_and_collects_unresolved():
    pool = _FakePool(
        alias_map={"acme": {"type": "customer", "id": "c-1"}},
        actor_map={},
    )
    repo = EntityAliasRepo(pool)  # type: ignore[arg-type]
    entities, unresolved = await resolve_entities_in_text(
        "Acme is at risk on the Globex deal", repo, _TENANT
    )
    assert {"type": "customer", "id": "c-1"} in entities
    # "Globex" looks like an entity but did not resolve -> unresolved bucket.
    assert "Globex" in unresolved


@pytest.mark.asyncio
async def test_resolve_entities_in_text_is_additive_over_seed():
    pool = _FakePool(alias_map={"acme": {"type": "customer", "id": "c-1"}}, actor_map={})
    repo = EntityAliasRepo(pool)  # type: ignore[arg-type]
    seed = [{"type": "customer", "id": "seed-1"}]
    entities, _ = await resolve_entities_in_text(
        "Acme", repo, _TENANT, seed_entities=seed
    )
    ids = {e["id"] for e in entities}
    assert "seed-1" in ids and "c-1" in ids


@pytest.mark.asyncio
async def test_resolve_entities_in_text_empty_text_returns_seed():
    repo = EntityAliasRepo(_FakePool({}, {}))  # type: ignore[arg-type]
    entities, unresolved = await resolve_entities_in_text("", repo, _TENANT)
    assert entities == [] and unresolved == []


@pytest.mark.asyncio
async def test_resolve_actor_ref_resolves_and_prefixes_channel():
    pool = _FakePool(
        alias_map={},
        actor_map={("slack", "U01"): UUID("22222222-2222-2222-2222-222222222222")},
    )
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    # Bare ref gets the channel prefix.
    actor_id, unresolved = await resolve_actor_ref("U01", "slack", repo)
    assert str(actor_id) == "22222222-2222-2222-2222-222222222222"
    assert unresolved is None


@pytest.mark.asyncio
async def test_resolve_actor_ref_unresolved_returns_text():
    repo = ActorRepo(_FakePool({}, {}))  # type: ignore[arg-type]
    actor_id, unresolved = await resolve_actor_ref("U404", "slack", repo)
    assert actor_id is None
    assert unresolved == "slack:U404"


@pytest.mark.asyncio
async def test_resolve_actor_ref_no_repo_or_ref():
    assert await resolve_actor_ref(None, "slack", None) == (None, None)
    assert await resolve_actor_ref("U01", "slack", None) == (None, None)
