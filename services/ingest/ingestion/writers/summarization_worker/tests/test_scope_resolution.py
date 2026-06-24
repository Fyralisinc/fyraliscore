"""Unit tests for the shared scope resolvers (factored out of core.py).

These lock the behavior that `core.py` previously had inline, so the
document-memory worker can rely on the same helpers (§4.3). A fake asyncpg pool
drives the real EntityAliasRepo / ActorRepo without a DB.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from services.ingest.ingestion.scope_resolution import (
    candidate_phrases,
    looks_like_entity,
    resolve_actor_ref,
    resolve_entities_in_text,
    resolve_owner_actor,
)
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo


_TENANT = UUID("33333333-3333-3333-3333-333333333333")


class _FakePool:
    def __init__(self, alias_map, actor_map, display_name_map=None):
        self._alias_map = alias_map
        self._actor_map = actor_map
        self._display_name_map = display_name_map or {}

    async def fetch(self, query, *args):
        if "FROM actors" in query:
            tenant_id = args[0]
            return [
                {
                    "id": UUID(actor_id),
                    "tenant_id": tenant_id,
                    "type": "human_internal",
                    "display_name": name,
                    "email": None,
                    "status": "active",
                    "metadata": "{}",
                    "specification_id": None,
                    "created_at": datetime(2026, 6, 1, tzinfo=timezone.utc),
                    "last_seen_at": None,
                }
                for name, actor_id in self._display_name_map.items()
            ]
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


# --- resolve_owner_actor: the action-item-owner display-name fallback (Task #4)


@pytest.mark.asyncio
async def test_resolve_owner_actor_prefers_source_ref_path():
    # When the bare name happens to be a known source ref, the source-ref path
    # wins (no display-name scan needed).
    actor = UUID("22222222-2222-2222-2222-222222222222")
    pool = _FakePool({}, {("slack", "alice"): actor})
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    resolved, unresolved = await resolve_owner_actor("alice", "slack", _TENANT, repo)
    assert resolved == actor and unresolved is None


@pytest.mark.asyncio
async def test_resolve_owner_actor_display_name_fallback():
    # The bare display string does NOT match a source ref; the display-name
    # fallback resolves it to a real active actor UUID. Never invents an ID.
    actor = "22222222-2222-2222-2222-222222222222"
    pool = _FakePool({}, {}, display_name_map={"Priya": actor})
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    resolved, unresolved = await resolve_owner_actor("Priya", "fireflies", _TENANT, repo)
    assert str(resolved) == actor and unresolved is None


@pytest.mark.asyncio
async def test_resolve_owner_actor_display_name_case_and_whitespace_insensitive():
    actor = "22222222-2222-2222-2222-222222222222"
    pool = _FakePool({}, {}, display_name_map={"  priya   raj ": actor})
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    resolved, _ = await resolve_owner_actor("Priya Raj", "fireflies", _TENANT, repo)
    assert str(resolved) == actor


@pytest.mark.asyncio
async def test_resolve_owner_actor_ambiguous_refuses_to_guess():
    pool = _FakePool(
        {},
        {},
        display_name_map={
            "Priya": "22222222-2222-2222-2222-222222222222",
            " priya": "55555555-5555-5555-5555-555555555555",
        },
    )
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    resolved, unresolved = await resolve_owner_actor("Priya", "fireflies", _TENANT, repo)
    assert resolved is None
    assert unresolved == "Priya"


@pytest.mark.asyncio
async def test_resolve_owner_actor_unmatched_stays_text():
    pool = _FakePool({}, {}, display_name_map={"Someone Else": "22222222-2222-2222-2222-222222222222"})
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    resolved, unresolved = await resolve_owner_actor("Priya", "fireflies", _TENANT, repo)
    assert resolved is None and unresolved == "Priya"


@pytest.mark.asyncio
async def test_resolve_owner_actor_empty_or_no_repo():
    assert await resolve_owner_actor(None, "slack", _TENANT, None) == (None, None)
    assert await resolve_owner_actor("  ", "slack", _TENANT, None) == (None, None)
    pool = _FakePool({}, {})
    repo = ActorRepo(pool)  # type: ignore[arg-type]
    resolved, unresolved = await resolve_owner_actor("Priya", "slack", _TENANT, None)
    assert resolved is None and unresolved == "Priya"
