"""Pure-unit tests for document-memory Layer-2 scope re-resolution (Phase 1).

These exercise the worker-side scope logic WITHOUT a live stack: a fake asyncpg
pool drives the blessed resolvers (EntityAliasRepo / ActorRepo) so we can assert
the §4.3 contract — re-resolve over the structured summary, scope_actors carry
resolved UUIDs only (§8 scope-actor existence), unresolved owners stay as text,
and key_points are noise-gated out of the resolution text (§8).
"""
from __future__ import annotations

import json
from uuid import UUID

import pytest

from services.ingest.ingestion.writers.summarization_worker.doc_memory import (
    DocMemoryScope,
    doc_memory_enabled,
    resolve_document_scope,
    structured_scope_text,
)


_ACME_ID = "11111111-1111-1111-1111-111111111111"
_PRIYA_ACTOR = "22222222-2222-2222-2222-222222222222"


class _FakePool:
    """Minimal asyncpg.Pool stand-in for the two resolver queries.

    - ``fetch`` backs EntityAliasRepo.fast_path_resolve_many: it returns a row
      for any normalized alias present in ``alias_map``.
    - ``fetchval`` backs ActorRepo.resolve_by_source_actor_ref: it returns a
      UUID for any (channel, ref) present in ``actor_map``.
    """

    def __init__(self, alias_map: dict[str, dict], actor_map: dict[tuple[str, str], str]):
        self._alias_map = alias_map
        self._actor_map = actor_map

    async def fetch(self, query: str, *args):
        # args = (tenant_id, norms_list)
        norms = args[1]
        rows = []
        for norm in norms:
            ref = self._alias_map.get(norm)
            if ref is not None:
                rows.append(
                    {"normalized": norm, "resolved_entity_ref": json.dumps(ref)}
                )
        return rows

    async def fetchval(self, query: str, *args):
        # args = (source_channel, source_actor_ref)
        return self._actor_map.get((args[0], args[1]))


_STRUCTURED = {
    "summary": "Acme weekly sync recap.",
    "key_points": ["Globex pinged about onboarding"],  # noise — must be gated
    "decisions": ["renew the Acme contract before Q3"],
    "action_items": [
        {"who": "Priya", "what": "send Acme the SOW", "due": "2026-06-17"},
    ],
    "risks": ["SOC2 slip endangers Acme renewal"],
}


def test_doc_memory_enabled_defaults_off(monkeypatch):
    monkeypatch.delenv("INGEST_DOC_MEMORY_ENABLED", raising=False)
    assert doc_memory_enabled() is False
    for val in ("1", "true", "TRUE", "yes", "on"):
        monkeypatch.setenv("INGEST_DOC_MEMORY_ENABLED", val)
        assert doc_memory_enabled() is True
    for val in ("0", "false", "off", "", "nope"):
        monkeypatch.setenv("INGEST_DOC_MEMORY_ENABLED", val)
        assert doc_memory_enabled() is False


def test_structured_scope_text_gates_summary_and_key_points():
    text = structured_scope_text(_STRUCTURED)
    # decisions / commitments / risks are present...
    assert "renew the Acme contract before Q3" in text
    assert "send Acme the SOW" in text
    assert "SOC2 slip endangers Acme renewal" in text
    # ...the commitment owner is included for resolution...
    assert "Priya" in text
    # ...but prose recap (summary + key_points) is NOT in the resolution blob.
    assert "weekly sync recap" not in text
    assert "Globex" not in text


@pytest.mark.asyncio
async def test_resolve_document_scope_resolves_entities_and_actor_owner():
    # "Acme" resolves to a customer entity; the action-item owner "Priya"
    # resolves (via the channel-prefixed ref) to a real actor UUID.
    pool = _FakePool(
        alias_map={"acme": {"type": "customer", "id": _ACME_ID}},
        actor_map={("fireflies", "Priya"): UUID(_PRIYA_ACTOR)},
    )
    scope = await resolve_document_scope(
        pool=pool,  # type: ignore[arg-type]
        tenant_id=UUID("33333333-3333-3333-3333-333333333333"),
        source_channel="fireflies",
        structured=_STRUCTURED,
    )
    assert isinstance(scope, DocMemoryScope)
    # entities_mentioned + scope_entities carry the resolved Acme ref.
    assert {"type": "customer", "id": _ACME_ID} in scope.scope_entities
    assert any(e.get("id") == _ACME_ID for e in scope.entities_mentioned)
    # The owner resolved to a real UUID -> goes into scope_actors.
    assert _PRIYA_ACTOR in scope.scope_actors
    assert "Priya" not in scope.unresolved_actor_refs


@pytest.mark.asyncio
async def test_unresolved_owner_stays_text_never_in_scope_actors():
    # Nothing resolves: no entity alias, no actor mapping. The owner must NOT be
    # invented into scope_actors (§8 scope-actor existence) — it stays as text.
    pool = _FakePool(alias_map={}, actor_map={})
    scope = await resolve_document_scope(
        pool=pool,  # type: ignore[arg-type]
        tenant_id=UUID("33333333-3333-3333-3333-333333333333"),
        source_channel="fireflies",
        structured=_STRUCTURED,
    )
    assert scope.scope_actors == []
    assert "Priya" in scope.unresolved_actor_refs
    # Every scope_actor that IS present must be a valid UUID string.
    for actor in scope.scope_actors:
        UUID(actor)  # raises if not a UUID


@pytest.mark.asyncio
async def test_observation_actor_seeds_scope_actors():
    pool = _FakePool(alias_map={}, actor_map={})
    obs_actor = UUID("44444444-4444-4444-4444-444444444444")
    scope = await resolve_document_scope(
        pool=pool,  # type: ignore[arg-type]
        tenant_id=UUID("33333333-3333-3333-3333-333333333333"),
        source_channel="fireflies",
        structured=_STRUCTURED,
        actor_id=obs_actor,
    )
    assert str(obs_actor) in scope.scope_actors


@pytest.mark.asyncio
async def test_existing_entities_preserved_additively():
    existing = [{"type": "customer", "id": "99999999-9999-9999-9999-999999999999"}]
    pool = _FakePool(
        alias_map={"acme": {"type": "customer", "id": _ACME_ID}},
        actor_map={},
    )
    scope = await resolve_document_scope(
        pool=pool,  # type: ignore[arg-type]
        tenant_id=UUID("33333333-3333-3333-3333-333333333333"),
        source_channel="fireflies",
        structured=_STRUCTURED,
        existing_entities=existing,
    )
    ids = {e["id"] for e in scope.entities_mentioned}
    # Prior resolution kept AND the new Acme ref added.
    assert "99999999-9999-9999-9999-999999999999" in ids
    assert _ACME_ID in ids
