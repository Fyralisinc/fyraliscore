from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from lib.shared.ids import uuid7
from lib.llm.provider import LLMConfig, LLMProvider
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.ingest.ingestion.core import ingest, ingest_from_draft
from services.ingest.ingestion.handlers import ObservationDraft
from services.ingest.ingestion.source_identity import (
    StructuredSourceIdentityClaim,
)
from services.workers.entity_resolver.context import build_context
from services.workers.entity_resolver.worker import EntityResolverWorker


SITE = "acme.atlassian.net"
PROJECT_NATIVE_ID = f"jira:{SITE}:project:10000"


class _ClaimAwareResolver(LLMProvider):
    def __init__(self, resource_id) -> None:
        super().__init__(
            LLMConfig(
                provider="anthropic",
                api_key="synthetic",
                model="synthetic",
            )
        )
        self._resource_id = resource_id

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: dict[str, Any] | None,
    ) -> str:
        del system, temperature, max_tokens, schema_hint
        if "Phrase to resolve: 'ENG'" in user:
            return json.dumps(
                {
                    "canonical_ref": {
                        "type": "resource",
                        "id": str(self._resource_id),
                    },
                    "confidence": 0.99,
                    "reasoning": "selected authenticated Jira project",
                }
            )
        return json.dumps(
            {
                "canonical_ref": None,
                "confidence": 0.1,
                "reasoning": "no source-bound identity",
            }
        )


def _jira_issue(
    *,
    project_id: str = "10000",
    project_key: str = "ENG",
    summary: str = "SALES text must not impersonate project identity",
    updated: str = "2026-07-15T12:30:00.000+0000",
) -> dict:
    return {
        "_fyralis_record_type": "issue",
        "_fyralis_site": SITE,
        "id": "10001",
        "key": "ENG-42",
        "fields": {
            "summary": summary,
            "updated": updated,
            "project": {
                "id": project_id,
                "key": project_key,
                "name": "Engineering",
            },
        },
    }


async def _seed_resource_binding(
    pool: asyncpg.Pool,
    *,
    tenant_id,
    native_id: str = PROJECT_NATIVE_ID,
    source_system: str = "jira",
    archived_at: datetime | None = None,
):
    resource_id = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO resources (
                id, tenant_id, kind, identity, current_value,
                metadata, archived_at
            ) VALUES (
                $1, $2, 'capacity', 'Engineering',
                '{"name":"Engineering"}'::jsonb,
                '{"semantic_kind":"project"}'::jsonb, $3
            )
            """,
            resource_id,
            tenant_id,
            archived_at,
        )
    binding = await SourceIdentityBindingRepo(pool).bind(
        tenant_id=tenant_id,
        source_system=source_system,
        source_native_identifier=native_id,
        source_identity_authority_ref=(
            f"{source_system}-project-object-contract-v1"
        ),
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=(f"source-project:{native_id}",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return resource_id, binding


async def test_jira_ingest_attaches_only_preexisting_governed_binding(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    resource_id, binding = await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
    )

    result = await ingest(
        "jira:issue",
        _jira_issue(),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    async with gateway_pool.acquire() as conn:
        attachment = await conn.fetchrow(
            """
            SELECT binding_id, source_surface,
                   normalized_source_surface, attachment_authority_ref
            FROM observation_source_identity_bindings
            WHERE tenant_id=$1 AND observation_id=$2
            """,
            tenant_id,
            result.observation.id,
        )
    assert attachment is not None
    assert str(attachment["binding_id"]) == binding.binding_id
    assert attachment["source_surface"] == "ENG"
    assert attachment["normalized_source_surface"] == "eng"
    assert attachment["attachment_authority_ref"] == (
        "ingestion-structured-claim:"
        "jira-handler:structured-project-field-v1"
    )
    assert result.observation.content["_unresolved_phrases"][0] == "ENG"

    exact = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="ENG",
    )
    forged_text = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="SALES",
    )
    assert exact.source_identity_binding is not None
    assert exact.source_identity_binding.canonical_ref["id"] == str(
        resource_id
    )
    assert forged_text.source_identity_binding is None

    worker = EntityResolverWorker(
        pool=gateway_pool,
        llm=_ClaimAwareResolver(resource_id),
        alias_repo=EntityAliasRepo(gateway_pool),
    )
    decisions = dict(
        await worker.process_observation(
            result.observation.id,
            tenant_id,
        )
    )
    assert decisions["ENG"] == "resolved"
    async with gateway_pool.acquire() as conn:
        admission = await conn.fetchval(
            """
            SELECT decision
            FROM grounding_admission_decisions admission
            JOIN grounding_traces trace
              ON trace.tenant_id=admission.tenant_id
             AND trace.grounding_admission_id=admission.id
            WHERE trace.tenant_id=$1
              AND trace.source_observation_id=$2
              AND trace.phrase='ENG'
            ORDER BY trace.created_at DESC
            LIMIT 1
            """,
            tenant_id,
            result.observation.id,
        )
    assert admission["genuine_source_binding"][
        "source_native_identifier"
    ] == PROJECT_NATIVE_ID


async def test_absent_binding_is_harmless_and_replay_can_attach_later(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    payload = _jira_issue()
    first = await ingest(
        "jira:issue",
        payload,
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    assert await gateway_pool.fetchval(
        """
        SELECT count(*) FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        first.observation.id,
    ) == 0

    await _seed_resource_binding(gateway_pool, tenant_id=tenant_id)
    replay = await ingest(
        "jira:issue",
        payload,
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    assert replay.deduped is True
    assert replay.observation.id == first.observation.id
    assert await gateway_pool.fetchval(
        """
        SELECT count(*) FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        first.observation.id,
    ) == 1


async def test_source_and_native_mismatches_do_not_attach(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id=f"jira:{SITE}:project:99999",
    )
    native_mismatch = await ingest(
        "jira:issue",
        _jira_issue(),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    assert await gateway_pool.fetchval(
        """
        SELECT count(*) FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        native_mismatch.observation.id,
    ) == 0

    await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id="linear:project:10000",
        source_system="linear",
    )
    occurred_at = datetime.now(timezone.utc)
    source_mismatch = await ingest_from_draft(
        channel="jira:issue",
        draft=ObservationDraft(
            source_channel="jira:issue",
            content_text="ENG source mismatch",
            content={"object_type": "issue"},
            occurred_at=occurred_at,
            trust_tier="authoritative",
            external_id=f"source-mismatch:{uuid7()}",
            source_identity_claims=[
                StructuredSourceIdentityClaim(
                    source_system="linear",
                    source_native_identifier="linear:project:10000",
                    source_surface="ENG",
                    claim_authority_ref="forged-cross-source-claim",
                )
            ],
        ),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    assert await gateway_pool.fetchval(
        """
        SELECT count(*) FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        source_mismatch.observation.id,
    ) == 0


async def test_delayed_jira_event_uses_event_time_target_liveness(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    event_time = datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc)
    resource_id, _ = await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        archived_at=event_time + timedelta(hours=1),
    )
    result = await ingest(
        "jira:issue",
        _jira_issue(updated="2026-07-15T12:30:00.000+0000"),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    context = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="ENG",
    )

    assert context.source_identity_binding is not None
    assert context.source_identity_binding.canonical_ref["id"] == str(
        resource_id
    )
