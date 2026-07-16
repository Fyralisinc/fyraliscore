from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import asyncpg

from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.ingest.ingestion.core import ingest
from services.workers.entity_resolver.context import build_context
from services.workers.entity_resolver.worker import EntityResolverWorker


PROJECT_ID = "project-uuid"
PROJECT_NATIVE_ID = f"linear:project:{PROJECT_ID}"
TEAM_ID = "team-uuid"
TEAM_NATIVE_ID = f"linear:team:{TEAM_ID}"


class _LinearClaimResolver(LLMProvider):
    def __init__(self, team_resource_id) -> None:
        super().__init__(
            LLMConfig(
                provider="anthropic",
                api_key="synthetic",
                model="synthetic",
            )
        )
        self._team_resource_id = team_resource_id

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
                        "id": str(self._team_resource_id),
                    },
                    "confidence": 0.99,
                    "reasoning": "selected authenticated Linear team",
                }
            )
        return json.dumps(
            {
                "canonical_ref": None,
                "confidence": 0.1,
                "reasoning": "no exact source-bound identity",
            }
        )


def _linear_issue(
    *,
    issue_id: str = "issue-uuid",
    team: dict | None = None,
    project: dict | None = None,
) -> dict:
    return {
        "action": "create",
        "type": "Issue",
        "data": {
            "id": issue_id,
            "identifier": "ENG-123",
            "title": "SALES text must not impersonate source identity",
            "team": (
                team
                if team is not None
                else {
                    "id": TEAM_ID,
                    "key": "ENG",
                    "name": "Engineering",
                }
            ),
            "project": (
                project
                if project is not None
                else {
                    "id": PROJECT_ID,
                    "name": "Billing Reliability",
                }
            ),
            "createdAt": "2026-04-21T10:00:00Z",
        },
        "createdAt": "2026-04-21T10:00:00Z",
    }


async def _seed_resource_binding(
    pool: asyncpg.Pool,
    *,
    tenant_id,
    native_id: str,
    identity: str,
    semantic_kind: str,
    source_system: str = "linear",
):
    resource_id = uuid7()
    await pool.execute(
        """
        INSERT INTO resources (
            id, tenant_id, kind, identity, current_value, metadata
        ) VALUES (
            $1, $2, 'capacity', $3,
            jsonb_build_object('name', $3::text),
            jsonb_build_object('semantic_kind', $4::text)
        )
        """,
        resource_id,
        tenant_id,
        identity,
        semantic_kind,
    )
    binding = await SourceIdentityBindingRepo(pool).bind(
        tenant_id=tenant_id,
        source_system=source_system,
        source_native_identifier=native_id,
        source_identity_authority_ref=(
            f"{source_system}-{semantic_kind}-object-contract-v1"
        ),
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=(f"source-object:{native_id}",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    return resource_id, binding


async def test_linear_issue_attaches_project_and_team_bindings(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    project_resource_id, project_binding = await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id=PROJECT_NATIVE_ID,
        identity="Billing Reliability",
        semantic_kind="project",
    )
    team_resource_id, team_binding = await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id=TEAM_NATIVE_ID,
        identity="Engineering",
        semantic_kind="team",
    )

    result = await ingest(
        "linear:webhook",
        _linear_issue(),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    rows = await gateway_pool.fetch(
        """
        SELECT binding_id, source_surface, normalized_source_surface
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        ORDER BY source_surface
        """,
        tenant_id,
        result.observation.id,
    )
    assert [
        (
            str(row["binding_id"]),
            row["source_surface"],
            row["normalized_source_surface"],
        )
        for row in rows
    ] == [
        (
            project_binding.binding_id,
            "Billing Reliability",
            "billing reliability",
        ),
        (team_binding.binding_id, "ENG", "eng"),
        (team_binding.binding_id, "Engineering", "engineering"),
    ]

    project_context = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="Billing Reliability",
    )
    team_key_context = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="ENG",
    )
    forged_text_context = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase="SALES",
    )

    assert project_context.source_identity_binding is not None
    assert project_context.source_identity_binding.canonical_ref["id"] == str(
        project_resource_id
    )
    assert team_key_context.source_identity_binding is not None
    assert team_key_context.source_identity_binding.canonical_ref["id"] == str(
        team_resource_id
    )
    assert forged_text_context.source_identity_binding is None

    worker = EntityResolverWorker(
        pool=gateway_pool,
        llm=_LinearClaimResolver(team_resource_id),
        alias_repo=EntityAliasRepo(gateway_pool),
    )
    decisions = dict(
        await worker.process_observation(
            result.observation.id,
            tenant_id,
        )
    )
    assert decisions["ENG"] == "resolved"


async def test_linear_claims_fail_closed_without_matching_bindings(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id="jira:acme.atlassian.net:project:project-uuid",
        identity="Billing Reliability",
        semantic_kind="project",
        source_system="jira",
    )

    result = await ingest(
        "linear:webhook",
        _linear_issue(),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    assert await gateway_pool.fetchval(
        """
        SELECT count(*)
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        result.observation.id,
    ) == 0


async def test_linear_claims_fail_closed_without_ids_or_surfaces(
    gateway_pool: asyncpg.Pool,
    tenant_id,
    _DeterministicEmbedder,
) -> None:
    await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id=PROJECT_NATIVE_ID,
        identity="Billing Reliability",
        semantic_kind="project",
    )
    await _seed_resource_binding(
        gateway_pool,
        tenant_id=tenant_id,
        native_id=TEAM_NATIVE_ID,
        identity="Engineering",
        semantic_kind="team",
    )

    missing_ids = await ingest(
        "linear:webhook",
        _linear_issue(
            issue_id="issue-missing-ids",
            team={"key": "ENG", "name": "Engineering"},
            project={"name": "Billing Reliability"},
        ),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )
    missing_surfaces = await ingest(
        "linear:webhook",
        _linear_issue(
            issue_id="issue-missing-surfaces",
            team={"id": TEAM_ID},
            project={"id": PROJECT_ID},
        ),
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=_DeterministicEmbedder(),
        enqueue_trigger=False,
    )

    assert await gateway_pool.fetchval(
        """
        SELECT count(*)
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1
          AND observation_id=ANY($2::uuid[])
        """,
        tenant_id,
        [
            missing_ids.observation.id,
            missing_surfaces.observation.id,
        ],
    ) == 0
