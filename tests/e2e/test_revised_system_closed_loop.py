"""Production-shaped proof of the first complete revised-system loop."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg
import pytest

from lib.contracts.agency import OutcomeRecordingCommand
from lib.evaluation.closed_loop import (
    ClosedLoopEvaluationScope,
    evaluate_closed_loop_state,
    render_closed_loop_markdown,
)
from lib.llm.provider import LLMConfig, LLMProvider
from lib.shared.ids import uuid7
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.ingestion.core import ingest_from_draft
from services.ingest.ingestion.handlers.slack import handle_slack_message
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.intervention_episode_coordinator import (
    InterventionEpisodeCoordinatorWorker,
    InterventionEpisodeCoordinatorWorkerStats,
)
from services.workers.source_semantic_worker import SourceSemanticWorker
from tests.e2e.revised_closed_loop_harness import run_closed_loop_vertical


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _DeterministicEmbedder:
    class _Config:
        expected_dim = 768

    config = _Config()

    async def embed(self, _text: str) -> list[float]:
        return [0.01] * self.config.expected_dim


class _ScriptedResolver(LLMProvider):
    def __init__(self) -> None:
        super().__init__(
            LLMConfig(provider="anthropic", api_key="test", model="test")
        )
        self.calls = 0

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: dict[str, Any] | None,
    ) -> str:
        del system, user, temperature, max_tokens, schema_hint
        self.calls += 1
        return json.dumps(
            {
                "canonical_ref": {
                    "type": "customer",
                    "id": "customer-nimbus",
                },
                "confidence": 0.97,
                "reasoning": (
                    "NBI selects the independently adjudicated tenant entity"
                ),
            }
        )


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _slack_payload(*, occurred_at: datetime) -> dict[str, Any]:
    return {
        "team_id": "T-FYRALIS",
        "event": {
            "type": "message",
            "user": "U-NORTHSTAR",
            "text": "NBI is blocked",
            "ts": f"{occurred_at.timestamp():.6f}",
            "channel": "C-MIGRATIONS",
            "channel_type": "channel",
        },
    }


async def _install_json_codecs(pool: asyncpg.Pool) -> None:
    """Configure every pooled connection like the production-shaped DB fixtures."""

    async def configure_one() -> None:
        conn = await pool.acquire()
        try:
            for type_name in ("json", "jsonb"):
                await conn.set_type_codec(
                    type_name,
                    encoder=lambda value: (
                        json.dumps(value) if not isinstance(value, str) else value
                    ),
                    decoder=json.loads,
                    schema="pg_catalog",
                )
        finally:
            await pool.release(conn)

    await asyncio.gather(*(configure_one() for _ in range(pool.get_max_size())))


@pytest.mark.timeout(120)
async def test_real_slack_signal_closes_one_intervention_feedback_loop(
    fresh_db: asyncpg.Pool,
) -> None:
    """Join company physics, intent, agency, outcome and feedback once."""

    await _install_json_codecs(fresh_db)
    tenant_id = uuid7()
    alias_repo = EntityAliasRepo(fresh_db)
    await alias_repo.insert_alias(
        phrase="Nimbus Bank",
        resolved_entity_ref={"type": "customer", "id": "customer-nimbus"},
        source="manual",
        confidence=0.99,
        tenant_id=tenant_id,
        extra_metadata={
            "identity_basis_class": "independently_adjudicated",
            "identity_basis_ref": "test-adjudication:customer:nimbus",
        },
    )

    occurred_at = datetime.now(timezone.utc) - timedelta(seconds=5)
    draft = await handle_slack_message(
        _slack_payload(occurred_at=occurred_at),
        {},
    )
    ingest_result = await ingest_from_draft(
        channel="slack:message",
        draft=draft,
        pool=fresh_db,
        tenant_id=tenant_id,
        actor_repo=None,
        alias_repo=alias_repo,
        embedder=_DeterministicEmbedder(),
    )
    assert ingest_result.observation.content["_unresolved_phrases"] == ["NBI"]

    resolver = _ScriptedResolver()
    grounding_worker = EntityResolverWorker(
        pool=fresh_db,
        llm=resolver,
        alias_repo=alias_repo,
    )
    assert await grounding_worker.process_observation(
        ingest_result.observation.id,
        tenant_id,
    ) == [("NBI", "resolved")]
    assert resolver.calls == 1

    semantic_worker = SourceSemanticWorker(
        pool=fresh_db,
        worker_id=f"pytest:closed-loop:{tenant_id}",
    )
    await semantic_worker.process_batch(limit=1000)

    async with fresh_db.acquire() as conn:
        semantic_row = await conn.fetchrow(
            """
            SELECT ssi.source_assertion, ssi.semantic_frame,
                   ssad.admitted_model_id
            FROM source_semantic_interpretations ssi
            JOIN source_semantic_admission_decisions ssad
              ON ssad.tenant_id=ssi.tenant_id
             AND ssad.interpretation_id=ssi.id
            WHERE ssi.tenant_id=$1
              AND ssi.source_observation_id=$2
            """,
            tenant_id,
            ingest_result.observation.id,
        )
    assert semantic_row is not None
    assertion = _json(semantic_row["source_assertion"])
    frame = _json(semantic_row["semantic_frame"])
    model_id = semantic_row["admitted_model_id"]
    assert model_id is not None

    loop_started_at = datetime.now(timezone.utc)
    artifacts = await run_closed_loop_vertical(
        pool=fresh_db,
        tenant_id=tenant_id,
        source_observation_id=ingest_result.observation.id,
        model_id=model_id,
        source_assertion_ref=assertion["assertion_id"],
        semantic_frame_ref=frame["frame_id"],
        started_at=loop_started_at,
        finalize_episode_manifest=False,
    )
    coordinator_stats = InterventionEpisodeCoordinatorWorkerStats()
    coordinator = InterventionEpisodeCoordinatorWorker(
        pool=fresh_db,
        worker_id=f"pytest:episode-manifest:{tenant_id}",
    )
    assert await coordinator.process_batch(
        limit=100,
        stats=coordinator_stats,
    ) == 10
    assert coordinator_stats.applied == 10
    assert coordinator_stats.terminal_failures == 0
    assert coordinator_stats.retries_scheduled == 0

    async with fresh_db.acquire() as conn:
        state = await evaluate_closed_loop_state(
            conn,
            scope=ClosedLoopEvaluationScope(
                tenant_id=tenant_id,
                start=occurred_at - timedelta(minutes=1),
                end=artifacts.completed_at + timedelta(minutes=1),
                run_id="pytest-real-slack-closed-loop",
            ),
            artifact_refs=("pytest://real-slack-closed-loop",),
        )
        cardinalities = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM models WHERE tenant_id=$1) AS beliefs,
              (SELECT count(*) FROM intent_proposals WHERE tenant_id=$1)
                AS intent_proposals,
              (SELECT count(*) FROM intent_aggregate_heads WHERE tenant_id=$1)
                AS intents,
              (SELECT count(*) FROM concern_heads WHERE tenant_id=$1) AS concerns,
              (SELECT count(*) FROM consequential_proposals WHERE tenant_id=$1)
                AS proposals,
              (SELECT count(*) FROM consequential_predictions WHERE tenant_id=$1)
                AS predictions,
              (SELECT count(*) FROM consequential_authorization_decisions
                 WHERE tenant_id=$1) AS authorizations,
              (SELECT count(*) FROM agency_workflow_run_heads WHERE tenant_id=$1)
                AS workflows,
              (SELECT count(*) FROM agency_task_heads WHERE tenant_id=$1) AS tasks,
              (SELECT count(*) FROM work_obligation_heads WHERE tenant_id=$1)
                AS work_items,
              (SELECT count(*) FROM external_effect_attempt_heads
                 WHERE tenant_id=$1) AS effects,
              (SELECT count(*) FROM consequential_outcomes WHERE tenant_id=$1)
                AS outcomes,
              (SELECT count(*) FROM consequential_settlements WHERE tenant_id=$1)
                AS settlements,
              (SELECT count(*) FROM consequential_attributions WHERE tenant_id=$1)
                AS attributions,
              (SELECT count(*) FROM intervention_episode_heads WHERE tenant_id=$1)
                AS episodes
            """,
            tenant_id,
        )
        outcome_command = await conn.fetchval(
            """
            SELECT command
            FROM agency_command_results
            WHERE tenant_id=$1 AND command_kind='record_independent_outcome'
            """,
            tenant_id,
        )
        manifest_work_fates = dict(
            await conn.fetch(
                """
                SELECT status, count(*)::integer AS count
                FROM intervention_episode_manifest_work_items
                WHERE tenant_id=$1
                GROUP BY status
                """,
                tenant_id,
            )
        )
        manifest_commands = await conn.fetch(
            """
            SELECT command
            FROM agency_command_results
            WHERE tenant_id=$1
              AND writer_id='EpisodeCoordinator'
              AND semantic_idempotency_key LIKE 'episode-manifest:%'
            ORDER BY created_at, id
            """,
            tenant_id,
        )

    assert state.episode_count == 1
    assert state.complete_episode_count == 1
    assert state.closed_loop_completion_rate == 1.0
    assert state.incident_counts == {}
    assert state.component_violation_counts == {}
    assert set(state.stage_coverage_rates.values()) == {1.0}
    assert set(state.continuity_rates.values()) == {1.0}
    assert state.violation_count == 0
    assert "None observed in scope." in render_closed_loop_markdown(state)
    assert state.manifest_work_item_count == 10
    assert state.manifest_work_applied_count == 10
    assert state.manifest_work_completion_rate == 1.0
    assert cardinalities is not None
    assert set(dict(cardinalities).values()) == {1}
    assert manifest_work_fates == {"applied": 10}
    assert len(manifest_commands) == 10
    for row in manifest_commands:
        command = _json(row["command"])
        authority = command["context"]["processing_authority"]
        assert authority["principal_or_service_id"] == (
            "service:intervention-episode-coordinator"
        )
        assert authority["purpose"] == (
            "intervention_episode_manifest_projection"
        )
        assert authority["operation"] == "link_revalidated_stage"
        assert authority["object_types"] == {
            "universe": False,
            "values": ["intervention_episode"],
        }
        assert authority["source_labels"] == {
            "universe": False,
            "values": ["agency-canonical-event"],
        }
        assert len(authority["authority_basis_refs"]) == 1
        assert authority["authority_basis_refs"][0].startswith(
            "canonical-event:"
        )

    # The exact replays embedded in the harness must not create second heads.
    await semantic_worker.process_batch(limit=1000)
    assert await coordinator.process_batch(limit=100) == 0
    async with fresh_db.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id=$1",
            tenant_id,
        ) == 1
        assert await conn.fetchval(
            "SELECT count(*) FROM intervention_episode_heads WHERE tenant_id=$1",
            tenant_id,
        ) == 1
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM intervention_episode_manifest_work_items
            WHERE tenant_id=$1
            """,
            tenant_id,
        ) == 10
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM intervention_episode_versions
            WHERE tenant_id=$1
            """,
            tenant_id,
        ) == 11

    # An execution receipt cannot be relabeled as an independent Outcome.
    invalid_command = _json(outcome_command)
    invalid_command["outcome"]["independent_of_execution_claim"] = False
    invalid_command["outcome"]["source_evidence_refs"] = (
        f"execution-receipt:{artifacts.execution_receipt_id}",
    )
    with pytest.raises(ValueError):
        OutcomeRecordingCommand.model_validate(invalid_command)
