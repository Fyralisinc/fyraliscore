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
from services.workers.agency_activation_worker import AgencyActivationWorker
from services.workers.entity_resolver.worker import EntityResolverWorker
from services.workers.external_effect_executor import (
    ActionAdapterRequest,
    ActionDispatchFate,
    ActionDispatchResult,
    ActionPreflightResult,
    StaticActionAdapterRegistry,
)
from services.workers.external_effect_executor.worker import (
    ExternalEffectExecutorWorker,
)
from services.workers.intervention_episode_coordinator import (
    InterventionEpisodeCoordinatorWorker,
    InterventionEpisodeCoordinatorWorkerStats,
)
from services.workers.source_semantic_worker import SourceSemanticWorker
from services.workers.work_scheduler_worker import WorkSchedulerWorker
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


class _DeterministicActionAdapter:
    adapter_name = "simulated-slack-message-delivery"
    provider_name = "simulated-slack"

    def __init__(self) -> None:
        self.preflight_requests: list[ActionAdapterRequest] = []
        self.dispatch_requests: list[ActionAdapterRequest] = []

    async def preflight(
        self,
        request: ActionAdapterRequest,
    ) -> ActionPreflightResult:
        self.preflight_requests.append(request)
        return ActionPreflightResult(
            evidence_refs=("simulated-slack-channel:exists",),
        )

    async def dispatch(
        self,
        request: ActionAdapterRequest,
    ) -> ActionDispatchResult:
        self.dispatch_requests.append(request)
        return ActionDispatchResult(
            fate=ActionDispatchFate.SUCCEEDED,
            reason="deterministic simulated provider persisted one exact message",
            provider_observation_refs=("simulated-slack:ok",),
            external_state_evidence_refs=(
                "simulated-slack-message:C-CUSTOMER-SUCCESS:1717.001",
            ),
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

    occurred_at = datetime.now(timezone.utc) - timedelta(minutes=20)
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

    loop_started_at = occurred_at + timedelta(minutes=1)
    activation_worker = AgencyActivationWorker(
        pool=fresh_db,
        worker_id=f"pytest:agency-activation:{tenant_id}",
    )
    work_scheduler = WorkSchedulerWorker(
        pool=fresh_db,
        worker_id=f"pytest:work-scheduler:{tenant_id}",
    )
    action_adapter = _DeterministicActionAdapter()
    effect_executor = ExternalEffectExecutorWorker(
        pool=fresh_db,
        worker_id=f"pytest:effect-executor:{tenant_id}",
        adapter_registry=StaticActionAdapterRegistry((action_adapter,)),
    )
    artifacts = await run_closed_loop_vertical(
        pool=fresh_db,
        tenant_id=tenant_id,
        source_observation_id=ingest_result.observation.id,
        model_id=model_id,
        source_assertion_ref=assertion["assertion_id"],
        semantic_frame_ref=frame["frame_id"],
        started_at=loop_started_at,
        activation_worker=activation_worker,
        work_scheduler=work_scheduler,
        effect_executor=effect_executor,
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
        activation_work_fates = dict(
            await conn.fetch(
                """
                SELECT status, count(*)::integer AS count
                FROM authorized_agency_activation_work_items
                WHERE tenant_id=$1
                GROUP BY status
                """,
                tenant_id,
            )
        )
        activation_commands = await conn.fetch(
            """
            SELECT command_id, object_type, command
            FROM agency_command_results
            WHERE tenant_id=$1
              AND writer_id='AgencyStateApplier'
              AND semantic_idempotency_key LIKE 'agency-activation:%'
            ORDER BY object_type
            """,
            tenant_id,
        )
        scheduling_work_fates = dict(
            await conn.fetch(
                """
                SELECT status, count(*)::integer AS count
                FROM registered_work_scheduling_items
                WHERE tenant_id=$1
                GROUP BY status
                """,
                tenant_id,
            )
        )
        scheduling_commands = await conn.fetch(
            """
            SELECT command_id, command_kind, command
            FROM agency_command_results
            WHERE tenant_id=$1
              AND writer_id='WorkLedgerApplier'
              AND semantic_idempotency_key LIKE 'work-scheduling:%'
            ORDER BY command_kind
            """,
            tenant_id,
        )
        effect_execution_work = await conn.fetchrow(
            """
            SELECT status, attempt_count, effect_attempt_id,
                   applied_effect_version, execution_receipt_id,
                   applied_effect_state
            FROM leased_work_effect_execution_items
            WHERE tenant_id=$1
            """,
            tenant_id,
        )
        effect_receipts = await conn.fetch(
            """
            SELECT receipt_id, effect_version, effect_state, receipt
            FROM execution_receipts
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            ORDER BY effect_version
            """,
            tenant_id,
            artifacts.effect_attempt_id,
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
    assert state.activation_work_item_count == 1
    assert state.activation_work_activated_count == 1
    assert state.activation_work_completion_rate == 1.0
    assert state.scheduling_work_item_count == 1
    assert state.scheduling_work_leased_count == 1
    assert state.scheduling_work_completion_rate == 1.0
    assert state.effect_execution_item_count == 1
    assert state.effect_execution_successful_dispatch_count == 1
    assert state.effect_execution_completion_rate == 1.0
    assert cardinalities is not None
    assert set(dict(cardinalities).values()) == {1}
    assert manifest_work_fates == {"applied": 10}
    assert activation_work_fates == {"activated": 1}
    assert scheduling_work_fates == {"leased": 1}
    assert effect_execution_work is not None
    assert effect_execution_work["status"] == "dispatched"
    assert effect_execution_work["attempt_count"] == 1
    assert effect_execution_work["effect_attempt_id"] == artifacts.effect_attempt_id
    assert effect_execution_work["applied_effect_state"] == "succeeded"
    assert effect_execution_work["execution_receipt_id"] == (
        artifacts.execution_receipt_id
    )
    assert [row["effect_version"] for row in effect_receipts] == [2, 3, 4]
    assert [row["effect_state"] for row in effect_receipts] == [
        "dispatch_intent_recorded",
        "acknowledged",
        "succeeded",
    ]
    assert effect_receipts[-1]["receipt_id"] == artifacts.execution_receipt_id
    assert effect_receipts[-1]["effect_version"] == (
        effect_execution_work["applied_effect_version"]
    )
    assert effect_receipts[-1]["effect_state"] == "succeeded"
    assert len(action_adapter.preflight_requests) == 1
    assert len(action_adapter.dispatch_requests) == 1
    dispatched_request = action_adapter.dispatch_requests[0]
    assert dispatched_request.effect_attempt_id == artifacts.effect_attempt_id
    assert dispatched_request.operation == "send_message"
    assert dispatched_request.parameters == {
        "channel_id": "C-CUSTOMER-SUCCESS",
        "text": "Nimbus Bank is blocked; owner review is required.",
    }
    assert action_adapter.preflight_requests[0] == dispatched_request
    assert len(scheduling_commands) == 2
    assert all(row["command_id"].version == 5 for row in scheduling_commands)
    for row in scheduling_commands:
        command = _json(row["command"])
        authority = command["context"]["processing_authority"]
        assert authority["principal_or_service_id"] == (
            "service:work-scheduler-worker"
        )
        assert authority["purpose"] == "registered_work_scheduling"
        assert authority["object_types"] == {
            "universe": False,
            "values": ["work_obligation"],
        }
    assert len(activation_commands) == 2
    assert {row["object_type"] for row in activation_commands} == {
        "workflow_run",
        "task",
    }
    assert all(row["command_id"].version == 5 for row in activation_commands)
    for row in activation_commands:
        command = _json(row["command"])
        authority = command["context"]["processing_authority"]
        assert authority["principal_or_service_id"] == (
            "service:agency-activation-worker"
        )
        assert authority["purpose"] == (
            "authorized_internal_agency_activation"
        )
        assert authority["object_types"] == {
            "universe": False,
            "values": [row["object_type"]],
        }
        assert set(authority["source_labels"]["values"]) == {
            "agency-canonical-event",
            "authorization-decision",
            "intervention-spec",
        }
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
    assert await activation_worker.process_batch(limit=10) == 0
    assert await work_scheduler.process_batch(limit=10) == 0
    assert await effect_executor.process_batch(limit=10) == 0
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
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM leased_work_effect_execution_items
            WHERE tenant_id=$1
            """,
            tenant_id,
        ) == 1
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM execution_receipts
            WHERE tenant_id=$1 AND effect_attempt_id=$2
            """,
            tenant_id,
            artifacts.effect_attempt_id,
        ) == len(effect_receipts)
    assert len(action_adapter.preflight_requests) == 1
    assert len(action_adapter.dispatch_requests) == 1

    # An execution receipt cannot be relabeled as an independent Outcome.
    invalid_command = _json(outcome_command)
    invalid_command["outcome"]["independent_of_execution_claim"] = False
    invalid_command["outcome"]["source_evidence_refs"] = (
        f"execution-receipt:{artifacts.execution_receipt_id}",
    )
    with pytest.raises(ValueError):
        OutcomeRecordingCommand.model_validate(invalid_command)
