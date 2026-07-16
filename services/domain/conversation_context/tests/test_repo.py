from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import asyncpg
import pytest

from lib.contracts import (
    AgencyWriteContext,
    CandidateContextLayer,
    CommitInterpretationContextCommand,
    ContextBudget,
    ContextCandidateCost,
    ContextProbeEnvelope,
    ContextProbeResult,
    ContextRiskTier,
    ContextSelectionPolicy,
    ConversationContextCandidate,
    ConversationEpisodeHypothesis,
    InterpretationContextHeadExpectation,
    InterpretationContextRequest,
    InterpretationMode,
    ProcessingAuthorityContext,
    RestrictionSet,
    SelectedContextItem,
    WriterCutoverState,
    WriterScopeEpoch,
    canonical_sha256,
)
from lib.architecture_registry import load_architecture_registry
from lib.evaluation.conversation_context import (
    ConversationContextEvaluationScope,
    build_conversation_context_invariant_evidence,
    evaluate_conversation_context_state,
)
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.domain.conversation_context.repo import GroundingAnnotationAppender
from services.domain.entity_grounding.episode import prepare_context_selection
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection


NOW = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
TENANT = UUID("11111111-1111-4111-8111-111111111111")
FOCAL = "slack:C-finance:102.1:v1"
ROOT = "slack:C-finance:100.1:v1"
REPO_ROOT = Path(__file__).resolve().parents[4]


async def _seed_observation(
    conn: asyncpg.Connection,
    *,
    observation_id: UUID,
    content_text: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            content, content_text, trust_tier, entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'slack:message', $4::jsonb, $5,
            'attested_agent', '[]'::jsonb
        )
        """,
        observation_id,
        TENANT,
        NOW,
        json.dumps({"text": content_text}),
        content_text,
    )


def _mention_commands(
    *,
    observation_id: UUID,
    phrase: str,
    content_text: str,
):
    context_command, context_outcome = prepare_context_selection(
        tenant_id=TENANT,
        observation_id=observation_id,
        phrase=phrase,
        occurred_at=NOW,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=NOW + timedelta(minutes=1),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=TENANT,
        observation_id=observation_id,
        phrase=phrase,
        content_text=content_text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=NOW + timedelta(minutes=1),
    )
    return context_command, context_outcome, mention_command


def _command(
    *,
    key: str,
    expected_version: int = 0,
    expected_snapshot_id: UUID | None = None,
    include_root: bool = True,
    semantic_output: str = "payment incident",
    at: datetime = NOW,
) -> CommitInterpretationContextCommand:
    allowed_ids = (FOCAL, ROOT) if include_root else (FOCAL,)
    authority = ProcessingAuthorityContext(
        tenant_id=TENANT,
        principal_or_service_id="service:context-probe",
        purpose="entity_grounding",
        operation="select_interpretation_context",
        object_types=RestrictionSet.only("conversation_event_revision"),
        object_ids=RestrictionSet.only(*allowed_ids),
        fields=RestrictionSet.only("content", "author", "source_topology"),
        source_labels=RestrictionSet.only("slack:message"),
        authority_basis_refs=frozenset({"policy:context-selection-v1"}),
        policy_version="context-processing-v1",
        authority_epoch=7,
        decision_time=at - timedelta(minutes=1),
        expires_at=at + timedelta(hours=1),
    )
    request = InterpretationContextRequest(
        request_id=f"request:{key}",
        tenant_id=TENANT,
        focal_event_revision_ids=(FOCAL,),
        mode=InterpretationMode.AS_KNOWN_AT_CUTOFF,
        effective_query_time=at,
        evidence_cutoff=NOW,
        knowledge_cutoff=at,
        source_topology_version="slack-topology-v4",
        processing_authority=authority,
        allowed_source_spaces=RestrictionSet.only("C-finance"),
        risk_tier=ContextRiskTier.MEDIUM,
        required_probe_surfaces=("source_topology", "boundary_sensitivity"),
        budget=ContextBudget(
            max_events=12,
            max_topology_hops=3,
            max_source_reads=4,
            max_model_calls=2,
            max_tokens=2_048,
            max_latency_ms=5_000,
        ),
        policy_versions=("context-candidates-v1",),
        self_contained_source=False,
    )
    items = [
        SelectedContextItem(
            event_revision_id=FOCAL,
            source_space="C-finance",
            emitted_at=NOW,
            layer=CandidateContextLayer.FOCAL,
            inclusion_reasons=("focal message",),
            source_version=FOCAL,
            authority_label="slack:message",
            relation_to_focal="focal",
        )
    ]
    layers = [CandidateContextLayer.FOCAL]
    if include_root:
        items.append(
            SelectedContextItem(
                event_revision_id=ROOT,
                source_space="C-finance",
                emitted_at=NOW - timedelta(minutes=2),
                layer=CandidateContextLayer.SOURCE_TOPOLOGY,
                inclusion_reasons=("thread root",),
                source_version=ROOT,
                authority_label="slack:message",
                relation_to_focal="thread_root",
            )
        )
        layers.append(CandidateContextLayer.SOURCE_TOPOLOGY)
    hypothesis = ConversationEpisodeHypothesis.build(
        membership_weights={item.event_revision_id: 1.0 for item in items},
        boundary_alternatives=("thread", "temporal burst"),
        topic_state="payment incident",
        continuity_evidence_refs=tuple(item.event_revision_id for item in items),
        split_merge_evidence_refs=(),
        boundary_confidence=0.8,
        generator_version="episode-probe-v1",
        configuration_version="context-candidates-v1",
    )
    candidate = ConversationContextCandidate.build(
        candidate_id=uuid7(),
        request_id=request.request_id,
        selected_items=tuple(items),
        topology_edge_ids=(("reply:100.1->102.1",) if include_root else ()),
        embedded_episode_hypotheses=(hypothesis,),
        discourse_referents=(),
        layer_coverage=tuple(layers),
        omitted_lane_reasons={},
        cost=ContextCandidateCost(
            event_count=len(items),
            token_count=180,
            source_reads=1,
            model_calls=1,
            latency_ms=40,
        ),
        generator_version="context-candidates-v1",
        configuration_version="context-candidates-config-v1",
    )
    probe = ContextProbeEnvelope(
        candidate_id=candidate.candidate_id,
        probe=ContextProbeResult(
            probe_id=f"probe:{candidate.candidate_id}",
            probe_version="context-light-parser-v1",
            tested_context_hash=candidate.candidate_content_hash,
            unresolved_dependency_refs=(),
            alternative_interpretation_refs=(),
            perturbation_results={"boundary_substitution": 0.01},
            future_or_authority_incident_refs=(),
            expected_value_of_expansion=0.0,
            cost_of_expansion=0.2,
        ),
        completed_probe_surfaces=("source_topology", "boundary_sensitivity"),
        failed_probe_surfaces={},
        semantic_output_digest=canonical_sha256(semantic_output),
        contamination_score=0.01,
    )
    return CommitInterpretationContextCommand(
        context=AgencyWriteContext(
            command_id=uuid7(),
            tenant_id=TENANT,
            processing_authority=authority,
            writer_scope_epoch=WriterScopeEpoch(
                scope_id="legacy-grounding-annotation",
                tenant_id=TENANT,
                semantic_responsibility="interpretation_context",
                source_partition="C-finance",
                writer_owner="GroundingAnnotationAppender",
                epoch=1,
                state=WriterCutoverState.LEGACY,
            ),
            idempotency_key=key,
            issued_at=at - timedelta(seconds=1),
            expires_at=at + timedelta(minutes=30),
        ),
        proposed_snapshot_id=uuid7(),
        proposed_dependency_id=uuid7(),
        selection_subject="entity-mention:the billing thing",
        request=request,
        candidates=(candidate,),
        probes=(probe,),
        policy=ContextSelectionPolicy(
            policy_version="context-selection-v1",
            max_semantic_perturbation=0.1,
            max_contamination_score=0.1,
        ),
        expected=InterpretationContextHeadExpectation(
            expected_aggregate_version=expected_version,
            expected_snapshot_id=expected_snapshot_id,
        ),
        invalidation_keys=("slack:C-finance:102.1",),
        prepared_at=at,
    )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_context_selection_is_atomic_idempotent_and_supersedable(
    fresh_db: asyncpg.Pool,
) -> None:
    applier = GroundingAnnotationAppender()
    first_command = _command(key="context:first")
    async with fresh_db.acquire() as conn, conn.transaction():
        first = await applier.apply_context(
            conn=conn,
            command=first_command,
            now=NOW,
        )
        duplicate = await applier.apply_context(
            conn=conn,
            command=first_command,
            now=NOW,
        )
        assert first.object_version == 1
        assert duplicate.duplicate

        head = await conn.fetchrow(
            """
            SELECT * FROM interpretation_context_heads
            WHERE tenant_id=$1 AND selection_key=$2
            """,
            TENANT,
            first_command.selection_key,
        )
        assert head["current_aggregate_version"] == 1
        first_snapshot_id = head["current_snapshot_id"]
        snapshot = await conn.fetchrow(
            "SELECT * FROM interpretation_context_snapshots WHERE id=$1",
            first_snapshot_id,
        )
        assert snapshot["contract_version"] == "conversation-context-selection-v1"
        assert snapshot["focal_event_revision_ids"] == [FOCAL]
        assert snapshot["supersedes_snapshot_id"] is None
        assert await conn.fetchval(
            "SELECT count(*) FROM conversation_context_candidate_records WHERE snapshot_id=$1",
            first_snapshot_id,
        ) == 1

        second_command = _command(
            key="context:second",
            expected_version=1,
            expected_snapshot_id=first_snapshot_id,
            semantic_output="payment incident clarified",
            at=NOW + timedelta(minutes=1),
        )
        second = await applier.apply_context(
            conn=conn,
            command=second_command,
            now=NOW + timedelta(minutes=1),
        )
        assert second.object_version == 2
        second_snapshot_id = second.object_id
        assert await conn.fetchval(
            "SELECT supersedes_snapshot_id FROM interpretation_context_snapshots WHERE id=$1",
            second_snapshot_id,
        ) == first_snapshot_id
        assert await conn.fetchval(
            "SELECT current_snapshot_id FROM interpretation_context_heads WHERE tenant_id=$1 AND selection_key=$2",
            TENANT,
            first_command.selection_key,
        ) == second_snapshot_id

        counts = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM agency_command_results
               WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'),
              (SELECT count(*) FROM agency_canonical_events
               WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'),
              (SELECT count(*) FROM agency_outbox_records o
               JOIN agency_canonical_events e ON e.id=o.event_id
               WHERE e.tenant_id=$1 AND e.writer_id='GroundingAnnotationAppender')
            """,
            TENANT,
        )
        assert tuple(counts) == (2, 2, 2)

        state = await evaluate_conversation_context_state(
            conn,
            scope=ConversationContextEvaluationScope(
                tenant_id=TENANT,
                start=NOW - timedelta(days=1),
                end=NOW + timedelta(days=1),
                run_id="context-selection-component",
            ),
            artifact_refs=("pytest://context-selection-component",),
        )
        assert state.head_integrity_rate == 1.0
        assert state.selection_reconstructability_rate == 1.0
        assert state.selection_replay_equivalence_rate == 1.0
        assert state.candidate_probe_fate_coverage == 1.0
        assert state.required_probe_surface_coverage == 1.0
        assert state.selection_dependency_coverage == 1.0
        assert state.command_event_coverage == 1.0
        assert state.command_outbox_coverage == 1.0
        assert state.immutable_storage_guard_rate == 1.0
        assert state.incident_counts == {}
        evidence = build_conversation_context_invariant_evidence(
            state,
            registry=load_architecture_registry(
                REPO_ROOT / "architecture/registry.yaml"
            ),
            executed_scenario_ids=frozenset(
                {
                    "TRACE-RECONSTRUCT",
                    "DERIVED-EMBEDDING",
                    "BITEMPORAL-ALGEBRA",
                    "TRANSPORT-ATOMICITY",
                }
            ),
        )
        assert {item.invariant_id for item in evidence} == {
            "INV-16",
            "INV-25",
            "INV-27",
            "INV-29",
        }


@pytest.mark.asyncio
@pytest.mark.integration
async def test_context_selection_rejects_stale_head_and_history_mutation(
    fresh_db: asyncpg.Pool,
) -> None:
    applier = GroundingAnnotationAppender()
    async with fresh_db.acquire() as conn, conn.transaction():
        command = _command(key="context:create")
        created = await applier.apply_context(conn=conn, command=command, now=NOW)
        with pytest.raises(InvariantViolation, match="head does not match"):
            await applier.apply_context(
                conn=conn,
                command=_command(key="context:stale"),
                now=NOW,
            )
        with pytest.raises(
            asyncpg.IntegrityConstraintViolationError,
            match="append-only",
        ):
            await conn.execute(
                "UPDATE interpretation_context_snapshots SET phrase='tampered' WHERE id=$1",
                created.object_id,
            )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_context_selection_rolls_back_partial_protocol_bundle(
    fresh_db: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.domain.conversation_context import repo as context_repo

    async def fail_before_event(*args, **kwargs):
        raise RuntimeError("injected failure before event/outbox")

    monkeypatch.setattr(
        context_repo,
        "insert_protocol_event_and_outbox",
        fail_before_event,
    )
    command = _command(key="context:rollback")
    applier = GroundingAnnotationAppender()
    async with fresh_db.acquire() as conn:
        with pytest.raises(RuntimeError, match="injected failure"):
            async with conn.transaction():
                await applier.apply_context(conn=conn, command=command, now=NOW)
        assert await conn.fetchval(
            "SELECT count(*) FROM interpretation_context_heads WHERE tenant_id=$1",
            TENANT,
        ) == 0
        assert await conn.fetchval(
            """
            SELECT count(*) FROM agency_command_results
            WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'
            """,
            TENANT,
        ) == 0


@pytest.mark.asyncio
@pytest.mark.integration
async def test_mention_detection_is_atomic_exact_and_idempotent(
    fresh_db: asyncpg.Pool,
) -> None:
    observation_id = uuid7()
    content_text = "NBI blocked the renewal; nbi still needs audit proof"
    context_command, context_outcome, mention_command = _mention_commands(
        observation_id=observation_id,
        phrase="NBI",
        content_text=content_text,
    )
    applier = GroundingAnnotationAppender()

    async with fresh_db.acquire() as conn, conn.transaction():
        await _seed_observation(
            conn,
            observation_id=observation_id,
            content_text=content_text,
        )
        await applier.apply_context(
            conn=conn,
            command=context_command,
            now=context_outcome.snapshot.frozen_at,
        )
        first = await applier.apply_mention_detection(
            conn=conn,
            command=mention_command,
            now=mention_command.prepared_at,
        )
        duplicate = await applier.apply_mention_detection(
            conn=conn,
            command=mention_command,
            now=mention_command.prepared_at,
        )

        assert first.object_id == mention_command.detection.detection_id
        assert first.object_version == 1
        assert first.result["fate"] == "detected"
        assert duplicate.duplicate

        row = await conn.fetchrow(
            "SELECT * FROM entity_mention_detections WHERE id=$1",
            mention_command.detection.detection_id,
        )
        mention = row["mention"]
        if isinstance(mention, str):
            mention = json.loads(mention)
        anchors = [
            mention["primary_anchor"],
            *mention["alternate_anchors"],
        ]
        assert [anchor["surface_form"] for anchor in anchors] == ["NBI", "nbi"]
        assert [
            (
                anchor["coordinate"]["span_start"],
                anchor["coordinate"]["span_end"],
            )
            for anchor in anchors
        ] == [(0, 3), (25, 28)]
        assert row["context_snapshot_id"] == UUID(
            context_outcome.snapshot.snapshot_id
        )
        assert row["source_content_hash"] == canonical_sha256(content_text)

        head = await conn.fetchrow(
            """
            SELECT * FROM entity_mention_detection_heads
            WHERE tenant_id=$1 AND detection_key=$2
            """,
            TENANT,
            mention_command.detection_key,
        )
        assert head["current_detection_version"] == 1
        assert head["current_detection_id"] == mention_command.detection.detection_id

        counts = await conn.fetchrow(
            """
            SELECT
              (SELECT count(*) FROM agency_command_results
               WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'),
              (SELECT count(*) FROM agency_canonical_events
               WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'),
              (SELECT count(*) FROM agency_outbox_records o
               JOIN agency_canonical_events e ON e.id=o.event_id
               WHERE e.tenant_id=$1 AND e.writer_id='GroundingAnnotationAppender')
            """,
            TENANT,
        )
        assert tuple(counts) == (2, 2, 2)

        with pytest.raises(
            asyncpg.IntegrityConstraintViolationError,
            match="append-only",
        ):
            await conn.execute(
                "UPDATE entity_mention_detections SET candidate_surface='tampered' WHERE id=$1",
                mention_command.detection.detection_id,
            )


@pytest.mark.asyncio
@pytest.mark.integration
async def test_mention_detection_persists_rejected_unanchored_fate(
    fresh_db: asyncpg.Pool,
) -> None:
    observation_id = uuid7()
    content_text = "The renewal is blocked pending audit proof"
    context_command, context_outcome, mention_command = _mention_commands(
        observation_id=observation_id,
        phrase="Nimbus Bank",
        content_text=content_text,
    )
    applier = GroundingAnnotationAppender()

    async with fresh_db.acquire() as conn, conn.transaction():
        await _seed_observation(
            conn,
            observation_id=observation_id,
            content_text=content_text,
        )
        await applier.apply_context(
            conn=conn,
            command=context_command,
            now=context_outcome.snapshot.frozen_at,
        )
        result = await applier.apply_mention_detection(
            conn=conn,
            command=mention_command,
            now=mention_command.prepared_at,
        )

        assert result.result["fate"] == "rejected_not_anchored"
        assert result.result["mention_id"] is None
        row = await conn.fetchrow(
            "SELECT fate, mention_id, mention FROM entity_mention_detections WHERE id=$1",
            mention_command.detection.detection_id,
        )
        assert tuple(row) == ("rejected_not_anchored", None, None)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_mention_detection_rejects_stale_head_and_rolls_back_source_drift(
    fresh_db: asyncpg.Pool,
) -> None:
    observation_id = uuid7()
    prepared_content = "NBI is blocked"
    context_command, context_outcome, mention_command = _mention_commands(
        observation_id=observation_id,
        phrase="NBI",
        content_text=prepared_content,
    )
    applier = GroundingAnnotationAppender()

    async with fresh_db.acquire() as conn:
        await _seed_observation(
            conn,
            observation_id=observation_id,
            content_text="NBI changed after preparation",
        )
        with pytest.raises(InvariantViolation, match="content changed before commit"):
            async with conn.transaction():
                await applier.apply_context(
                    conn=conn,
                    command=context_command,
                    now=context_outcome.snapshot.frozen_at,
                )
                await applier.apply_mention_detection(
                    conn=conn,
                    command=mention_command,
                    now=mention_command.prepared_at,
                )

        assert await conn.fetchval(
            "SELECT count(*) FROM interpretation_context_snapshots WHERE tenant_id=$1",
            TENANT,
        ) == 0
        assert await conn.fetchval(
            "SELECT count(*) FROM entity_mention_detections WHERE tenant_id=$1",
            TENANT,
        ) == 0
        assert await conn.fetchval(
            """
            SELECT count(*) FROM agency_command_results
            WHERE tenant_id=$1 AND writer_id='GroundingAnnotationAppender'
            """,
            TENANT,
        ) == 0

        await conn.execute(
            "UPDATE observations SET content_text=$2, content=$3::jsonb WHERE id=$1",
            observation_id,
            prepared_content,
            json.dumps({"text": prepared_content}),
        )
        async with conn.transaction():
            await applier.apply_context(
                conn=conn,
                command=context_command,
                now=context_outcome.snapshot.frozen_at,
            )
            await applier.apply_mention_detection(
                conn=conn,
                command=mention_command,
                now=mention_command.prepared_at,
            )
            stale = prepare_entity_mention_detection(
                tenant_id=TENANT,
                observation_id=observation_id,
                phrase="NBI",
                content_text=prepared_content,
                source_channel="slack:message",
                context_command=context_command,
                context_outcome=context_outcome,
                now=NOW + timedelta(minutes=1),
            )
            assert stale.detection_key == mention_command.detection_key
            with pytest.raises(InvariantViolation, match="head does not match"):
                await applier.apply_mention_detection(
                    conn=conn,
                    command=stale,
                    now=stale.prepared_at,
                )
