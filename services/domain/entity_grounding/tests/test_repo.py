from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from lib.shared.errors import InvariantViolation
from services.domain.agency_protocol import AgencyCommitResult
from services.domain.conversation_context.repo import GroundingAnnotationAppender
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    build_grounding_episode,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.entity_grounding.repo import EntityGroundingRepo


class _Transaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Connection:
    def __init__(
        self,
        *,
        content_text: str = "",
        context_snapshot_hash: str = "0" * 64,
    ) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.content_text = content_text
        self.context_snapshot_hash = context_snapshot_hash

    def transaction(self):
        return _Transaction()

    async def fetchrow(self, sql: str, *args):
        if "FROM interpretation_context_snapshots" in sql:
            return {"snapshot_content_hash": self.context_snapshot_hash}
        if "FROM observations" in sql:
            return {"content_text": self.content_text}
        return None

    async def execute(self, sql: str, *args):
        self.statements.append((sql, args))
        return "INSERT 0 1"


def _build_episode(
    *,
    tenant_id,
    observation_id,
    phrase: str,
    content_text: str,
    now: datetime,
):
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        occurred_at=now,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=now + timedelta(minutes=1),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        content_text=content_text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=now + timedelta(minutes=1),
    )
    episode = build_grounding_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        occurred_at=now,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        candidates=(
            GroundingCandidateInput(
                canonical_ref={"type": "customer", "id": "customer:nimbus"},
                candidate_source="tenant_aliases",
                positive_evidence_refs=("entity-alias:Nimbus Bank",),
                independent_identity_evidence_refs=(
                    "manual-alias-adjudication:1",
                ),
            ),
        ),
        model_candidate_id=None,
        model_canonical_ref={"type": "customer", "id": "customer:nimbus"},
        model_confidence=0.91,
        model_reasoning="bounded candidate match",
        high_confidence=0.8,
        review_min=0.5,
        prepared_context_command=context_command,
        prepared_context_outcome=context_outcome,
        prepared_mention_detection_command=mention_command,
        now=now + timedelta(minutes=1),
    )
    return episode


@pytest.mark.asyncio
async def test_repo_appends_six_linked_sidecars_and_never_mutates_truth() -> None:
    now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
    tenant_id = uuid4()
    observation_id = uuid4()
    content_text = "NBI is ready for review"
    episode = _build_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        content_text=content_text,
        now=now,
    )
    conn = _Connection(
        content_text=content_text,
        context_snapshot_hash=episode.context_snapshot.snapshot_content_hash,
    )
    repo = EntityGroundingRepo(pool=object())  # type: ignore[arg-type]

    await repo.append_episode(
        episode=episode,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        phrase="NBI",
        conn=conn,  # type: ignore[arg-type]
    )

    sql = "\n".join(statement for statement, _ in conn.statements)
    for table in (
        "agency_command_results",
        "interpretation_context_snapshots",
        "interpretation_context_heads",
        "conversation_context_candidate_records",
        "entity_mention_detections",
        "entity_mention_detection_heads",
        "agency_canonical_events",
        "agency_outbox_records",
        "entity_candidate_generation_requests",
        "entity_candidate_sets",
        "resolution_assessments",
        "grounding_admission_decisions",
        "grounding_traces",
    ):
        assert f"INSERT INTO {table}" in sql
    assert "UPDATE observations" not in sql
    assert "INSERT INTO observations" not in sql
    assert "entity_aliases" not in sql
    request_sql, request_args = next(
        (statement, args)
        for statement, args in conn.statements
        if "INSERT INTO entity_candidate_generation_requests" in statement
    )
    detection = episode.mention_detection_command.detection
    assert "entity_mention_detection_id" in request_sql
    assert "entity_mention_id" in request_sql
    assert request_args[6] == detection.detection_id
    assert detection.mention is not None
    assert request_args[7] == detection.detection_id
    trace_sql, trace_args = next(
        (statement, args)
        for statement, args in conn.statements
        if "INSERT INTO grounding_traces" in statement
    )
    assert "FALSE, FALSE" in trace_sql
    assert trace_args[5] == detection.detection_id
    assert trace_args[6] == detection.detection_id
    assert trace_args[11] == "resolved_for_consumer"
    work_sql, work_args = next(
        (statement, args)
        for statement, args in conn.statements
        if "INSERT INTO entity_grounding_work_items" in statement
    )
    assert "INSERT INTO entity_grounding_work_items" in work_sql
    assert work_args[4] == "resolved_for_consumer"


@pytest.mark.asyncio
async def test_retryable_fate_is_durable_and_has_a_due_time() -> None:
    conn = _Connection()
    repo = EntityGroundingRepo(pool=object())  # type: ignore[arg-type]
    due = datetime(2026, 7, 16, 10, 5, tzinfo=timezone.utc)

    await repo.record_retryable_fate(
        tenant_id=uuid4(),
        source_observation_id=uuid4(),
        phrase="it",
        failure_class="provider_timeout",
        failure_reason="timed out",
        next_attempt_at=due,
        conn=conn,  # type: ignore[arg-type]
    )

    assert len(conn.statements) == 1
    sql, args = conn.statements[0]
    assert "INSERT INTO entity_grounding_work_items" in sql
    assert "status = 'retry_scheduled'" in sql
    assert args[4] == due
    assert args[5] == "provider_timeout"


@pytest.mark.asyncio
async def test_rejected_mention_commits_terminal_fate_without_candidate_artifacts() -> None:
    now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
    tenant_id = uuid4()
    observation_id = uuid4()
    phrase = "NBI"
    content_text = "The source text does not contain the proposed entity"
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        occurred_at=now,
        source_channel="slack:message",
        source_space="C-finance",
        topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(),
        selection_dependency_refs=(),
        now=now + timedelta(minutes=1),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase=phrase,
        content_text=content_text,
        source_channel="slack:message",
        context_command=context_command,
        context_outcome=context_outcome,
        now=now + timedelta(minutes=1),
    )
    conn = _Connection(
        content_text=content_text,
        context_snapshot_hash=context_outcome.snapshot.snapshot_content_hash,
    )
    repo = EntityGroundingRepo(pool=object())  # type: ignore[arg-type]

    detection_id = await repo.append_rejected_mention(
        context_command=context_command,
        context_outcome=context_outcome,
        mention_detection_command=mention_command,
        tenant_id=tenant_id,
        source_observation_id=observation_id,
        phrase=phrase,
        conn=conn,  # type: ignore[arg-type]
    )

    assert detection_id == mention_command.detection.detection_id
    sql = "\n".join(statement for statement, _ in conn.statements)
    assert "INSERT INTO interpretation_context_snapshots" in sql
    assert "INSERT INTO entity_mention_detections" in sql
    assert "INSERT INTO entity_grounding_work_items" in sql
    assert "INSERT INTO entity_candidate_generation_requests" not in sql
    assert "INSERT INTO entity_candidate_sets" not in sql
    assert "INSERT INTO resolution_assessments" not in sql
    assert "INSERT INTO grounding_admission_decisions" not in sql
    assert "INSERT INTO grounding_traces" not in sql
    work_sql, work_args = next(
        (statement, args)
        for statement, args in conn.statements
        if "INSERT INTO entity_grounding_work_items" in statement
    )
    assert "'unresolved'" in work_sql
    assert '"fate_kind": "mention_rejected"' in work_args[4]
    assert str(detection_id) in work_args[4]


@pytest.mark.asyncio
async def test_repo_rejects_detection_bound_to_a_different_phrase() -> None:
    now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
    tenant_id = uuid4()
    observation_id = uuid4()
    episode = _build_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        content_text="NBI is ready",
        now=now,
    )
    conn = _Connection(
        content_text="NBI is ready",
        context_snapshot_hash=episode.context_snapshot.snapshot_content_hash,
    )
    repo = EntityGroundingRepo(pool=object())  # type: ignore[arg-type]

    with pytest.raises(InvariantViolation, match="one grounding opportunity"):
        await repo.append_episode(
            episode=episode,
            tenant_id=tenant_id,
            source_observation_id=observation_id,
            phrase="different phrase",
            conn=conn,  # type: ignore[arg-type]
        )

    assert conn.statements == []


@pytest.mark.asyncio
async def test_repo_rejects_a_replayed_detection_with_different_content(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
    tenant_id = uuid4()
    observation_id = uuid4()
    episode = _build_episode(
        tenant_id=tenant_id,
        observation_id=observation_id,
        phrase="NBI",
        content_text="NBI is ready",
        now=now,
    )
    detection = episode.mention_detection_command.detection

    async def mismatched_replay(self, *, conn, command, now=None):
        return AgencyCommitResult(
            command_result_id=uuid4(),
            event_id=uuid4(),
            object_id=detection.detection_id,
            object_version=detection.detection_version,
            result={
                "detection_digest": "0" * 64,
                "fate": detection.fate.value,
                "mention_id": detection.mention.mention_id,
                "context_snapshot_id": str(detection.context_snapshot_id),
                "context_snapshot_digest": detection.context_snapshot_digest,
                "source_content_hash": detection.source_content_hash,
            },
            duplicate=True,
        )

    monkeypatch.setattr(
        GroundingAnnotationAppender,
        "apply_mention_detection",
        mismatched_replay,
    )
    conn = _Connection(
        content_text="NBI is ready",
        context_snapshot_hash=episode.context_snapshot.snapshot_content_hash,
    )
    repo = EntityGroundingRepo(pool=object())  # type: ignore[arg-type]

    with pytest.raises(InvariantViolation, match="prepared detection"):
        await repo.append_episode(
            episode=episode,
            tenant_id=tenant_id,
            source_observation_id=observation_id,
            phrase="NBI",
            conn=conn,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_repo_rejects_a_snapshot_that_differs_from_pre_llm_selection() -> None:
    now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
    tenant_id = uuid4()
    observation_id = uuid4()
    def episode_for(phrase: str):
        return _build_episode(
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
            content_text=f"Discussion of {phrase}",
            now=now,
        )

    original = episode_for("NBI")
    mismatched = replace(
        original,
        context_snapshot=episode_for("Nimbus").context_snapshot,
    )
    conn = _Connection(
        content_text="Discussion of NBI",
        context_snapshot_hash=original.context_snapshot.snapshot_content_hash,
    )
    repo = EntityGroundingRepo(pool=object())  # type: ignore[arg-type]

    with pytest.raises(InvariantViolation, match="snapshot differs"):
        await repo.append_episode(
            episode=mismatched,
            tenant_id=tenant_id,
            source_observation_id=observation_id,
            phrase="NBI",
            conn=conn,  # type: ignore[arg-type]
        )

    assert conn.statements == []
