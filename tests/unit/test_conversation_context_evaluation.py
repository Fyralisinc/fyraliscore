from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from lib.evaluation.conversation_context import (
    ConversationContextEvaluationScope,
    analyze_conversation_context_rows,
    render_conversation_context_markdown,
)


def test_zero_context_exposure_is_unknown_not_perfect() -> None:
    now = datetime(2026, 7, 16, 10, 0, tzinfo=timezone.utc)
    state = analyze_conversation_context_rows(
        scope=ConversationContextEvaluationScope(
            tenant_id=uuid4(),
            start=now,
            end=now + timedelta(hours=1),
            run_id="empty-context-scope",
        ),
        heads=(),
        snapshots=(),
        candidate_records=(),
        commands=(),
        events=(),
        outboxes=(),
        immutable_tables=(
            "interpretation_context_snapshots",
            "conversation_context_candidate_records",
        ),
        guarded_immutable_tables=(
            "interpretation_context_snapshots",
            "conversation_context_candidate_records",
        ),
        artifact_refs=("pytest://empty-context-scope",),
    )

    assert state.head_integrity_rate is None
    assert state.selection_reconstructability_rate is None
    assert state.selection_replay_equivalence_rate is None
    assert state.candidate_probe_fate_coverage is None
    assert state.required_probe_surface_coverage is None
    assert state.selection_dependency_coverage is None
    assert "unknown/not exposed" in render_conversation_context_markdown(state)
