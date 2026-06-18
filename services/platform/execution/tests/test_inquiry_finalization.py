from __future__ import annotations

from uuid import UUID, uuid4

from services.platform.execution import inquiry, inquiry_finalization
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import Hypothesis
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=UUID("00000000-0000-0000-0000-000000000001"),
        seed_entity_ids=[],
        seed_natural_text="Does the launch have a blocker?",
        seed_occurred_at=None,
        scope_actors=[],
    )


def test_inquiry_imports_finalization_phase_from_canonical_module() -> None:
    assert inquiry._finalize_inquiry_run is inquiry_finalization._finalize_inquiry_run


def test_finalize_inquiry_run_builds_result_notes_and_packet() -> None:
    trigger = _trigger()
    session_id = uuid4()
    stage_timings: list[dict[str, object]] = []
    result = inquiry_finalization._finalize_inquiry_run(
        trigger=trigger,
        cfg=InquiryConfig(),
        session_id=session_id,
        route="DEEP_INQUIRY_PATH",
        mode="deep",
        top_n=5,
        candidate_top_n=5,
        effective_top_n=5,
        baseline_top_n=5,
        signal_class="material",
        weak_signal=False,
        cold_weak_noop_gate={"used": False, "reason": None},
        max_rounds=0,
        hypotheses=(
            Hypothesis(
                id="H1",
                claim="The launch has a dependency blocker",
                confidence=0.6,
                impact_if_true="delay",
            ),
        ),
        all_questions=[],
        all_actions=[],
        answers=[],
        evidence_by_key={},
        retrieval_results=[RetrievalResult(trigger=trigger)],
        unknowns={"dependency owner"},
        stop_status="insufficient_defer",
        stop_reason="no questions",
        action_timing_notes=[],
        stage_timing_notes=stage_timings,
        question_planning_notes=[],
        reconstruction_notes=[],
        baseline_action_cache_notes={"seeded": False},
        sage_reader_notes={"questions": {}},
        total_started=0.0,
    )

    assert result.session_id == session_id
    assert result.notes["execution_engine"] == "inquiry"
    assert result.notes["evidence_count"] == 0
    assert result.notes["retrieval_runtime"]["total_ms"] >= 0
    assert result.retrieval_result.notes["inquiry"] is result.notes
    assert {note["stage"] for note in stage_timings} >= {
        "evidence_rank",
        "final_result_merge",
        "context_packet_compile",
    }
