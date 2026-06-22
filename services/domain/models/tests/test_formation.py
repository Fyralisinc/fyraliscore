from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from services.domain.models.formation import build_model_formation_candidates


def _obs(actor_id, text: str, *, oid=None):
    return SimpleNamespace(
        id=oid or uuid4(),
        actor_id=actor_id,
        content_text=text,
        occurred_at=datetime(2026, 6, 22, tzinfo=timezone.utc),
    )


def _model(actor_id, text: str, *, mid=None, confidence=0.78):
    return SimpleNamespace(
        id=mid or uuid4(),
        scope_actors=[actor_id],
        natural=text,
        confidence=confidence,
        created_at=datetime(2026, 6, 22, tzinfo=timezone.utc),
    )


def _bundle(*, observations=None, models=None):
    return SimpleNamespace(
        observations=list(observations or []),
        models=list(models or []),
    )


def test_employee_formation_candidates_are_deterministic_for_repeated_actor_evidence():
    actor_id = uuid4()
    first = _obs(actor_id, "Alice shipped the onboarding recovery workflow.")
    second = _obs(actor_id, "Alice handled a messy customer escalation and unblocked support.")
    duplicate = _obs(actor_id, "duplicate should not matter", oid=first.id)

    normal = build_model_formation_candidates(
        None,
        _bundle(observations=[first, second, duplicate]),
    )
    shuffled = build_model_formation_candidates(
        None,
        _bundle(observations=[second, duplicate, first]),
    )

    assert [candidate.candidate_id for candidate in normal] == [
        candidate.candidate_id for candidate in shuffled
    ]
    capability = next(
        candidate for candidate in normal if candidate.spec_id == "employee.capability"
    )
    assert capability.subject_id == actor_id
    assert capability.evidence_observation_ids == tuple(
        sorted({first.id, second.id}, key=str)
    )
    assert capability.allowed_resolutions == (
        "formed",
        "updated",
        "deferred",
        "rejected",
        "already_covered",
    )


def test_employee_formation_candidates_keep_actors_separate():
    alice = uuid4()
    morgan = uuid4()

    candidates = build_model_formation_candidates(
        None,
        _bundle(
            observations=[
                _obs(alice, "Alice needs clearer ownership before starting."),
                _obs(alice, "Alice was blocked waiting for a product decision."),
                _obs(morgan, "Morgan shipped the billing patch."),
                _obs(morgan, "Morgan delivered the renewal import fix."),
            ],
        ),
    )

    by_subject = {(candidate.subject_id, candidate.spec_id) for candidate in candidates}
    assert (alice, "employee.support_need") in by_subject
    assert (morgan, "employee.capability") in by_subject
    assert (alice, "employee.capability") not in by_subject
    assert (morgan, "employee.support_need") not in by_subject


def test_employee_formation_candidates_include_existing_model_evidence():
    actor_id = uuid4()
    existing = _model(
        actor_id,
        "Alice needs written decision boundaries before ambiguous support work.",
    )

    candidates = build_model_formation_candidates(
        None,
        _bundle(
            observations=[
                _obs(actor_id, "Alice was blocked by unclear owner boundaries."),
            ],
            models=[existing],
        ),
    )

    support_need = next(
        candidate
        for candidate in candidates
        if candidate.spec_id == "employee.support_need"
    )
    assert support_need.existing_model_ids == (existing.id,)
    assert support_need.evidence_model_ids == (existing.id,)
    assert str(existing.id) in support_need.to_prompt_dict()["existing_model_ids"]
