from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation
from lib.shared.types import ModelCreate
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.compiled_reasoning import (
    BatchMemoryCandidateDecision,
    _claim_op_from_batch_decision,
)
from services.reasoning.think.evidence_manifest import (
    authorize_compiler_evidence_manifest,
)
from services.reasoning.think.truth_admission import build_think_admission_command


def _manifest(observation_id, body):
    return [
        {
            "observation_id": str(observation_id),
            "body": body,
            "content_digest": canonical_sha256(body),
        }
    ]


def test_authorizes_exact_persisted_compiler_evidence() -> None:
    observation_id = uuid4()
    body = "Atlas release still has no clearly recorded certificate owner."

    authorize_compiler_evidence_manifest(
        selected_observation_ids=(observation_id,),
        manifest=_manifest(observation_id, body),
        persisted_observations=[{"id": observation_id, "content_text": body}],
    )


def test_rejects_stale_or_tampered_manifest_body() -> None:
    observation_id = uuid4()
    with pytest.raises(
        InvariantViolation,
        match="manifest body does not match",
    ):
        authorize_compiler_evidence_manifest(
            selected_observation_ids=(observation_id,),
            manifest=_manifest(observation_id, "Atlas is blocked."),
            persisted_observations=[
                {"id": observation_id, "content_text": "Atlas is ready."}
            ],
        )


def test_rejects_foreign_valid_observation_id_outside_authorized_subset() -> None:
    authorized_id = uuid4()
    foreign_id = uuid4()
    body = "Atlas is blocked."

    with pytest.raises(InvariantViolation, match="cannot exceed compiler-authorized"):
        authorize_compiler_evidence_manifest(
            selected_observation_ids=(foreign_id,),
            manifest=_manifest(authorized_id, body),
            persisted_observations=[{"id": foreign_id, "content_text": body}],
        )


def test_rejects_tampered_manifest_digest_even_when_body_matches() -> None:
    observation_id = uuid4()
    body = "Atlas is blocked."
    manifest = _manifest(observation_id, body)
    manifest[0]["content_digest"] = "0" * 64

    with pytest.raises(InvariantViolation, match="digest does not match"):
        authorize_compiler_evidence_manifest(
            selected_observation_ids=(observation_id,),
            manifest=manifest,
            persisted_observations=[{"id": observation_id, "content_text": body}],
        )


@pytest.mark.asyncio
async def test_canonical_admission_reopens_exact_manifest_body() -> None:
    observation_id = uuid4()
    tenant_id = uuid4()
    body = "Atlas release still has no clearly recorded certificate owner."
    occurred_at = datetime(2026, 7, 18, tzinfo=timezone.utc)

    class _Connection:
        async def fetch(self, _sql, *_args):
            return [
                {
                    "id": observation_id,
                    "occurred_at": occurred_at,
                    "source_channel": "slack:message",
                    "content_text": body,
                    "trust_tier": "authoritative",
                }
            ]

    proposed = ModelCreate(
        tenant_id=tenant_id,
        born_from_event_id=observation_id,
        proposition={
            "kind": "belief",
            "claim_role": "fact",
            "subject": "Atlas release",
            "assertion": "Atlas release lacks a certificate owner",
            "evidence_observation_manifest": _manifest(observation_id, body),
        },
        natural="Atlas release lacks a clearly recorded certificate owner.",
        embedding=[],
        scope_temporal={},
        confidence=0.69,
        confidence_at_assertion=0.69,
        supporting_event_ids=[observation_id],
    )

    command = await build_think_admission_command(
        _Connection(),  # type: ignore[arg-type]
        proposed=proposed,
        model_id=uuid4(),
        evidence_observation_ids=(observation_id,),
        admitted_at=occurred_at,
    )

    assert command.version.evidence[0].evidence_id == str(observation_id)
    assert "evidence_observation_manifest" not in command.version.proposition


def test_internal_compiler_preserves_authorized_manifest_for_admission() -> None:
    observation_id = uuid4()
    body = "Atlas release still has no clearly recorded certificate owner."
    candidate = {
        "candidate_id": "MDC_WS_atlas_release",
        "op_family": "claim_insert",
        "proposed_text": "Atlas release lacks a certificate owner.",
        "member_observation_ids": [str(observation_id)],
        "semantic_scope": ["Atlas release"],
        "observation_evidence": _manifest(observation_id, body),
    }
    decision = BatchMemoryCandidateDecision(
        candidate_id=candidate["candidate_id"],
        decision="accept",
        operation="claim",
        confidence=0.8,
        claim_role="fact",
        claim_text=candidate["proposed_text"],
        reason="Exact local evidence supports this claim.",
    )
    trigger = TriggerContext(kind="T1", tenant_id=uuid4())

    op, _, error = _claim_op_from_batch_decision(candidate, decision, trigger)

    assert error == ""
    assert op is not None and op.entry is not None
    assert op.entry["proposition"]["evidence_observation_manifest"] == (
        op.entry["evidence_observation_manifest"]
    )


@pytest.mark.asyncio
async def test_canonical_admission_rejects_t4_shaped_unsupported_recommendation() -> None:
    observation_id = uuid4()
    tenant_id = uuid4()
    proposed = ModelCreate(
        tenant_id=tenant_id,
        born_from_event_id=observation_id,
        proposition={
            "kind": "norm",
            "claim_role": "recommendation",
            "target_act_ref": None,
            "target_actor_id": None,
            "expected_impact": None,
            "qualitative_impact": "Turns pressure into an owner-facing review.",
            "proposed_change": {
                "operation": "create",
                "payload": {"kind": "decision_pressure"},
            },
        },
        natural="Review owner and next action for Atlas release.",
        embedding=[],
        scope_temporal={},
        confidence=0.69,
        confidence_at_assertion=0.69,
        supporting_event_ids=[observation_id],
    )

    with pytest.raises(
        InvariantViolation, match="recommendations remain outside canonical truth"
    ):
        await build_think_admission_command(
            object(),  # type: ignore[arg-type]
            proposed=proposed,
            model_id=uuid4(),
            evidence_observation_ids=(observation_id,),
            admitted_at=datetime(2026, 7, 18, tzinfo=timezone.utc),
        )
