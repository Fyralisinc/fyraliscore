from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.domain.substrate_candidates import SubstrateCandidate
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.think.prompt import _build_candidate_substrate_section
import services.reasoning.think.substrate_builder as substrate_builder
from services.reasoning.think.substrate_builder import (
    build_substrate_candidates,
    candidate_specs_from_observations,
)


def _obs(
    *,
    source_channel: str,
    source_actor_ref: str | None,
    text: str,
    entities_mentioned: list[dict] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        tenant_id=uuid4(),
        occurred_at=datetime.now(timezone.utc),
        source_channel=source_channel,
        source_actor_ref=source_actor_ref,
        actor_id=None,
        content={},
        content_text=text,
        trust_tier="medium",
        entities_mentioned=entities_mentioned or [],
    )


def test_actor_candidates_merge_email_and_handle_aliases() -> None:
    observations = [
        _obs(
            source_channel="slack:message",
            source_actor_ref="rachel@alpenlabs.io",
            text="Raised a PR for STR-101.",
        ),
        _obs(
            source_channel="github:webhook",
            source_actor_ref="rachel",
            text="Opened pull request #42 in Fyralisinc/saas-api-mocks.",
        ),
    ]

    specs = candidate_specs_from_observations(observations)

    actors = [spec for spec in specs if spec.kind == "actor"]
    rachel = [spec for spec in actors if spec.fingerprint == "actor:rachel"]
    assert len(rachel) == 1
    alias_refs = {alias["full_ref"] for alias in rachel[0].aliases}
    assert "slack:message:rachel@alpenlabs.io" in alias_refs
    assert "github:webhook:rachel" in alias_refs
    assert len(rachel[0].evidence_observation_ids) == 2


def test_repeated_pr_language_preserves_actor_and_issue_context() -> None:
    observations = [
        _obs(
            source_channel="slack:message",
            source_actor_ref="alice@alpenlabs.io",
            text="raised a PR for STR-101",
        ),
        _obs(
            source_channel="slack:message",
            source_actor_ref="bob@alpenlabs.io",
            text="raised a PR for STR-202",
        ),
    ]

    specs = candidate_specs_from_observations(observations)

    contextual_commitments = [
        spec
        for spec in specs
        if spec.kind == "commitment"
        and spec.fingerprint.startswith("commitment:context:")
    ]
    fingerprints = {spec.fingerprint for spec in contextual_commitments}
    assert len(fingerprints) == 2
    assert any("actor:alice" in fingerprint for fingerprint in fingerprints)
    assert any("actor:bob" in fingerprint for fingerprint in fingerprints)
    assert any("str:101" in fingerprint for fingerprint in fingerprints)
    assert any("str:202" in fingerprint for fingerprint in fingerprints)

    patterns = [spec for spec in specs if spec.kind == "pattern"]
    assert patterns, "repeated signal shape should become a discovered pattern"


def test_customer_commitment_relation_preserves_observation_context() -> None:
    observation = _obs(
        source_channel="slack:message",
        source_actor_ref="sam@alpenlabs.io",
        text="Opened PR #77 for Beta Corp Inc renewal blocker.",
    )

    specs = candidate_specs_from_observations([observation])

    customers = [spec for spec in specs if spec.kind == "customer"]
    commitments = [spec for spec in specs if spec.kind == "commitment"]
    assert customers
    assert commitments
    customer = customers[0]
    commitment = commitments[0]
    assert ("commitment", commitment.fingerprint) in customer.related_fingerprints
    assert ("customer", customer.fingerprint) in commitment.related_fingerprints
    relation_metadata = customer.metadata["related_candidates"][0]
    assert relation_metadata["basis"] == "same_observation_customer_commitment"
    assert relation_metadata["evidence_observation_id"] == str(observation.id)


def test_machine_source_actor_becomes_system_not_actor() -> None:
    observations = [
        _obs(
            source_channel="github:webhook",
            source_actor_ref="dependabot[bot]",
            text="Opened pull request #7 in Fyralisinc/saas-api-mocks.",
        )
    ]

    specs = candidate_specs_from_observations(observations)

    assert not [
        spec
        for spec in specs
        if spec.kind == "actor" and "dependabot" in spec.fingerprint
    ]
    assert [
        spec
        for spec in specs
        if spec.kind == "system" and "dependabot" in spec.fingerprint
    ]


def test_candidate_substrate_prompt_renders_exact_scope_ref() -> None:
    candidate_id = uuid4()
    evidence_id = uuid4()
    bundle = ContextBundle(
        notes={
            "substrate_candidates": [
                {
                    "id": str(candidate_id),
                    "kind": "actor",
                    "label": "Rachel",
                    "status": "proposed",
                    "confidence": 0.86,
                    "aliases": [{"source_channel": "slack", "source_actor_ref": "rachel"}],
                    "evidence_observation_ids": [str(evidence_id)],
                    "metadata": {"basis": "source_actor_ref"},
                    "scope_ref": {
                        "type": "candidate_actor",
                        "id": str(candidate_id),
                    },
                }
            ]
        }
    )

    section = "\n".join(_build_candidate_substrate_section(bundle))

    assert "<candidate_substrate>" in section
    assert '"type": "candidate_actor"' in section
    assert str(candidate_id) in section
    assert str(evidence_id) in section


@pytest.mark.asyncio
async def test_build_substrate_candidates_opens_bounded_actor_clarification(monkeypatch) -> None:
    tenant_id = uuid4()
    observation = _obs(
        source_channel="slack:message",
        source_actor_ref="sam",
        text="Sam raised a PR for STR-101.",
    )
    opened: list[str] = []

    async def fake_upsert(conn, **kwargs):
        return SubstrateCandidate(
            id=uuid4(),
            tenant_id=kwargs["tenant_id"],
            kind=kwargs["kind"],
            label=kwargs["label"],
            status="proposed",
            confidence=0.5 if kwargs["kind"] == "actor" else kwargs["confidence"],
            fingerprint=kwargs["fingerprint"],
            aliases=list(kwargs.get("aliases") or []),
            evidence_observation_ids=list(kwargs.get("evidence_observation_ids") or []),
            evidence_model_ids=[],
            related_candidate_ids=[],
            metadata={**dict(kwargs.get("metadata") or {}), "ambiguous": True},
        )

    async def fake_open(conn, *, candidate):
        opened.append(candidate.kind)
        return uuid4()

    async def fake_auto_promote(conn, *, candidate):
        return None

    monkeypatch.setattr(substrate_builder, "upsert_substrate_candidate", fake_upsert)
    monkeypatch.setattr(substrate_builder, "open_candidate_clarification", fake_open)
    monkeypatch.setattr(substrate_builder, "auto_promote_candidate", fake_auto_promote)

    candidates = await build_substrate_candidates(
        object(),
        tenant_id=tenant_id,
        observations=[observation],
        clarification_limit=1,
    )

    assert opened == ["actor"]
    actor = next(candidate for candidate in candidates if candidate["kind"] == "actor")
    assert actor["status"] == "needs_clarification"
    assert actor["clarification_requested"] is True
    assert actor["promotion_plan"]["action"] == "ask_user"


@pytest.mark.asyncio
async def test_build_substrate_candidates_does_not_clarify_routine_source_system(
    monkeypatch,
) -> None:
    tenant_id = uuid4()
    observation = _obs(
        source_channel="github:webhook",
        source_actor_ref=None,
        text="Opened pull request #42 in Fyralisinc/saas-api-mocks.",
    )
    opened: list[str] = []

    async def fake_upsert(conn, **kwargs):
        return SubstrateCandidate(
            id=uuid4(),
            tenant_id=kwargs["tenant_id"],
            kind=kwargs["kind"],
            label=kwargs["label"],
            status="proposed",
            confidence=kwargs["confidence"],
            fingerprint=kwargs["fingerprint"],
            aliases=list(kwargs.get("aliases") or []),
            evidence_observation_ids=list(kwargs.get("evidence_observation_ids") or []),
            evidence_model_ids=[],
            related_candidate_ids=[],
            metadata=dict(kwargs.get("metadata") or {}),
        )

    async def fake_open(conn, *, candidate):
        opened.append(candidate.kind)
        return uuid4()

    async def fake_auto_promote(conn, *, candidate):
        return None

    monkeypatch.setattr(substrate_builder, "upsert_substrate_candidate", fake_upsert)
    monkeypatch.setattr(substrate_builder, "open_candidate_clarification", fake_open)
    monkeypatch.setattr(substrate_builder, "auto_promote_candidate", fake_auto_promote)

    candidates = await build_substrate_candidates(
        object(),
        tenant_id=tenant_id,
        observations=[observation],
        clarification_limit=3,
    )

    assert opened == []
    system = next(candidate for candidate in candidates if candidate["kind"] == "system")
    assert "clarification_requested" not in system
    assert system["promotion_plan"]["action"] in {"ask_user", "promote_resource"}


@pytest.mark.asyncio
async def test_build_substrate_candidates_auto_promotes_safe_candidate(monkeypatch) -> None:
    tenant_id = uuid4()
    resource_id = uuid4()
    observation = _obs(
        source_channel="slack:message",
        source_actor_ref=None,
        text="Alpen Inc is waiting on contract review.",
    )
    promoted: list[str] = []

    async def fake_upsert(conn, **kwargs):
        return SubstrateCandidate(
            id=uuid4(),
            tenant_id=kwargs["tenant_id"],
            kind="customer",
            label="Alpen Inc",
            status="proposed",
            confidence=0.86,
            fingerprint="customer:alpen-inc",
            aliases=list(kwargs.get("aliases") or []),
            evidence_observation_ids=list(kwargs.get("evidence_observation_ids") or []),
            evidence_model_ids=[],
            related_candidate_ids=[],
            metadata={},
        )

    async def fake_open(conn, *, candidate):
        raise AssertionError("safe candidate should not ask for clarification")

    async def fake_auto_promote(conn, *, candidate):
        promoted.append(candidate.kind)
        return {
            "canonical_ref": {
                "type": "customer",
                "id": str(resource_id),
                "resource_id": str(resource_id),
            },
            "resource_id": resource_id,
            "backfilled_models": 2,
        }

    monkeypatch.setattr(substrate_builder, "upsert_substrate_candidate", fake_upsert)
    monkeypatch.setattr(substrate_builder, "open_candidate_clarification", fake_open)
    monkeypatch.setattr(substrate_builder, "auto_promote_candidate", fake_auto_promote)

    candidates = await build_substrate_candidates(
        object(),
        tenant_id=tenant_id,
        observations=[observation],
        clarification_limit=3,
    )

    assert promoted
    assert all(candidate["status"] == "promoted" for candidate in candidates)
    assert all(candidate["promotion_ref"]["type"] == "customer" for candidate in candidates)
    assert all(candidate["promotion_result"]["backfilled_models"] == 2 for candidate in candidates)
