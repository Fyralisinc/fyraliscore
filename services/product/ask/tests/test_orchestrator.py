from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.shared.types import ModelRow, ObservationRow
from services.product.ask.orchestrator import AskOrchestrator
from services.product.ask.schemas import (
    AskScope,
    AskSessionCreateRequest,
    AskTurnRequest,
)
from services.product.ask.store import InMemoryAskStore


TENANT = uuid4()
VIEWER = uuid4()


class _FakeConn:
    async def fetch(self, *args, **kwargs):
        return []


class _ConnProvider:
    @asynccontextmanager
    async def _cm(self):
        yield _FakeConn()

    def __call__(self):
        return self._cm()


class _FakeReader:
    async def read(self, **kwargs):
        model = ModelRow(
            id=uuid4(),
            tenant_id=TENANT,
            born_from_event_id=uuid4(),
            proposition={"text": "Acme onboarding is blocked by SSO owner ambiguity."},
            natural="Acme onboarding is blocked by SSO owner ambiguity.",
            embedding=[0.0] * 3,
            scope_actors=[],
            scope_entities=[],
            scope_temporal={},
            confidence=0.82,
            activation=0.91,
            falsifier=None,
            signal_readings=[],
            reading_contestable=True,
            supporting_event_ids=[],
            supporting_model_ids=[],
            evidential_weight=0.7,
            status="active",
            archived_at=None,
            archive_reason=None,
            created_at=datetime.now(timezone.utc),
            last_retrieved_at=None,
            retrieval_count=0,
            evaluate_at=None,
            resolution_criteria=None,
            contributing_models=[],
            visible_to_subjects=True,
            proposition_kind=None,
            claim_role=None,
            abstraction_level=None,
            time_mode=None,
            modality=None,
            polarity=None,
            domain_tags=[],
            memory_grammar_version="v1",
            confirmed_count=0,
            contested_count=0,
            last_confirmed_at=None,
            confidence_at_assertion=0.82,
            resolved_at=None,
            resolution_outcome=None,
            activation_coefficient=1.0,
            target_actor_id=None,
            caused_act_change_id=None,
        )
        obs = ObservationRow(
            id=uuid4(),
            tenant_id=TENANT,
            occurred_at=datetime.now(timezone.utc),
            ingested_at=datetime.now(timezone.utc),
            kind="signal",
            source_channel="slack",
            source_actor_ref="u1",
            actor_id=VIEWER,
            content={"text": "SSO still lacks an accountable owner."},
            content_text="SSO still lacks an accountable owner.",
            embedding=None,
            embedding_pending=False,
            trust_tier="reputable",
            external_id=None,
            cause_id=None,
            sequence_num=1,
            entities_mentioned=[],
        )
        return type(
            "ReaderResult",
            (),
            {
                "models": (model,),
                "observations": (obs,),
                "projected_evidence": (
                    {
                        "evidence_id": str(obs.id),
                        "evidence_kind": "observation",
                        "summary": obs.content_text,
                        "role": "supporting",
                    },
                ),
                "omitted_projection": ((str(model.id), "budget_exhausted"),),
                "debug": {"fake": True},
            },
        )()


class _ChainReader:
    async def read(self, **kwargs):
        base = await _FakeReader().read(**kwargs)
        obs_a = ObservationRow(
            id=uuid4(),
            tenant_id=TENANT,
            occurred_at=datetime(2026, 1, 1, 9, tzinfo=timezone.utc),
            ingested_at=datetime(2026, 1, 1, 9, tzinfo=timezone.utc),
            kind="signal",
            source_channel="slack",
            source_actor_ref="u1",
            actor_id=VIEWER,
            content={"text": "Bob assumed API latency came from repeated database queries."},
            content_text="Bob assumed API latency came from repeated database queries.",
            embedding=None,
            embedding_pending=False,
            trust_tier="reputable",
            external_id=None,
            cause_id=None,
            sequence_num=1,
            entities_mentioned=[],
        )
        obs_b = ObservationRow(
            id=uuid4(),
            tenant_id=TENANT,
            occurred_at=datetime(2026, 1, 2, 9, tzinfo=timezone.utc),
            ingested_at=datetime(2026, 1, 2, 9, tzinfo=timezone.utc),
            kind="signal",
            source_channel="slack",
            source_actor_ref="u2",
            actor_id=VIEWER,
            content={"text": "The workaround caused stale cache responses after rollout."},
            content_text="The workaround caused stale cache responses after rollout.",
            embedding=None,
            embedding_pending=False,
            trust_tier="reputable",
            external_id=None,
            cause_id=None,
            sequence_num=2,
            entities_mentioned=[],
        )
        return type(
            "ReaderResult",
            (),
            {
                "models": base.models,
                "observations": (obs_a, obs_b),
                "projected_evidence": (),
                "omitted_projection": (),
                "debug": {"fake": True},
            },
        )()


class _FinalityReader:
    async def read(self, **kwargs):
        base = await _FakeReader().read(**kwargs)
        obs_a = ObservationRow(
            id=uuid4(),
            tenant_id=TENANT,
            occurred_at=datetime(2026, 2, 1, 9, tzinfo=timezone.utc),
            ingested_at=datetime(2026, 2, 1, 9, tzinfo=timezone.utc),
            kind="signal",
            source_channel="linear",
            source_actor_ref="u1",
            actor_id=VIEWER,
            content={"text": "Impact assessment: rollback blocked the smart scaling feature."},
            content_text="Impact assessment: rollback blocked the smart scaling feature.",
            embedding=None,
            embedding_pending=False,
            trust_tier="reputable",
            external_id=None,
            cause_id=None,
            sequence_num=1,
            entities_mentioned=[],
        )
        obs_b = ObservationRow(
            id=uuid4(),
            tenant_id=TENANT,
            occurred_at=datetime(2026, 2, 2, 9, tzinfo=timezone.utc),
            ingested_at=datetime(2026, 2, 2, 9, tzinfo=timezone.utc),
            kind="signal",
            source_channel="linear",
            source_actor_ref="u2",
            actor_id=VIEWER,
            content={
                "text": (
                    "DECISION: split the work into basic rule-based scaling now "
                    "and advanced scaling after monitoring is replaced."
                )
            },
            content_text=(
                "DECISION: split the work into basic rule-based scaling now "
                "and advanced scaling after monitoring is replaced."
            ),
            embedding=None,
            embedding_pending=False,
            trust_tier="reputable",
            external_id=None,
            cause_id=None,
            sequence_num=2,
            entities_mentioned=[],
        )
        obs_c = ObservationRow(
            id=uuid4(),
            tenant_id=TENANT,
            occurred_at=datetime(2026, 2, 3, 9, tzinfo=timezone.utc),
            ingested_at=datetime(2026, 2, 3, 9, tzinfo=timezone.utc),
            kind="signal",
            source_channel="linear",
            source_actor_ref="u3",
            actor_id=VIEWER,
            content={"text": "Completed: basic rule-based scaling was deployed."},
            content_text="Completed: basic rule-based scaling was deployed.",
            embedding=None,
            embedding_pending=False,
            trust_tier="reputable",
            external_id=None,
            cause_id=None,
            sequence_num=3,
            entities_mentioned=[],
        )
        return type(
            "ReaderResult",
            (),
            {
                "models": base.models,
                "observations": (obs_a, obs_b, obs_c),
                "projected_evidence": (),
                "omitted_projection": (),
                "debug": {"fake": True},
            },
        )()


class _PremiseReader:
    async def read(self, **kwargs):
        base = await _FakeReader().read(**kwargs)

        def obs(text: str, sequence_num: int) -> ObservationRow:
            return ObservationRow(
                id=uuid4(),
                tenant_id=TENANT,
                occurred_at=datetime(2026, 3, sequence_num, 9, tzinfo=timezone.utc),
                ingested_at=datetime(2026, 3, sequence_num, 9, tzinfo=timezone.utc),
                kind="signal",
                source_channel="slack",
                source_actor_ref=f"u{sequence_num}",
                actor_id=VIEWER,
                content={"text": text},
                content_text=text,
                embedding=None,
                embedding_pending=False,
                trust_tier="authoritative",
                external_id=None,
                cause_id=None,
                sequence_num=sequence_num,
                entities_mentioned=[],
            )

        observations = (
            obs(
                "Acme state changed from Commit to At Risk after data migration became active.",
                1,
            ),
            obs(
                "Acme onboarding is missing the security review owner assignment step before procurement can move forward.",
                2,
            ),
            obs(
                "Recurring trap: security review stalls unless ownership is assigned early.",
                3,
            ),
            obs(
                "SSO is not the only blocker; data migration is also active and the CRM Commit premise is unsupported.",
                4,
            ),
            obs(
                "No explicit owner is represented in Synthesis for security review.",
                5,
            ),
        )
        return type(
            "ReaderResult",
            (),
            {
                "models": base.models,
                "observations": observations,
                "projected_evidence": (),
                "omitted_projection": (),
                "debug": {"fake": True},
            },
        )()


class _AlternativeValueReader:
    async def read(self, **kwargs):
        base = await _FakeReader().read(**kwargs)

        def obs(text: str, sequence_num: int) -> ObservationRow:
            return ObservationRow(
                id=uuid4(),
                tenant_id=TENANT,
                occurred_at=datetime(2026, 4, sequence_num, 9, tzinfo=timezone.utc),
                ingested_at=datetime(2026, 4, sequence_num, 9, tzinfo=timezone.utc),
                kind="signal",
                source_channel="catalog",
                source_actor_ref=f"page-{sequence_num}",
                actor_id=VIEWER,
                content={"text": text},
                content_text=text,
                embedding=None,
                embedding_pending=False,
                trust_tier="authoritative",
                external_id=None,
                cause_id=None,
                sequence_num=sequence_num,
                entities_mentioned=[],
            )

        observations = (
            obs("Standard Laptop order page checkbox choice count = 3.", 1),
            obs("Developer Laptop (Mac) order page checkbox choice count = 5.", 2),
            obs("Sales Laptop order page checkbox choice count = 7.", 3),
        )
        return type(
            "ReaderResult",
            (),
            {
                "models": base.models,
                "observations": observations,
                "projected_evidence": (),
                "omitted_projection": (),
                "debug": {"fake": True},
            },
        )()


@pytest.fixture(autouse=True)
def _allow_access(monkeypatch):
    async def fake_can_read(*args, **kwargs):
        return type("Decision", (), {"allowed": True})()

    monkeypatch.setattr(
        "services.product.ask.orchestrator.can_read",
        fake_can_read,
        raising=True,
    )


async def test_answer_turn_uses_sage_reader_and_persists_evidence():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FakeReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Acme deal"),
            source_route="/today",
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(query="Why is Acme blocked?"),
    )

    assert response.payload.confidence > 0.7
    assert response.payload.evidence[0].summary == "SSO still lacks an accountable owner."
    evidence, omitted = await store.list_evidence(response.retrieval_run_id)
    assert len(evidence) == 1
    assert omitted[0].omitted_reason == "budget_exhausted"


async def test_answer_turn_adds_composed_chain_for_causal_observations():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_ChainReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Latency review"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(
            query="During Bob's work, what was his mental model and what did it cause?",
        ),
    )

    first = response.payload.evidence[0]
    assert first.source_kind == "composed_chain"
    assert "Bob assumed API latency" in first.summary
    assert "caused stale cache responses" in first.summary
    assert "accessible evidence chain says" in response.payload.answer
    assert "Bob assumed API latency" in response.payload.answer
    assert len(first.raw_payload["source_observation_ids"]) == 2


async def test_external_tool_question_surfaces_unknown_boundary():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_ChainReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Repository review"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(
            query=(
                "Clone the repository and inspect the repository code: "
                "what exact line number caused the issue?"
            ),
        ),
    )

    assert any(
        "require repository" in unknown
        for unknown in response.payload.unknowns
    )


async def test_final_solution_answer_uses_decision_and_outcome_chain():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FinalityReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Scaling review"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(
            query="After the rollback, what was the final solution?",
        ),
    )

    assert "accessible evidence chain says" in response.payload.answer
    assert "DECISION: split the work" in response.payload.answer
    assert "Completed: basic rule-based scaling was deployed" in response.payload.answer
    assert not any("missing expected answer roles" in item for item in response.payload.unknowns)


async def test_answer_turn_challenges_stale_premise_with_state_contract():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_PremiseReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Acme deal"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(
            query="Why is Acme onboarding blocked by SSO and still marked Commit?",
        ),
    )

    assert response.payload.premise_check["status"] == "stale_or_incomplete"
    assert "That premise is incomplete" in response.payload.answer
    assert "data migration" in response.payload.answer
    assert response.payload.possible_state_change is not None
    slots = {fact["slot"] for fact in response.payload.state_facts}
    assert {
        "current_blocker",
        "current_stage",
        "dynamic_state",
        "premise_challenge",
        "workflow_missing_step",
    } <= slots
    assert any("Commit" in item for item in response.payload.counterevidence)
    assert any("premise" in item.casefold() for item in response.payload.unknowns)


async def test_owner_question_answers_missing_owner_slot_directly():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_PremiseReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Acme deal"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(query="Who owns security review?"),
    )

    assert "No explicit owner is represented in Synthesis" in response.payload.answer
    assert any(
        fact["slot"] == "current_owner" and fact["status"] == "missing"
        for fact in response.payload.state_facts
    )


async def test_alternative_value_question_surfaces_option_temporal_and_exact_roles():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_AlternativeValueReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Catalog comparison"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(
            query=(
                "Which page has the largest checkbox choice count?\n\n"
                "A. Standard Laptop\n"
                "B. Developer Laptop (Mac)\n"
                "C. Sales Laptop"
            ),
        ),
    )

    evidence_text = "\n".join(item.summary for item in response.payload.evidence[:4])
    assert "Standard Laptop" in evidence_text
    assert "Developer Laptop (Mac)" in evidence_text
    assert "Sales Laptop" in evidence_text
    assert any(fact["slot"] == "exact_value" for fact in response.payload.state_facts)
    assert any(fact["slot"] == "temporal_anchor" for fact in response.payload.state_facts)
    assert not any(
        "alternative_coverage" in unknown
        for unknown in response.payload.unknowns
    )


async def test_state_gap_query_creates_validation_gated_change():
    store = InMemoryAskStore()
    orch = AskOrchestrator(
        store=store,
        conn_provider=_ConnProvider(),
        reader=_FakeReader(),
    )
    session = await orch.create_session(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        body=AskSessionCreateRequest(
            initial_scope=AskScope(type="current_page", label="Acme deal"),
        ),
    )

    response = await orch.answer_turn(
        tenant_id=TENANT,
        viewer_id=VIEWER,
        session_id=session.id,
        body=AskTurnRequest(query="Are we sure this is not missing a blocker?"),
    )

    change = response.payload.possible_state_change
    assert change is not None
    assert change.status == "proposed"
    accepted = await orch.act_on_proposed_change(
        tenant_id=TENANT,
        change_id=change.id,
        action="accept",
        note=None,
        delegate_to=None,
    )
    assert accepted.status == "accepted"
    assert accepted.linked_trigger_id is not None
