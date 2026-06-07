from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.models.repo import _SELECT_COLS_SQL, _hydrate_row
from services.retrieval.primary import TriggerContext
from services.sage.reader import SynthesisReader
from services.think.diff_schema import ClaimOp
from services.topology import ExpectedPair, run_topology_eval
from services.topology.field import LatentTopologyService, impact_signature


def _model(natural: str, *, kind: str = "concern") -> ModelRow:
    now = datetime.now(timezone.utc)
    return ModelRow(
        id=uuid7(),
        tenant_id=uuid7(),
        born_from_event_id=uuid7(),
        proposition={"kind": kind, "about": "Nimbus", "nature": natural},
        natural=natural,
        embedding=[1.0] + [0.0] * 767,
        scope_actors=[uuid7()],
        scope_entities=[{"type": "customer", "id": str(uuid7())}],
        scope_temporal={},
        confidence=0.72,
        activation=0.63,
        falsifier=None,
        signal_readings=[],
        supporting_event_ids=[],
        supporting_model_ids=[],
        status="active",
        created_at=now,
        confidence_at_assertion=0.72,
        proposition_kind=kind,
    )


def test_impact_signature_tracks_consequence_not_just_text() -> None:
    model = _model(
        "Nimbus renewal is blocked because SOC2 security evidence is delayed",
    )

    sig = impact_signature(model)

    assert "money" in sig.flows
    assert "risk" in sig.flows
    assert "blocker" in sig.pressures
    assert "deadline" in sig.pressures
    assert any(surface.startswith("customer:") for surface in sig.surfaces)
    assert "revenue" in sig.stakes
    assert sig.time_shape == "deadline_bound"


def test_relocate_claim_op_is_not_part_of_active_topology() -> None:
    with pytest.raises(Exception):
        ClaimOp(
            op="relocate",  # type: ignore[arg-type]
            model_id=uuid7(),
            relocate_target={"kind": "model_id", "value": str(uuid7())},
        )


async def test_latent_topology_skips_models_without_active_embedding(
    tx_conn,
) -> None:
    service = LatentTopologyService()
    missing_embedding = _model("Sparse concern without an embedding").model_copy(
        update={"embedding": []}
    )
    inactive = _model("Archived concern with an embedding").model_copy(
        update={"status": "archived"}
    )

    missing_result = await service.generate_for_model(
        tx_conn,
        model=missing_embedding,
    )
    inactive_result = await service.generate_for_model(tx_conn, model=inactive)

    assert missing_result.skipped_reason == "model_missing_embedding"
    assert inactive_result.skipped_reason == "model_not_active"


async def test_latent_topology_generates_candidates_and_t4_trigger(
    tx_conn,
    tenant,
    born_from_event,
    actor_id,
) -> None:
    customer_id = uuid7()

    async def insert_model(natural: str, *, confidence: float = 0.82) -> ModelRow:
        mid = uuid7()
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, activation, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status,
                confidence_at_assertion
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                $7::uuid[], $8::jsonb, '{}'::jsonb,
                $9, 0.72, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active',
                $9
            )
            """,
            mid,
            tenant,
            born_from_event,
            json.dumps({
                "kind": "concern",
                "subject": "Nimbus",
                "assertion": natural,
            }),
            natural,
            _model(natural).embedding,
            [actor_id],
            json.dumps([{"type": "customer", "id": str(customer_id)}]),
            confidence,
        )
        row = await tx_conn.fetchrow(
            f"SELECT {_SELECT_COLS_SQL} FROM models WHERE id = $1",
            mid,
        )
        assert row is not None
        return _hydrate_row(row)

    seed = await insert_model(
        "Nimbus renewal revenue is blocked because SOC2 security evidence is delayed"
    )
    await insert_model(
        "Nimbus contract renewal trust is at risk because compliance audit is late"
    )
    await insert_model(
        "Nimbus launch delivery depends on blocked security approval for the customer"
    )

    result = await LatentTopologyService(
        raw_candidate_limit=20,
        candidate_insert_limit=6,
        min_insert_score=0.35,
        min_think_score=0.35,
    ).generate_for_model(tx_conn, model=seed)

    assert result.inserted_candidates
    assert result.enqueued_think_triggers == 1
    row = await tx_conn.fetchrow(
        """
        SELECT source, basis, metadata
        FROM relationship_candidates
        WHERE tenant_id = $1
        ORDER BY judgment_leverage_score DESC
        LIMIT 1
        """,
        tenant,
    )
    assert row is not None
    assert row["source"] == "latent_topology"
    # `blocks`/`enables` now carry `causal_hypothesis` basis (mechanism
    # required). Other allowed kinds (same_issue_as / early_warning_for /
    # contradicts / analogous_to / supports) keep `topology_suggested`.
    assert row["basis"] in {
        "topology_suggested",
        "causal_hypothesis",
        "ontology_gap",
    }
    metadata = (
        json.loads(row["metadata"])
        if isinstance(row["metadata"], str)
        else row["metadata"]
    )
    assert metadata["topology"]["kind"] == "latent_relationship_field"
    trigger = await tx_conn.fetchrow(
        """
        SELECT trigger_kind, trigger_subkind, payload
        FROM think_trigger_queue
        WHERE tenant_id = $1
        ORDER BY enqueued_at DESC
        LIMIT 1
        """,
        tenant,
    )
    assert trigger is not None
    assert trigger["trigger_kind"] == "T4"
    assert trigger["trigger_subkind"] == "latent_relationship_candidate"
    payload = (
        json.loads(trigger["payload"])
        if isinstance(trigger["payload"], str)
        else trigger["payload"]
    )
    assert payload["relationship_candidate_id"]


async def test_latent_topology_sweep_is_bounded_and_inserts_candidates(
    tx_conn,
    tenant,
    born_from_event,
    actor_id,
) -> None:
    customer_id = uuid7()

    async def insert_model(natural: str, activation: float) -> None:
        await tx_conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, activation, falsifier, signal_readings,
                supporting_event_ids, supporting_model_ids,
                contributing_models, status,
                confidence_at_assertion
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                $7::uuid[], $8::jsonb, '{}'::jsonb,
                0.82, $9, NULL, '[]'::jsonb,
                '{}'::uuid[], '{}'::uuid[],
                '{}'::uuid[], 'active',
                0.82
            )
            """,
            uuid7(),
            tenant,
            born_from_event,
            json.dumps({
                "kind": "concern",
                "subject": "Nimbus",
                "assertion": natural,
            }),
            natural,
            _model(natural).embedding,
            [actor_id],
            json.dumps([{"type": "customer", "id": str(customer_id)}]),
            activation,
        )

    await insert_model(
        "Nimbus renewal revenue is blocked by delayed SOC2 security evidence",
        0.91,
    )
    await insert_model(
        "Nimbus contract renewal trust is at risk from late compliance audit",
        0.88,
    )
    await insert_model(
        "Nimbus delivery launch depends on blocked security approval",
        0.83,
    )

    report = await LatentTopologyService(
        raw_candidate_limit=20,
        candidate_insert_limit=6,
        min_insert_score=0.35,
        min_think_score=0.35,
    ).sweep_tenant(
        tx_conn,
        tenant_id=tenant,
        limit=2,
        min_activation=0.8,
    )

    assert report.models_seen == 2
    assert report.candidates_inserted > 0
    assert report.think_triggers_enqueued >= 1
    assert not report.errors


async def test_topology_eval_finds_hidden_pair_without_shared_scope(
    tx_conn,
    tenant,
    born_from_event,
) -> None:
    left = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural=(
            "Beacon renewal revenue is blocked because enterprise SOC2 "
            "security evidence is delayed"
        ),
        embedding=_axis_embedding(0),
        scope_entities=[{"type": "customer", "id": str(uuid7())}],
    )
    right = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural=(
            "Security audit evidence for the enterprise account is blocked "
            "by legal review"
        ),
        embedding=_axis_embedding(1),
        scope_entities=[{"type": "resource", "id": str(uuid7())}],
    )
    unrelated = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural="Office lunch planning is progressing normally",
        embedding=_axis_embedding(2),
        scope_entities=[{"type": "resource", "id": str(uuid7())}],
    )

    report = await run_topology_eval(
        tx_conn,
        tenant_id=tenant,
        seed_models=[left, right, unrelated],
        expected_pairs=[
            ExpectedPair(
                left_model_id=left.id,
                right_model_id=right.id,
                label="security_evidence_blocks_renewal",
                allowed_edge_kinds=("blocks", "same_issue_as", "early_warning_for"),
            )
        ],
        service=LatentTopologyService(
            raw_candidate_limit=20,
            candidate_insert_limit=6,
            min_insert_score=0.30,
            min_think_score=0.95,
        ),
    )

    assert report.recall == 1.0
    hit_id = report.pair_hits["security_evidence_blocks_renewal"]
    row = await tx_conn.fetchrow(
        "SELECT metadata FROM relationship_candidates WHERE id = $1",
        hit_id,
    )
    assert row is not None
    metadata = (
        json.loads(row["metadata"])
        if isinstance(row["metadata"], str)
        else row["metadata"]
    )
    assert "consequence" in metadata["topology"]["selection_sources"]


async def test_topology_candidate_selection_uses_evidence_lane(
    tx_conn,
    tenant,
    born_from_event,
) -> None:
    seed = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural="Platform migration decision is waiting on architecture review",
        embedding=_axis_embedding(3),
        scope_entities=[{"type": "decision", "id": str(uuid7())}],
    )
    downstream = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural="Customer delivery timeline is blocked by migration uncertainty",
        embedding=_axis_embedding(4),
        scope_entities=[{"type": "commitment", "id": str(uuid7())}],
        supporting_model_ids=[seed.id],
    )

    result = await LatentTopologyService(
        raw_candidate_limit=20,
        candidate_insert_limit=6,
        min_insert_score=0.30,
        min_think_score=0.95,
    ).generate_for_model(tx_conn, model=seed, enqueue_think=False)

    assert result.inserted_candidates
    matched = [
        row for row in result.inserted_candidates
        if (
            {row["source_model_id"], row["target_model_id"]} == {seed.id, downstream.id}
            if row["candidate_kind"] == "edge"
            else set(row["member_model_ids"] or []) == {seed.id, downstream.id}
        )
    ]
    assert matched
    metadata = matched[0]["metadata"]
    assert "evidence" in metadata["topology"]["selection_sources"]


async def test_latent_topology_generates_edge_type_candidate_for_decision_gate(
    tx_conn,
    tenant,
    born_from_event,
) -> None:
    seed = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural=(
            "Beacon launch is blocked until the executive approval decision "
            "for the security exception is made"
        ),
        embedding=_axis_embedding(8),
        scope_entities=[{"type": "customer", "id": str(uuid7())}],
    )
    decision = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural=(
            "Executive sign off decision for Beacon security exception is "
            "waiting on approval and blocks release authority"
        ),
        embedding=_axis_embedding(9),
        scope_entities=[{"type": "decision", "id": str(uuid7())}],
    )

    result = await LatentTopologyService(
        raw_candidate_limit=20,
        candidate_insert_limit=6,
        min_insert_score=0.25,
        min_think_score=0.25,
    ).generate_for_model(tx_conn, model=seed)

    edge_type_rows = [
        row for row in result.inserted_candidates
        if row["candidate_kind"] == "edge_type"
        and set(row["member_model_ids"] or []) == {seed.id, decision.id}
    ]
    assert edge_type_rows
    row = edge_type_rows[0]
    assert row["basis"] == "ontology_gap"
    assert row["proposed_proposition"]["proposed_edge_kind"] == "gated_by_decision"
    assert row["proposed_proposition"]["parent_kind"] == "blocks"
    assert row["metadata"]["ontology_gap"]["retrieval_fallback_kind"] == "blocks"
    assert row["metadata"]["topology"]["object_type"] == "edge_type_candidate"

    trigger = await tx_conn.fetchrow(
        """
        SELECT payload
        FROM think_trigger_queue
        WHERE tenant_id = $1
          AND payload->>'relationship_candidate_id' = $2
        """,
        tenant,
        str(row["id"]),
    )
    assert trigger is not None


async def test_topology_edge_type_candidate_feeds_sage_hidden_model_retrieval(
    tx_conn,
    tenant,
    born_from_event,
) -> None:
    seed = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural=(
            "Beacon launch is blocked until the executive approval decision "
            "for the security exception is made"
        ),
        embedding=_axis_embedding(10),
        scope_entities=[{"type": "customer", "id": str(uuid7())}],
    )
    decision = await _insert_model_row(
        tx_conn,
        tenant=tenant,
        born_from_event=born_from_event,
        natural=(
            "Executive sign off decision for Beacon security exception is "
            "waiting on approval and blocks release authority"
        ),
        embedding=_axis_embedding(11),
        scope_entities=[{"type": "decision", "id": str(uuid7())}],
    )

    topology_result = await LatentTopologyService(
        raw_candidate_limit=20,
        candidate_insert_limit=6,
        min_insert_score=0.25,
        min_think_score=0.25,
    ).generate_for_model(tx_conn, model=seed)

    assert any(
        row["candidate_kind"] == "edge_type"
        and row["proposed_proposition"]["proposed_edge_kind"] == "gated_by_decision"
        and set(row["member_model_ids"] or []) == {seed.id, decision.id}
        for row in topology_result.inserted_candidates
    )

    result = await SynthesisReader().read(
        conn=tx_conn,
        tenant_id=tenant,
        trigger=TriggerContext(
            kind="T1",
            tenant_id=tenant,
            observation_id=born_from_event,
            seed_natural_text="What is blocking Beacon launch?",
            precomputed_seed_vector=[0.0] * 768,
        ),
        question_id="Q_DEPENDENCY",
        question="What is blocking Beacon launch?",
        question_primitive="DEPENDENCY",
        hypotheses=(),
    )

    assert decision.id in {model.id for model in result.models}
    trace = next(trace for trace in result.activations if trace.model_id == decision.id)
    assert trace.selected is True
    assert any("propagated:blocks" in reason for reason in trace.activation_reasons)


def _axis_embedding(axis: int) -> list[float]:
    values = [0.0] * 768
    values[axis] = 1.0
    return values


async def _insert_model_row(
    conn,
    *,
    tenant,
    born_from_event,
    natural: str,
    embedding: list[float],
    scope_entities: list[dict],
    supporting_model_ids: list | None = None,
) -> ModelRow:
    mid = uuid7()
    await conn.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation, falsifier, signal_readings,
            supporting_event_ids, supporting_model_ids,
            contributing_models, status,
            confidence_at_assertion
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            '{}'::uuid[], $7::jsonb, '{}'::jsonb,
            0.82, 0.86, NULL, '[]'::jsonb,
            '{}'::uuid[], $8::uuid[],
            '{}'::uuid[], 'active',
            0.82
        )
        """,
        mid,
        tenant,
        born_from_event,
        json.dumps({
            "kind": "concern",
            "subject": "Topology eval",
            "assertion": natural,
        }),
        natural,
        embedding,
        json.dumps(scope_entities),
        supporting_model_ids or [],
    )
    row = await conn.fetchrow(
        f"SELECT {_SELECT_COLS_SQL} FROM models WHERE id = $1",
        mid,
    )
    assert row is not None
    return _hydrate_row(row)
