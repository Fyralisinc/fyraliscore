"""
services/models/tests/test_model_quality_suite.py — integration tests
locking in the model-layer quality chain delivered by the spec revamp:

    split (Phase 2) → reconcile (Phase 3) → quality_gate (Phase 4)
        → insert + topo_embedding init (Phase 0)
        → per-kind relationship candidate generation (Phase 5)
        → T4 adjudication structural validation (Phase 5)
        + extended SituationProposition (Phase 1)

These tests use `apply_diff` for end-to-end coverage. Pure-unit
behavior is covered by per-module test files; this suite asserts the
behaviors the integrated chain is supposed to produce.
"""
from __future__ import annotations

import uuid
from typing import Any

import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.models.repo import ModelsRepo
from services.relationships.candidates import TOPOLOGY_EMITTABLE_EDGE_KINDS
from services.think.applier import apply_diff
from services.think.diff_schema import ClaimOp, ValidatedDiff
from services.think.quality_gate import QualityContext, apply_verdict, score_quality
from services.think.splitter import is_compound, split_compound_claim_op


# Async tests opt in via per-function @pytest.mark.asyncio rather than
# a module-level mark — some unit-style tests in this file are sync.


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _make_embedding(text: str) -> list[float]:
    from services.think.tests.conftest import make_embedding
    return make_embedding(text)


async def _seed_observation(conn, tenant_id: uuid.UUID, text: str) -> uuid.UUID:
    from pgvector.asyncpg import register_vector
    try:
        await register_vector(conn)
    except Exception:
        pass
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, kind, source_channel,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), 'signal', 'test', '{}'::jsonb, $3,
                $4, FALSE, 'authoritative')
        """,
        oid, tenant_id, text, _make_embedding(text),
    )
    return oid


def _insert_op(
    *,
    tenant_id: uuid.UUID,
    born_from_event_id: uuid.UUID,
    natural: str,
    kind: str = "state",
    subject: str = "subject",
    assertion: str | None = None,
    confidence: float = 0.7,
    falsifier: dict[str, Any] | None = None,
    scope_actors: list[Any] | None = None,
    scope_entities: list[Any] | None = None,
    extra_proposition: dict[str, Any] | None = None,
) -> ClaimOp:
    if kind == "concern":
        proposition: dict[str, Any] = {
            "kind": "concern",
            "about": subject,
            "nature": assertion or natural,
            "raised_by": "test-suite",
        }
    elif kind == "pattern_instance":
        proposition = {
            "kind": "pattern_instance",
            "subject": subject,
            "assertion": assertion or natural,
        }
    else:
        proposition = {
            "kind": kind,
            "subject": subject,
            "assertion": assertion or natural,
        }
    if extra_proposition:
        proposition.update(extra_proposition)
    return ClaimOp(op="insert", entry={
        "tenant_id": str(tenant_id),
        "born_from_event_id": str(born_from_event_id),
        "proposition": proposition,
        "natural": natural,
        "embedding": _make_embedding(natural),
        "scope_actors": scope_actors or [],
        "scope_entities": scope_entities or [],
        "scope_temporal": {},
        "confidence": confidence,
        "confidence_at_assertion": confidence,
        "falsifier": falsifier or {
            "kind": "external_evidence",
            "description": "External audit shows otherwise.",
        },
    })


# =====================================================================
# Compound / splitting
# =====================================================================


@pytest.mark.asyncio
async def test_atomic_claim_passes_through_without_split(
    fresh_db, tenant
):
    """Single-clause claim → exactly 1 model_id, no synthesized situation."""
    async with fresh_db.acquire() as conn:
        oid = await _seed_observation(conn, tenant, "Acme renewed the contract")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[_insert_op(
                tenant_id=tenant,
                born_from_event_id=oid,
                natural="Acme renewed the contract on schedule.",
            )],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )
        assert len(result["applied_model_ids"]) == 1
        assert result.get("split_summary", {}).get(
            "compound_inputs", 0
        ) == 0


@pytest.mark.asyncio
async def test_compound_signal_splits_into_primitives_plus_situation(
    fresh_db, tenant
):
    """4-clause compound → ≥3 atomic models + 1 situation with member_ids."""
    compound = (
        "HarborRail procurement evidence is delayed, "
        "sponsor confidence is dropping, "
        "ARR renewal is at risk, "
        "and SOC2 audit review is blocked."
    )
    async with fresh_db.acquire() as conn:
        oid = await _seed_observation(conn, tenant, compound)
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[_insert_op(
                tenant_id=tenant,
                born_from_event_id=oid,
                natural=compound,
                kind="concern",
                falsifier={"kind": "external_evidence", "description": "External audit attests SOC2 evidence is delivered."},
            )],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )
            split_summary = result["split_summary"]
            assert split_summary["compound_inputs"] == 1
            assert split_summary["atomic_outputs"] >= 3
            assert split_summary["synthesized_situations"] == 1
            kinds = await conn.fetch(
                "SELECT claim_role FROM models WHERE tenant_id = $1",
                tenant,
            )
            kind_list = [r["claim_role"] for r in kinds]
            import json as _json
            assert "situation" in kind_list, (
                f"ops: {_json.dumps(result['claim_ops'], default=str, indent=2)}"
            )
            sit_row = await conn.fetchrow(
                """
                SELECT proposition
                FROM models
                WHERE tenant_id = $1 AND claim_role = 'situation'
                ORDER BY created_at DESC LIMIT 1
                """,
                tenant,
            )
            assert sit_row is not None
            members = sit_row["proposition"]
            if isinstance(members, str):
                import json
                members = json.loads(members)
            assert members.get("member_model_ids"), members


@pytest.mark.asyncio
async def test_synthesized_situation_is_queryable_by_grammar_and_membership(
    fresh_db, tenant
):
    """A split situation must be a composite belief and a sidecar-backed query anchor."""
    compound = (
        "HarborRail procurement evidence is delayed, "
        "sponsor confidence is dropping, "
        "ARR renewal is at risk, "
        "and SOC2 audit review is blocked."
    )
    async with fresh_db.acquire() as conn:
        oid = await _seed_observation(conn, tenant, compound)
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[_insert_op(
                tenant_id=tenant,
                born_from_event_id=oid,
                natural=compound,
                kind="concern",
                falsifier={
                    "kind": "external_evidence",
                    "description": (
                        "Procurement evidence lands, sponsor confidence "
                        "recovers, renewal is confirmed, and SOC2 unblocks."
                    ),
                },
            )],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )

            rows = await conn.fetch(
                """
                SELECT id, proposition_kind, claim_role, abstraction_level,
                       time_mode, modality, polarity, domain_tags, proposition
                FROM models
                WHERE tenant_id = $1 AND born_from_event_id = $2
                ORDER BY created_at, id::text
                """,
                tenant,
                oid,
            )
            situation_rows = [r for r in rows if r["claim_role"] == "situation"]
            atomic_rows = [r for r in rows if r["claim_role"] != "situation"]

            assert result["split_summary"]["compound_inputs"] == 1
            assert result["split_summary"]["synthesized_situations"] == 1
            assert len(situation_rows) == 1
            assert len(atomic_rows) >= 3

            sit = situation_rows[0]
            prop = sit["proposition"]
            if isinstance(prop, str):
                import json
                prop = json.loads(prop)

            assert sit["proposition_kind"] == "belief"
            assert sit["claim_role"] == "situation"
            assert sit["abstraction_level"] == "composite"
            assert sit["time_mode"] == "current"
            assert sit["modality"] == "inferred"
            assert sit["polarity"] == "mixed"
            assert {"customers", "execution", "risk"}.issubset(
                set(sit["domain_tags"])
            )
            assert prop["kind"] == "belief"
            assert prop["claim_role"] == "situation"
            assert prop["pressure_type"] == "revenue"
            assert prop["shared_mechanism"]
            assert prop["judgment_change"]
            assert prop["open_falsifier"].startswith("Procurement evidence lands")
            assert "member_model_pending" not in prop

            member_ids = {uuid.UUID(raw) for raw in prop["member_model_ids"]}
            atomic_ids = {r["id"] for r in atomic_rows}
            assert member_ids == atomic_ids
            assert len(member_ids) == len(prop["member_model_ids"])

            sidecar_rows = await conn.fetch(
                """
                SELECT member_model_id, source, evidence_event_ids
                FROM model_composition_members
                WHERE tenant_id = $1 AND composite_model_id = $2
                ORDER BY member_model_id::text
                """,
                tenant,
                sit["id"],
            )
            assert {r["member_model_id"] for r in sidecar_rows} == member_ids
            assert all(r["source"] == "model_proposition" for r in sidecar_rows)
            assert all(list(r["evidence_event_ids"]) == [] for r in sidecar_rows)

            grammar_query_id = await conn.fetchval(
                """
                SELECT id
                FROM models
                WHERE tenant_id = $1
                  AND proposition_kind = 'belief'
                  AND claim_role = 'situation'
                  AND abstraction_level = 'composite'
                  AND time_mode = 'current'
                  AND modality = 'inferred'
                  AND polarity = 'mixed'
                  AND domain_tags @> ARRAY['customers','execution','risk']::text[]
                LIMIT 1
                """,
                tenant,
            )
            assert grammar_query_id == sit["id"]

            reverse_lookup_id = await conn.fetchval(
                """
                SELECT composite_model_id
                FROM model_composition_members
                WHERE tenant_id = $1 AND member_model_id = $2
                LIMIT 1
                """,
                tenant,
                next(iter(member_ids)),
            )
            assert reverse_lookup_id == sit["id"]


# =====================================================================
# Quality gate
# =====================================================================


@pytest.mark.asyncio
async def test_vague_ephemeral_model_downgraded_or_rejected(
    fresh_db, tenant
):
    """Pure-sentiment ephemeral claim → no model insert, summary says so."""
    async with fresh_db.acquire() as conn:
        oid = await _seed_observation(conn, tenant, "rough call")
        diff = ValidatedDiff(
            trigger_ref=uuid7(),
            tenant_id=tenant,
            claim_ops=[_insert_op(
                tenant_id=tenant,
                born_from_event_id=oid,
                natural="Yesterday's call with the customer felt rough.",
                kind="state",
                confidence=0.45,
                falsifier={"kind": "self_correction", "description": "I changed my mind about how it went."},
            )],
        )
        repo = ModelsRepo(fresh_db, embedder=None)
        async with conn.transaction():
            result = await apply_diff(
                diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid,
                models_repo=repo,
            )
        decisions = {
            entry.get("op"): entry
            for entry in result["claim_ops"]
        }
        assert "skip" in decisions, decisions
        skip = decisions["skip"]
        assert skip["reason"] in {
            "quality_gate_reject",
            "quality_gate_downgrade_to_evidence",
        }


def test_pattern_instance_without_pattern_id_rejected_by_gate():
    """Quality gate unit-level: pattern_instance kind without pattern_id → reject."""
    op = ClaimOp(op="insert", entry={
        "proposition": {
            "kind": "pattern_instance",
            "subject": "deploy",
            "assertion": "matches pattern X",
        },
        "natural": "this deploy matches pattern X",
        "confidence_at_assertion": 0.7,
        "falsifier": {"kind": "external_evidence", "description": "External evidence shows it doesn't match."},
    })
    verdict = score_quality(op, QualityContext(
        reconcile_result=None, trigger_kind="T1", tenant_id=uuid7(),
    ))
    assert verdict.kind_fit_score == 0.0
    assert verdict.decision == "reject"
    out_op, side_ops = apply_verdict(op, verdict)
    assert out_op is None
    assert side_ops == []


# =====================================================================
# Topology embedding (Phase 0)
# =====================================================================


# Topology embedding (Phase 0) is covered by
# services/models/tests/test_repo.py::test_insert_initializes_topo_embedding
# and services/topology/tests/test_anchor.py. Not duplicated here.


# =====================================================================
# Relationship candidate discipline (Phase 5)
# =====================================================================


def test_topology_emittable_edge_kinds_is_restricted():
    """LLM-only kinds must NOT appear in TOPOLOGY_EMITTABLE_EDGE_KINDS."""
    llm_only = {"explains", "causes", "predicts", "weakens",
                "instance_of", "contributes_to_resolution",
                "co_occurs_with", "alternative_to", "superseded_by"}
    overlap = llm_only.intersection(TOPOLOGY_EMITTABLE_EDGE_KINDS)
    assert overlap == set(), (
        f"topology must not emit LLM-only kinds; leaked: {overlap}"
    )


# =====================================================================
# Splitter unit-level guards (re-asserted at the suite level so this
# file fails loudly if Phase 2's behavior regresses)
# =====================================================================


def test_splitter_passes_through_atomic_claim():
    op = ClaimOp(op="insert", entry={
        "proposition": {"kind": "state", "subject": "x", "assertion": "ships"},
        "natural": "x ships",
    })
    out = split_compound_claim_op(op)
    assert len(out) == 1
    assert out[0] is op


def test_is_compound_flags_multi_clause_compound():
    flagged, reasons = is_compound({
        "natural": (
            "ARR is at risk and SOC2 is missing and "
            "the team is overloaded and the deal is slipping."
        ),
        "proposition": {"kind": "concern", "subject": "deal", "assertion": "compound"},
    })
    assert flagged is True
    assert reasons


# =====================================================================
# Situation composition (Phase 1)
# =====================================================================


@pytest.mark.asyncio
async def test_situation_with_compositional_fields_persists(
    fresh_db, tenant
):
    """Phase 1: SituationProposition carries pressure_type + shared_mechanism."""
    async with fresh_db.acquire() as conn:
        oid_a = await _seed_observation(conn, tenant, "atomic A")
        oid_b = await _seed_observation(conn, tenant, "atomic B")
        repo = ModelsRepo(fresh_db, embedder=None)

        async with conn.transaction():
            diff_a = ValidatedDiff(
                trigger_ref=uuid7(), tenant_id=tenant,
                claim_ops=[_insert_op(
                    tenant_id=tenant, born_from_event_id=oid_a,
                    natural="HarborRail security review is blocked.",
                    kind="concern",
                )],
            )
            res_a = await apply_diff(
                diff_a, conn, trigger_kind="T1",
                trigger_cause_event_id=oid_a, models_repo=repo,
            )
            assert res_a["applied_model_ids"]
            mid_a = res_a["applied_model_ids"][0]

            diff_b = ValidatedDiff(
                trigger_ref=uuid7(), tenant_id=tenant,
                claim_ops=[_insert_op(
                    tenant_id=tenant, born_from_event_id=oid_b,
                    natural="HarborRail renewal is at risk this quarter.",
                    kind="concern",
                )],
            )
            res_b = await apply_diff(
                diff_b, conn, trigger_kind="T1",
                trigger_cause_event_id=oid_b, models_repo=repo,
            )
            assert res_b["applied_model_ids"]
            mid_b = res_b["applied_model_ids"][0]

            # Now insert a situation that composes them.
            sit_op = ClaimOp(op="insert", entry={
                "tenant_id": str(tenant),
                "born_from_event_id": str(oid_a),
                "proposition": {
                    "kind": "situation",
                    "situation": "HarborRail renewal-risk situation",
                    "summary": (
                        "Security review blocker plus renewal risk; "
                        "two atomic concerns share one operational mechanism."
                    ),
                    "member_model_ids": [str(mid_a), str(mid_b)],
                    "relationship_summary": (
                        "Both concerns reference the same HarborRail renewal."
                    ),
                    "status": "active",
                    "pressure_type": "compliance",
                    "shared_mechanism": (
                        "SOC2 evidence gating renewal sign-off."
                    ),
                    "judgment_change": (
                        "Seen together, the procurement delay reframes "
                        "renewal risk as a compliance dependency."
                    ),
                    "affected_decisions": ["renewal_sign_off"],
                    "affected_customers": ["HarborRail"],
                    "affected_teams": ["security", "sales"],
                    "open_falsifier": (
                        "SOC2 evidence delivered AND renewal confirmed."
                    ),
                },
                "natural": "HarborRail renewal-risk situation",
                "embedding": _make_embedding("HarborRail renewal-risk situation"),
                "scope_actors": [],
                "scope_entities": [],
                "scope_temporal": {},
                "confidence": 0.7,
                "confidence_at_assertion": 0.7,
                "falsifier": {"kind": "external_evidence", "description": "SOC2 evidence delivered AND renewal confirmed."},
            })
            sit_diff = ValidatedDiff(
                trigger_ref=uuid7(), tenant_id=tenant,
                claim_ops=[sit_op],
            )
            res_sit = await apply_diff(
                sit_diff, conn, trigger_kind="T1",
                trigger_cause_event_id=oid_a, models_repo=repo,
            )
            assert res_sit["applied_model_ids"], res_sit.get("claim_ops")
            sit_id = res_sit["applied_model_ids"][0]

            row = await conn.fetchrow(
                "SELECT proposition FROM models WHERE id = $1",
                sit_id,
            )
            import json
            prop = row["proposition"]
            if isinstance(prop, str):
                prop = json.loads(prop)
            assert prop["pressure_type"] == "compliance"
            assert prop["shared_mechanism"]
            assert prop["affected_customers"] == ["HarborRail"]
            assert prop["open_falsifier"]
