"""Retrieval quality harness.

These tests treat retrieval as a product-quality contract: a business
entry point must surface the Models a human would expect, and it must
avoid seductive but wrong neighbors. The cases are intentionally
scenario-shaped rather than pathway-shaped.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from services.models.edges_repo import EdgesRepo
from services.models.repo import ModelsRepo
from services.retrieval.assembler import AccessContext, assemble_context
from services.retrieval.config import RetrievalConfig
from services.retrieval.primary import RetrievalResult, TriggerContext, primary_retrieve
from services.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


@dataclass(slots=True)
class RetrievalQualityCase:
    """A compact, reusable assertion contract for retrieval behavior."""

    name: str
    trigger: TriggerContext
    expected_model_ids: set[UUID] = field(default_factory=set)
    expected_top_model_ids: set[UUID] = field(default_factory=set)
    excluded_model_ids: set[UUID] = field(default_factory=set)
    expected_resource_ids: set[UUID] = field(default_factory=set)
    required_pathways: set[str] = field(default_factory=set)
    top_n: int = 40
    min_models: int = 1
    min_expected_recall: float = 1.0
    max_expected_rank: int | None = None
    max_known_negative_hits: int = 0
    max_latency_ms: float | None = None
    rationale: str = ""


async def _assert_quality_case(
    case: RetrievalQualityCase,
    conn,
    *,
    top_n: int | None = None,
) -> RetrievalResult:
    started = time.perf_counter()
    result = await primary_retrieve(case.trigger, conn, top_n=top_n or case.top_n)
    latency_ms = (time.perf_counter() - started) * 1000.0
    model_ids = {m.id for m in result.models}
    resource_ids = {r.id for r in result.resources}
    pathways_run = set(result.notes.get("pathways_run") or [])
    ranks = {m.id: idx for idx, m in enumerate(result.models, start=1)}
    expected_hits = case.expected_model_ids & model_ids
    expected_recall = (
        len(expected_hits) / len(case.expected_model_ids)
        if case.expected_model_ids
        else 1.0
    )
    negative_hits = case.excluded_model_ids & model_ids

    result.notes["quality_report"] = {
        "case": case.name,
        "latency_ms": latency_ms,
        "models_returned": len(result.models),
        "expected_total": len(case.expected_model_ids),
        "expected_hits": len(expected_hits),
        "expected_recall": expected_recall,
        "known_negative_hits": len(negative_hits),
        "best_expected_rank": min(
            (ranks[mid] for mid in expected_hits),
            default=None,
        ),
        "pathways_run": sorted(pathways_run),
    }

    assert len(result.models) >= case.min_models, case.rationale or case.name
    assert expected_recall >= case.min_expected_recall, (
        f"{case.name}: expected recall {expected_recall:.3f} below "
        f"{case.min_expected_recall:.3f}; missing="
        f"{case.expected_model_ids - model_ids}; rationale={case.rationale}"
    )
    assert len(negative_hits) <= case.max_known_negative_hits, (
        f"{case.name}: retrieved excluded Models "
        f"{negative_hits}; rationale={case.rationale}"
    )
    assert case.expected_resource_ids <= resource_ids, (
        f"{case.name}: missing expected Resources "
        f"{case.expected_resource_ids - resource_ids}; rationale={case.rationale}"
    )
    assert case.required_pathways <= pathways_run, (
        f"{case.name}: missing pathways {case.required_pathways - pathways_run}"
    )
    if case.expected_top_model_ids:
        assert case.max_expected_rank is not None
        for mid in case.expected_top_model_ids:
            assert ranks.get(mid, 10**9) <= case.max_expected_rank, (
                f"{case.name}: expected {mid} by rank "
                f"{case.max_expected_rank}, got {ranks.get(mid)}"
            )
    if case.max_latency_ms is not None:
        assert latency_ms <= case.max_latency_ms, (
            f"{case.name}: retrieval took {latency_ms:.1f}ms, "
            f"budget={case.max_latency_ms:.1f}ms"
        )
    return result


async def test_quality_customer_entrypoint_reaches_linked_commitment_memory(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_observations=40,
        n_models=45,
        n_commitments=12,
        n_goals=6,
        n_customers=3,
    )
    expected = set(fs.scope_by_commitment[fs.hero_commitment_id])
    case = RetrievalQualityCase(
        name="customer_to_commitment_memory",
        trigger=TriggerContext(
            kind="T1",
            tenant_id=tenant,
            seed_entity_ids=[
                {"type": "customer", "id": str(fs.hero_customer_id)}
            ],
            seed_natural_text="customer-0 churn risk",
            seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            precomputed_seed_vector=make_embedding("customer-0 churn risk"),
        ),
        expected_model_ids=expected,
        expected_resource_ids={fs.hero_customer_id},
        required_pathways={"A", "B", "C", "G"},
        rationale=(
            "Customer-facing retrieval must cross customer_commitments and "
            "surface Models scoped to the linked renewal/commitment."
        ),
    )

    result = await _assert_quality_case(case, tx_conn)
    assert result.model_scores


async def test_quality_customer_title_fallback_covers_missing_bridge_row(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_observations=35,
        n_models=35,
        n_commitments=8,
        n_goals=5,
        n_customers=2,
    )
    await tx_conn.execute(
        "DELETE FROM customer_commitments WHERE tenant_id = $1",
        tenant,
    )
    await tx_conn.execute(
        "UPDATE resources SET identity = 'Globex Inc' WHERE id = $1",
        fs.hero_customer_id,
    )
    await tx_conn.execute(
        "UPDATE commitments SET title = 'Renew Globex contract' WHERE id = $1",
        fs.hero_commitment_id,
    )

    case = RetrievalQualityCase(
        name="customer_title_fallback",
        trigger=TriggerContext(
            kind="T1",
            tenant_id=tenant,
            seed_entity_ids=[
                {"type": "customer", "id": str(fs.hero_customer_id)}
            ],
            seed_natural_text="Globex renewal risk",
            seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
            precomputed_seed_vector=make_embedding("Globex renewal risk"),
        ),
        expected_model_ids=set(fs.scope_by_commitment[fs.hero_commitment_id]),
        expected_resource_ids={fs.hero_customer_id},
        required_pathways={"A"},
        rationale=(
            "When the explicit customer_commitments bridge is absent, obvious "
            "customer-name/title matches should still preserve reachability."
        ),
    )

    await _assert_quality_case(case, tx_conn)


async def test_quality_model_edge_entrypoint_reaches_hidden_blocker(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_observations=45,
        n_models=55,
        n_commitments=14,
        n_goals=6,
        n_customers=3,
    )
    seed_id = fs.hero_model_id
    blocker_id = fs.model_ids[48]
    await EdgesRepo().link(
        tx_conn,
        source=seed_id,
        target=blocker_id,
        kind="blocks",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.9,
        explanation="The hidden blocker directly blocks the seeded operating fact.",
    )

    case = RetrievalQualityCase(
        name="model_edge_hidden_blocker",
        trigger=TriggerContext(
            kind="T2",
            tenant_id=tenant,
            model_id=seed_id,
            seed_natural_text="hidden blocker graph traversal",
            precomputed_seed_vector=make_embedding("hidden blocker graph traversal"),
        ),
        expected_model_ids={seed_id, blocker_id},
        required_pathways={"A", "B", "D", "G", "F"},
        rationale=(
            "Typed model edges are the memory graph's non-obvious connection "
            "surface; a T2 model entry point must traverse them."
        ),
    )

    result = await _assert_quality_case(case, tx_conn)
    g = next(pr for pr in result.pathway_results if pr.source_pathway == "G")
    assert blocker_id in {m.id for m in g.models}


async def test_quality_rejected_edges_do_not_pull_false_neighbors(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_observations=45,
        n_models=55,
        n_commitments=14,
        n_goals=6,
        n_customers=3,
    )
    seed_id = fs.hero_model_id
    false_neighbor_id = fs.model_ids[49]
    await EdgesRepo().link(
        tx_conn,
        source=seed_id,
        target=false_neighbor_id,
        kind="early_warning_for",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.9,
        explanation="Rejected evidence should not be used for retrieval.",
        review_status="rejected",
    )

    case = RetrievalQualityCase(
        name="rejected_edge_exclusion",
        trigger=TriggerContext(
            kind="T2",
            tenant_id=tenant,
            model_id=seed_id,
            seed_natural_text="rejected warning edge",
            precomputed_seed_vector=make_embedding("rejected warning edge"),
        ),
        expected_model_ids={seed_id},
        excluded_model_ids={false_neighbor_id},
        required_pathways={"G"},
        rationale="Rejected graph edges must not create false reachability.",
    )

    result = await _assert_quality_case(case, tx_conn)
    g = next(pr for pr in result.pathway_results if pr.source_pathway == "G")
    assert false_neighbor_id not in {m.id for m in g.models}


async def test_quality_harness_can_run_multiple_cases_as_a_suite(
    tx_conn,
    fresh_db,
    tenant,
    other_tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_observations=36,
        n_models=42,
        n_commitments=10,
        n_goals=5,
        n_customers=2,
    )
    await build_fixture(
        tx_conn,
        other_tenant,
        pool=fresh_db,
        n_observations=20,
        n_models=25,
        n_commitments=8,
        n_goals=4,
        n_customers=2,
    )

    cases = [
        RetrievalQualityCase(
            name="suite_customer_reachability",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {"type": "customer_resource", "id": str(fs.hero_customer_id)}
                ],
                seed_natural_text="customer-0 churn risk",
                seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                precomputed_seed_vector=make_embedding("customer-0 churn risk"),
            ),
            expected_model_ids=set(fs.scope_by_commitment[fs.hero_commitment_id]),
            expected_resource_ids={fs.hero_customer_id},
        ),
        RetrievalQualityCase(
            name="suite_commitment_reachability",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {"type": "commitment", "id": str(fs.hero_commitment_id)}
                ],
                seed_natural_text="commitment execution risk",
                seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                precomputed_seed_vector=make_embedding("commitment execution risk"),
            ),
            expected_model_ids=set(fs.scope_by_commitment[fs.hero_commitment_id]),
        ),
    ]

    for case in cases:
        result = await _assert_quality_case(case, tx_conn)
        assert all(m.tenant_id == tenant for m in result.models), case.name
        assert all(r.tenant_id == tenant for r in result.resources), case.name


async def test_quality_eval_corpus_mixed_entrypoints_regression_gate(
    tx_conn,
    fresh_db,
    tenant,
    other_tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        rng_seed=9001,
        n_actors=12,
        n_observations=160,
        n_models=125,
        n_commitments=24,
        n_goals=12,
        n_customers=6,
        n_decisions=8,
    )
    await build_fixture(
        tx_conn,
        other_tenant,
        pool=fresh_db,
        rng_seed=9002,
        n_actors=8,
        n_observations=80,
        n_models=70,
        n_commitments=16,
        n_goals=8,
        n_customers=4,
        n_decisions=4,
    )

    edge_repo = EdgesRepo()
    graph_seed_id = fs.hero_model_id
    graph_bridge_id = fs.model_ids[84]
    graph_target_id = fs.model_ids[96]
    rejected_neighbor_id = fs.model_ids[97]
    archived_neighbor_id = fs.model_ids[98]
    await edge_repo.link(
        tx_conn,
        source=graph_seed_id,
        target=graph_bridge_id,
        kind="same_issue_as",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.91,
        explanation="Bridge model shares the same operational issue.",
    )
    await edge_repo.link(
        tx_conn,
        source=graph_bridge_id,
        target=graph_target_id,
        kind="early_warning_for",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.88,
        explanation="Second-hop warning should remain reachable.",
    )
    await edge_repo.link(
        tx_conn,
        source=graph_seed_id,
        target=rejected_neighbor_id,
        kind="early_warning_for",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.95,
        explanation="Rejected neighbor must not leak into retrieval.",
        review_status="rejected",
    )
    await edge_repo.link(
        tx_conn,
        source=graph_seed_id,
        target=archived_neighbor_id,
        kind="blocks",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.95,
        explanation="Archived endpoint must not be hydrated.",
    )
    await tx_conn.execute(
        """
        UPDATE models
        SET status = 'archived',
            archived_at = now(),
            archive_reason = 'quality_eval_negative'
        WHERE id = $1
        """,
        archived_neighbor_id,
    )

    actor_rows = await tx_conn.fetch(
        """
        SELECT model_id
        FROM model_scope_actors
        WHERE tenant_id = $1
          AND actor_id = $2
        ORDER BY model_id
        """,
        tenant,
        fs.hero_actor_id,
    )
    actor_model_ids = {r["model_id"] for r in actor_rows}
    decision_commit_rows = await tx_conn.fetch(
        """
        SELECT commitment_id
        FROM constrained_by
        WHERE decision_id = $1
        """,
        fs.decision_ids[0],
    )
    decision_model_ids: set[UUID] = set()
    for row in decision_commit_rows:
        decision_model_ids.update(fs.scope_by_commitment.get(row["commitment_id"], []))

    compact_commitment_expected = set(
        fs.scope_by_commitment[fs.hero_commitment_id][:2]
    )
    second_customer_commitment_id = fs.commitment_ids[5]
    seed_time = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)

    cases = [
        RetrievalQualityCase(
            name="large_customer_to_commitment_scope",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {"type": "customer", "id": str(fs.hero_customer_id)}
                ],
                seed_natural_text="customer-0 renewal risk and delivery confidence",
                seed_occurred_at=seed_time,
                precomputed_seed_vector=make_embedding(
                    "customer-0 renewal risk and delivery confidence"
                ),
            ),
            expected_model_ids=set(fs.scope_by_commitment[fs.hero_commitment_id]),
            expected_resource_ids={fs.hero_customer_id},
            required_pathways={"A", "B", "C", "G"},
            top_n=80,
            max_latency_ms=5000,
            rationale="Large customer entrypoint must recover linked commitment memory.",
        ),
        RetrievalQualityCase(
            name="large_commitment_to_customer_bridge",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {"type": "commitment", "id": str(fs.hero_commitment_id)}
                ],
                seed_natural_text="commitment 0 customer delivery risk",
                seed_occurred_at=seed_time,
                precomputed_seed_vector=make_embedding(
                    "commitment 0 customer delivery risk"
                ),
            ),
            expected_model_ids=set(fs.scope_by_commitment[fs.hero_commitment_id]),
            expected_resource_ids={fs.hero_customer_id},
            required_pathways={"A", "B", "C", "G"},
            top_n=80,
            rationale="Commitment entrypoint should retain its customer bridge.",
        ),
        RetrievalQualityCase(
            name="actor_only_signal_uses_structural_scope",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                scope_actors=[fs.hero_actor_id],
                seed_natural_text="actor reliability and ownership context",
                seed_occurred_at=seed_time,
                precomputed_seed_vector=make_embedding(
                    "actor reliability and ownership context"
                ),
            ),
            expected_model_ids=actor_model_ids,
            required_pathways={"A", "B", "C", "G"},
            top_n=80,
            min_expected_recall=0.80,
            rationale="Signals scoped only to an actor still need useful memory.",
        ),
        RetrievalQualityCase(
            name="decision_constraint_finds_operating_models",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {"type": "decision", "id": str(fs.decision_ids[0])}
                ],
                seed_natural_text="decision 0 constraint impact",
                seed_occurred_at=seed_time,
                precomputed_seed_vector=make_embedding("decision 0 constraint impact"),
            ),
            expected_model_ids=decision_model_ids,
            required_pathways={"A", "B", "C", "G"},
            top_n=80,
            min_expected_recall=0.75,
            rationale="Decision constraints should recover scoped commitment models.",
        ),
        RetrievalQualityCase(
            name="pattern_background_retrieves_patterns_and_instances",
            trigger=TriggerContext(
                kind="T4",
                tenant_id=tenant,
                seed_signature={"regex": "^hotfix"},
            ),
            expected_model_ids=set(
                fs.pattern_model_ids + fs.pattern_instance_model_ids
            ),
            required_pathways={"D"},
            top_n=80,
            rationale="Background pattern retrieval should include instances.",
        ),
        RetrievalQualityCase(
            name="two_hop_model_edge_hidden_warning",
            trigger=TriggerContext(
                kind="T2",
                tenant_id=tenant,
                model_id=graph_seed_id,
                seed_natural_text="two hop warning through model graph",
                precomputed_seed_vector=make_embedding(
                    "two hop warning through model graph"
                ),
            ),
            expected_model_ids={graph_seed_id, graph_bridge_id, graph_target_id},
            expected_top_model_ids={graph_target_id},
            excluded_model_ids={rejected_neighbor_id, archived_neighbor_id},
            required_pathways={"A", "B", "D", "F", "G"},
            top_n=80,
            max_expected_rank=25,
            rationale="Non-obvious two-hop warning edges are core memory value.",
        ),
        RetrievalQualityCase(
            name="tight_top_n_still_returns_some_relevant_memory",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {"type": "commitment", "id": str(fs.hero_commitment_id)}
                ],
                seed_natural_text="very tight context budget commitment retrieval",
                seed_occurred_at=seed_time,
                precomputed_seed_vector=make_embedding(
                    "very tight context budget commitment retrieval"
                ),
            ),
            expected_model_ids=compact_commitment_expected,
            required_pathways={"A", "B", "C", "G"},
            top_n=3,
            min_expected_recall=0.50,
            rationale="Small contexts should still carry at least one sharp hit.",
        ),
        RetrievalQualityCase(
            name="second_customer_does_not_collapse_to_hero_customer",
            trigger=TriggerContext(
                kind="T1",
                tenant_id=tenant,
                seed_entity_ids=[
                    {
                        "type": "customer_resource",
                        "id": str(fs.customer_resource_ids[1]),
                    }
                ],
                seed_natural_text="customer-1 churn risk",
                seed_occurred_at=seed_time,
                precomputed_seed_vector=make_embedding("customer-1 churn risk"),
            ),
            expected_model_ids=set(fs.scope_by_commitment[second_customer_commitment_id]),
            excluded_model_ids=set(fs.scope_by_commitment[fs.hero_commitment_id]),
            expected_resource_ids={fs.customer_resource_ids[1]},
            required_pathways={"A", "B", "C", "G"},
            top_n=80,
            rationale="Customer scope must stay precise across adjacent customers.",
        ),
    ]

    reports = []
    for case in cases:
        result = await _assert_quality_case(case, tx_conn)
        reports.append(result.notes["quality_report"])
        assert all(m.tenant_id == tenant for m in result.models), case.name
        assert all(r.tenant_id == tenant for r in result.resources), case.name

    expected_total = sum(r["expected_total"] for r in reports)
    expected_hits = sum(r["expected_hits"] for r in reports)
    negative_hits = sum(r["known_negative_hits"] for r in reports)

    assert len(cases) >= 8
    assert expected_total > 0
    assert expected_hits / expected_total >= 0.85
    assert negative_hits == 0
    assert max(r["latency_ms"] for r in reports) <= 5000


async def test_quality_high_value_graph_memory_survives_context_assembly(
    tx_conn,
    fresh_db,
    tenant,
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        rng_seed=9901,
        n_actors=10,
        n_observations=120,
        n_models=110,
        n_commitments=24,
        n_goals=12,
        n_customers=5,
        n_decisions=6,
    )
    graph_seed_id = fs.hero_model_id
    bridge_id = fs.model_ids[82]
    target_id = fs.model_ids[93]
    await EdgesRepo().link(
        tx_conn,
        source=graph_seed_id,
        target=bridge_id,
        kind="same_issue_as",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.92,
        explanation="Bridge model connects the operating fact to a warning.",
    )
    await EdgesRepo().link(
        tx_conn,
        source=bridge_id,
        target=target_id,
        kind="early_warning_for",
        tenant_id=tenant,
        detected_by="manual",
        confidence=0.9,
        explanation="Second-hop graph target should survive prompt assembly.",
    )

    trigger = TriggerContext(
        kind="T2",
        tenant_id=tenant,
        model_id=graph_seed_id,
        seed_natural_text="assembly should preserve hidden graph warning",
        precomputed_seed_vector=make_embedding(
            "assembly should preserve hidden graph warning"
        ),
    )
    retrieval = await primary_retrieve(trigger, tx_conn, top_n=80)
    retrieved_ids = {m.id for m in retrieval.models}
    assert {graph_seed_id, bridge_id, target_id} <= retrieved_ids

    access = AccessContext(tenant_id=tenant, requestor_actor_id=None)
    bundle = await assemble_context(
        retrieval,
        access,
        tx_conn,
        budget_models=20,
    )
    bundle_ids = {m.id for m in bundle.models}
    assert target_id in bundle_ids
    selection = bundle.notes["model_selection"]
    assert str(target_id) in selection["selected_model_ids"]
    assert selection["pathway_survival"]["G"]["selected_count"] >= 3

    mmr_bundle = await assemble_context(
        retrieval,
        access,
        tx_conn,
        budget_models=20,
        config=RetrievalConfig(
            scoring_mode="rrf",
            assembler_use_mmr=True,
            context_budget_tokens=2_000,
            mmr_lambda_diversity=0.8,
        ),
    )
    mmr_ids = {m.id for m in mmr_bundle.models}
    assert target_id in mmr_ids
    assert mmr_bundle.notes["mmr"]["used"] is True
    assert mmr_bundle.notes["mmr"]["graph_anchor_count"] >= 3
    assert str(target_id) in mmr_bundle.notes["model_selection"]["selected_model_ids"]
