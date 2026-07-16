from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.contracts.kernel import canonical_sha256
from services.reasoning.sage.company_profile import (
    build_latent_pattern_profile_input,
    build_company_learning_profile,
    CompanyLearningProfile,
    load_company_learning_profile,
    LearningPrior,
)
from services.reasoning.sage.patterns import (
    PatternScoutCandidate,
    build_structural_signature,
    pattern_model_repair_proposals_from_profile,
    scout_global_patterns,
)
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.sage.retrieval_policy import (
    SageRouteUtility,
    plan_primary_retrieval,
)
from tests.unit.sage._seed import seed_model, seed_observation


def test_company_learning_profile_compacts_existing_sage_surfaces() -> None:
    tenant_id = uuid4()
    route = SageRouteUtility(
        signature_hash="sig-route",
        path="B",
        signal_type="T4",
        attempts=8,
        wins=6,
        selected_evidence=4,
        utility_score=0.72,
        confidence=0.81,
    )
    signatures = [
        build_structural_signature(
            _source("sales", "Approval review blocked renewal commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("security", "Approval review blocked audit commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("legal", "Approval review blocked contract commitment"),
            source_kind="model",
        ),
    ]
    scout = scout_global_patterns(signatures, min_support=3, min_surface_domains=3)

    profile = build_company_learning_profile(
        tenant_id=tenant_id,
        route_utilities=[route],
        question_policy_stats=[
            {
                "primitive": "RECURRENCE",
                "attempts": 5,
                "successes": 4,
                "utility_score": 0.5,
            }
        ],
        negative_memories=[{"signature_hash": "bad-route", "count": 3}],
        latent_pattern_candidates=scout.candidates,
        residuals=[
            {
                "id": uuid4(),
                "residual_kind": "validation_dropped_value",
                "status": "open",
            }
        ],
        recent_drift_signals=[
            {
                "id": uuid4(),
                "drift_kind": "counterevidence_unattached",
            }
        ],
        source_reliability_stats=[
            {
                "source_key": "shortcut",
                "attempts": 6,
                "successes": 4,
                "total_credit": 3.2,
                "avg_activation": 0.71,
            }
        ],
        actor_reliability_stats=[
            {
                "actor_key": "actor-1",
                "proposition_kind": "pattern",
                "attempts": 7,
                "successes": 5,
                "avg_asserted_confidence": 0.62,
            }
        ],
        structural_signatures=signatures,
    )

    assert profile.tenant_id == tenant_id
    assert profile.sample_count > 0
    assert profile.confidence > 0
    assert profile.best_prior(kind="route", key="B") is not None
    assert profile.best_prior(kind="question", key="RECURRENCE") is not None
    assert profile.best_prior(kind="negative_memory", key="bad-route") is not None
    assert profile.priors_for_kind("latent_pattern")
    assert profile.priors_for_kind("residual")
    assert profile.priors_for_kind("drift")
    source_prior = profile.best_prior(kind="source_reliability", key="shortcut")
    assert source_prior is not None
    assert source_prior.metadata["salience_only"] is True
    assert source_prior.metadata["authority_effect"] == "none"
    actor_prior = profile.best_prior(kind="actor_reliability", key="actor-1:pattern")
    assert actor_prior is not None
    assert actor_prior.metadata["salience_only"] is True

    notes = profile.to_policy_notes()
    assert notes["canonical_write"] is False
    assert all("evidence_refs" not in prior for prior in notes["priors"])
    assert any(prior.get("evidence_refs_redacted") is True for prior in notes["priors"])
    assert any(
        prior["kind"] == "latent_pattern"
        and prior["metadata"]["canonical_write"] is False
        for prior in notes["priors"]
    )


def test_latent_pattern_profile_input_feeds_profile_without_hot_path_scouting() -> None:
    tenant_id = uuid4()
    signatures = [
        build_structural_signature(
            _source("sales", "Approval review blocked renewal commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("security", "Approval review blocked audit commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("legal", "Approval review blocked contract commitment"),
            source_kind="model",
        ),
    ]

    scout_input = build_latent_pattern_profile_input(
        signatures,
        min_support=3,
        min_surface_domains=3,
    )
    profile = build_company_learning_profile(
        tenant_id=tenant_id,
        **scout_input.to_profile_kwargs(),
    )

    assert scout_input.canonical_write is False
    assert scout_input.scout_notes["canonical_write"] is False
    assert scout_input.scout_notes["all_pairs_avoided_estimate"] >= 0
    assert profile.priors_for_kind("latent_pattern")
    assert profile.priors_for_kind("structural")


def test_policy_notes_redact_reference_shaped_metadata_by_default() -> None:
    tenant_id = uuid4()
    raw_observation_id = str(uuid4())
    raw_source_ref = f"observation:{raw_observation_id}"
    profile = build_company_learning_profile(
        tenant_id=tenant_id,
        latent_pattern_candidates=[
            PatternScoutCandidate(
                candidate_hash="raw-ref-pattern",
                scout_kind="global_structural_neighborhood",
                shared_facets=("pressure:customer_risk", "outcome:blocked_commitment"),
                support_signature_hashes=("sig-a", "sig-b", "sig-c"),
                support_source_refs=(raw_source_ref,),
                support_count=3,
                surface_domain_count=3,
                surface_distance_score=0.8,
                outcome_cohesion_score=0.7,
                confidence=0.8,
                metadata={
                    "source_refs": [raw_source_ref],
                    "nested": {"observation_ids": [raw_observation_id]},
                },
            )
        ],
    )

    notes = profile.to_policy_notes()
    prior = notes["priors"][0]

    assert notes["authority_effect"] == "none"
    assert prior["evidence_ref_count"] == 1
    assert prior["evidence_refs_redacted"] is True
    assert "evidence_refs" not in prior
    assert prior["metadata"]["source_refs_count"] == 1
    assert prior["metadata"]["source_refs_redacted"] is True
    assert "source_refs" not in prior["metadata"]
    assert prior["metadata"]["nested"]["observation_ids_count"] == 1
    assert prior["metadata"]["nested"]["observation_ids_redacted"] is True
    assert "observation_ids" not in prior["metadata"]["nested"]
    assert raw_source_ref not in str(notes)
    assert raw_observation_id not in str(notes)


def test_policy_notes_allow_explicit_explanation_safe_refs() -> None:
    raw_ref = "aggregate:public-sample"
    profile = _profile(
        LearningPrior(
            kind="route",
            key="semantic",
            score=0.5,
            confidence=0.8,
            sample_count=3,
            evidence_refs=(raw_ref,),
            metadata={
                "explanation_safe_evidence_refs": True,
                "source_refs": [raw_ref],
            },
        )
    )

    prior = profile.to_policy_notes()["priors"][0]

    assert prior["evidence_refs"] == [raw_ref]
    assert prior["metadata"]["source_refs"] == [raw_ref]
    assert prior["metadata"]["explanation_safe_evidence_refs"] is True


def test_latent_pattern_drift_decays_prior_and_proposes_pattern_model_repair() -> None:
    tenant_id = uuid4()
    pattern_model_id = uuid4()
    candidate = PatternScoutCandidate(
        candidate_hash="pattern-drifted",
        scout_kind="global_structural_neighborhood",
        shared_facets=("coordination:approval_loop", "outcome:blocked_commitment"),
        support_signature_hashes=("sig-a", "sig-b", "sig-c"),
        support_source_refs=("model-a", "model-b", "model-c"),
        support_count=3,
        surface_domain_count=3,
        surface_distance_score=0.9,
        outcome_cohesion_score=0.8,
        counterexample_count=2,
        utility_score=0.7,
        confidence=0.82,
        metadata={
            "semantic_drift_score": 0.6,
            "promoted_pattern_model_id": str(pattern_model_id),
        },
    )

    profile = build_company_learning_profile(
        tenant_id=tenant_id,
        latent_pattern_candidates=[candidate],
    )

    prior = profile.best_prior(kind="latent_pattern", key="pattern-drifted")
    assert prior is not None
    assert prior.decay < 0.6
    assert prior.effective_score < round(prior.score * prior.confidence, 4)
    assert "semantic_drift:0.600" in prior.metadata["decay_reasons"]
    assert "latent_pattern_decay:applied" in profile.notes
    proposals = pattern_model_repair_proposals_from_profile(profile)

    assert len(proposals) == 1
    proposal = proposals[0]
    assert proposal.canonical_write is False
    assert proposal.pattern_model_id == str(pattern_model_id)
    assert proposal.repair_intent == "review_pattern_model_drift"
    assert proposal.payload["repair_source"] == "sage_latent_pattern_drift"
    assert proposal.payload["model_ids"] == [str(pattern_model_id)]


@pytest.mark.asyncio
async def test_load_company_learning_profile_reads_existing_utility_surfaces() -> None:
    tenant_id = uuid4()
    route = SageRouteUtility(
        signature_hash="sig-route",
        path="semantic",
        signal_type="T4",
        attempts=4,
        wins=3,
        utility_score=0.6,
        confidence=0.7,
    )
    conn = _FakeProfileConn(
        existing_tables={
            "negative_memory",
            "discovery_shortcuts",
            "retrieval_affordance_profiles",
            "model_structural_features",
            "model_residual_evidence",
            "sage_reader_decision_attributions",
            "calibration_stats",
        }
    )

    profile = await load_company_learning_profile(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        route_utilities=[route],
        question_policy_stats=[
            {
                "primitive": "OWNERSHIP",
                "attempts": 3,
                "successes": 2,
                "utility_score": 0.5,
            }
        ],
    )

    kinds = {prior.kind for prior in profile.priors}
    assert {
        "route",
        "question",
        "negative_memory",
        "shortcut",
        "affordance",
        "structural_feature",
        "residual",
        "drift",
        "source_reliability",
        "actor_reliability",
    }.issubset(kinds)
    assert profile.to_policy_notes()["canonical_write"] is False


@pytest.mark.asyncio
async def test_source_reliability_merges_reader_and_grounding_operational_yield() -> None:
    tenant_id = uuid4()
    conn = _FakeProfileConn(
        existing_tables={
            "sage_reader_decision_attributions",
            "grounding_traces",
            "interpretation_context_snapshots",
            "resolution_assessments",
            "source_semantic_interpretations",
            "source_semantic_admission_decisions",
            "models",
        }
    )

    profile = await load_company_learning_profile(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
    )

    prior = profile.best_prior(kind="source_reliability", key="shortcut")
    assert prior is not None
    assert prior.sample_count == 7
    assert prior.metadata["source"] == (
        "grounding_context_source_semantic_outcomes"
        "+sage_reader_decision_attributions"
    )
    assert prior.metadata["salience_only"] is True
    assert prior.metadata["canonical_write"] is False
    grounding_queries = [
        query for query, _args in conn.fetch_calls if "WITH recent_traces AS" in query
    ]
    assert len(grounding_queries) == 1
    assert "candidate_distribution" not in grounding_queries[0]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_grounding_outcomes_reuse_source_salience_without_truth_writes(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
) -> None:
    corrected_source = "slack:corrected"
    useful_source = "slack:useful"
    neutral_source = "slack:no-admission"
    pending_source = "slack:pending-source-semantics"
    foreign_source = "slack:foreign"

    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            wrong_trace_id, wrong_model_id = await _seed_grounding_source_outcome(
                gateway_pool,
                conn=conn,
                tenant_id=tenant_id,
                source_channel=corrected_source,
            )
            assert wrong_model_id is not None
            await conn.execute(
                """
                UPDATE models
                SET status = 'archived',
                    archived_at = now(),
                    archive_reason = 'superseded'
                WHERE tenant_id = $1 AND id = $2
                """,
                tenant_id,
                wrong_model_id,
            )
            await _seed_grounding_source_outcome(
                gateway_pool,
                conn=conn,
                tenant_id=tenant_id,
                source_channel=corrected_source,
                supersedes_trace_id=wrong_trace_id,
            )
            for _ in range(3):
                await _seed_grounding_source_outcome(
                    gateway_pool,
                    conn=conn,
                    tenant_id=tenant_id,
                    source_channel=useful_source,
                )
            for _ in range(2):
                await _seed_grounding_source_outcome(
                    gateway_pool,
                    conn=conn,
                    tenant_id=tenant_id,
                    source_channel=neutral_source,
                    source_semantic_disposition="no_admission",
                )
            for _ in range(2):
                await _seed_grounding_source_outcome(
                    gateway_pool,
                    conn=conn,
                    tenant_id=tenant_id,
                    source_channel=pending_source,
                    source_semantic_disposition=None,
                )

            foreign_tenant_id = uuid4()
            for _ in range(3):
                await _seed_grounding_source_outcome(
                    gateway_pool,
                    conn=conn,
                    tenant_id=foreign_tenant_id,
                    source_channel=foreign_source,
                )

            model_truth_before = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT id, proposition, status, archived_at, archive_reason
                    FROM models
                    WHERE tenant_id = $1
                    ORDER BY id
                    """,
                    tenant_id,
                )
            ]
            grounding_truth_before = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT id, current_fate, identity_registry_mutated,
                           source_observation_mutated, trace
                    FROM grounding_traces
                    WHERE tenant_id = $1
                    ORDER BY id
                    """,
                    tenant_id,
                )
            ]

            profile = await load_company_learning_profile(
                conn,
                tenant_id=tenant_id,
            )

            model_truth_after = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT id, proposition, status, archived_at, archive_reason
                    FROM models
                    WHERE tenant_id = $1
                    ORDER BY id
                    """,
                    tenant_id,
                )
            ]
            grounding_truth_after = [
                dict(row)
                for row in await conn.fetch(
                    """
                    SELECT id, current_fate, identity_registry_mutated,
                           source_observation_mutated, trace
                    FROM grounding_traces
                    WHERE tenant_id = $1
                    ORDER BY id
                    """,
                    tenant_id,
                )
            ]

    corrected_prior = profile.best_prior(
        kind="source_reliability",
        key=corrected_source,
    )
    useful_prior = profile.best_prior(
        kind="source_reliability",
        key=useful_source,
    )
    neutral_prior = profile.best_prior(
        kind="source_reliability",
        key=neutral_source,
    )
    assert corrected_prior is not None
    assert useful_prior is not None
    assert neutral_prior is not None
    assert corrected_prior.sample_count == 2
    assert corrected_prior.effective_score <= 0.0
    assert useful_prior.sample_count == 3
    assert useful_prior.effective_score > 0.22
    assert neutral_prior.sample_count == 2
    assert -0.10 < neutral_prior.effective_score <= 0.0
    assert profile.best_prior(kind="source_reliability", key=pending_source) is None
    assert profile.best_prior(kind="source_reliability", key=foreign_source) is None

    weights = {"A": 0.30, "B": 0.26, "L": 0.12, "C": 0.16, "G": 0.16}
    corrected_trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="Corrected customer launch dependency",
        seed_signature={"source_channel": corrected_source},
    )
    useful_trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="Useful customer launch dependency",
        seed_signature={"source_channel": useful_source},
    )
    baseline_corrected = _plan_source_policy(corrected_trigger, weights=weights)
    learned_corrected = _plan_source_policy(
        corrected_trigger,
        weights=weights,
        profile=profile,
    )
    baseline_useful = _plan_source_policy(useful_trigger, weights=weights)
    learned_useful = _plan_source_policy(
        useful_trigger,
        weights=weights,
        profile=profile,
    )

    assert learned_corrected.decision_for("L") is not None
    assert baseline_corrected.decision_for("L") is not None
    assert (
        learned_corrected.decision_for("L").weight_multiplier
        <= baseline_corrected.decision_for("L").weight_multiplier
    )
    assert "source_actor_reliability_raised_salience" not in learned_corrected.reasons
    assert learned_useful.decision_for("L") is not None
    assert baseline_useful.decision_for("L") is not None
    assert (
        learned_useful.decision_for("L").weight_multiplier
        > baseline_useful.decision_for("L").weight_multiplier
    )
    assert "source_actor_reliability_raised_salience" in learned_useful.reasons

    useful_effects = learned_useful.notes()["profile_effects"]
    assert useful_effects
    assert all(effect["canonical_write"] is False for effect in useful_effects)
    assert all(effect["salience_only"] is True for effect in useful_effects)
    assert all(effect["authority_effect"] == "none" for effect in useful_effects)
    assert profile.to_policy_notes()["canonical_write"] is False
    assert model_truth_after == model_truth_before
    assert grounding_truth_after == grounding_truth_before


@pytest.mark.asyncio
async def test_load_company_learning_profile_accepts_offline_latent_pattern_inputs() -> None:
    tenant_id = uuid4()
    signatures = [
        build_structural_signature(
            _source("sales", "Approval review blocked renewal commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("security", "Approval review blocked audit commitment"),
            source_kind="model",
        ),
        build_structural_signature(
            _source("legal", "Approval review blocked contract commitment"),
            source_kind="model",
        ),
    ]
    scout_input = build_latent_pattern_profile_input(
        signatures,
        min_support=3,
        min_surface_domains=3,
    )
    conn = _FakeProfileConn(existing_tables=set())

    profile = await load_company_learning_profile(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        **scout_input.to_profile_kwargs(),
    )

    assert profile.priors_for_kind("latent_pattern")
    assert profile.priors_for_kind("structural")
    assert all("sage_global_scout" not in query for query, _ in conn.fetch_calls)


@pytest.mark.asyncio
async def test_load_company_learning_profile_degrades_when_tables_are_missing() -> None:
    tenant_id = uuid4()
    conn = _FakeProfileConn(existing_tables=set())

    profile = await load_company_learning_profile(
        conn,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        route_utilities=[],
        question_policy_stats=[],
    )

    assert profile.priors == ()
    assert profile.notes == ("empty_company_learning_profile",)


def _source(domain: str, text: str) -> dict:
    return {
        "id": uuid4(),
        "domain_tags": [domain],
        "claim_role": "pattern",
        "abstraction_level": "pattern",
        "time_mode": "recurring",
        "polarity": "negative",
        "proposition": {
            "statement": text,
            "observed_tendency": text,
            "pressure_type": "revenue_risk",
            "trigger_conditions": "approval review blocks commitment",
            "expected_outcome": "blocked_commitment_delay",
            "observed_outcome": "blocked_commitment_delay",
        },
        "metadata": {
            "expected_outcome": "blocked_commitment_delay",
            "observed_outcome": "blocked_commitment_delay",
        },
    }


class _FakeProfileConn:
    def __init__(self, *, existing_tables: set[str]) -> None:
        self.existing_tables = existing_tables
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> str | None:
        self.fetchval_calls.append((query, args))
        table = str(args[0]).removeprefix("public.") if args else ""
        return str(args[0]) if table in self.existing_tables else None

    async def fetch(self, query: str, *args: object) -> list[dict]:
        self.fetch_calls.append((query, args))
        if "FROM negative_memory" in query:
            return [
                {
                    "memory_type": "noisy_path",
                    "reason": "semantic route is noisy for this lane",
                    "path": "semantic",
                    "signal_type": "T4",
                    "question_primitive": "DEPENDENCY",
                    "confidence": 0.8,
                    "count": 2,
                }
            ]
        if "FROM sage_reader_decision_attributions" in query:
            return [
                {
                    "source_key": "shortcut",
                    "attempts": 5,
                    "successes": 3,
                    "total_credit": 2.2,
                    "avg_activation": 0.68,
                    "provenance_source": "sage_reader_decision_attributions",
                }
            ]
        if "WITH recent_traces AS" in query:
            return [
                {
                    "source_key": "shortcut",
                    "attempts": 2,
                    "successes": 2,
                    "total_credit": 1.4,
                    "avg_activation": 0.0,
                    "provenance_source": (
                        "grounding_context_source_semantic_outcomes"
                    ),
                }
            ]
        if "FROM calibration_stats" in query:
            return [
                {
                    "actor_key": "actor-1",
                    "proposition_kind": "pattern",
                    "attempts": 4,
                    "successes": 3,
                    "avg_asserted_confidence": 0.65,
                    "provenance_source": "calibration_stats",
                }
            ]
        if "FROM discovery_shortcuts" in query:
            return [
                {
                    "shortcut_key": "shortcut-1",
                    "utility_score": 0.7,
                    "support_count": 5,
                }
            ]
        if "FROM retrieval_affordance_profiles" in query:
            return [
                {
                    "model_id": uuid4(),
                    "utility_score": 0.6,
                    "attempts": 2,
                }
            ]
        if "FROM model_structural_features" in query:
            return [
                {
                    "model_id": uuid4(),
                    "degree_total": 8,
                    "bridge_score": 0.7,
                    "hub_score": 0.2,
                }
            ]
        if "FROM model_residual_evidence" in query:
            return [
                {
                    "id": uuid4(),
                    "residual_kind": "validation_dropped_value",
                    "status": "open",
                }
            ]
        return []


def _profile(*priors: LearningPrior) -> CompanyLearningProfile:
    return CompanyLearningProfile(
        tenant_id=uuid4(),
        built_at=datetime.now(timezone.utc),
        priors=tuple(priors),
        sample_count=sum(prior.sample_count for prior in priors),
        confidence=0.8,
    )


def _plan_source_policy(
    trigger: TriggerContext,
    *,
    weights: dict[str, float],
    profile: CompanyLearningProfile | None = None,
):
    return plan_primary_retrieval(
        trigger=trigger,
        weights=weights,
        effective_seed_entities=[],
        effective_scope_actors=[],
        projection_enabled=True,
        semantic_terms_enabled=True,
        semantic_k=20,
        exploration_rate=0.0,
        company_profile=profile,
    )


async def _seed_grounding_source_outcome(
    pool: asyncpg.Pool,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    source_channel: str,
    supersedes_trace_id: UUID | None = None,
    source_semantic_disposition: str | None = "belief_applied",
) -> tuple[UUID, UUID | None]:
    observation_id = await seed_observation(
        pool,
        conn=conn,
        tenant_id=tenant_id,
        source_channel=source_channel,
        content_text=f"{source_channel} reports a launch dependency",
    )
    context_snapshot_id = uuid4()
    candidate_request_id = uuid4()
    candidate_set_id = uuid4()
    resolution_assessment_id = uuid4()
    grounding_admission_id = uuid4()
    grounding_trace_id = uuid4()
    interpretation_id = uuid4()
    source_semantic_admission_id = uuid4()
    entity_mention_id = uuid4()
    candidate_id = f"candidate:{uuid4()}"
    selected_referent = {
        "type": "customer",
        "id": str(uuid4()),
        "version": 1,
    }
    request_digest = canonical_sha256(
        {"tenant_id": tenant_id, "request_id": candidate_request_id}
    )

    await conn.execute(
        """
        INSERT INTO interpretation_context_snapshots (
          id, tenant_id, focal_observation_id, phrase, source_channel,
          source_space, evidence_cutoff, processing_authority_fingerprint,
          snapshot_content_hash, snapshot
        ) VALUES (
          $1, $2, $3, 'the customer', $4, 'pytest',
          now(), $5, $6, '{}'::jsonb
        )
        """,
        context_snapshot_id,
        tenant_id,
        observation_id,
        source_channel,
        canonical_sha256({"authority": str(context_snapshot_id)}),
        canonical_sha256({"snapshot": str(context_snapshot_id)}),
    )
    await conn.execute(
        """
        INSERT INTO entity_candidate_generation_requests (
          id, tenant_id, context_snapshot_id, source_observation_id,
          phrase, mention_ref, request_digest,
          processing_authority_fingerprint, required_lanes, request
        ) VALUES (
          $1, $2, $3, $4, 'the customer', $5, $6, $7,
          ARRAY['exact_alias'], '{}'::jsonb
        )
        """,
        candidate_request_id,
        tenant_id,
        context_snapshot_id,
        observation_id,
        f"observation:{observation_id}:the customer",
        request_digest,
        canonical_sha256({"authority": str(candidate_request_id)}),
    )
    await conn.execute(
        """
        INSERT INTO entity_candidate_sets (
          id, tenant_id, request_id, request_digest, lane_fates,
          candidates, candidate_set_hash, candidate_set,
          registry_version, expires_at
        ) VALUES (
          $1, $2, $3, $4, '[]'::jsonb, '[]'::jsonb,
          $5, '{}'::jsonb, 'pytest-v1', now() + interval '1 day'
        )
        """,
        candidate_set_id,
        tenant_id,
        candidate_request_id,
        request_digest,
        canonical_sha256({"candidate_set": str(candidate_set_id)}),
    )
    await conn.execute(
        """
        INSERT INTO resolution_assessments (
          id, tenant_id, candidate_set_id, candidate_distribution,
          selected_candidate_id, suggested_canonical_ref,
          model_output, assessment, scorer_and_calibration_version,
          assessed_at, expires_at
        ) VALUES (
          $1, $2, $3, $4::jsonb, $5, $6::jsonb,
          '{}'::jsonb, '{}'::jsonb, 'pytest-v1',
          now(), now() + interval '1 day'
        )
        """,
        resolution_assessment_id,
        tenant_id,
        candidate_set_id,
        json.dumps({candidate_id: 0.99}),
        candidate_id,
        json.dumps(selected_referent),
    )
    await conn.execute(
        """
        INSERT INTO grounding_admission_decisions (
          id, tenant_id, assessment_id, consumer, purpose, operation,
          risk_tier, disposition, selected_referent, reason_codes,
          consumption_authority_fingerprint, decision, decided_at, expires_at
        ) VALUES (
          $1, $2, $3, 'source_semantics', 'pytest', 'read',
          'low', 'single_referent', $4::jsonb, ARRAY['pytest_seed'],
          $5, '{}'::jsonb, now(), now() + interval '1 day'
        )
        """,
        grounding_admission_id,
        tenant_id,
        resolution_assessment_id,
        json.dumps(selected_referent),
        canonical_sha256({"authority": str(grounding_admission_id)}),
    )
    trace_payload = (
        {"supersedes_grounding_trace_id": str(supersedes_trace_id)}
        if supersedes_trace_id is not None
        else {}
    )
    await conn.execute(
        """
        INSERT INTO grounding_traces (
          id, tenant_id, source_observation_id, phrase,
          context_snapshot_id, candidate_request_id, candidate_set_id,
          resolution_assessment_id, grounding_admission_id,
          current_fate, selected_referent, identity_registry_mutated,
          source_observation_mutated, trace
        ) VALUES (
          $1, $2, $3, 'the customer',
          $4, $5, $6, $7, $8,
          'resolved_for_consumer', $9::jsonb, FALSE, FALSE, $10::jsonb
        )
        """,
        grounding_trace_id,
        tenant_id,
        observation_id,
        context_snapshot_id,
        candidate_request_id,
        candidate_set_id,
        resolution_assessment_id,
        grounding_admission_id,
        json.dumps(selected_referent),
        json.dumps(trace_payload),
    )
    if source_semantic_disposition is None:
        return grounding_trace_id, None

    admitted_model_id: UUID | None = None
    if source_semantic_disposition == "belief_applied":
        admitted_model_id = await seed_model(
            pool,
            conn=conn,
            tenant_id=tenant_id,
            born_from_event_id=observation_id,
            natural=f"{source_channel} launch dependency",
            proposition={
                "kind": "belief",
                "subject": f"{source_channel} launch dependency",
                "source_channel": source_channel,
            },
            confidence=0.90,
        )
    await conn.execute(
        """
        INSERT INTO source_semantic_interpretations (
          id, tenant_id, grounding_trace_id, source_observation_id,
          context_snapshot_id, entity_mention_id, resolution_assessment_id,
          grounding_admission_id, source_content_hash, source_assertion,
          semantic_frame, speech_act, grounding_continuity, bundle_digest,
          extractor_version, recorded_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb,
          $10, 'pytest-v1', now()
        )
        """,
        interpretation_id,
        tenant_id,
        grounding_trace_id,
        observation_id,
        context_snapshot_id,
        entity_mention_id,
        resolution_assessment_id,
        grounding_admission_id,
        canonical_sha256({"source": str(observation_id)}),
        canonical_sha256({"bundle": str(interpretation_id)}),
    )
    if source_semantic_disposition == "belief_applied":
        await conn.execute(
            """
            INSERT INTO source_semantic_admission_decisions (
              id, tenant_id, interpretation_id, disposition, reason_codes,
              proposed_belief_assertion, admitted_model_id,
              decision_digest, decided_at
            ) VALUES (
              $1, $2, $3, 'belief_applied', ARRAY['pytest_seed'],
              $4::jsonb, $5, $6, now()
            )
            """,
            source_semantic_admission_id,
            tenant_id,
            interpretation_id,
            json.dumps(
                {
                    "kind": "asserted_state",
                    "source_channel": source_channel,
                }
            ),
            admitted_model_id,
            canonical_sha256({"decision": str(source_semantic_admission_id)}),
        )
    else:
        assert source_semantic_disposition == "no_admission"
        await conn.execute(
            """
            INSERT INTO source_semantic_admission_decisions (
              id, tenant_id, interpretation_id, disposition, reason_codes,
              proposed_belief_assertion, admitted_model_id,
              decision_digest, decided_at
            ) VALUES (
              $1, $2, $3, 'no_admission', ARRAY['pytest_neutral'],
              NULL, NULL, $4, now()
            )
            """,
            source_semantic_admission_id,
            tenant_id,
            interpretation_id,
            canonical_sha256({"decision": str(source_semantic_admission_id)}),
        )
    return grounding_trace_id, admitted_model_id
