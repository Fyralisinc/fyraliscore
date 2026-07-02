from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

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
from services.reasoning.sage.retrieval_policy import SageRouteUtility


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
