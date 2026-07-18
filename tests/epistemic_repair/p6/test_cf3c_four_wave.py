from __future__ import annotations

from copy import deepcopy
from uuid import uuid4

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.evaluation.epistemic_repair.cf3c_four_wave import (
    _PREFIX_METRICS,
    evaluate_cf3c_four_wave,
)


def _passing_metrics(*_args, **_kwargs):
    return {
        name: {"status": "pass", "value": 1.0}
        for name in _PREFIX_METRICS
    }


def _artifact() -> dict:
    population = build_p6_population()
    atlas_synthesis = dict(population.synthesis_signal_by_storyline)["atlas"]
    prior_model_id = str(uuid4())
    prior_version_id = str(uuid4())
    composite_id = str(uuid4())
    composite_version_id = str(uuid4())
    prior = {
        "id": prior_model_id,
        "truth_version_id": prior_version_id,
        "natural_text": "Certificate ownership changed during the Atlas handoff.",
        "proposition": {
            "abstraction_level": "atomic",
            "scope_ref": "workstream:atlas-release",
            "evidence_contract": {
                "evidence_status": "evidence_bound",
                "supporting_event_count": 1,
            },
            "evidence_event_ids": [str(uuid4())],
        },
    }
    composite_snapshot = {
        "id": composite_id,
        "truth_version_id": composite_version_id,
        "proposition": {
            "abstraction_level": "composite",
            "claim_role": "situation",
            "synthesis_contract": True,
            "scope_ref": "workstream:atlas-release",
        },
        "natural_text": "Atlas release delay and slip followed the ownership handoff.",
    }
    waves = []
    for batch in range(1, 5):
        trigger_id = f"trigger-{batch}"
        context_use = {}
        context_decisions = []
        if batch == 2:
            context_use = {
                "selected_model_ids": [prior_model_id],
                "referenced_model_ids": [prior_model_id],
                "trace_referenced_model_ids": [prior_model_id],
                "reasoning_trace_context_decision_used": True,
                "prior_memory_effects": [{
                    "source": "prior_memory_effect",
                    "effect_scope": "candidate",
                    "candidate_id": "atlas-owner",
                    "prior_model_id": prior_model_id,
                    "relation": "supports",
                    "action": "confirm",
                    "material": True,
                    "reasoning_trace_accounted": True,
                }],
            }
            context_decisions = [{
                "batch_id": trigger_id,
                "context_item_kind": "accepted_model",
                "context_item_id": prior_model_id,
                "selected": True,
                "referenced": True,
            }]
        accepted = [prior]
        if batch == 4:
            accepted.append(composite_snapshot)
        waves.append({
            "batch_number": batch,
            "status": "success",
            "execution": {
                "member_count": 25,
                "observation_count": 25,
                "trigger_id": trigger_id,
                "run": {
                    "status": "success",
                    "ops_applied": {
                        "context_use": context_use,
                        "applied_model_ids": (
                            [composite_id] if batch == 4 else []
                        ),
                        "relation_claim_ops": [],
                    },
                },
            },
            "snapshot": {
                "accepted_models": accepted,
                "context_decisions": context_decisions,
                "pending_work": {"truth_critical": {"total": 0}},
            },
            "barrier_receipt": {
                "barrier_id": str(uuid4()),
                "receipt_digest": "a" * 64,
                "reopened_exactly": True,
                "truth_critical_pending_count": 0,
            },
            "elapsed_s": 10.0,
        })

    prior_claim = {
        **prior,
        "evidence_signal_ids": ["p6-b01-s01"],
        "direct_evidence_signal_ids": ["p6-b01-s01"],
        "scope_entities": [{
            "type": "workstream", "id": "workstream:atlas-release"
        }],
    }
    claims = [prior_claim, {
        **composite_snapshot,
        "is_canonical_synthesis": True,
        "direct_evidence_signal_ids": [atlas_synthesis],
        "evidence_signal_ids": [atlas_synthesis, "p6-b01-s01"],
        "source_model_version_ids": [prior_version_id],
        "scope_entities": [{
            "type": "workstream", "id": "workstream:atlas-release"
        }],
        "proposition": {
            **composite_snapshot["proposition"],
            "member_model_ids": [prior_model_id],
        },
    }]
    relation_id = str(uuid4())
    relations = [{
        "id": relation_id,
        "relation_kind": "causal_influence",
        "participants": [
            {"role": "cause", "claim_id": prior_model_id},
            {"role": "effect", "claim_id": composite_id},
        ],
    }]
    waves[3]["execution"]["run"]["ops_applied"]["relation_claim_ops"] = [{
        "op": "accept",
        "relation_instance_id": relation_id,
        "edge_kind": "causal_influence",
        "source_model_id": prior_model_id,
        "target_model_id": composite_id,
    }]
    fates = [{
        "signal_id": signal.signal_id,
        "boundary_fate": "assigned",
        "mention_fate": "terminal",
        "mutation_fate": "accepted" if signal.signal_id == atlas_synthesis else "no_op",
    } for batch in population.batches[:4] for signal in batch.signals]
    evidence = {
        "claims": claims,
        "relations": relations,
        "signal_fates": fates,
        "query_receipts": [{"query": "frozen"}],
    }
    evidence["source_digest"] = canonical_sha256(evidence)
    return {
        "complete": True,
        "completed_batches": 4,
        "target_batches": 4,
        "elapsed_s": 40.0,
        "population_digest": population.population_digest,
        "waves": waves,
        "postfreeze_evidence": evidence,
        "zero_seed_preflight": {
            "accepted_model_count": 0,
            "accepted_relation_count": 0,
        },
        "founder_identity_bootstrap": {
            "applied_before_enqueue": True,
            "semantic_truth_unchanged": True,
            "no_behavioral_models_seeded": True,
        },
        "run_provenance": {"git_commit": "abc", "worktree_clean": True},
        "mixed_llm_attempt_count": 0,
        "expected_llm_configuration": {
            "provider": "codex",
            "model": "gpt-5.3-codex-spark",
            "transport": "cli",
        },
        "llm_attempt_receipts": [{
            "physical_attempt_id": str(uuid4()),
            "think_run_id": str(uuid4()),
            "provider": "codex",
            "model": "gpt-5.3-codex-spark",
            "usage_exactness": "reported",
            "input_tokens": 100,
            "output_tokens": 10,
            "cache_tokens": 20,
        }],
    }


def _patch_metrics(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.evaluation.epistemic_repair.cf3c_four_wave._score_boundaries",
        _passing_metrics,
    )
    monkeypatch.setattr(
        "services.evaluation.epistemic_repair.cf3c_four_wave._score_mentions",
        lambda *_args, **_kwargs: {
            name: {"status": "pass", "value": 1.0}
            for name in (
                "exact_mention_f1",
                "entity_type_accuracy",
                "canonical_link_precision",
                "canonical_link_recall",
            )
        },
    )
    monkeypatch.setattr(
        "services.evaluation.epistemic_repair.cf3c_four_wave._score_claims_and_theses",
        lambda *_args, **_kwargs: {
            name: {"status": "pass", "value": 1.0}
            for name in (
                "atomic_claim_precision",
                "atomic_claim_recall",
                "atomic_claim_f1",
                "evidence_lineage_coverage",
                "scope_precision",
                "scope_recall",
            )
        },
    )


def test_green_requires_four_wave_composite_and_material_prior_use(monkeypatch) -> None:
    _patch_metrics(monkeypatch)
    report = evaluate_cf3c_four_wave(_artifact())

    assert report["verdict"] == "green"
    assert report["failed_gates"] == []
    assert report["measurements"]["signal_count"] == 100
    assert report["measurements"]["materially_used_prior_version_ids"]


def test_composite_before_opportunity_fails_even_if_final_claim_is_valid(monkeypatch) -> None:
    _patch_metrics(monkeypatch)
    artifact = _artifact()
    artifact["waves"][2]["snapshot"]["accepted_models"].append(
        deepcopy(artifact["waves"][3]["snapshot"]["accepted_models"][-1])
    )

    report = evaluate_cf3c_four_wave(artifact)

    assert report["verdict"] == "red"
    assert not report["gates"]["no_composite_before_batch_four_opportunity"]


def test_selected_or_traced_prior_without_material_effect_fails(monkeypatch) -> None:
    _patch_metrics(monkeypatch)
    artifact = _artifact()
    artifact["waves"][1]["execution"]["run"]["ops_applied"]["context_use"][
        "prior_memory_effects"
    ] = []

    report = evaluate_cf3c_four_wave(artifact)

    assert report["verdict"] == "red"
    assert not report["gates"]["material_earlier_model_use_in_batches_2_to_4"]


def test_direct_prior_phase_observation_or_unaccepted_member_version_fails(
    monkeypatch,
) -> None:
    _patch_metrics(monkeypatch)
    artifact = _artifact()
    composite = next(
        row for row in artifact["postfreeze_evidence"]["claims"]
        if row.get("is_canonical_synthesis") is True
    )
    composite["direct_evidence_signal_ids"].append("p6-b01-s01")
    composite["source_model_version_ids"] = [str(uuid4())]

    report = evaluate_cf3c_four_wave(artifact)

    assert report["verdict"] == "red"
    assert not report["gates"]["composite_cites_exact_prior_model_versions"]
    assert not report["gates"][
        "composite_direct_evidence_local_prior_phases_transitive"
    ]


def test_full_p6_shape_cannot_be_reinterpreted_as_cf3c(monkeypatch) -> None:
    _patch_metrics(monkeypatch)
    artifact = _artifact()
    artifact["target_batches"] = 12

    report = evaluate_cf3c_four_wave(artifact)

    assert report["verdict"] == "red"
    assert not report["gates"]["exactly_four_successful_batches_of_25"]


def test_arbitrary_atlas_relation_is_not_treated_as_supported(monkeypatch) -> None:
    _patch_metrics(monkeypatch)
    artifact = _artifact()
    composite_id = next(
        row["id"] for row in artifact["postfreeze_evidence"]["claims"]
        if row.get("is_canonical_synthesis") is True
    )
    artifact["postfreeze_evidence"]["relations"] = [{
        "id": str(uuid4()),
        "relation_kind": "supports",
        "participants": [
            {"role": "source", "claim_id": composite_id},
            {"role": "target", "claim_id": composite_id},
        ],
    }]

    report = evaluate_cf3c_four_wave(artifact)

    assert report["verdict"] == "red"
    assert not report["gates"]["unsupported_canonical_relations_zero"]


def test_real_prefix_boundary_scorer_uses_all_one_hundred_members(monkeypatch) -> None:
    monkeypatch.setattr(
        "services.evaluation.epistemic_repair.cf3c_four_wave._score_mentions",
        lambda *_args, **_kwargs: {
            name: {"status": "pass", "value": 1.0}
            for name in (
                "exact_mention_f1", "entity_type_accuracy",
                "canonical_link_precision", "canonical_link_recall",
            )
        },
    )
    monkeypatch.setattr(
        "services.evaluation.epistemic_repair.cf3c_four_wave._score_claims_and_theses",
        lambda *_args, **_kwargs: {
            name: {"status": "pass", "value": 1.0}
            for name in (
                "atomic_claim_precision", "atomic_claim_recall",
                "atomic_claim_f1", "evidence_lineage_coverage",
                "scope_precision", "scope_recall",
            )
        },
    )
    artifact = _artifact()
    population = build_p6_population()
    gold = {row.signal_id: row for row in population.gold}
    artifact["postfreeze_evidence"]["boundaries"] = [{
        "signal_id": signal.signal_id,
        "predicted_boundary_id": (
            gold[signal.signal_id].storyline_id or signal.signal_id
        ),
    } for batch in population.batches[:4] for signal in batch.signals]

    report = evaluate_cf3c_four_wave(artifact)

    boundary = report["measurements"]["continuous_metrics"][
        "boundary_b_cubed_f1"
    ]
    assert boundary["status"] == "pass"
    assert boundary["value"] == 1.0
    assert len(boundary["source_ids"]) == 100
