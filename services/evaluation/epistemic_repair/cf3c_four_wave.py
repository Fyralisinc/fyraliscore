"""Dedicated frozen-artifact evaluator for the four-wave CF3-C canary."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_postfreeze_scorer import (
    _score_boundaries,
    _score_claims_and_theses,
    _score_mentions,
    _text_matches_facets,
)
from lib.evaluation.epistemic_repair.p7_population import build_p7_semantic_oracles
from services.evaluation.epistemic_repair.cf3b_two_wave import (
    _authorized_prior_memory_effects,
    _barrier_complete,
    _context_use,
    _ids,
    _mapping,
    _run,
    _sequence,
)


_PREFIX_METRICS = (
    "boundary_b_cubed_f1",
    "exact_mention_f1",
    "entity_type_accuracy",
    "canonical_link_precision",
    "canonical_link_recall",
    "atomic_claim_precision",
    "atomic_claim_recall",
    "atomic_claim_f1",
    "evidence_lineage_coverage",
    "scope_precision",
    "scope_recall",
)


def _proposition(row: Mapping[str, Any]) -> Mapping[str, Any]:
    value = row.get("proposition")
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    return _mapping(value)


def _scope_ref(row: Mapping[str, Any]) -> str:
    proposition = _proposition(row)
    direct = str(proposition.get("scope_ref") or "")
    if direct:
        return direct
    refs = {
        str(item.get("canonical_ref") or item.get("id") or "")
        for item in _sequence(row.get("scope_entities"))
        if isinstance(item, Mapping)
    }
    refs.discard("")
    return next(iter(refs)) if len(refs) == 1 else ""


def _is_composite(row: Mapping[str, Any]) -> bool:
    proposition = _proposition(row)
    return (
        row.get("is_canonical_synthesis") is True
        and proposition.get("claim_role") == "situation"
        and proposition.get("abstraction_level") == "composite"
        and proposition.get("synthesis_contract") is True
    )


def _prior_material_use(
    waves: list[Mapping[str, Any]],
) -> tuple[set[str], set[str]]:
    """Return materially used earlier Model ids and their exact prior versions."""

    earlier_versions: dict[str, str] = {}
    used_ids: set[str] = set()
    used_versions: set[str] = set()
    for wave in waves:
        batch_number = int(wave.get("batch_number") or 0)
        snapshot = _mapping(wave.get("snapshot"))
        if batch_number >= 2:
            context = _context_use(wave)
            selected = _ids(context.get("selected_model_ids")) & set(earlier_versions)
            referenced = _ids(context.get("referenced_model_ids")) & selected
            traced = _ids(context.get("trace_referenced_model_ids")) & selected
            trigger_id = str(_mapping(wave.get("execution")).get("trigger_id") or "")
            durable_rows = [
                _mapping(row)
                for row in _sequence(snapshot.get("context_decisions"))
                if isinstance(row, Mapping)
                and str(row.get("batch_id") or "") == trigger_id
                and row.get("context_item_kind") == "accepted_model"
            ]
            durable_selected = {
                str(row.get("context_item_id")) for row in durable_rows
                if row.get("selected") is True
            }
            durable_referenced = {
                str(row.get("context_item_id")) for row in durable_rows
                if row.get("referenced") is True
            }
            effects = _authorized_prior_memory_effects(
                context,
                selected_prior_ids=selected,
            )
            effect_ids = {str(effect.get("prior_model_id")) for effect in effects}
            if context.get("reasoning_trace_context_decision_used") is True:
                material = (
                    selected & referenced & traced & effect_ids
                    & durable_selected & durable_referenced
                )
                used_ids.update(material)
                used_versions.update(earlier_versions[item] for item in material)
        for model in _sequence(snapshot.get("accepted_models")):
            model = _mapping(model)
            model_id = str(model.get("id") or "")
            version_id = str(model.get("truth_version_id") or "")
            if model_id and version_id:
                earlier_versions[model_id] = version_id
    return used_ids, used_versions


def _provider_receipts_valid(artifact: Mapping[str, Any]) -> bool:
    expected = _mapping(artifact.get("expected_llm_configuration"))
    receipts = [
        _mapping(row) for row in _sequence(artifact.get("llm_attempt_receipts"))
        if isinstance(row, Mapping)
    ]
    return (
        expected.get("provider") == "codex"
        and expected.get("transport") == "cli"
        and bool(expected.get("model"))
        and bool(receipts)
        and len({str(row.get("physical_attempt_id")) for row in receipts})
        == len(receipts)
        and all(
            row.get("provider") == expected.get("provider")
            and row.get("model") == expected.get("model")
            and row.get("usage_exactness") == "reported"
            and all(
                isinstance(row.get(key), int)
                and not isinstance(row.get(key), bool)
                and row[key] >= 0
                for key in ("input_tokens", "output_tokens", "cache_tokens")
            )
            and bool(row.get("physical_attempt_id"))
            and bool(row.get("think_run_id"))
            for row in receipts
        )
    )


def evaluate_cf3c_four_wave(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Score only the preregistered four-wave synthesis prefix."""

    population = build_p6_population()
    waves = [
        _mapping(row) for row in _sequence(artifact.get("waves"))
        if isinstance(row, Mapping)
    ]
    evidence = _mapping(artifact.get("postfreeze_evidence"))
    exact_waves = (
        len(waves) == 4
        and [int(row.get("batch_number") or 0) for row in waves] == [1, 2, 3, 4]
        and all(
            row.get("status") == "success"
            and _run(row).get("status") == "success"
            and _mapping(row.get("execution")).get("member_count") == 25
            and _mapping(row.get("execution")).get("observation_count") == 25
            for row in waves
        )
        and artifact.get("complete") is True
        and artifact.get("completed_batches") == 4
        and artifact.get("target_batches") == 4
    )

    metric_rows: dict[str, dict[str, Any]] = {}
    metric_rows.update(_score_boundaries(dict(artifact), population))
    metric_rows.update(_score_mentions(dict(artifact), population))
    metric_rows.update(_score_claims_and_theses(dict(artifact), population))
    prefix_metrics = {name: metric_rows[name] for name in _PREFIX_METRICS}

    claims = [
        _mapping(row) for row in _sequence(evidence.get("claims"))
        if isinstance(row, Mapping)
    ]
    relations = [
        _mapping(row) for row in _sequence(evidence.get("relations"))
        if isinstance(row, Mapping)
    ]
    composites = [row for row in claims if _is_composite(row)]
    atlas_signal = dict(population.synthesis_signal_by_storyline)["atlas"]
    signal_gold = {row.signal_id: row for row in population.gold}
    signal_batch = {row.signal_id: row.batch_number for row in population.signals}
    prior_snapshot_models = {
        str(model.get("id")): _mapping(model)
        for wave in waves[:3]
        for model in _sequence(_mapping(wave.get("snapshot")).get("accepted_models"))
        if isinstance(model, Mapping) and model.get("id")
    }
    prior_versions = {
        str(model.get("truth_version_id"))
        for model in prior_snapshot_models.values()
        if model.get("truth_version_id")
    }
    composite = composites[0] if len(composites) == 1 else {}
    direct_ids = {str(value) for value in _sequence(composite.get(
        "direct_evidence_signal_ids"
    ))}
    transitive_ids = {
        str(value) for value in _sequence(composite.get("evidence_signal_ids"))
    } - direct_ids
    member_versions = {
        str(value) for value in _sequence(composite.get("source_model_version_ids"))
    }
    member_models = {
        str(value) for value in _sequence(_proposition(composite).get("member_model_ids"))
    }
    exact_members = (
        bool(member_versions)
        and bool(member_models)
        and member_models <= set(prior_snapshot_models)
        and member_versions == {
            str(prior_snapshot_models[model_id].get("truth_version_id"))
            for model_id in member_models
        }
        and member_versions <= prior_versions
        and all(
            _scope_ref(prior_snapshot_models[model_id])
            == "workstream:atlas-release"
            for model_id in member_models
        )
    )
    b4_snapshot = _mapping(waves[3].get("snapshot")) if len(waves) == 4 else {}
    b4_snapshot_models = {
        str(model.get("id")): _mapping(model)
        for model in _sequence(b4_snapshot.get("accepted_models"))
        if isinstance(model, Mapping) and model.get("id")
    }
    composite_current_in_b4 = (
        bool(composite.get("id"))
        and str(composite.get("id")) in b4_snapshot_models
        and str(b4_snapshot_models[str(composite.get("id"))].get(
            "truth_version_id"
        ) or "") == str(composite.get("truth_version_id") or "")
    )
    direct_local = (
        atlas_signal in direct_ids
        and bool(direct_ids)
        and all(
            signal_batch.get(signal_id) == 4
            and signal_gold.get(signal_id) is not None
            and signal_gold[signal_id].storyline_id == "atlas"
            for signal_id in direct_ids
        )
        and bool(transitive_ids)
        and all(signal_batch.get(signal_id, 99) < 4 for signal_id in transitive_ids)
    )
    no_early_composites = all(
        not any(
            _mapping(model.get("proposition")).get("abstraction_level") == "composite"
            for model in _sequence(_mapping(wave.get("snapshot")).get("accepted_models"))
            if isinstance(model, Mapping)
        )
        for wave in waves[:3]
    )

    gold_roles = {row.signal_id: row.role for row in population.gold}
    noise_claim_ids = {
        str(row.get("id")) for row in claims
        if row.get("evidence_signal_ids")
        and all(
            gold_roles.get(str(source)) in {"noise", "high_similarity_distractor"}
            for source in _sequence(row.get("evidence_signal_ids"))
        )
    }
    relation_claim_ids = [
        {
            str(item.get("claim_id"))
            for item in _sequence(row.get("participants"))
            if isinstance(item, Mapping) and item.get("claim_id")
        }
        for row in relations
    ]
    noise_relations = sum(
        bool(ids) and ids <= noise_claim_ids for ids in relation_claim_ids
    )
    claims_by_id = {str(row.get("id")): row for row in claims if row.get("id")}
    atlas_oracle = next(
        row for row in build_p7_semantic_oracles(population).relations
        if row.storyline_id == "atlas"
    )
    valid_atlas_relation_ids: set[str] = set()
    supported_relations_only = True
    for row in relations:
        participants = {
            str(item.get("role")): claims_by_id.get(str(item.get("claim_id")))
            for item in _sequence(row.get("participants"))
            if isinstance(item, Mapping)
        }
        cause = participants.get(atlas_oracle.cause_role)
        effect = participants.get(atlas_oracle.effect_role)
        if not (
            len(participants) == 2
            and cause is not None
            and effect is not None
            and cause is not effect
            and _scope_ref(cause) == "workstream:atlas-release"
            and _scope_ref(effect) == "workstream:atlas-release"
            and row.get("relation_kind") == atlas_oracle.relation_kind
            and _text_matches_facets(cause, atlas_oracle.cause_participant_facets)
            and _text_matches_facets(effect, atlas_oracle.effect_participant_facets)
        ):
            supported_relations_only = False
        elif row.get("id"):
            valid_atlas_relation_ids.add(str(row["id"]))
    b4_ops = _mapping(_run(waves[3]).get("ops_applied")) if len(waves) == 4 else {}
    b4_relation_summaries = [
        _mapping(row) for row in _sequence(b4_ops.get("relation_claim_ops"))
        if isinstance(row, Mapping) and row.get("op") != "skip"
    ]
    coadmitted_relation_ids = {
        str(row.get("relation_instance_id") or row.get("relation_claim_id") or "")
        for row in b4_relation_summaries
    }
    coadmitted_relation_ids.discard("")
    composite_relation_coadmitted = (
        str(composite.get("id") or "") in _ids(b4_ops.get("applied_model_ids"))
        and bool(valid_atlas_relation_ids & coadmitted_relation_ids)
    )

    expected_scope_by_storyline = {
        "atlas": "workstream:atlas-release",
        "beacon": "workstream:beacon-migration",
        "cobalt": "commitment:cobalt-renewal",
        "delta": "workstream:delta-handoff",
    }
    cross_story_clean = True
    for row in claims:
        storylines = {
            signal_gold[str(source)].storyline_id
            for source in _sequence(row.get("evidence_signal_ids"))
            if str(source) in signal_gold
            and signal_gold[str(source)].storyline_id is not None
        }
        if len(storylines) > 1:
            cross_story_clean = False
            break
        if len(storylines) == 1:
            storyline = next(iter(storylines))
            if _scope_ref(row) != expected_scope_by_storyline[storyline]:
                cross_story_clean = False
                break

    used_model_ids, used_version_ids = _prior_material_use(waves)
    fates = [
        _mapping(row) for row in _sequence(evidence.get("signal_fates"))
        if isinstance(row, Mapping)
    ]
    receipts_complete = (
        len(fates) == 100
        and len({str(row.get("signal_id")) for row in fates}) == 100
        and all(
            row.get("boundary_fate") and row.get("mention_fate")
            and row.get("mutation_fate") for row in fates
        )
        and all(_barrier_complete(wave) for wave in waves)
        and _provider_receipts_valid(artifact)
    )
    bootstrap = _mapping(artifact.get("founder_identity_bootstrap"))
    zero_seed = _mapping(artifact.get("zero_seed_preflight"))
    provenance = _mapping(artifact.get("run_provenance"))
    source_digest = str(evidence.get("source_digest") or "")
    digest_body = {key: value for key, value in evidence.items() if key != "source_digest"}
    evidence_valid = (
        bool(source_digest)
        and source_digest == canonical_sha256(digest_body)
        and bool(evidence.get("query_receipts"))
    )

    gates = {
        "exactly_four_successful_batches_of_25": exact_waves,
        **{
            f"metric_{name}": prefix_metrics[name].get("status") == "pass"
            for name in _PREFIX_METRICS
        },
        "exactly_one_expected_atlas_composite": (
            len(composites) == 1
            and _scope_ref(composite) == "workstream:atlas-release"
            and atlas_signal in set(map(str, composite.get("evidence_signal_ids") or ()))
        ),
        "no_composite_before_batch_four_opportunity": no_early_composites,
        "composite_cites_exact_prior_model_versions": exact_members,
        "composite_current_version_present_in_batch_four_snapshot": (
            composite_current_in_b4
        ),
        "composite_direct_evidence_local_prior_phases_transitive": direct_local,
        "false_models_or_relations_from_noise_zero": (
            not noise_claim_ids and noise_relations == 0
        ),
        "cross_story_canonical_contamination_zero": cross_story_clean,
        "exact_expected_atlas_relation_present": (
            len(relations) == 1 and len(valid_atlas_relation_ids) == 1
        ),
        "unsupported_canonical_relations_zero": supported_relations_only,
        "composite_and_atlas_relation_coadmitted_in_batch_four": (
            composite_relation_coadmitted
        ),
        "material_earlier_model_use_in_batches_2_to_4": bool(used_version_ids),
        "all_barriers_fates_and_provider_receipts_complete": receipts_complete,
        "zero_seed_and_founder_bootstrap_valid": (
            zero_seed.get("accepted_model_count") == 0
            and zero_seed.get("accepted_relation_count") == 0
            and bootstrap.get("applied_before_enqueue") is True
            and bootstrap.get("semantic_truth_unchanged") is True
            and bootstrap.get("no_behavioral_models_seeded") is True
        ),
        "clean_single_commit_and_postfreeze_evidence_valid": (
            bool(provenance.get("git_commit"))
            and provenance.get("worktree_clean") is True
            and artifact.get("mixed_llm_attempt_count") == 0
            and artifact.get("population_digest") == population.population_digest
            and evidence_valid
        ),
    }
    receipts = list(_sequence(artifact.get("llm_attempt_receipts")))
    elapsed = [float(row.get("elapsed_s") or 0.0) for row in waves]
    measurements = {
        "signal_count": sum(
            int(_mapping(row.get("execution")).get("member_count") or 0)
            for row in waves
        ),
        "batch_elapsed_seconds": elapsed,
        "total_elapsed_seconds": float(artifact.get("elapsed_s") or sum(elapsed)),
        "llm_call_count": len(receipts),
        "llm_calls_per_signal": len(receipts) / 100,
        "composite_count": len(composites),
        "composite_model_id": composite.get("id"),
        "composite_member_model_ids": sorted(member_models),
        "composite_member_version_ids": sorted(member_versions),
        "materially_used_prior_model_ids": sorted(used_model_ids),
        "materially_used_prior_version_ids": sorted(used_version_ids),
        "continuous_metrics": prefix_metrics,
    }
    failed = sorted(name for name, passed in gates.items() if not passed)
    payload = {
        "schema_version": "cf3c-four-wave-evaluation-v1",
        "proof_claim": "four_wave_scope_local_synthesis_with_material_prior_memory_use",
        "measurements": measurements,
        "gates": gates,
        "failed_gates": failed,
        "verdict": "green" if not failed else "red",
        "proof_boundary": (
            "Only batches one through four are scored.",
            "Later-use means material use of an earlier accepted Model in batches two through four; the batch-four composite need not be reused before it exists.",
            "Reuse of the batch-four composite itself is deferred to CF4.",
            "Composite-relation co-admission is proven by one batch-four Apply receipt containing both the current composite Model id and the frozen canonical relation id.",
            "Gold is joined only after the production artifact is frozen.",
        ),
    }
    return {**payload, "content_digest": canonical_sha256(payload)}


__all__ = ["evaluate_cf3c_four_wave"]
