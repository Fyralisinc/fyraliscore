"""Pure, gold-joined P6 post-freeze scorer.

The production runner is intentionally unaware of this module.  This scorer
accepts a frozen execution dictionary plus the sealed P6 population, verifies
both immutable inputs, and emits every Section 19.5/19.6 endpoint.  Missing
member-level evidence is never converted into a zero-denominator pass.
"""

from __future__ import annotations

import math
import re
from statistics import mean, median
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import (
    P6_BATCH_COUNT,
    P6_SIGNAL_COUNT,
    P6_SIGNALS_PER_BATCH,
    P6_STORYLINES,
    P6Population,
)
from lib.evaluation.epistemic_repair.p7_population import build_p7_semantic_oracles
from lib.evaluation.epistemic_repair.p7_postfreeze_oracle import entails_structured_claim


MetricSpec = tuple[str, float]

_SPECS: dict[str, MetricSpec] = {
    "boundary_b_cubed_f1": (">=", 0.90),
    "selected_context_contamination": ("<=", 0.05),
    "sufficient_context_recall": (">=", 0.95),
    "exact_mention_f1": (">=", 0.92),
    "entity_type_accuracy": (">=", 0.95),
    "canonical_link_precision": (">=", 0.98),
    "canonical_link_recall": (">=", 0.90),
    "atomic_claim_precision": (">=", 0.90),
    "atomic_claim_recall": (">=", 0.85),
    "atomic_claim_f1": (">=", 0.875),
    "evidence_lineage_coverage": ("=", 1.0),
    "scope_precision": (">=", 0.95),
    "scope_recall": (">=", 0.90),
    "direct_thesis_accuracy": ("=", 1.0),
    "mean_thesis_facet_completeness": (">=", 0.90),
    "relation_joint_precision": (">=", 0.95),
    "relation_joint_recall": (">=", 0.80),
    "lifecycle_expected_transition_accuracy": ("=", 1.0),
    "historical_reopening_reason_coverage": ("=", 1.0),
    "mature_actual_model_use_share": (">=", 0.70),
    "mature_unnecessary_historical_observation_use": ("<=", 0.10),
    "resolved_outcome_model_ece": ("<=", 0.15),
    "resolved_outcome_model_brier": ("<=", 0.20),
    "selected_context_utilization": (">=", 0.80),
    "false_model_relation_from_noise": ("=", 0.0),
    "duplicate_causal_credit_fanout": ("=", 0.0),
    "clean_t1_p95_seconds": ("<=", 120.0),
    "clean_max_over_median": ("<=", 3.0),
    "metered_llm_calls_per_signal": ("<=", 0.08),
    "question_planning_batch_share": ("<=", 0.25),
    "truth_critical_pending_at_barriers": ("=", 0.0),
    "refresh_key_duplicate_processing_ratio": ("<=", 1.10),
}


def _metric(
    name: str,
    numerator: float | None,
    denominator: int | None,
    *,
    source_ids: list[str] | None = None,
    worst_cases: list[dict[str, Any]] | None = None,
    status_override: str | None = None,
) -> dict[str, Any]:
    operator, threshold = _SPECS[name]
    measured = numerator is not None and denominator is not None and denominator > 0
    value = numerator / denominator if measured else None
    if status_override is not None:
        status = status_override
    elif not measured:
        status = "unmeasured"
    else:
        passed = (
            value >= threshold if operator == ">="
            else value <= threshold if operator == "<="
            else value == threshold
        )
        status = "pass" if passed else "fail"
    return {
        "numerator": numerator,
        "denominator": denominator,
        "value": value,
        "operator": operator,
        "threshold": threshold,
        "status": status,
        "source_ids": sorted(set(source_ids or ())),
        "worst_cases": list(worst_cases or ()),
    }


def _f1(tp: int, predicted: int, expected: int) -> tuple[float, float, float]:
    precision = tp / predicted if predicted else 0.0
    recall = tp / expected if expected else 0.0
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return precision, recall, f1


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    low, high = math.floor(position), math.ceil(position)
    if low == high:
        return ordered[low]
    return ordered[low] + (ordered[high] - ordered[low]) * (position - low)


def _ece(rows: list[dict[str, Any]], bins: int = 10) -> float:
    buckets: list[list[dict[str, Any]]] = [[] for _ in range(bins)]
    for row in rows:
        confidence = float(row["confidence"])
        buckets[min(int(confidence * bins), bins - 1)].append(row)
    return sum(
        len(bucket) / len(rows)
        * abs(
            mean(float(row["confidence"]) for row in bucket)
            - mean(float(row["label"]) for row in bucket)
        )
        for bucket in buckets if bucket
    )


def _gold_maps(population: P6Population) -> dict[str, Any]:
    signals = {item.signal_id: item for item in population.signals}
    gold = {item.signal_id: item for item in population.gold}
    return {"signals": signals, "gold": gold}


def _record_index(raw: dict[str, Any], key: str) -> list[dict[str, Any]]:
    records = (raw.get("postfreeze_evidence") or {}).get(key)
    return [dict(row) for row in records] if isinstance(records, list) else []


def _evidence_has(raw: dict[str, Any], key: str) -> bool:
    return key in (raw.get("postfreeze_evidence") or {})


def _score_mentions(
    raw: dict[str, Any], population: P6Population,
) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "mentions")
    expected = {
        item.signal_id: item for item in population.gold if item.entity_surface
    }
    if not rows:
        return {
            name: _metric(name, None, None)
            for name in (
                "exact_mention_f1", "entity_type_accuracy",
                "canonical_link_precision", "canonical_link_recall",
            )
        }
    exact = type_ok = link_ok = 0
    predicted_links = 0
    seen_expected: set[str] = set()
    worst = []
    signal_text = {signal.signal_id: signal.text for signal in population.signals}
    for row in rows:
        signal_id = str(row.get("signal_id") or "")
        item = expected.get(signal_id)
        if item is None:
            worst.append({"source_id": signal_id, "reason": "unexpected_mention"})
            continue
        start = signal_text[signal_id].find(str(item.entity_surface))
        exact_match = (
            row.get("surface") == item.entity_surface
            and row.get("span_start") == start
            and row.get("span_end") == start + len(str(item.entity_surface))
        )
        if exact_match:
            exact += 1
            seen_expected.add(signal_id)
        if exact_match and row.get("entity_type") == item.entity_type:
            type_ok += 1
        if row.get("canonical_ref") is not None:
            predicted_links += 1
            if exact_match and row.get("canonical_ref") == item.canonical_ref:
                link_ok += 1
    _, _, mention_f1 = _f1(exact, len(rows), len(expected))
    ids = list(expected)
    return {
        "exact_mention_f1": _metric("exact_mention_f1", mention_f1, 1, source_ids=ids, worst_cases=worst),
        "entity_type_accuracy": _metric("entity_type_accuracy", type_ok, exact, source_ids=ids),
        "canonical_link_precision": _metric("canonical_link_precision", link_ok, predicted_links, source_ids=ids),
        "canonical_link_recall": _metric("canonical_link_recall", link_ok, len(expected), source_ids=ids),
    }


def _score_context(raw: dict[str, Any], population: P6Population) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "context_items")
    evidence = raw.get("postfreeze_evidence") or {}
    # Retrieval recall needs the complete preregistered opportunity set.  The
    # current decision table preserves selected items but not the target signal
    # coordinate, so it is dishonest to infer a denominator from predictions.
    opportunities_complete = evidence.get("context_opportunities_complete") is True
    coordinates_complete = bool(rows) and all(
        row.get("target_signal_id") and row.get("source_signal_id") for row in rows
    )
    if not opportunities_complete or not coordinates_complete:
        return {name: _metric(name, None, None) for name in (
            "selected_context_contamination", "sufficient_context_recall",
            "historical_reopening_reason_coverage", "mature_actual_model_use_share",
            "mature_unnecessary_historical_observation_use", "selected_context_utilization",
        )}
    gold = {item.signal_id: item for item in population.gold}
    selected = [row for row in rows if row.get("selected")]
    contamination = 0
    sufficient_expected = sum(
        1 for row in rows
        if (
            (target := gold.get(str(row.get("target_signal_id") or ""))) is not None
            and (source := gold.get(str(row.get("source_signal_id") or ""))) is not None
            and target.storyline_id is not None
            and source.storyline_id == target.storyline_id
            and source.role not in {"noise", "high_similarity_distractor"}
        )
    )
    sufficient_selected = 0
    historical = historical_reasoned = 0
    mature = [row for row in selected if int(row.get("batch_number") or 0) >= 11]
    mature_models = mature_unnecessary = 0
    utilized = 0
    worst = []
    for row in selected:
        target = gold.get(str(row.get("target_signal_id") or ""))
        source = gold.get(str(row.get("source_signal_id") or ""))
        if target is None or source is None:
            contamination += 1
            worst.append({"source_id": row.get("source_signal_id"), "reason": "unbound_context"})
        elif source.storyline_id != target.storyline_id or source.role in {
            "noise", "high_similarity_distractor",
        }:
            contamination += 1
        if (
            target is not None and source is not None
            and target.storyline_id is not None
            and source.storyline_id == target.storyline_id
            and source.role not in {"noise", "high_similarity_distractor"}
        ):
            sufficient_selected += 1
        source_signal = next(
            (signal for signal in population.signals
             if signal.signal_id == row.get("source_signal_id")), None
        )
        target_signal = next(
            (signal for signal in population.signals
             if signal.signal_id == row.get("target_signal_id")), None
        )
        is_historical = bool(
            source_signal and target_signal
            and source_signal.batch_number < target_signal.batch_number
        )
        if is_historical:
            historical += 1
            historical_reasoned += int(bool(row.get("historical_reopen_reason")))
        utilized += int(bool(row.get("referenced")))
    for row in mature:
        mature_models += int(row.get("context_item_kind") == "model")
        mature_unnecessary += int(
            row.get("context_item_kind") == "observation"
            and any(
                signal.signal_id == row.get("source_signal_id")
                and signal.batch_number <= 10
                for signal in population.signals
            )
            and not row.get("necessary")
        )
    sufficient_total = sufficient_expected
    ids = [str(row.get("source_signal_id")) for row in rows]
    return {
        "selected_context_contamination": _metric(
            "selected_context_contamination", contamination, len(selected),
            source_ids=ids, worst_cases=worst,
        ),
        "sufficient_context_recall": _metric(
            "sufficient_context_recall", sufficient_selected, sufficient_total,
            source_ids=ids,
        ),
        "historical_reopening_reason_coverage": _metric(
            "historical_reopening_reason_coverage", historical_reasoned, historical,
            source_ids=ids,
        ),
        "mature_actual_model_use_share": _metric(
            "mature_actual_model_use_share", mature_models, len(mature), source_ids=ids,
        ),
        "mature_unnecessary_historical_observation_use": _metric(
            "mature_unnecessary_historical_observation_use",
            mature_unnecessary, len(mature), source_ids=ids,
        ),
        "selected_context_utilization": _metric(
            "selected_context_utilization", utilized, len(selected), source_ids=ids,
        ),
    }


def _score_boundaries(
    raw: dict[str, Any], population: P6Population,
) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "boundaries")
    if not rows:
        return {"boundary_b_cubed_f1": _metric("boundary_b_cubed_f1", None, None)}
    predicted = {
        str(row.get("signal_id")): str(row.get("predicted_boundary_id"))
        for row in rows if row.get("signal_id") and row.get("predicted_boundary_id")
    }
    gold = {
        item.signal_id: (
            item.storyline_id if item.storyline_id is not None else item.signal_id
        ) for item in population.gold
    }
    if set(predicted) != set(gold):
        return {"boundary_b_cubed_f1": _metric(
            "boundary_b_cubed_f1", None, None,
            worst_cases=[{"reason": "boundary_source_denominator_mismatch"}],
        )}
    precisions, recalls = [], []
    ids = sorted(gold)
    for signal_id in ids:
        predicted_cluster = {key for key, value in predicted.items()
                             if value == predicted[signal_id]}
        gold_cluster = {key for key, value in gold.items() if value == gold[signal_id]}
        overlap = len(predicted_cluster & gold_cluster)
        precisions.append(overlap / len(predicted_cluster))
        recalls.append(overlap / len(gold_cluster))
    precision, recall = mean(precisions), mean(recalls)
    score = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    return {"boundary_b_cubed_f1": _metric(
        "boundary_b_cubed_f1", score, 1, source_ids=ids,
    )}


def _scope_refs(value: Any) -> set[str]:
    refs = set()
    for item in value or ():
        if isinstance(item, dict):
            ref = item.get("canonical_ref") or item.get("id")
        else:
            ref = item
        if ref:
            refs.add(str(ref))
    return refs


def _score_claims_and_theses(
    raw: dict[str, Any], population: P6Population,
) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "claims")
    names = (
        "atomic_claim_precision", "atomic_claim_recall", "atomic_claim_f1",
        "evidence_lineage_coverage", "scope_precision", "scope_recall",
        "direct_thesis_accuracy", "mean_thesis_facet_completeness",
    )
    if not rows:
        return {name: _metric(name, None, None) for name in names}
    gold = {item.signal_id: item for item in population.gold}
    expected_claims = {item.claim_id for item in population.gold if item.claim_id}
    oracle = build_p7_semantic_oracles(population)
    represented_claims: set[str] = set()
    coherent_model_phases: dict[str, list[set[str]]] = {
        storyline: [] for storyline in P6_STORYLINES
    }
    precise_claims = lineage_ok = 0
    predicted_scope_refs: set[str] = set()
    # This denominator is sealed-gold truth and therefore cannot shrink when a
    # prediction omits a scope binding.
    expected_scope_refs = {
        item.canonical_ref for item in population.gold if item.canonical_ref
    }
    source_ids: list[str] = []
    worst = []
    for row in rows:
        evidence_ids = [str(value) for value in row.get("evidence_signal_ids") or ()]
        source_ids.extend(evidence_ids)
        evidence = [gold[value] for value in evidence_ids if value in gold]
        exact_lineage = bool(evidence_ids) and len(evidence) == len(set(evidence_ids))
        lineage_ok += int(exact_lineage)
        storylines = {item.storyline_id for item in evidence if item.storyline_id}
        pure = (
            exact_lineage and len(storylines) == 1
            and all(item.role not in {"noise", "high_similarity_distractor"}
                    for item in evidence)
        )
        storyline = next(iter(storylines)) if len(storylines) == 1 else None
        semantic = False
        if pure and storyline:
            claim_oracle = next(
                item for item in oracle.claims if item.storyline_id == storyline
            )
            semantic = entails_structured_claim(row, claim_oracle)
        if pure and semantic and storyline:
            precise_claims += 1
            represented_claims.update(
                item.claim_id for item in evidence if item.claim_id
            )
            coherent_model_phases[storyline].append({
                item.lifecycle_phase for item in evidence
            })
        else:
            worst.append({
                "source_id": str(row.get("id") or ""),
                "reason": "impure_lineage_or_semantic_nonentailment",
            })
        predicted_refs = _scope_refs(row.get("scope_entities"))
        if pure and semantic:
            predicted_scope_refs.update(predicted_refs)
    precision, recall, f1 = _f1(
        precise_claims, len(rows), len(expected_claims)
    )
    # Claim recall is evidence-coordinate coverage, not number of Models.
    recall = len(represented_claims & expected_claims) / len(expected_claims)
    f1 = 0.0 if precision + recall == 0 else 2 * precision * recall / (precision + recall)
    expected_phases = {"weak_initial", "corroboration", "contradiction", "correction", "external_outcome"}
    completeness = {
        storyline: max(
            (
                len(phases & expected_phases) / len(expected_phases)
                for phases in coherent_model_phases[storyline]
            ),
            default=0.0,
        )
        for storyline in P6_STORYLINES
    }
    thesis_direct = sum(value == 1.0 for value in completeness.values())
    scope_tp = len(expected_scope_refs & predicted_scope_refs)
    scope_predicted = len(predicted_scope_refs)
    scope_coordinates_canonical = (
        (raw.get("postfreeze_evidence") or {}).get("scope_coordinates_canonical") is True
    )
    return {
        "atomic_claim_precision": _metric(
            "atomic_claim_precision", precision, 1, source_ids=source_ids,
            worst_cases=worst[:10],
        ),
        "atomic_claim_recall": _metric(
            "atomic_claim_recall", recall, 1, source_ids=source_ids,
        ),
        "atomic_claim_f1": _metric("atomic_claim_f1", f1, 1, source_ids=source_ids),
        "evidence_lineage_coverage": _metric(
            "evidence_lineage_coverage", lineage_ok, len(rows), source_ids=source_ids,
        ),
        "scope_precision": _metric(
            "scope_precision",
            scope_tp if scope_coordinates_canonical else None,
            scope_predicted if scope_coordinates_canonical else None,
            source_ids=source_ids,
        ),
        "scope_recall": _metric(
            "scope_recall",
            scope_tp if scope_coordinates_canonical else None,
            len(expected_scope_refs) if scope_coordinates_canonical else None,
            source_ids=source_ids,
        ),
        "direct_thesis_accuracy": _metric(
            "direct_thesis_accuracy", thesis_direct, len(P6_STORYLINES),
            source_ids=list(P6_STORYLINES),
        ),
        "mean_thesis_facet_completeness": _metric(
            "mean_thesis_facet_completeness", sum(completeness.values()),
            len(completeness), source_ids=list(P6_STORYLINES),
            worst_cases=[
                {"storyline_id": key, "facet_completeness": value}
                for key, value in sorted(completeness.items(), key=lambda item: item[1])
            ],
        ),
    }


def _text_matches_facets(row: dict[str, Any], facets: tuple[tuple[str, ...], ...]) -> bool:
    text = f"{row.get('natural_text') or ''} {row.get('proposition') or ''}".casefold()
    return all(any(term.casefold() in text for term in group) for group in facets)


def _score_relations(
    raw: dict[str, Any], population: P6Population,
) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "relations")
    if not rows:
        return {
            name: _metric(name, None, None)
            for name in ("relation_joint_precision", "relation_joint_recall")
        }
    claims = {str(row.get("id")): row for row in _record_index(raw, "claims")}
    gold = {item.signal_id: item for item in population.gold}
    oracles = {
        item.storyline_id: item
        for item in build_p7_semantic_oracles(population).relations
    }
    matched: set[str] = set()
    valid = 0
    worst = []
    for row in rows:
        participants = {
            str(item.get("role")): claims.get(str(item.get("claim_id")))
            for item in row.get("participants") or () if isinstance(item, dict)
        }
        evidence_storylines = {
            gold[source].storyline_id
            for participant in participants.values() if participant
            for source in participant.get("evidence_signal_ids") or ()
            if source in gold and gold[source].storyline_id
        }
        storyline = next(iter(evidence_storylines)) if len(evidence_storylines) == 1 else None
        oracle = oracles.get(storyline or "")
        cause = participants.get(oracle.cause_role) if oracle else None
        effect = participants.get(oracle.effect_role) if oracle else None
        correct = bool(
            oracle and cause and effect and cause is not effect
            and row.get("relation_kind") == oracle.relation_kind
            and _text_matches_facets(cause, oracle.cause_participant_facets)
            and _text_matches_facets(effect, oracle.effect_participant_facets)
        )
        valid += int(correct)
        if correct and storyline:
            matched.add(storyline)
        else:
            worst.append({"source_id": row.get("id"), "reason": "joint_relation_miss"})
    ids = [str(row.get("id") or "") for row in rows]
    return {
        "relation_joint_precision": _metric(
            "relation_joint_precision", valid, len(rows), source_ids=ids,
            worst_cases=worst[:10],
        ),
        "relation_joint_recall": _metric(
            "relation_joint_recall", len(matched), len(P6_STORYLINES), source_ids=ids,
        ),
    }


def _score_lifecycle(
    raw: dict[str, Any], population: P6Population,
) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "lifecycle_events")
    if not rows:
        return {"lifecycle_expected_transition_accuracy": _metric(
            "lifecycle_expected_transition_accuracy", None, None
        )}
    gold = {item.signal_id: item for item in population.gold}
    matched = set()
    worst = []
    for row in rows:
        evidence = [
            gold.get(str(value)) for value in row.get("evidence_signal_ids") or ()
        ]
        storylines = {
            item.storyline_id for item in evidence
            if item is not None and item.lifecycle_phase == "correction"
        }
        evidence_batches = {
            population_signal.batch_number
            for value in row.get("evidence_signal_ids") or ()
            if (population_signal := next(
                (signal for signal in population.signals if signal.signal_id == value),
                None,
            )) is not None
        }
        valid = (
            len(storylines) == 1
            and row.get("action") in {"falsify", "supersede", "archive", "correct"}
            and bool(evidence_batches & {9, 10})
        )
        if valid:
            matched.update(storylines)
        else:
            worst.append({"source_id": row.get("id"), "reason": "unexpected_transition"})
    return {"lifecycle_expected_transition_accuracy": _metric(
        "lifecycle_expected_transition_accuracy", len(matched), len(P6_STORYLINES),
        source_ids=[str(row.get("id") or "") for row in rows],
        worst_cases=worst[:10],
    )}
def score_p6_frozen_execution(
    *, raw_execution: dict[str, Any], sealed_population: P6Population,
) -> dict[str, Any]:
    """Return a complete fail-closed Section 19 scorecard."""

    raw_digest = canonical_sha256(raw_execution)
    population_digest = sealed_population.population_digest
    digest_match = raw_execution.get("population_digest") == population_digest
    waves = list(raw_execution.get("waves") or ())
    batch_sizes = [
        (
            (wave.get("execution") or {}).get("member_count"),
            (wave.get("execution") or {}).get("observation_count"),
        )
        for wave in waves if wave.get("status") == "success"
    ]
    exact_batch_members = (
        len(batch_sizes) == P6_BATCH_COUNT
        and all(
            member_count == observation_count == P6_SIGNALS_PER_BATCH
            for member_count, observation_count in batch_sizes
        )
    )
    signal_count = (
        sum(int(member_count) for member_count, _ in batch_sizes)
        if exact_batch_members else None
    )
    exact_batches = (
        len(waves) == P6_BATCH_COUNT
        and [int(wave.get("batch_number") or 0) for wave in waves]
        == list(range(1, P6_BATCH_COUNT + 1))
    )
    evidence = raw_execution.get("postfreeze_evidence") or {}
    fates = _record_index(raw_execution, "signal_fates")
    fate_ids = [str(row.get("signal_id") or "") for row in fates]
    exact_fates = len(fate_ids) == P6_SIGNAL_COUNT and set(fate_ids) == {
        signal.signal_id for signal in sealed_population.signals
    }
    barriers = [wave.get("barrier_receipt") for wave in waves]
    barrier_ok = exact_batches and all(
        isinstance(row, dict)
        and int(row.get("truth_critical_pending_count") or 0) == 0
        and row.get("reopened_exactly") is True
        for row in barriers
    )

    metrics: dict[str, dict[str, Any]] = {}
    metrics.update(_score_mentions(raw_execution, sealed_population))
    metrics.update(_score_context(raw_execution, sealed_population))
    metrics.update(_score_boundaries(raw_execution, sealed_population))
    metrics.update(_score_claims_and_theses(raw_execution, sealed_population))
    metrics.update(_score_relations(raw_execution, sealed_population))
    metrics.update(_score_lifecycle(raw_execution, sealed_population))

    raw_predictions = _record_index(raw_execution, "resolved_outcomes")
    gold_by_signal = {item.signal_id: item for item in sealed_population.gold}
    outcome_labels = {
        item.storyline_id: float(item.outcome_label)
        for item in build_p7_semantic_oracles(sealed_population).outcomes
    }
    predictions = []
    opportunity_keys: set[tuple[str, str]] = set()
    duplicate_opportunities = False
    for row in raw_predictions:
        gold = gold_by_signal.get(str(row.get("outcome_signal_id") or ""))
        confidence = row.get("confidence")
        if (
            gold is None or gold.lifecycle_phase != "external_outcome"
            or gold.storyline_id not in outcome_labels
            or not isinstance(confidence, (int, float))
            or not 0 <= float(confidence) <= 1
        ):
            continue
        key = (
            str(row.get("outcome_signal_id")), str(row.get("model_id") or "")
        )
        if not key[1] or key in opportunity_keys:
            duplicate_opportunities = True
            continue
        opportunity_keys.add(key)
        predictions.append({
            **row,
            "label": outcome_labels[gold.storyline_id],
            "source_id": str(row.get("outcome_signal_id")),
        })
    if not _evidence_has(raw_execution, "resolved_outcomes") or duplicate_opportunities:
        metrics["resolved_outcome_model_ece"] = _metric(
            "resolved_outcome_model_ece", None, None
        )
        metrics["resolved_outcome_model_brier"] = _metric(
            "resolved_outcome_model_brier", None, None
        )
    elif len(predictions) < 20:
        metrics["resolved_outcome_model_ece"] = _metric(
            "resolved_outcome_model_ece", None, len(predictions) or None,
            status_override="insufficient_population",
        )
        metrics["resolved_outcome_model_brier"] = _metric(
            "resolved_outcome_model_brier", None, len(predictions) or None,
            status_override="insufficient_population",
        )
    else:
        ece = _ece(predictions)
        brier = sum((float(row["confidence"]) - float(row["label"])) ** 2 for row in predictions)
        ids = [str(row.get("source_id") or "") for row in predictions]
        metrics["resolved_outcome_model_ece"] = _metric(
            "resolved_outcome_model_ece", ece, 1, source_ids=ids,
        )
        metrics["resolved_outcome_model_brier"] = _metric(
            "resolved_outcome_model_brier", brier, len(predictions), source_ids=ids,
        )

    claim_rows = _record_index(raw_execution, "claims")
    relation_rows = _record_index(raw_execution, "relations")
    gold_roles = {item.signal_id: item.role for item in sealed_population.gold}
    noise = [
        {"source_id": str(row.get("id") or ""), "kind": "model"}
        for row in claim_rows
        if row.get("evidence_signal_ids")
        and all(
            gold_roles.get(str(source)) in {"noise", "high_similarity_distractor"}
            for source in row.get("evidence_signal_ids") or ()
        )
    ]
    claim_by_id = {str(row.get("id")): row for row in claim_rows}
    noise.extend({
        "source_id": str(row.get("id") or ""), "kind": "relation",
    } for row in relation_rows if row.get("participants") and all(
        participant_claim
        and participant_claim.get("evidence_signal_ids")
        and all(
            gold_roles.get(str(source)) in {"noise", "high_similarity_distractor"}
            for source in participant_claim.get("evidence_signal_ids") or ()
        )
        for participant in row.get("participants") or ()
        for participant_claim in [claim_by_id.get(str(participant.get("claim_id")))]
    ))
    metrics["false_model_relation_from_noise"] = _metric(
        "false_model_relation_from_noise",
        len(noise) if _evidence_has(raw_execution, "claims")
        and _evidence_has(raw_execution, "relations") else None,
        1 if _evidence_has(raw_execution, "claims")
        and _evidence_has(raw_execution, "relations") else None,
        source_ids=[str(row.get("source_id") or "") for row in noise],
        worst_cases=noise[:10],
    )
    credits = _record_index(raw_execution, "causal_credits")
    credit_keys = {
        (
            canonical_sha256(row.get("evidence_lineage") or ()),
            str(row.get("decision_id") or ""),
            str(row.get("outcome_object_kind") or ""),
            str(row.get("outcome_object_id") or ""),
        ) for row in credits
    }
    duplicate_credits = len(credits) - len(credit_keys)
    metrics["duplicate_causal_credit_fanout"] = _metric(
        "duplicate_causal_credit_fanout",
        max(0, duplicate_credits) if _evidence_has(raw_execution, "causal_credits") else None,
        1 if _evidence_has(raw_execution, "causal_credits") else None,
        source_ids=[str(row.get("id") or "") for row in credits],
    )
    elapsed = [
        float((wave.get("execution") or {}).get("elapsed_s"))
        for wave in waves
        if wave.get("status") == "success"
        and (wave.get("execution") or {}).get("elapsed_s") is not None
    ]
    batch_source_ids = [
        f"batch:{wave.get('batch_number')}" for wave in waves
        if wave.get("status") == "success"
        and (wave.get("execution") or {}).get("elapsed_s") is not None
    ]
    complete_latency = len(elapsed) == P6_BATCH_COUNT
    metrics["clean_t1_p95_seconds"] = _metric(
        "clean_t1_p95_seconds", _percentile(elapsed, 0.95) if complete_latency else None,
        1 if complete_latency else None, source_ids=batch_source_ids,
    )
    ratio = (
        max(elapsed) / median(elapsed)
        if complete_latency and median(elapsed) > 0 else None
    )
    metrics["clean_max_over_median"] = _metric(
        "clean_max_over_median", ratio, 1 if ratio is not None else None,
        source_ids=batch_source_ids,
    )
    receipts = list(raw_execution.get("llm_attempt_receipts") or ())
    successful_run_ids = {
        str(run_id) for wave in waves if wave.get("status") == "success"
        if (run_id := ((wave.get("execution") or {}).get("run") or {}).get("id"))
    }
    receipt_run_ids = {
        str(row.get("think_run_id")) for row in receipts if row.get("think_run_id")
    }
    receipt_identity_complete = (
        len(successful_run_ids) == P6_BATCH_COUNT
        and successful_run_ids <= receipt_run_ids
        and bool(receipts)
        and all(
            row.get("physical_attempt_id") and row.get("think_run_id")
            for row in receipts
        )
    )
    exact_usage_complete = receipt_identity_complete and all(
        row.get("usage_exactness") == "exact"
        and row.get("input_tokens") is not None
        and row.get("output_tokens") is not None
        for row in receipts
    )
    metrics["metered_llm_calls_per_signal"] = _metric(
        "metered_llm_calls_per_signal",
        len(receipts) if receipt_identity_complete else None,
        P6_SIGNAL_COUNT if receipt_identity_complete else None,
        source_ids=[str(row.get("physical_attempt_id") or "") for row in receipts],
    )
    planning_batches = {
        str(row.get("think_run_id")) for row in receipts
        if "question" in str(row.get("purpose") or "").casefold()
    }
    metrics["question_planning_batch_share"] = _metric(
        "question_planning_batch_share",
        len(planning_batches) if receipt_identity_complete else None,
        P6_BATCH_COUNT if receipt_identity_complete else None,
        source_ids=sorted(planning_batches),
    )
    complete_barrier_rows = [row for row in barriers if isinstance(row, dict)]
    pending = sum(
        int(row.get("truth_critical_pending_count") or 0)
        for row in complete_barrier_rows
    )
    metrics["truth_critical_pending_at_barriers"] = _metric(
        "truth_critical_pending_at_barriers",
        pending if len(complete_barrier_rows) == P6_BATCH_COUNT else None,
        len(complete_barrier_rows) if len(complete_barrier_rows) == P6_BATCH_COUNT else None,
        source_ids=[str(row.get("barrier_id") or "") for row in complete_barrier_rows],
    )
    refresh = _record_index(raw_execution, "refresh_events")
    refresh_keys = {str(row.get("refresh_key") or "") for row in refresh}
    metrics["refresh_key_duplicate_processing_ratio"] = _metric(
        "refresh_key_duplicate_processing_ratio",
        len(refresh) if _evidence_has(raw_execution, "refresh_events") else None,
        (len(refresh_keys) or None) if _evidence_has(raw_execution, "refresh_events") else None,
        source_ids=list(refresh_keys),
    )

    # Every declared endpoint must exist even when the frozen artifact did not
    # preserve enough member-level evidence to measure it.
    for name in _SPECS:
        metrics.setdefault(name, _metric(name, None, None))

    reviews = list(evidence.get("active_reviews") or ())
    high_consequence = reviews
    wrappers = [
        row for row in claim_rows
        if not row.get("evidence_signal_ids")
        or str((row.get("proposition") or {}).get("kind") or "").casefold()
        in {"wrapper", "control", "batch", "episode"}
    ]
    candidates = list(evidence.get("active_candidates") or ())
    participant_sets = [
        {
            str(item.get("claim_id")) for item in row.get("participants") or ()
        } for row in relation_rows
    ]
    invalid_relations = [
        row for row, participants in zip(relation_rows, participant_sets)
        if len(participants) < 2
    ]
    instruction_pattern = re.compile(
        r"\b(?:confirms?|falsif(?:y|ies)|update\s+memory)\b", re.IGNORECASE
    )
    input_instruction_incidents = [
        signal.signal_id for signal in sealed_population.signals
        if instruction_pattern.search(signal.text)
    ]
    evidence_digest = evidence.get("source_digest")
    evidence_without_digest = {
        key: value for key, value in evidence.items() if key != "source_digest"
    }
    evidence_digest_valid = bool(evidence_digest) and evidence_digest == canonical_sha256(
        evidence_without_digest
    ) and bool(evidence.get("query_receipts"))
    hard_gates = {
        "immutable_inputs_match": digest_match,
        "complete_execution": raw_execution.get("complete") is True,
        "exact_300_signals_12_batches": (
            exact_batches and exact_batch_members and signal_count == P6_SIGNAL_COUNT
        ),
        "complete_signal_fates": exact_fates,
        "complete_boundary_mention_mutation_fates": bool(fates) and all(
            row.get("boundary_fate") and row.get("mention_fate")
            and row.get("mutation_fate") for row in fates
        ),
        "high_consequence_incidents_zero": _evidence_has(
            raw_execution, "active_reviews"
        ) and not high_consequence,
        "wrapper_control_models_zero": _evidence_has(
            raw_execution, "claims"
        ) and not wrappers,
        "active_candidate_review_leakage_zero": _evidence_has(
            raw_execution, "active_candidates"
        ) and _evidence_has(raw_execution, "active_reviews")
        and not candidates and not reviews,
        "invalid_relations_zero": _evidence_has(
            raw_execution, "relations"
        ) and not invalid_relations,
        "external_outcome_instruction_leakage_zero": not input_instruction_incidents,
        "one_coherent_synthesis_per_thesis": (
            metrics["direct_thesis_accuracy"]["value"] == 1.0
        ),
        "all_truth_critical_barriers_close": barrier_ok,
        "single_commit_provider_configuration": bool(
            (raw_execution.get("run_provenance") or {}).get("git_commit")
            and (raw_execution.get("run_provenance") or {}).get("worktree_clean") is True
            and (raw_execution.get("expected_llm_configuration") or {}).get("provider")
            and (raw_execution.get("expected_llm_configuration") or {}).get("model")
            and raw_execution.get("mixed_llm_attempt_count") == 0
        ),
        "postfreeze_evidence_digest_valid": evidence_digest_valid,
        "durable_call_receipts": receipt_identity_complete and all(
            row.get("physical_attempt_id") and row.get("think_run_id")
            and row.get("provider")
            == (raw_execution.get("expected_llm_configuration") or {}).get("provider")
            and row.get("model")
            == (raw_execution.get("expected_llm_configuration") or {}).get("model")
            for row in receipts
        ),
        "exact_token_usage_receipts": exact_usage_complete,
        "all_hg_gates": bool(evidence.get("hg_gates"))
        and all(bool(value) for value in evidence.get("hg_gates", {}).values()),
    }
    calibration_names = {
        "resolved_outcome_model_ece", "resolved_outcome_model_brier"
    }
    metric_pass = all(
        row["status"] == "pass"
        for name, row in metrics.items() if name not in calibration_names
    ) and all(
        metrics[name]["status"] in {"pass", "insufficient_population"}
        for name in calibration_names
    )
    payload = {
        "schema_version": "epistemic-repair-p6-postfreeze-score-v1",
        "input_digests": {
            "raw_execution": raw_digest,
            "sealed_population": population_digest,
            "preregistration": sealed_population.preregistration_digest,
        },
        "source_population": {
            "signals": len(sealed_population.signals),
            "batches": len(sealed_population.batches),
        },
        "hard_gates": hard_gates,
        "continuous_metrics": metrics,
        "missing_evidence": sorted(
            name for name, row in metrics.items() if row["status"] == "unmeasured"
        ),
        "phase_exit_ready": all(hard_gates.values()) and metric_pass,
        "proof_boundary": (
            "Gold is joined only after the production artifact is frozen.",
            "Unpreserved member-level evidence is unmeasured and fails closed.",
            "Source IDs and worst cases are retained for every measured endpoint.",
        ),
    }
    return {**payload, "content_digest": canonical_sha256(payload)}


__all__ = ["score_p6_frozen_execution"]
