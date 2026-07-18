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
from typing import Any, Mapping

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
    "uncertainty_fate_precision": (">=", 0.95),
    "uncertainty_fate_coverage": (">=", 0.95),
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
    executed_source_ids = _executed_source_signal_ids(raw, population)
    gold_rows = [
        item for item in population.gold
        if executed_source_ids is None or item.signal_id in executed_source_ids
    ]
    required: dict[str, list[tuple[str, tuple[str, ...], str | None]]] = {}
    optional: dict[str, list[tuple[str, tuple[str, ...]]]] = {}
    for item in gold_rows:
        if item.entity_surface:
            required.setdefault(item.signal_id, []).append((
                item.entity_surface,
                (str(item.entity_type),) if item.entity_type else (),
                item.canonical_ref,
            ))
        for mention in item.local_mentions:
            target = required if mention.required else optional
            payload = (
                (mention.surface, mention.entity_types, None)
                if mention.required else (mention.surface, mention.entity_types)
            )
            target.setdefault(item.signal_id, []).append(payload)
    if not rows:
        return {
            name: _metric(name, None, None)
            for name in (
                "exact_mention_f1", "entity_type_accuracy",
                "canonical_link_precision", "canonical_link_recall",
            )
        }
    exact = type_ok = precision_link_ok = recall_link_ok = 0
    predicted_links = 0
    seen_expected: set[tuple[str, str]] = set()
    required_count = sum(len(items) for items in required.values())
    storyline_required_count = sum(
        1 for items in required.values() for _surface, _types, ref in items if ref
    )
    storyline_refs = {
        str(item.canonical_ref) for item in population.gold if item.canonical_ref
    }


    scored_prediction_count = 0
    worst = []
    signal_text = {signal.signal_id: signal.text for signal in population.signals}
    for row in rows:
        signal_id = str(row.get("signal_id") or "")
        surface = str(row.get("surface") or "")
        start = signal_text.get(signal_id, "").find(surface)
        exact_span = (
            start >= 0 and row.get("span_start") == start
            and row.get("span_end") == start + len(surface)
        )
        required_match = next((
            item for item in required.get(signal_id, ())
            if item[0] == surface and exact_span
            and (signal_id, surface) not in seen_expected
        ), None)
        optional_match = next((
            item for item in optional.get(signal_id, ())
            if item[0] == surface and exact_span
        ), None)
        if required_match is None and optional_match is None:
            scored_prediction_count += 1
            worst.append({"source_id": signal_id, "reason": "unexpected_mention"})
        elif required_match is not None:
            scored_prediction_count += 1
            exact += 1
            seen_expected.add((signal_id, surface))
            allowed_types = required_match[1]
            type_ok += int(row.get("entity_type") in allowed_types)
        canonical_ref = row.get("canonical_ref")
        if canonical_ref is not None:
            predicted_links += 1
            if required_match is not None and required_match[2] is not None:
                correct = canonical_ref == required_match[2]
                precision_link_ok += int(correct)
                recall_link_ok += int(correct)
            elif (
                (required_match is not None or optional_match is not None)
                and canonical_ref not in storyline_refs
            ):
                # Local entities may remain unresolved or gain their own local
                # identity, but must never alias a sealed storyline entity.
                precision_link_ok += 1
            else:
                worst.append({
                    "source_id": signal_id,
                    "reason": "local_entity_linked_to_storyline",
                })
    _, _, mention_f1 = _f1(exact, scored_prediction_count, required_count)
    ids = sorted(required)
    storyline_ids = sorted(
        signal_id for signal_id, items in required.items()
        if any(ref is not None for _surface, _types, ref in items)
    )
    return {
        "exact_mention_f1": _metric(
            "exact_mention_f1",
            mention_f1 if required_count else None,
            1 if required_count else None,
            source_ids=ids, worst_cases=worst,
        ),
        "entity_type_accuracy": _metric(
            "entity_type_accuracy",
            type_ok if required_count else None,
            exact if required_count else None,
            source_ids=ids,
        ),
        "canonical_link_precision": _metric(
            "canonical_link_precision", precision_link_ok, predicted_links,
            source_ids=storyline_ids, worst_cases=worst,
        ),
        "canonical_link_recall": _metric(
            "canonical_link_recall", recall_link_ok, storyline_required_count,
            source_ids=storyline_ids,
        ),
    }


def _score_uncertainty_fates(
    raw: dict[str, Any], population: P6Population,
) -> dict[str, dict[str, Any]]:
    """Score nonassertable weak signals without rewarding truth promotion."""

    executed = _executed_source_signal_ids(raw, population)
    expected = {
        item.signal_id for item in population.gold
        if item.lifecycle_phase == "weak_initial"
        and (coordinate := _claim_coordinate(item)) is not None
        and coordinate[2] in {2, 4}
        and (executed is None or item.signal_id in executed)
    }
    if not expected:
        return {
            name: _metric(name, None, None)
            for name in ("uncertainty_fate_precision", "uncertainty_fate_coverage")
        }
    fate_by_signal = {
        str(row.get("signal_id") or ""): row
        for row in _record_index(raw, "signal_fates")
    }
    open_question_sources: set[str] = set()
    for row in _record_index(raw, "open_questions"):
        open_question_sources.update(
            str(value) for value in (
                row.get("source_signal_id"), row.get("signal_id")
            ) if value
        )
        open_question_sources.update(
            str(value) for value in row.get("evidence_signal_ids") or ()
        )
    canonical_sources: set[str] = set()
    for row in _record_index(raw, "claims"):
        proposition = row.get("proposition")
        distinct_uncertainty_contract = bool(
            row.get("uncertainty_existence_contract") is True
            or isinstance(proposition, Mapping)
            and proposition.get("uncertainty_existence_contract") is True
        )
        if not distinct_uncertainty_contract:
            canonical_sources.update(
                expected & {
                    str(value) for value in row.get("evidence_signal_ids") or ()
                }
            )
    acceptable_fates = {
        "open_question", "residual", "clarification", "candidate",
        "needs_clarification",
    }
    correct: set[str] = set()
    incorrect: set[str] = set()
    worst: list[dict[str, Any]] = []
    for signal_id in sorted(expected):
        fate = fate_by_signal.get(signal_id) or {}
        mutation_fate = str(fate.get("mutation_fate") or "").casefold()
        justified_noop = mutation_fate in {"no_mutation", "no_op"} and any(
            str(fate.get(key) or "").strip()
            for key in ("mutation_reason", "no_op_reason", "justification", "reason")
        )
        acceptable = (
            signal_id in open_question_sources
            or mutation_fate in acceptable_fates
            or justified_noop
        )
        if signal_id in canonical_sources:
            incorrect.add(signal_id)
            worst.append({
                "source_id": signal_id,
                "reason": "nonassertable_uncertainty_promoted_to_canonical_claim",
            })
        elif acceptable:
            correct.add(signal_id)
        else:
            worst.append({
                "source_id": signal_id,
                "reason": "uncertainty_has_no_explicit_candidate_or_justified_noop_fate",
            })
    disposition_count = len(correct | incorrect)
    return {
        "uncertainty_fate_precision": _metric(
            "uncertainty_fate_precision", len(correct), disposition_count or None,
            source_ids=list(expected), worst_cases=worst[:12],
        ),
        "uncertainty_fate_coverage": _metric(
            "uncertainty_fate_coverage", len(correct), len(expected),
            source_ids=list(expected), worst_cases=worst[:12],
        ),
    }


def _score_context(raw: dict[str, Any], population: P6Population) -> dict[str, dict[str, Any]]:
    rows = _record_index(raw, "context_items")
    evidence = raw.get("postfreeze_evidence") or {}
    opportunities_complete = evidence.get("context_opportunities_complete") is True
    coordinates_complete = bool(rows) and all(
        row.get("think_run_id") and isinstance(row.get("input_signal_ids"), list)
        and isinstance(row.get("source_signal_ids"), list)
        and isinstance(row.get("output_evidence_signal_ids"), list)
        for row in rows
    )
    if not opportunities_complete or not coordinates_complete:
        return {name: _metric(name, None, None) for name in (
            "selected_context_contamination", "sufficient_context_recall",
            "historical_reopening_reason_coverage", "mature_actual_model_use_share",
            "mature_unnecessary_historical_observation_use", "selected_context_utilization",
        )}
    gold = {item.signal_id: item for item in population.gold}
    signals = {item.signal_id: item for item in population.signals}

    def source_ids(row: dict[str, Any]) -> set[str]:
        return {str(value) for value in row.get("source_signal_ids") or ()}

    # Current-batch observations are batch inputs, not retrieval choices. They
    # never become 25 target-specific context decisions and are excluded from
    # the selected-retrieval denominator.
    selected = [
        row for row in rows if row.get("selected")
        and not (source_ids(row) & {
            str(value) for value in row.get("input_signal_ids") or ()
        })
    ]
    contamination = 0
    historical = historical_reasoned = 0
    mature = [row for row in selected if int(row.get("batch_number") or 0) >= 11]
    mature_models = mature_unnecessary = 0
    utilized = 0
    worst = []
    output_needs: set[tuple[str, str]] = set()
    selected_coverage: set[tuple[str, str]] = set()
    for row in selected:
        sources = [gold[value] for value in source_ids(row) if value in gold]
        relevant_sources = [
            item for item in sources
            if item.role not in {"noise", "high_similarity_distractor"}
        ]
        if not sources or not relevant_sources:
            contamination += 1
            worst.append({
                "source_id": str(row.get("context_item_id") or ""),
                "reason": "unbound_or_noise_only_retrieval_context",
            })
        run_id = str(row["think_run_id"])
        selected_coverage.update(
            (run_id, str(item.storyline_id)) for item in relevant_sources
            if item.storyline_id
        )
        output_ids = {
            str(value) for value in row.get("output_evidence_signal_ids") or ()
        }
        output_needs.update(
            (run_id, str(item.storyline_id))
            for value in output_ids
            if (item := gold.get(value)) is not None and item.storyline_id
        )
        source_batches = {
            signals[value].batch_number for value in source_ids(row) if value in signals
        }
        batch_number = int(row.get("batch_number") or 0)
        is_historical = bool(source_batches and min(source_batches) < batch_number)
        if is_historical:
            historical += 1
            historical_reasoned += int(bool(row.get("historical_reopen_reason")))
        utilized += int(bool(row.get("referenced") or source_ids(row) & output_ids))
    # Output needs also occur on current-input decision rows, which are not in
    # the retrieval denominator but carry the same run/result lineage. Current
    # inputs can satisfy a claim's evidence need without becoming retrieval.
    for row in rows:
        run_id = str(row.get("think_run_id") or "")
        if row.get("selected"):
            selected_coverage.update(
                (run_id, str(item.storyline_id))
                for value in source_ids(row)
                if (item := gold.get(value)) is not None
                and item.storyline_id
                and item.role not in {"noise", "high_similarity_distractor"}
            )
        for value in row.get("output_evidence_signal_ids") or ():
            item = gold.get(str(value))
            if run_id and item is not None and item.storyline_id:
                output_needs.add((run_id, str(item.storyline_id)))
    for row in mature:
        mature_models += int(row.get("context_item_kind") == "model")
        mature_unnecessary += int(
            row.get("context_item_kind") == "observation"
            and any(
                signals[value].batch_number <= 10
                for value in source_ids(row) if value in signals
            )
            and not row.get("necessary_background")
        )
    ids = [
        str(row.get("context_item_id") or row.get("decision_id") or "")
        for row in rows
    ]
    sufficient_selected = len(output_needs & selected_coverage)
    sufficient_total = len(output_needs)
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
    executed_source_ids = _executed_source_signal_ids(raw, population)
    gold = {
        item.signal_id: (
            item.storyline_id if item.storyline_id is not None else item.signal_id
        ) for item in population.gold
        if executed_source_ids is None or item.signal_id in executed_source_ids
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


_WEAK_INITIAL_OBJECTS = {
    "atlas": {
        1: (("certificate",), ("owner", "ownership")),
        3: (("dashboard",), ("record", "incomplete", "optimistic")),
        5: (("rollout", "window"), ("moved", "delay", "slip")),
    },
    "beacon": {
        1: (("access review", "access"), ("owner", "ownership")),
        3: (("dashboard",), ("record", "incomplete", "optimistic")),
        5: (("completion",), ("moved", "delay", "slip")),
    },
    "cobalt": {
        1: (("customer approval", "approval email"), ("owner", "ownership")),
        3: (("crm",), ("record", "incomplete", "optimistic")),
        5: (("renewal", "signature"), ("moved", "delay", "slip")),
    },
    "delta": {
        1: (("support owner", "support"), ("owner", "ownership")),
        3: (("checklist",), ("record", "incomplete", "optimistic")),
        5: (("incident", "rate"), ("moved", "delay", "slip")),
    },
}
_ABSENCE_TERMS = (
    "no owner", "no clearly recorded", "missing", "unresolved", "unclear",
    "lacks", "lacking", "unowned",
)
_RELATION_CLAIM_RE = re.compile(
    r"\b(?:cause|causes|caused|causing|lead|leads|depends?|requires?|"
    r"blocked by|predicts?|indicator|due to|resulting in)\b"
)
_ATOMIC_STOPWORDS = {
    "after", "again", "before", "from", "into", "remains", "signal",
    "still", "that", "their", "there", "these", "this", "underlying",
    "update", "while", "with",
}


def _claim_coordinate(item: Any) -> tuple[str, str, int] | None:
    parts = str(item.claim_id or "").split(":")
    if len(parts) != 3:
        return None
    try:
        return parts[0], parts[1], int(parts[2])
    except ValueError:
        return None


def _directly_assertable_source(item: Any) -> bool:
    if item.role in {"noise", "high_similarity_distractor"} or not item.claim_id:
        return False
    coordinate = _claim_coordinate(item)
    if coordinate is None:
        return False
    _storyline, phase, ordinal = coordinate
    # Questions and unresolved pronoun references are valid candidate-plane
    # coordinates, but are not accepted-fact atomic recall obligations.
    return not (phase == "weak_initial" and ordinal in {2, 4})


def _row_semantic_text(row: dict[str, Any]) -> str:
    return f"{row.get('natural_text') or ''} {row.get('proposition') or ''}".casefold()


def _material_words(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", text.casefold())
        if len(token) >= 4 and token not in _ATOMIC_STOPWORDS
    }


def _source_signal_entails_atomic(
    row: dict[str, Any],
    *,
    signal_text: str,
    gold_item: Any,
) -> bool:
    if not _directly_assertable_source(gold_item):
        return False
    model_text = _row_semantic_text(row)
    source_text = signal_text.casefold()
    subject = str(gold_item.entity_surface or "").casefold()
    if subject and subject not in model_text and subject.split()[0] not in model_text:
        return False
    # A source-level atomic cannot introduce a causal/dependency/predictive
    # relation absent from the cited source sentence.
    if _RELATION_CLAIM_RE.search(model_text) and not _RELATION_CLAIM_RE.search(source_text):
        return False
    coordinate = _claim_coordinate(gold_item)
    if coordinate is None:
        return False
    storyline, phase, ordinal = coordinate
    if phase == "weak_initial":
        groups = _WEAK_INITIAL_OBJECTS.get(storyline, {}).get(ordinal)
        if groups is None:
            return False
        if ordinal == 1 and not any(term in model_text for term in _ABSENCE_TERMS):
            return False
        return all(any(term in model_text for term in group) for group in groups)
    # Later-phase source claims remain independently scored. Require the Model
    # to retain the storyline subject and at least two material source facets.
    return len(_material_words(model_text) & _material_words(source_text)) >= 2


def _executed_source_signal_ids(
    raw: dict[str, Any], population: P6Population,
) -> set[str] | None:
    """Return the sealed source coordinates actually executed by this run.

    Post-freeze extraction records database observation UUIDs, so translate
    those through its immutable observation-to-signal map.  Older artifacts do
    not carry that receipt; for them, successful wave numbers are an exact
    fallback.  ``None`` deliberately preserves the historical full-population
    denominator when neither proof is present.
    """

    evidence = raw.get("postfreeze_evidence") or {}
    observed = {str(value) for value in evidence.get("observed_source_ids") or ()}
    observation_map = evidence.get("observation_signal_map") or {}
    if observed and isinstance(observation_map, dict):
        translated = {
            str(observation_map[value])
            for value in observed
            if value in observation_map and observation_map[value]
        }
        if translated:
            return translated

    completed_batches = {
        int(wave.get("batch_number"))
        for wave in raw.get("waves") or ()
        if wave.get("status") == "success" and wave.get("batch_number") is not None
    }
    if completed_batches:
        return {
            signal.signal_id for signal in population.signals
            if signal.batch_number in completed_batches
        }
    return None


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
    signals = {item.signal_id: item for item in population.signals}
    executed_source_ids = _executed_source_signal_ids(raw, population)
    expected_claims = {
        item.claim_id for item in population.gold
        if _directly_assertable_source(item)
        and (executed_source_ids is None or item.signal_id in executed_source_ids)
    }
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
        atomic_semantic = False
        thesis_semantic = False
        if pure and storyline:
            claim_oracle = next(
                item for item in oracle.claims if item.storyline_id == storyline
            )
            thesis_semantic = entails_structured_claim(row, claim_oracle)
            atomic_semantic = all(
                signal_id in signals
                and _source_signal_entails_atomic(
                    row,
                    signal_text=signals[signal_id].text,
                    gold_item=gold[signal_id],
                )
                for signal_id in evidence_ids
            )
        if pure and atomic_semantic and storyline:
            precise_claims += 1
            represented_claims.update(
                item.claim_id
                for item in evidence
                if _directly_assertable_source(item)
            )
        else:
            worst.append({
                "source_id": str(row.get("id") or ""),
                "reason": "impure_lineage_or_atomic_source_nonentailment",
            })
        if pure and thesis_semantic and storyline:
            coherent_model_phases[storyline].append({
                item.lifecycle_phase for item in evidence
            })
        predicted_scope_refs.update(_scope_refs(row.get("scope_entities")))
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
    extracted_scope_coordinates_complete = (
        (raw.get("postfreeze_evidence") or {}).get(
            "extracted_scope_coordinates_complete"
        ) is True
    )
    return {
        "atomic_claim_precision": _metric(
            "atomic_claim_precision", precise_claims, len(rows), source_ids=source_ids,
            worst_cases=worst[:10],
        ),
        "atomic_claim_recall": _metric(
            "atomic_claim_recall",
            len(represented_claims & expected_claims),
            len(expected_claims),
            source_ids=source_ids,
        ),
        "atomic_claim_f1": _metric("atomic_claim_f1", f1, 1, source_ids=source_ids),
        "evidence_lineage_coverage": _metric(
            "evidence_lineage_coverage", lineage_ok, len(rows), source_ids=source_ids,
        ),
        "scope_precision": _metric(
            "scope_precision",
            scope_tp if extracted_scope_coordinates_complete else None,
            scope_predicted if extracted_scope_coordinates_complete else None,
            source_ids=source_ids,
        ),
        "scope_recall": _metric(
            "scope_recall",
            scope_tp if extracted_scope_coordinates_complete else None,
            len(expected_scope_refs) if extracted_scope_coordinates_complete else None,
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
    metrics.update(_score_uncertainty_fates(raw_execution, sealed_population))
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
        row.get("usage_exactness") == "reported"
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
        or str(
            (row.get("proposition") or {}).get("kind")
            if isinstance(row.get("proposition"), Mapping) else ""
        ).casefold()
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
