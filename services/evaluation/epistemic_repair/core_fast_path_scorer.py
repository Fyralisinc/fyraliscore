"""Post-execution scorer for the sealed 4x25 core fast-path population.

Runtime code supplies plain receipt mappings.  Only this evaluator receives the
sealed gold object, after execution has completed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.core_fast_path_gold import CoreFastPathGold


SCHEMA_VERSION = "core-fast-path-score-v1"
REQUIRED_RUNTIME_FIELDS = (
    "population_digest",
    "execution_id",
    "tenant_id",
    "batches[].batch_number",
    "batches[].input_signal_ids",
    "batches[].processed_signal_ids",
    "batches[].unbatched_signal_count",
    "batches[].groundings[]:{signal_id,canonical_ref,surface,authority}",
    "batches[].atomics[]:{signal_id,observation_id,evidence_bound,tenant_id}",
    "batches[].retrieval:{accepted_model_version_ids,observation_ids}",
    "batches[].accepted_models[]:{model_id,version_id,source_signal_id,"
    "proposition,natural_text,lifecycle,scope_refs,evidence_signal_ids,"
    "supporting_model_version_ids,commit_id,prior_version_id,"
    "supersedes_version_id,history_retained}",
    "batches[].accepted_relations[]:{relation_id,relation_version_id,kind,"
    "lifecycle,participant_model_version_ids,commit_id}",
    "batches[].relation_fates[]:{relation_id,relation_version_id,"
    "prior_relation_version_id,kind,lifecycle,prior_active_head_absent}",
    "batches[].barrier:{snapshot_validated,expected_head_count,"
    "matched_head_count,stale_head_count,missing_head_count}",
    "contamination:{gold_fields_seen,cross_tenant_row_count,oracle_imported}",
    "replay_digests",
)


def _items(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return ()
    return tuple(str(item) for item in value)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return min(1.0, max(0.0, float(numerator) / float(denominator))) if denominator else 0.0


def _f1(correct: int, predicted: int, expected: int) -> float:
    precision = _ratio(correct, predicted)
    recall = _ratio(correct, expected)
    return 2 * precision * recall / (precision + recall) if precision + recall else 0.0


def _batch_map(receipt: Mapping[str, Any]) -> dict[int, Mapping[str, Any]]:
    result: dict[int, Mapping[str, Any]] = {}
    for batch in _items(receipt.get("batches")):
        number = batch.get("batch_number")
        if isinstance(number, int) and number not in result:
            result[number] = batch
    return result


def _exact_synthesis_model(
    models: Sequence[Mapping[str, Any]], *, signal_id: str, thesis: str,
) -> Mapping[str, Any]:
    """Select the unique composite, never an atomic conclusion sibling."""

    matches = [
        row for row in models
        if row.get("source_signal_id") == signal_id
        and row.get("proposition") == thesis
        and row.get("abstraction_level") == "composite"
        and row.get("claim_role") == "situation"
        and len(_strings(row.get("supporting_model_version_ids"))) >= 2
    ]
    return matches[0] if len(matches) == 1 else {}


def _metric(score: float, **details: Any) -> dict[str, Any]:
    return {"score": round(min(1.0, max(0.0, score)), 6), **details}


def score_core_fast_path(
    runtime_receipt: Mapping[str, Any], *, gold: CoreFastPathGold,
) -> dict[str, Any]:
    """Score immutable runtime/database receipts against evaluator-only gold."""
    batches = _batch_map(runtime_receipt)
    gold_by_id = {item.signal_id: item for item in gold.signals}
    expected_ids = set(gold_by_id)
    expected_grounded = {
        item.signal_id: item for item in gold.signals if item.canonical_ref is not None
    }
    all_inputs: list[str] = []
    all_processed: list[str] = []
    all_groundings: list[Mapping[str, Any]] = []
    all_atomics: list[Mapping[str, Any]] = []
    for batch in batches.values():
        all_inputs.extend(_strings(batch.get("input_signal_ids")))
        all_processed.extend(_strings(batch.get("processed_signal_ids")))
        all_groundings.extend(_items(batch.get("groundings")))
        all_atomics.extend(_items(batch.get("atomics")))

    exact_batch_numbers = set(batches) == {1, 2, 3, 4}
    exact_batch_sizes = all(
        len(_strings(batches.get(number, {}).get("input_signal_ids"))) == 25
        for number in range(1, 5)
    )
    inputs_exact = len(all_inputs) == 100 and set(all_inputs) == expected_ids
    processed_exact = (
        len(all_processed) == 100 and set(all_processed) == expected_ids
        and len(all_processed) == len(set(all_processed))
    )
    no_unbatched = all(
        batches.get(number, {}).get("unbatched_signal_count") == 0
        for number in range(1, 5)
    )
    batch_integrity = sum((
        exact_batch_numbers, exact_batch_sizes, inputs_exact,
        processed_exact, no_unbatched,
    )) / 5

    correct_groundings = 0
    predicted_groundings = 0
    seen_grounding_ids: set[str] = set()
    for row in all_groundings:
        signal_id = str(row.get("signal_id", ""))
        if row.get("canonical_ref") is not None:
            predicted_groundings += 1
        expected = expected_grounded.get(signal_id)
        if (
            expected is not None and signal_id not in seen_grounding_ids
            and row.get("canonical_ref") == expected.canonical_ref
            and row.get("surface") == expected.expected_surface
            and row.get("authority") == expected.expected_authority
        ):
            correct_groundings += 1
            seen_grounding_ids.add(signal_id)
    grounding_score = _f1(
        correct_groundings, predicted_groundings, len(expected_grounded),
    )

    atomic_ids: set[str] = set()
    correct_atomics = 0
    tenant_id = str(runtime_receipt.get("tenant_id", ""))
    for row in all_atomics:
        signal_id = str(row.get("signal_id", ""))
        if (
            signal_id in expected_grounded and signal_id not in atomic_ids
            and bool(row.get("observation_id")) and row.get("evidence_bound") is True
            and str(row.get("tenant_id", "")) == tenant_id
        ):
            correct_atomics += 1
            atomic_ids.add(signal_id)
    atomics_score = _f1(correct_atomics, len(all_atomics), len(expected_grounded))

    batch2_retrieval = batches.get(2, {}).get("retrieval", {})
    if not isinstance(batch2_retrieval, Mapping):
        batch2_retrieval = {}
    retrieved_models = _strings(batch2_retrieval.get("accepted_model_version_ids"))
    retrieved_observations = _strings(batch2_retrieval.get("observation_ids"))
    retrieval_total = len(retrieved_models) + len(retrieved_observations)
    retrieval_score = _ratio(len(retrieved_models), max(1, retrieval_total))

    batch3_models = _items(batches.get(3, {}).get("accepted_models"))
    synthesis = _exact_synthesis_model(
        batch3_models,
        signal_id=gold.synthesis_signal_id,
        thesis=gold.expected_thesis,
    )
    synthesis_checks = (
        synthesis.get("proposition") == gold.expected_thesis,
        synthesis.get("lifecycle") == "active",
        gold.expected_scope_ref in _strings(synthesis.get("scope_refs")),
        _strings(synthesis.get("evidence_signal_ids")) == (
            gold.synthesis_signal_id,
        ),
        len(_strings(synthesis.get("supporting_model_version_ids"))) >= 2,
        bool(synthesis.get("model_id")) and bool(synthesis.get("version_id")),
    )
    synthesis_score = sum(bool(value) for value in synthesis_checks) / len(synthesis_checks)

    batch4_models = _items(batches.get(4, {}).get("accepted_models"))
    correction = next(
        (row for row in batch4_models
         if row.get("source_signal_id") == gold.correction_signal_id
         and row.get("abstraction_level") == "composite"
         and row.get("claim_role") == "situation"),
        {},
    )
    correction_checks = (
        bool(correction.get("model_id")) and bool(correction.get("version_id")),
        correction.get("lifecycle") == "active",
        bool(correction.get("prior_version_id")),
        correction.get("history_retained") is True,
        correction.get("supersedes_version_id") == correction.get("prior_version_id")
        and bool(correction.get("prior_version_id")),
        correction.get("proposition") == gold.expected_corrected_thesis,
        correction.get("natural_text") == correction.get("proposition")
        and correction.get("natural_text") == gold.expected_corrected_thesis,
    )
    correction_score = sum(bool(value) for value in correction_checks) / len(correction_checks)

    relations = _items(batches.get(3, {}).get("accepted_relations"))
    relation = next(
        (row for row in relations if row.get("kind") == gold.expected_relation_kind),
        {},
    )
    relation_checks = (
        bool(relation.get("relation_id")) and bool(relation.get("relation_version_id")),
        relation.get("lifecycle") == "active",
        len(set(_strings(relation.get("participant_model_version_ids")))) >= 2,
        bool(synthesis.get("commit_id"))
        and relation.get("commit_id") == synthesis.get("commit_id"),
    )
    relation_score = sum(bool(value) for value in relation_checks) / len(relation_checks)

    relation_fates = _items(batches.get(4, {}).get("relation_fates"))
    retired_relation = next((
        row for row in relation_fates
        if row.get("relation_id") == relation.get("relation_id")
        and row.get("kind") == gold.expected_relation_kind
    ), {})
    relation_retirement_checks = (
        bool(retired_relation.get("relation_version_id")),
        retired_relation.get("prior_relation_version_id")
        == relation.get("relation_version_id"),
        retired_relation.get("lifecycle")
        == gold.expected_post_correction_relation_lifecycle,
        retired_relation.get("prior_active_head_absent") is True,
    )
    relation_retirement_score = sum(
        bool(value) for value in relation_retirement_checks
    ) / len(relation_retirement_checks)

    barrier_checks: list[bool] = []
    for number in range(1, 5):
        barrier = batches.get(number, {}).get("barrier", {})
        if not isinstance(barrier, Mapping):
            barrier = {}
        barrier_checks.append(
            barrier.get("snapshot_validated") is True
            and barrier.get("expected_head_count") == barrier.get("matched_head_count")
            and barrier.get("stale_head_count") == 0
            and barrier.get("missing_head_count") == 0
        )
    barrier_score = sum(barrier_checks) / 4

    contamination = runtime_receipt.get("contamination", {})
    if not isinstance(contamination, Mapping):
        contamination = {}
    contamination_checks = (
        contamination.get("gold_fields_seen") in (0, []),
        contamination.get("cross_tenant_row_count") == 0,
        contamination.get("oracle_imported") is False,
    )
    contamination_score = sum(contamination_checks) / len(contamination_checks)

    replay_digests = _strings(runtime_receipt.get("replay_digests"))
    deterministic = len(replay_digests) >= 2 and len(set(replay_digests)) == 1
    digest_bound = runtime_receipt.get("population_digest") == gold.population_digest

    metrics = {
        "batch_integrity": _metric(batch_integrity, processed=len(all_processed)),
        "grounding": _metric(
            grounding_score, correct=correct_groundings,
            predicted=predicted_groundings, expected=len(expected_grounded),
        ),
        "atomics_evidence": _metric(
            atomics_score, correct=correct_atomics, predicted=len(all_atomics),
            expected=len(expected_grounded),
        ),
        "batch_2_accepted_model_retrieval": _metric(
            retrieval_score, accepted_models=len(retrieved_models),
            observations=len(retrieved_observations),
        ),
        "batch_3_synthesis": _metric(synthesis_score),
        "batch_4_lifecycle_correction_history": _metric(correction_score),
        "relation_atomicity": _metric(relation_score),
        "batch_4_relation_retirement": _metric(relation_retirement_score),
        "barriers": _metric(barrier_score, valid_batches=sum(barrier_checks)),
        "contamination": _metric(contamination_score),
        "determinism": _metric(1.0 if deterministic else 0.0, replay_count=len(replay_digests)),
    }
    gates = {
        "population_digest_bound": digest_bound,
        "batch_integrity": batch_integrity == 1.0,
        "grounding": grounding_score == 1.0,
        "atomics_evidence": atomics_score == 1.0,
        "batch_2_accepted_model_retrieval": bool(retrieved_models),
        "batch_3_synthesis": synthesis_score == 1.0,
        "batch_4_lifecycle_correction_history": correction_score == 1.0,
        "relation_atomicity": relation_score == 1.0,
        "batch_4_relation_retirement": relation_retirement_score == 1.0,
        "barriers": barrier_score == 1.0,
        "contamination": contamination_score == 1.0,
        "determinism": deterministic,
    }
    body = {
        "schema_version": SCHEMA_VERSION,
        "execution_id": str(runtime_receipt.get("execution_id", "")),
        "population_digest": str(runtime_receipt.get("population_digest", "")),
        "gold_digest": gold.gold_digest,
        "metrics": metrics,
        "gates": gates,
        "overall_pass": all(gates.values()),
        "minimum_dimension_score": min(item["score"] for item in metrics.values()),
    }
    return {**body, "artifact_digest": canonical_sha256(body)}


__all__ = ["REQUIRED_RUNTIME_FIELDS", "SCHEMA_VERSION", "score_core_fast_path"]
