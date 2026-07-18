"""Dedicated CF3-B evaluator for a production-shaped two-wave P6 artifact.

CF3-B asks a narrower question than the long-horizon retrieval-evolution
benchmark: did wave two *materially reason with* an exact Model version that
wave one learned?  Selection, retrieval, and lifecycle bookkeeping alone are
therefore measurements, not proof of learning.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return value
    return ()


def _ids(value: Any) -> set[str]:
    return {str(item) for item in _sequence(value) if str(item)}


def _strings(value: Any) -> set[str]:
    """Collect scalar strings from an op envelope without guessing field names."""

    if isinstance(value, Mapping):
        return set().union(*(_strings(item) for item in value.values()), set())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return set().union(*(_strings(item) for item in value), set())
    return {str(value)} if isinstance(value, str) else set()


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 0.0


def _token_total(receipts: Sequence[Mapping[str, Any]], key: str) -> int:
    return sum(
        value
        for receipt in receipts
        if isinstance((value := receipt.get(key)), int)
        and not isinstance(value, bool)
        and value >= 0
    )


def _run(wave: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(_mapping(wave.get("execution")).get("run"))


def _context_use(wave: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(_mapping(_run(wave).get("ops_applied")).get("context_use"))


def _evidence_backed(model: Mapping[str, Any]) -> bool:
    proposition = _mapping(model.get("proposition"))
    contract = _mapping(proposition.get("evidence_contract"))
    event_ids = _ids(proposition.get("evidence_event_ids"))
    contextual_ids = _ids(_mapping(proposition.get("contextual_frame")).get(
        "observation_ids"
    ))
    supporting_count = contract.get("supporting_event_count")
    return bool(event_ids or contextual_ids) and (
        contract.get("evidence_status") == "evidence_bound"
        or (
            isinstance(supporting_count, int)
            and not isinstance(supporting_count, bool)
            and supporting_count > 0
        )
    )


def _barrier_complete(wave: Mapping[str, Any]) -> bool:
    receipt = _mapping(wave.get("barrier_receipt"))
    pending = receipt.get("truth_critical_pending_count")
    snapshot_pending = _mapping(
        _mapping(_mapping(wave.get("snapshot")).get("pending_work")).get(
            "truth_critical"
        )
    ).get("total")
    # Production artifacts bind completion with a reopened receipt rather than
    # copying the durable row's status string into the artifact.
    return (
        bool(receipt.get("barrier_id"))
        and bool(receipt.get("receipt_digest"))
        and receipt.get("reopened_exactly") is True
        and pending == 0
        and snapshot_pending == 0
    )


def evaluate_cf3b_two_wave(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Score one raw P6 artifact against the complete CF3-B proof contract."""

    waves = [
        _mapping(wave) for wave in _sequence(artifact.get("waves"))
        if isinstance(wave, Mapping)
    ]
    exact_two_waves = len(waves) == 2
    b1 = waves[0] if len(waves) >= 1 else {}
    b2 = waves[1] if len(waves) >= 2 else {}

    successful_waves = sum(
        wave.get("status") == "success" and _run(wave).get("status") == "success"
        for wave in waves
    )
    exactly_two_successful_waves = (
        exact_two_waves
        and successful_waves == 2
        and artifact.get("complete") is True
        and artifact.get("completed_batches") == 2
    )

    b1_models = [
        _mapping(model)
        for model in _sequence(_mapping(b1.get("snapshot")).get("accepted_models"))
        if isinstance(model, Mapping)
    ]
    b1_evidence_backed = [model for model in b1_models if _evidence_backed(model)]
    b1_model_to_version = {
        str(model.get("id")): str(model.get("truth_version_id"))
        for model in b1_models
        if model.get("id") and model.get("truth_version_id")
    }
    b1_model_ids = set(b1_model_to_version)

    b2_ops = _mapping(_run(b2).get("ops_applied"))
    context = _mapping(b2_ops.get("context_use"))
    selected_b1_ids = _ids(context.get("selected_model_ids")) & b1_model_ids
    referenced_b1_ids = _ids(context.get("referenced_model_ids")) & selected_b1_ids
    trace_b1_ids = _ids(context.get("trace_referenced_model_ids")) & selected_b1_ids

    trigger_id = str(_mapping(b2.get("execution")).get("trigger_id") or "")
    durable_rows = [
        _mapping(row)
        for row in _sequence(
            _mapping(b2.get("snapshot")).get("context_decisions")
        )
        if isinstance(row, Mapping)
        and str(row.get("batch_id") or "") == trigger_id
        and row.get("context_item_kind") == "accepted_model"
    ]
    durable_selected_b1 = {
        str(row.get("context_item_id"))
        for row in durable_rows
        if row.get("selected") is True
        and str(row.get("context_item_id")) in selected_b1_ids
    }
    durable_referenced_b1 = {
        str(row.get("context_item_id"))
        for row in durable_rows
        if row.get("referenced") is True
        and str(row.get("context_item_id")) in selected_b1_ids
    }
    lifecycle_referenced_b1_ids = (
        _strings(b2_ops.get("memory_lifecycle_ops")) & selected_b1_ids
    )
    non_lifecycle_op_referenced_b1_ids = set()
    for key, value in b2_ops.items():
        if key not in {"context_use", "memory_lifecycle_ops", "reasoning_trace"}:
            non_lifecycle_op_referenced_b1_ids |= _strings(value) & selected_b1_ids
    lifecycle_only_b1_ids = (
        lifecycle_referenced_b1_ids - non_lifecycle_op_referenced_b1_ids
    )
    materially_used_b1_ids = (
        selected_b1_ids
        & referenced_b1_ids
        & trace_b1_ids
        & durable_selected_b1
        & durable_referenced_b1
    ) - lifecycle_only_b1_ids
    reasoning_decision_used = (
        context.get("reasoning_trace_context_decision_used") is True
    )

    bootstrap = _mapping(artifact.get("founder_identity_bootstrap"))
    manifest_digest = str(
        bootstrap.get("manifest_digest")
        or bootstrap.get("manifest_file_sha256")
        or ""
    )
    alias_count = bootstrap.get("alias_count")
    bootstrap_valid = (
        bootstrap.get("applied_before_enqueue") is True
        and bootstrap.get("semantic_truth_unchanged") is True
        and len(manifest_digest) == 64
        and all(character in "0123456789abcdef" for character in manifest_digest.lower())
        and isinstance(alias_count, int)
        and not isinstance(alias_count, bool)
        and alias_count > 0
        and bootstrap.get("no_behavioral_models_seeded") is True
    )

    expected_llm = _mapping(artifact.get("expected_llm_configuration"))
    expected_provider = str(expected_llm.get("provider") or "")
    expected_model = str(expected_llm.get("model") or "")
    expected_transport = str(expected_llm.get("transport") or "")
    llm_receipts = [
        _mapping(receipt)
        for receipt in _sequence(artifact.get("llm_attempt_receipts"))
        if isinstance(receipt, Mapping)
    ]

    def _reported_usage(receipt: Mapping[str, Any]) -> bool:
        return all(
            isinstance(receipt.get(key), int)
            and not isinstance(receipt.get(key), bool)
            and receipt[key] >= 0
            for key in ("input_tokens", "output_tokens", "cache_tokens")
        ) and receipt.get("usage_exactness") == "reported"

    matching_llm_receipts = [
        receipt for receipt in llm_receipts
        if receipt.get("provider") == expected_provider
        and receipt.get("model") == expected_model
        and _reported_usage(receipt)
        and bool(receipt.get("physical_attempt_id"))
    ]
    provider_evidence_eligible = (
        expected_provider == "codex"
        and expected_transport == "cli"
        and bool(expected_model)
        and bool(llm_receipts)
        and len(matching_llm_receipts) == len(llm_receipts)
        and len({
            str(receipt["physical_attempt_id"]) for receipt in llm_receipts
        }) == len(llm_receipts)
    )
    barriers_complete = exact_two_waves and all(_barrier_complete(wave) for wave in waves)

    gates = {
        "exactly_two_successful_waves": exactly_two_successful_waves,
        "b1_has_evidence_backed_model": len(b1_evidence_backed) >= 1,
        "b2_selects_exact_b1_model_version": bool(
            selected_b1_ids & durable_selected_b1
        ),
        "b2_materially_uses_exact_b1_model_version": (
            bool(materially_used_b1_ids) and reasoning_decision_used
        ),
        "both_barriers_complete_pending_zero": barriers_complete,
        "founder_bootstrap_receipt_valid": bootstrap_valid,
        "provider_evidence_eligible": provider_evidence_eligible,
    }

    measurements = {
        "wave_count": len(waves),
        "successful_wave_count": successful_waves,
        "elapsed_seconds_total": float(artifact.get("elapsed_s") or 0.0),
        "elapsed_seconds_per_wave": [float(wave.get("elapsed_s") or 0.0) for wave in waves],
        "b1_accepted_model_count": len(b1_models),
        "b1_evidence_backed_model_count": len(b1_evidence_backed),
        "b1_evidence_backed_ratio": _ratio(len(b1_evidence_backed), len(b1_models)),
        "b2_selected_b1_model_count": len(selected_b1_ids),
        "b2_selected_b1_version_ids": sorted(
            b1_model_to_version[model_id] for model_id in selected_b1_ids
        ),
        "b2_referenced_b1_model_count": len(referenced_b1_ids),
        "b2_trace_referenced_b1_model_count": len(trace_b1_ids),
        "b2_durable_referenced_b1_model_count": len(durable_referenced_b1),
        "b2_lifecycle_only_referenced_b1_model_count": len(lifecycle_only_b1_ids),
        "b2_materially_used_b1_model_count": len(materially_used_b1_ids),
        "b2_materially_used_b1_version_ids": sorted(
            b1_model_to_version[model_id] for model_id in materially_used_b1_ids
        ),
        "b2_selected_b1_reference_ratio": _ratio(
            len(referenced_b1_ids), len(selected_b1_ids)
        ),
        "b2_selected_b1_trace_reference_ratio": _ratio(
            len(trace_b1_ids), len(selected_b1_ids)
        ),
        "reasoning_trace_context_decision_used": reasoning_decision_used,
        "barriers_complete_count": sum(_barrier_complete(wave) for wave in waves),
        "founder_bootstrap_alias_count": alias_count if isinstance(alias_count, int) else 0,
        "founder_bootstrap_manifest_digest": manifest_digest or None,
        "provider_mode": str(artifact.get("provider_mode") or "unknown"),
        "expected_llm_provider": expected_provider or None,
        "expected_llm_model": expected_model or None,
        "expected_llm_transport": expected_transport or None,
        "llm_attempt_receipt_count": len(llm_receipts),
        "llm_matching_reported_usage_receipt_count": len(matching_llm_receipts),
        "llm_matching_reported_usage_ratio": _ratio(
            len(matching_llm_receipts), len(llm_receipts)
        ),
        "llm_input_tokens_total": _token_total(llm_receipts, "input_tokens"),
        "llm_output_tokens_total": _token_total(llm_receipts, "output_tokens"),
        "llm_cache_tokens_total": _token_total(llm_receipts, "cache_tokens"),
    }
    failed_gates = sorted(name for name, passed in gates.items() if not passed)
    return {
        "schema_version": "cf3b-two-wave-evaluation-v1",
        "proof_claim": "wave_two_materially_reasons_with_exact_wave_one_model_version",
        "measurements": measurements,
        "gates": gates,
        "failed_gates": failed_gates,
        "verdict": "green" if not failed_gates else "red",
    }


__all__ = ["evaluate_cf3b_two_wave"]
