"""Strict oracle and artifact contract for the sealed P5 vertical proof."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p5_population import (
    P5_BATCH_COUNT,
    P5_EPISODE_IDS,
    P5_SIGNAL_COUNT,
    P5_SIGNALS_PER_BATCH,
    P5Population,
)


P5_ARTIFACT_SCHEMA_VERSION = "epistemic-repair-p5-vertical-v1"


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class P5SignalReceipt(_FrozenModel):
    signal_id: str
    batch_number: int = Field(ge=1, le=3)
    position: int = Field(ge=1, le=25)
    episode_id: str
    observation_id: str
    sealed_content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    persisted_content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    persisted: bool
    decision_id: str
    route_id: str
    decision_fate: Literal["mutation", "validator_drop"]
    grounding_fate: str | None = None
    source_semantic_disposition: str | None = None


class P5VerticalReceipt(_FrozenModel):
    batch_1_model_id: str
    batch_1_model_version_id: str
    batch_1_atomic: bool
    batch_2_model_id: str
    batch_2_model_version_id: str
    batch_2_prior_retrieved: bool
    batch_2_prior_referenced: bool
    relation_disposition: Literal["accepted", "justified_no_relation"]
    relation_kind: str | None
    relation_id: str | None
    relation_version_id: str | None
    no_relation_reason: str | None
    batch_3_corrected_model_id: str
    batch_3_corrected_model_version_id: str
    batch_3_corrected_retrieved: bool
    batch_3_corrected_referenced: bool
    invalidated_model_id: str
    invalidated_model_version_id: str
    terminal_lifecycle: str
    stale_model_excluded: bool
    stale_relation_excluded: bool
    relation_repair_obligation_count: int = Field(ge=0)

    @model_validator(mode="after")
    def relation_fate_is_coherent(self) -> "P5VerticalReceipt":
        if self.relation_disposition == "accepted":
            if not self.relation_kind or not self.relation_id or not self.relation_version_id:
                raise ValueError("accepted relation must identify its kind and exact version")
            if self.no_relation_reason is not None:
                raise ValueError("accepted relation cannot carry a no-relation reason")
        else:
            if any((self.relation_kind, self.relation_id, self.relation_version_id)):
                raise ValueError("no-relation fate cannot identify canonical relation truth")
            if not self.no_relation_reason:
                raise ValueError("no-relation fate requires an explicit reason")
        return self


class P5BarrierReceipt(_FrozenModel):
    batch_id: str
    barrier_id: str
    barrier_version: int = Field(ge=1)
    expected_model_version_count: int = Field(ge=0)
    expected_relation_version_count: int = Field(ge=0)
    invalidated_model_version_count: int = Field(ge=0)
    truth_critical_pending_count: int = Field(ge=0)
    receipt_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class P5Metric(_FrozenModel):
    metric_id: str
    numerator: float = Field(ge=0.0)
    denominator: int = Field(gt=0)
    value: float = Field(ge=0.0, le=1.0)
    threshold: float = Field(ge=0.0, le=1.0)
    threshold_operator: Literal["=", "<="]
    threshold_met: bool


class P5HardGate(_FrozenModel):
    gate_id: str
    status: Literal["pass", "fail"]
    eligible_count: int = Field(ge=0)
    conforming_count: int = Field(ge=0)
    incident_ids: tuple[str, ...]
    detail: str


class P5Artifact(_FrozenModel):
    schema_version: str
    population_version: str
    population_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_count: int
    signals_per_batch: int
    normalized_signal_count: int
    semantic_episode_ids: tuple[str, ...]
    provider_call_count: int
    zero_seed_initial_model_count: int
    signal_receipts: tuple[P5SignalReceipt, ...]
    vertical_receipt: P5VerticalReceipt
    barrier_receipts: tuple[P5BarrierReceipt, ...]
    continuous_metrics: dict[str, P5Metric]
    hard_gates: dict[str, P5HardGate]
    phase_exit_ready: bool
    missing_evidence: tuple[str, ...]
    database_evidence: dict[str, Any]
    timings_ms: dict[str, float]
    proof_boundary: tuple[str, ...]
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def artifact_is_coherent(self) -> "P5Artifact":
        if canonical_sha256(
            self.model_dump(mode="json", exclude={"content_digest"})
        ) != self.content_digest:
            raise ValueError("P5 artifact content digest does not match its payload")
        expected_exit = (
            not self.missing_evidence
            and all(gate.status == "pass" for gate in self.hard_gates.values())
            and all(metric.threshold_met for metric in self.continuous_metrics.values())
        )
        if self.phase_exit_ready != expected_exit:
            raise ValueError("P5 phase-exit status contradicts its gates or metrics")
        return self


def _metric(
    metric_id: str,
    numerator: float,
    denominator: int,
    *,
    threshold: float = 1.0,
    operator: Literal["=", "<="] = "=",
) -> P5Metric:
    value = numerator / denominator
    threshold_met = value == threshold if operator == "=" else value <= threshold
    return P5Metric(
        metric_id=metric_id,
        numerator=numerator,
        denominator=denominator,
        value=value,
        threshold=threshold,
        threshold_operator=operator,
        threshold_met=threshold_met,
    )


def _gate(gate_id: str, passed: bool, eligible: int, detail: str) -> P5HardGate:
    return P5HardGate(
        gate_id=gate_id,
        status="pass" if passed else "fail",
        eligible_count=eligible,
        conforming_count=eligible if passed else 0,
        incident_ids=() if passed else (gate_id.lower(),),
        detail=detail,
    )


def build_p5_artifact(
    *,
    population: P5Population,
    signals: tuple[P5SignalReceipt, ...],
    vertical: P5VerticalReceipt,
    barriers: tuple[P5BarrierReceipt, ...],
    zero_seed_initial_model_count: int,
    provider_call_count: int,
    database_evidence: dict[str, Any],
    timings_ms: dict[str, float],
) -> P5Artifact:
    expected_by_id = {item.signal_id: item for item in population.signals}
    receipts_by_id = {item.signal_id: item for item in signals}
    if len(receipts_by_id) != P5_SIGNAL_COUNT or set(receipts_by_id) != set(expected_by_id):
        raise ValueError("P5 signal receipts must bind every sealed signal exactly once")
    db_signal_rows = database_evidence.get("signal_rows")
    db_decision_rows = database_evidence.get("decision_rows")
    if not isinstance(db_signal_rows, list) or not isinstance(db_decision_rows, list):
        raise ValueError("P5 database evidence requires signal and decision identity rows")
    persisted_by_id = {str(item.get("signal_id")): item for item in db_signal_rows}
    decisions_by_id = {str(item.get("signal_id")): item for item in db_decision_rows}
    if (
        len(persisted_by_id) != P5_SIGNAL_COUNT
        or len(decisions_by_id) != P5_SIGNAL_COUNT
        or set(persisted_by_id) != set(expected_by_id)
        or set(decisions_by_id) != set(expected_by_id)
    ):
        raise ValueError("P5 database evidence has duplicate, missing, or extra signal identities")
    for signal_id, sealed in expected_by_id.items():
        receipt = receipts_by_id[signal_id]
        persisted = persisted_by_id[signal_id]
        decision = decisions_by_id[signal_id]
        sealed_digest = canonical_sha256(sealed.text)
        if (
            receipt.sealed_content_digest != sealed_digest
            or receipt.persisted_content_digest != sealed_digest
            or persisted.get("content_digest") != sealed_digest
            or receipt.observation_id != persisted.get("observation_id")
            or receipt.decision_id != decision.get("decision_id")
            or receipt.decision_fate != decision.get("decision_fate")
            or receipt.route_id != decision.get("route_id")
            or decision.get("context_item_id")
            not in {receipt.observation_id, vertical.batch_1_model_id,
                    vertical.batch_3_corrected_model_id}
        ):
            raise ValueError(f"P5 signal receipt does not reconcile to DB truth: {signal_id}")
    preflight = database_evidence.get("preflight")
    if not isinstance(preflight, dict) or not {
        "accepted_model_count", "accepted_relation_count"
    } <= set(preflight):
        raise ValueError("P5 database evidence requires queried zero-seed preflight counts")
    if (
        int(preflight["accepted_model_count"]) != zero_seed_initial_model_count
        or int(preflight["accepted_relation_count"]) != 0
    ):
        raise ValueError("P5 zero-seed claim disagrees with DB preflight evidence")
    semantic_rows = database_evidence.get("semantic_rows")
    if not isinstance(semantic_rows, list) or len(semantic_rows) != 3:
        raise ValueError("P5 database evidence requires exactly three semantic rows")
    semantic_by_signal = {str(item.get("signal_id")): item for item in semantic_rows}
    expected_semantic_ids = {
        population.oracle.atomic_signal_id,
        population.oracle.reuse_relation_signal_id,
        population.oracle.correction_signal_id,
    }
    if set(semantic_by_signal) != expected_semantic_ids:
        raise ValueError("P5 semantic DB rows do not match the sealed vertical oracle")
    if any(
        item.get("grounding_fate") != "resolved_for_consumer"
        or item.get("source_semantic_disposition") != "belief_applied"
        for item in semantic_rows
    ):
        raise ValueError("P5 semantic DB rows do not prove admitted grounded reports")
    model_versions = {
        str(item.get("version_id")): item
        for item in database_evidence.get("model_version_rows", [])
    }
    relation_heads = database_evidence.get("relation_head_rows")
    obligations = database_evidence.get("repair_obligation_rows")
    accepted_versions = set(database_evidence.get("accepted_model_version_ids", []))
    if (
        vertical.batch_1_model_version_id not in model_versions
        or vertical.batch_2_model_version_id not in model_versions
        or vertical.batch_3_corrected_model_version_id not in model_versions
        or vertical.batch_1_model_version_id in accepted_versions
        or vertical.batch_2_model_version_id not in accepted_versions
        or vertical.batch_3_corrected_model_version_id not in accepted_versions
        or not isinstance(relation_heads, list)
        or len(relation_heads) != 1
        or relation_heads[0].get("relation_id") != vertical.relation_id
        or relation_heads[0].get("lifecycle") != "disputed"
        or not isinstance(obligations, list)
        or not any(
            item.get("invalidated_model_version_id")
            == vertical.invalidated_model_version_id
            and item.get("affected_id") == vertical.relation_version_id
            for item in obligations
        )
    ):
        raise ValueError("P5 lifecycle/accepted-view evidence does not reconcile")
    batch_cardinality = all(
        sum(item.batch_number == number for item in signals) == P5_SIGNALS_PER_BATCH
        for number in range(1, P5_BATCH_COUNT + 1)
    )
    interleaving = all(
        tuple(
            dict.fromkeys(
                item.episode_id
                for item in sorted(signals, key=lambda row: row.position)
                if item.batch_number == number
            )
        )
        == P5_EPISODE_IDS
        for number in range(1, P5_BATCH_COUNT + 1)
    )
    persistence_count = sum(item.persisted for item in signals)
    explicit_fate_count = sum(
        item.decision_fate in {"mutation", "validator_drop"} for item in signals
    )
    semantic_receipts = tuple(item for item in signals if item.decision_fate == "mutation")
    exact_semantic_shape = (
        len(semantic_receipts) == 3
        and {item.signal_id for item in semantic_receipts} == expected_semantic_ids
        and all(item.episode_id == P5_EPISODE_IDS[0] for item in semantic_receipts)
    )
    relation_ok = (
        vertical.relation_disposition == "accepted"
        and vertical.relation_kind == "dependency_constraint"
        and bool(vertical.relation_version_id)
    ) or (
        vertical.relation_disposition == "justified_no_relation"
        and bool(vertical.no_relation_reason)
    )
    barrier_ok = (
        len(barriers) == 3
        and tuple(item.barrier_version for item in barriers) == (1, 2, 3)
        and all(item.truth_critical_pending_count == 0 for item in barriers)
    )
    metrics = {
        "normalized_signal_persistence": _metric(
            "normalized_signal_persistence", persistence_count, P5_SIGNAL_COUNT
        ),
        "explicit_signal_fate_coverage": _metric(
            "explicit_signal_fate_coverage", explicit_fate_count, P5_SIGNAL_COUNT
        ),
        "batch_cardinality": _metric(
            "batch_cardinality", sum(batch_cardinality for _ in range(3)), 3
        ),
        "semantic_restraint": _metric(
            "semantic_restraint", float(exact_semantic_shape), 1
        ),
        "accepted_model_retrieval_and_reference": _metric(
            "accepted_model_retrieval_and_reference",
            float(vertical.batch_2_prior_retrieved and vertical.batch_2_prior_referenced),
            1,
        ),
        "relation_or_no_relation_correctness": _metric(
            "relation_or_no_relation_correctness", float(relation_ok), 1
        ),
        "exact_model_falsification": _metric(
            "exact_model_falsification",
            float(
                vertical.terminal_lifecycle == "falsified"
                and vertical.invalidated_model_id == vertical.batch_1_model_id
                and vertical.invalidated_model_version_id
                == vertical.batch_1_model_version_id
            ),
            1,
        ),
        "stale_truth_exclusion": _metric(
            "stale_truth_exclusion",
            float(vertical.stale_model_excluded and vertical.stale_relation_excluded),
            1,
        ),
        "corrected_state_reuse": _metric(
            "corrected_state_reuse",
            float(
                vertical.batch_3_corrected_retrieved
                and vertical.batch_3_corrected_referenced
            ),
            1,
        ),
        "barrier_completion": _metric(
            "barrier_completion",
            sum(item.truth_critical_pending_count == 0 for item in barriers),
            3,
        ),
        "provider_independence": _metric(
            "provider_independence", float(provider_call_count == 0), 1
        ),
        "cross_tenant_contamination": _metric(
            "cross_tenant_contamination",
            float(database_evidence.get("cross_tenant_contamination_count", 0)),
            max(1, int(database_evidence.get("accepted_object_count", 1))),
            threshold=0.0,
            operator="<=",
        ),
    }
    gates = {
        "P5-HG-01": _gate(
            "P5-HG-01",
            zero_seed_initial_model_count == 0,
            1,
            "The tenant begins without accepted Models or relations.",
        ),
        "P5-HG-02": _gate(
            "P5-HG-02",
            batch_cardinality and interleaving and persistence_count == P5_SIGNAL_COUNT,
            P5_SIGNAL_COUNT,
            "Exactly three 25-signal batches persist with all three episodes interleaved.",
        ),
        "P5-HG-03": _gate(
            "P5-HG-03",
            exact_semantic_shape and vertical.batch_1_atomic,
            3,
            "Only the central episode creates the three preregistered semantic transitions.",
        ),
        "P5-HG-04": _gate(
            "P5-HG-04",
            vertical.batch_2_prior_retrieved and vertical.batch_2_prior_referenced,
            1,
            "Batch 2 retrieves and references the accepted Batch-1 Model.",
        ),
        "P5-HG-05": _gate(
            "P5-HG-05",
            relation_ok,
            1,
            "Batch 2 admits the typed relation or records an explicit justified no-relation fate.",
        ),
        "P5-HG-06": _gate(
            "P5-HG-06",
            vertical.terminal_lifecycle == "falsified"
            and vertical.invalidated_model_version_id == vertical.batch_1_model_version_id,
            1,
            "Batch 3 falsifies the exact Batch-1 ModelVersion.",
        ),
        "P5-HG-07": _gate(
            "P5-HG-07",
            vertical.stale_model_excluded
            and vertical.stale_relation_excluded
            and vertical.relation_repair_obligation_count > 0,
            1,
            "Stale Model and dependent relation truth are fenced with a durable repair obligation.",
        ),
        "P5-HG-08": _gate(
            "P5-HG-08",
            vertical.batch_3_corrected_retrieved
            and vertical.batch_3_corrected_referenced,
            1,
            "The post-correction decision uses the corrected accepted state.",
        ),
        "P5-HG-09": _gate(
            "P5-HG-09",
            barrier_ok,
            3,
            "All three visibility barriers complete in order with no truth-critical work pending.",
        ),
        "P5-HG-10": _gate(
            "P5-HG-10",
            provider_call_count == 0
            and database_evidence.get("cross_tenant_contamination_count", 0) == 0,
            1,
            "The run is provider-free and tenant-isolated.",
        ),
    }
    missing = tuple(gate.gate_id for gate in gates.values() if gate.status != "pass")
    payload: dict[str, Any] = {
        "schema_version": P5_ARTIFACT_SCHEMA_VERSION,
        "population_version": population.version,
        "population_digest": population.population_digest,
        "batch_count": P5_BATCH_COUNT,
        "signals_per_batch": P5_SIGNALS_PER_BATCH,
        "normalized_signal_count": P5_SIGNAL_COUNT,
        "semantic_episode_ids": P5_EPISODE_IDS,
        "provider_call_count": provider_call_count,
        "zero_seed_initial_model_count": zero_seed_initial_model_count,
        "signal_receipts": signals,
        "vertical_receipt": vertical,
        "barrier_receipts": barriers,
        "continuous_metrics": metrics,
        "hard_gates": gates,
        "phase_exit_ready": not missing
        and all(metric.threshold_met for metric in metrics.values()),
        "missing_evidence": missing,
        "database_evidence": database_evidence,
        "timings_ms": timings_ms,
        "proof_boundary": (
            "Proves one rollback-isolated PostgreSQL execution beginning with normalized persisted signals; ingestion listeners are out of scope.",
            "Proves production grounding, source semantics, truth admission, accepted retrieval, typed relation admission, lifecycle fencing, and learning barriers for this sealed three-episode population.",
            "Does not prove live-provider generalization, connector durability, concurrent worker scheduling, or customer-domain validity.",
        ),
    }
    normalized = P5Artifact.model_construct(
        **payload,
        content_digest="0" * 64,
    ).model_dump(mode="json", exclude={"content_digest"})
    payload["content_digest"] = canonical_sha256(normalized)
    return P5Artifact.model_validate(payload)


__all__ = [
    "P5_ARTIFACT_SCHEMA_VERSION",
    "P5Artifact",
    "P5BarrierReceipt",
    "P5HardGate",
    "P5Metric",
    "P5SignalReceipt",
    "P5VerticalReceipt",
    "build_p5_artifact",
]
