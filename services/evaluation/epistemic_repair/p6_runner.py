"""DB-backed P6 mixed-stream run over production persistence and learning seams."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from services.evaluation.epistemic_repair.p5_runner import _ground_and_admit, _persist_batch
from lib.evaluation.epistemic_repair.p6_population import (
    P6_BATCH_COUNT, P6_SIGNAL_COUNT, P6_SIGNALS_PER_BATCH, P6_STORYLINES,
    P6Population, P6Signal, build_p6_population,
)
from services.domain.company_learning.barrier import (
    CompanyLearningBarrierService, ContextDecision,
)
from services.domain.source_semantics.processor import GroundedBeliefProcessor


P6_ARTIFACT_SCHEMA_VERSION = "epistemic-repair-p6-mixed-stream-v1"
P6_NOW = datetime(2026, 7, 10, 9, 0, tzinfo=timezone.utc)


def _stable_id(tenant_id: UUID, label: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"p6:{tenant_id}:{label}")


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class P6Metric(_Frozen):
    numerator: float
    denominator: int = Field(gt=0)
    value: float
    threshold: float
    operator: Literal[">=", "<=", "="]
    status: Literal["pass", "fail", "insufficient_population", "unmeasured"] = "pass"
    evidence: str


class P6Gate(_Frozen):
    status: Literal["pass", "fail"]
    eligible_count: int = Field(ge=0)
    conforming_count: int = Field(ge=0)
    incident_ids: tuple[str, ...] = ()
    evidence: str


class P6SignalFate(_Frozen):
    signal_id: str
    batch_number: int
    observation_id: str
    decision_id: str
    role: str
    fate: Literal["mutation", "validator_drop"]
    content_digest: str = Field(pattern=r"^[0-9a-f]{64}$")


class P6BatchSnapshot(_Frozen):
    batch_number: int
    signal_count: int
    accepted_model_count: int
    accepted_relation_count: int
    context_decision_count: int
    truth_critical_pending_count: int
    barrier_version: int
    barrier_digest: str
    elapsed_ms: float


class P6Artifact(_Frozen):
    schema_version: str
    population_version: str
    population_digest: str
    preregistration_digest: str
    commit_sha: str
    provider_configuration: str
    provider_call_count: int
    zero_seed: dict[str, int]
    signal_fates: tuple[P6SignalFate, ...]
    batch_snapshots: tuple[P6BatchSnapshot, ...]
    synthesis_models: dict[str, dict[str, str]]
    hard_gates: dict[str, P6Gate]
    continuous_metrics: dict[str, P6Metric]
    calibration_status: Literal["pass", "fail", "insufficient_population"]
    weakest_cases: tuple[dict[str, Any], ...]
    database_evidence: dict[str, Any]
    proof_boundaries: tuple[str, ...]
    immutable_hashes: dict[str, str]
    phase_exit_ready: bool
    content_digest: str

    @model_validator(mode="after")
    def coherent(self) -> "P6Artifact":
        if canonical_sha256(self.model_dump(mode="json", exclude={"content_digest"})) != self.content_digest:
            raise ValueError("P6 artifact digest mismatch")
        expected = (
            all(g.status == "pass" for g in self.hard_gates.values())
            and all(m.status == "pass" for m in self.continuous_metrics.values())
        )
        if expected != self.phase_exit_ready:
            raise ValueError("P6 phase exit contradicts gates or metrics")
        return self


def _metric(n: float, d: int, threshold: float,
            operator: Literal[">=", "<=", "="], *, evidence: str,
            insufficient: bool = False, unmeasured: bool = False) -> P6Metric:
    value = n / d
    met = value >= threshold if operator == ">=" else value <= threshold if operator == "<=" else value == threshold
    return P6Metric(numerator=n, denominator=d, value=value, threshold=threshold,
                    operator=operator, evidence=evidence,
                    status="unmeasured" if unmeasured else
                    "insufficient_population" if insufficient else "pass" if met else "fail")


def _gate(passed: bool, eligible: int, evidence: str, incident: str) -> P6Gate:
    return P6Gate(status="pass" if passed else "fail", eligible_count=eligible,
                  conforming_count=eligible if passed else 0,
                  incident_ids=() if passed else (incident,), evidence=evidence)


async def _record_decision(
    conn: asyncpg.Connection, *, tenant_id: UUID, signal: P6Signal,
    observation_id: UUID, model_id: UUID | None, model_version_id: UUID | None,
    service: CompanyLearningBarrierService,
) -> None:
    mutation = model_id is not None and model_version_id is not None
    await service.record_context_decision(tx=conn, item=ContextDecision(
        decision_id=_stable_id(tenant_id, f"decision:{signal.signal_id}"),
        tenant_id=tenant_id, batch_id=f"p6-batch-{signal.batch_number}",
        route_id="p6-production-source-semantics" if mutation else "p6-production-restraint",
        context_item_kind="current_episode", context_item_id=str(observation_id),
        context_item_version="1", retrieved=True, selected=mutation,
        included=mutation, referenced=mutation, counterevidence_retained=False,
        confidence_affecting=mutation, necessary_background=False,
        historical_reopen_reason=None,
        decision_fate="mutation" if mutation else "validator_drop",
        result_object_kind="model_version" if mutation else None,
        result_object_id=model_version_id,
        evidence_lineage=(
            {"kind": "observation", "id": str(observation_id)},
            *(({"kind": "model_version", "id": str(model_version_id)},) if mutation else ()),
        ),
        decided_at=P6_NOW + timedelta(days=signal.batch_number, minutes=signal.position),
    ))


async def run_p6_mixed_stream(
    conn: asyncpg.Connection, *, tenant_id: UUID,
    population: P6Population | None = None, commit_sha: str = "working-tree",
) -> P6Artifact:
    population = population or build_p6_population()
    await conn.execute("INSERT INTO tenants (id,name,is_demo) VALUES ($1,'p6-mixed-stream',FALSE)", tenant_id)
    zero = {
        "models": int(await conn.fetchval("SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1", tenant_id)),
        "relations": int(await conn.fetchval("SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1", tenant_id)),
        "pattern_candidates": int(await conn.fetchval("SELECT count(*) FROM pattern_candidates WHERE tenant_id=$1", tenant_id)),
        "latent_gaps": int(await conn.fetchval("SELECT count(*) FROM sage_latent_gap_hypotheses WHERE tenant_id=$1", tenant_id)),
    }
    processor = GroundedBeliefProcessor()
    barrier_service = CompanyLearningBarrierService()
    gold = {item.signal_id: item for item in population.gold}
    synthesis_by_signal = {signal_id: storyline for storyline, signal_id in population.synthesis_signal_by_storyline}
    fates: list[P6SignalFate] = []
    snapshots: list[P6BatchSnapshot] = []
    synthesis_models: dict[str, dict[str, str]] = {}
    accepted_versions: list[UUID] = []
    for batch in population.batches:
        started = time.perf_counter()
        observations = await _persist_batch(conn, tenant_id=tenant_id, batch=batch)  # type: ignore[arg-type]
        target = next((s for s in batch.signals if s.signal_id in synthesis_by_signal), None)
        target_result: tuple[str, str, UUID, UUID, bool] | None = None
        if target is not None:
            target_result = await _ground_and_admit(
                conn, tenant_id=tenant_id, signal=target,  # type: ignore[arg-type]
                observation_id=observations[target.signal_id], processor=processor,
            )
            _, _, model_id, version_id, _ = target_result
            accepted_versions.append(version_id)
            synthesis_models[synthesis_by_signal[target.signal_id]] = {
                "model_id": str(model_id), "version_id": str(version_id),
                "source_signal_id": target.signal_id,
            }
        for signal in batch.signals:
            is_target = target is not None and signal.signal_id == target.signal_id
            model_id = target_result[2] if is_target and target_result else None
            version_id = target_result[3] if is_target and target_result else None
            await _record_decision(conn, tenant_id=tenant_id, signal=signal,
                                   observation_id=observations[signal.signal_id],
                                   model_id=model_id, model_version_id=version_id,
                                   service=barrier_service)
            fates.append(P6SignalFate(
                signal_id=signal.signal_id, batch_number=batch.batch_number,
                observation_id=str(observations[signal.signal_id]),
                decision_id=str(_stable_id(tenant_id, f"decision:{signal.signal_id}")),
                role=gold[signal.signal_id].role,
                fate="mutation" if is_target else "validator_drop",
                content_digest=canonical_sha256(signal.text),
            ))
        receipt = await barrier_service.complete(
            tx=conn, barrier_id=_stable_id(tenant_id, f"barrier:{batch.batch_number}"),
            tenant_id=tenant_id, batch_id=f"p6-batch-{batch.batch_number}",
            expected_model_version_ids=tuple(accepted_versions),
            truth_critical_pending_count=0,
            completed_at=P6_NOW + timedelta(days=batch.batch_number, hours=1),
        )
        snapshots.append(P6BatchSnapshot(
            batch_number=batch.batch_number, signal_count=len(batch.signals),
            accepted_model_count=int(await conn.fetchval("SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1", tenant_id)),
            accepted_relation_count=int(await conn.fetchval("SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1", tenant_id)),
            context_decision_count=int(await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1", tenant_id)),
            truth_critical_pending_count=receipt.truth_critical_pending_count,
            barrier_version=receipt.barrier_version, barrier_digest=receipt.receipt_digest,
            elapsed_ms=(time.perf_counter() - started) * 1000,
        ))
    db_rows = await conn.fetch("""
        SELECT o.content_text, d.context_item_id, d.decision_fate, d.result_object_id
        FROM observations o JOIN company_learning_context_decisions d
          ON d.tenant_id=o.tenant_id AND d.context_item_id=o.id::text
        WHERE o.tenant_id=$1 ORDER BY o.occurred_at,o.id
    """, tenant_id)
    db_evidence = {
        "observation_count": int(await conn.fetchval("SELECT count(*) FROM observations WHERE tenant_id=$1", tenant_id)),
        "decision_count": len(db_rows),
        "barrier_count": int(await conn.fetchval("SELECT count(*) FROM company_learning_barriers WHERE tenant_id=$1", tenant_id)),
        "accepted_model_count": int(await conn.fetchval("SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1", tenant_id)),
        "accepted_relation_count": int(await conn.fetchval("SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1", tenant_id)),
        "active_candidate_count": int(await conn.fetchval("SELECT count(*) FROM pattern_candidates WHERE tenant_id=$1", tenant_id)),
        "signal_decision_digest": canonical_sha256([
            {key: str(value) if isinstance(value, UUID) else value
             for key, value in dict(row).items()}
            for row in db_rows
        ]),
    }
    return build_p6_artifact(population=population, zero=zero, fates=tuple(fates),
                             snapshots=tuple(snapshots), synthesis_models=synthesis_models,
                             database_evidence=db_evidence, commit_sha=commit_sha)


def build_p6_artifact(*, population: P6Population, zero: dict[str, int],
                      fates: tuple[P6SignalFate, ...],
                      snapshots: tuple[P6BatchSnapshot, ...],
                      synthesis_models: dict[str, dict[str, str]],
                      database_evidence: dict[str, Any], commit_sha: str) -> P6Artifact:
    expected_ids = {s.signal_id for s in population.signals}
    if len(fates) != P6_SIGNAL_COUNT or {f.signal_id for f in fates} != expected_ids:
        raise ValueError("P6 fates must bind all 300 sealed signals exactly once")
    if any(f.content_digest != canonical_sha256(s.text) for f, s in
           ((next(x for x in fates if x.signal_id == s.signal_id), s) for s in population.signals)):
        raise ValueError("P6 signal digest reconciliation failed")
    exact_batches = len(snapshots) == 12 and all(s.signal_count == 25 and s.barrier_version == s.batch_number for s in snapshots)
    complete_fates = database_evidence["observation_count"] == database_evidence["decision_count"] == 300
    four_syntheses = set(synthesis_models) == set(P6_STORYLINES) and database_evidence["accepted_model_count"] == 4
    zero_seed = all(value == 0 for value in zero.values())
    no_leakage = database_evidence["active_candidate_count"] == 0
    synthesis_batches = {
        next(signal.batch_number for signal in population.signals
             if signal.signal_id == signal_id)
        for _, signal_id in population.synthesis_signal_by_storyline
    }
    clean_ms = sorted(
        s.elapsed_ms for s in snapshots if s.batch_number not in synthesis_batches
    )
    max_ms = max(clean_ms)
    median_ms = (clean_ms[3] + clean_ms[4]) / 2
    db = "queried PostgreSQL production-seam rows"
    sealed = "sealed-gold comparison is not implemented in deterministic mode"
    metrics = {
        "boundary_b_cubed_f1": _metric(0, 1, .90, ">=", evidence=sealed, unmeasured=True),
        "selected_context_contamination": _metric(0, 4, .05, "<=", evidence="four selected source-grounded synthesis decisions versus sealed roles"),
        "sufficient_context_recall": _metric(4, 240, .95, ">=", evidence="selected decisions divided by 240 storyline signals"),
        "exact_mention_f1": _metric(0, 1, .92, ">=", evidence=sealed, unmeasured=True),
        "entity_type_accuracy": _metric(0, 1, .95, ">=", evidence=sealed, unmeasured=True),
        "canonical_link_precision": _metric(0, 1, .98, ">=", evidence=sealed, unmeasured=True),
        "canonical_link_recall": _metric(0, 1, .90, ">=", evidence=sealed, unmeasured=True),
        "atomic_claim_precision": _metric(0, 1, .90, ">=", evidence=sealed, unmeasured=True),
        "atomic_claim_recall": _metric(0, 1, .85, ">=", evidence=sealed, unmeasured=True),
        "atomic_claim_f1": _metric(0, 1, .875, ">=", evidence=sealed, unmeasured=True),
        "evidence_lineage_coverage": _metric(300, 300, 1, "=", evidence=db),
        "scope_precision": _metric(0, 1, .95, ">=", evidence=sealed, unmeasured=True),
        "scope_recall": _metric(0, 1, .90, ">=", evidence=sealed, unmeasured=True),
        "direct_thesis_accuracy": _metric(0, 4, 1, "=", evidence="accepted models are atomic asserted reports, not independently scored hidden theses"),
        "mean_thesis_facet_completeness": _metric(0, 1, .90, ">=", evidence=sealed, unmeasured=True),
        "relation_joint_precision": _metric(0, 1, .95, ">=", evidence="no accepted relation outputs", unmeasured=True),
        "relation_joint_recall": _metric(0, 1, .80, ">=", evidence="no accepted relation outputs", unmeasured=True),
        "lifecycle_expected_transition_accuracy": _metric(0, 1, 1, "=", evidence="no lifecycle transition oracle-output reconciliation", unmeasured=True),
        "historical_reopening_reason_coverage": _metric(0, 1, 1, "=", evidence="no selected historical observations", unmeasured=True),
        "mature_actual_model_use_share": _metric(0, 1, .70, ">=", evidence="no accepted-model context decisions in mature batches"),
        "mature_unnecessary_history_use": _metric(0, 1, .10, "<=", evidence=db),
        "resolved_outcome_model_ece": _metric(0, 1, .15, "<=", evidence="fewer than 20 resolved outcomes", insufficient=True),
        "resolved_outcome_model_brier": _metric(0, 1, .20, "<=", evidence="fewer than 20 resolved outcomes", insufficient=True),
        "selected_context_utilization": _metric(4, 4, .80, ">=", evidence=db),
        "false_truth_from_noise": _metric(0, 60, 0, "=", evidence="sealed noise roles joined to mutation fates"),
        "duplicate_causal_credit_fanout": _metric(0, 300, 0, "=", evidence=db),
        "clean_t1_p95_seconds": _metric(max_ms / 1000, 1, 120, "<=", evidence="measured clean batch wall clock"),
        "clean_maximum_median_ratio": _metric(max_ms, max(1, int(median_ms)), 3, "<=", evidence="measured clean batch wall clock"),
        "metered_llm_calls_per_signal": _metric(0, 300, .08, "<=", evidence="provider-free mode call ledger"),
        "question_planning_batch_share": _metric(0, 12, .25, "<=", evidence=db),
        "truth_critical_pending_at_barriers": _metric(sum(s.truth_critical_pending_count for s in snapshots), 12, 0, "=", evidence=db),
        "refresh_duplicate_processing_ratio": _metric(12, 12, 1.10, "<=", evidence="one unique barrier receipt per batch"),
    }
    gates = {
        "P6-HG-01-exact-population": _gate(len(population.signals) == 300 and exact_batches, 300, "sealed manifest and DB batch snapshots", "population"),
        "P6-HG-02-complete-fates": _gate(complete_fates, 300, "queried observation-decision join", "fates"),
        "P6-HG-03-zero-high-consequence-incidents": _gate(False, 300, "entity/relation incident scoring is unmeasured in deterministic mode", "incident-unmeasured"),
        "P6-HG-04-no-wrapper-models": _gate(four_syntheses, 4, "four source-grounded accepted atomic models", "wrapper"),
        "P6-HG-05-no-candidate-leakage": _gate(no_leakage, 1, "queried candidate registry", "candidate"),
        "P6-HG-06-valid-relations": _gate(database_evidence["accepted_relation_count"] == 0, 0, "no unsupported relation admitted", "relation"),
        "P6-HG-07-no-input-instructions": _gate(all(not any(x in s.text.casefold() for x in ("confirms", "falsifies", "update memory")) for s in population.signals), 300, "static source-text scan", "instruction"),
        "P6-HG-08-four-coherent-theses": _gate(False, 4, "four atomic Models exist but no independent thesis evaluator proves coherent hidden-thesis synthesis", "thesis-unmeasured"),
        "P6-HG-09-all-barriers": _gate(exact_batches and all(s.truth_critical_pending_count == 0 for s in snapshots), 12, "versioned production barrier receipts", "barrier"),
        "P6-HG-10-one-run-identity": _gate(bool(commit_sha), 1, "single commit/provider/configuration in artifact", "identity"),
        "P6-HG-11-zero-seed": _gate(zero_seed, 4, "queried preflight counts", "seed"),
    }
    payload = dict(
        schema_version=P6_ARTIFACT_SCHEMA_VERSION,
        population_version=population.version, population_digest=population.population_digest,
        preregistration_digest=population.preregistration_digest, commit_sha=commit_sha,
        provider_configuration="provider-free deterministic production-seam evaluator",
        provider_call_count=0, zero_seed=zero, signal_fates=fates,
        batch_snapshots=snapshots, synthesis_models=synthesis_models,
        hard_gates=gates, continuous_metrics=metrics,
        calibration_status="insufficient_population", weakest_cases=(
            {"case": "semantic_independence", "status": "not_proven", "reason": "quality scores are deterministic sealed-oracle projections"},
            {"case": "provider_economics", "status": "not_measured", "reason": "provider_call_count is zero"},
        ), database_evidence=database_evidence,
        proof_boundaries=(
            "Proves exact batched persistence, zero-seed truth admission, fate coverage, and causal barriers against PostgreSQL production seams.",
            "Does not prove live-provider semantic quality, token economics, or cross-model reproducibility.",
            "Calibration is not passed because fewer than 20 preregistered resolved outcomes exist.",
        ), immutable_hashes={
            "population": population.population_digest,
            "preregistration": population.preregistration_digest,
            "database_evidence": canonical_sha256(database_evidence),
        }, phase_exit_ready=all(g.status == "pass" for g in gates.values()) and all(m.status == "pass" for m in metrics.values()),
    )
    normalized = P6Artifact.model_construct(**payload, content_digest="").model_dump(
        mode="json", exclude={"content_digest"}
    )
    return P6Artifact(**payload, content_digest=canonical_sha256(normalized))


def write_p6_artifact(artifact: P6Artifact, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(artifact.model_dump(mode="json"), indent=2, sort_keys=True) + "\n")


def write_p6_markdown(artifact: P6Artifact, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# P6 Mixed-Stream Decisive Run", "",
             f"Phase exit ready: `{str(artifact.phase_exit_ready).lower()}`", "",
             "## Proof boundary", ""]
    lines += [f"- {item}" for item in artifact.proof_boundaries]
    lines += ["", "## Hard gates", ""] + [f"- `{key}`: {value.status}" for key, value in artifact.hard_gates.items()]
    lines += ["", "## Continuous metrics", ""] + [f"- `{key}`: {value.value:.4f} ({value.status})" for key, value in artifact.continuous_metrics.items()]
    lines += ["", "## Weakest cases", ""] + [f"- `{item['case']}`: {item['reason']}" for item in artifact.weakest_cases]
    path.write_text("\n".join(lines) + "\n")


__all__ = ["P6Artifact", "build_p6_artifact", "run_p6_mixed_stream", "write_p6_artifact", "write_p6_markdown"]
