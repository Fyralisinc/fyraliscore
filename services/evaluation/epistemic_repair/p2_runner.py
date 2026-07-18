"""PostgreSQL-backed, implementation-independent P2 truth-kernel evaluator.

The runner translates the sealed population at its boundary and observes public
truth services plus accepted views.  Unsupported probes remain explicitly
missing; absence of an exception or row is never silently interpreted as proof.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
from typing import Any
from uuid import UUID, uuid4

from pydantic import ValidationError

from lib.contracts.truth_admission import (
    AdmissionDecision, AdmissionDisposition, AdmitModelCommand,
    AdvanceModelHeadCommand, CandidateReviewState, ModelHeadExpectation,
    ModelTruthTransition, ModelVersion, TruthCandidate,
    TruthCandidateKind,
)
from lib.contracts.truth_evidence import (
    ClaimScopeBinding, ClaimScopeRole, EvidenceAuthority, ScopeSubjectKind,
    TruthEvidenceCoordinate, TruthEvidenceKind, TruthEvidenceReference,
    TruthEvidenceRole,
)
from lib.evaluation.epistemic_repair.p2_exit import build_p2_exit_artifact
from lib.evaluation.epistemic_repair.p2_oracles import (
    P2CaseObservation, P2RaceObservation, P2_GATE_IDS, evaluate_gate,
    race_conforms, stable_digest,
)
from lib.evaluation.epistemic_repair.p2_population import P2Case, build_p2_population
from lib.evaluation.epistemic_repair.p9_contributions import (
    attach_p9_member_evidence,
    git_run_provenance,
)
from lib.evaluation.epistemic_repair.reader_cutover import scan_reader_cutover
from services.evaluation.epistemic_repair.p2_hg10_probes import (
    probe_derived_writer_rejection,
    probe_projection_idempotence,
)
from services.evaluation.epistemic_repair.p2_race_probes import (
    probe_concurrent_transitions,
    probe_fault_rollback_and_retry,
)
from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel.repository import AsyncpgTruthKernelStorage
from services.domain.truth_kernel.relations.contracts import (
    DirectionAssertion, RelationCandidate, RelationDisposition, RelationEvidence,
    RelationKind, RelationParticipant, ROLE_SCHEMA,
)
from services.domain.truth_kernel.relations.repository import AsyncpgRelationKernelStorage
from services.domain.truth_kernel.relations.service import (
    AdmitRelationCommand,
    RelationTruthKernel,
    evidence_confidence,
)
from services.domain.truth_kernel.service import TruthKernelService


NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _evidence(tenant_id: UUID, ordinal: int, *, kind: TruthEvidenceKind = TruthEvidenceKind.OBSERVATION, evidence_id: str | None = None) -> TruthEvidenceReference:
    return TruthEvidenceReference(
        reference_id=uuid4(), tenant_id=tenant_id, kind=kind,
        evidence_id=evidence_id or f"p2-evidence-{ordinal}", evidence_version=1,
        evidence_digest=stable_digest({"evidence": ordinal}), role=TruthEvidenceRole.SUPPORT,
        coordinate=TruthEvidenceCoordinate(
            source_system="p2-simulator", source_object_id=f"batch-1/signal-{ordinal}",
            source_revision="1", span_start=0, span_end=12,
        ),
        authority=EvidenceAuthority(
            authority_ref="p2-sealed-authority", policy_version="1",
            authority_epoch=1, decided_at=NOW - timedelta(days=1),
        ),
        occurred_at=NOW - timedelta(hours=2), recorded_at=NOW - timedelta(hours=1),
        cutoff_at=NOW,
    )


def _admission(tenant_id: UUID, ordinal: int, *, kind: TruthCandidateKind = TruthCandidateKind.ATOMIC_CLAIM, evidence_kind: TruthEvidenceKind = TruthEvidenceKind.OBSERVATION, evidence_id: str | None = None) -> AdmitModelCommand:
    evidence = _evidence(tenant_id, ordinal, kind=evidence_kind, evidence_id=evidence_id)
    model_id, version_id, candidate_id, decision_id, subject_id = (uuid4() for _ in range(5))
    scope = (ClaimScopeBinding(
        subject_id=subject_id, subject_kind=ScopeSubjectKind.PROJECT,
        role=ClaimScopeRole.SUBJECT,
        claim_local_evidence_refs=(evidence.reference_id,),
    ),)
    candidate = TruthCandidate(
        candidate_id=candidate_id, tenant_id=tenant_id, kind=kind,
        review_state=CandidateReviewState.PROPOSED,
        natural=f"Project P2-{ordinal} is blocked by review.",
        proposition={"subject": f"P2-{ordinal}", "predicate": "blocked_by", "object": "review"},
        proposed_evidence=(evidence,), proposed_scope=scope, created_at=NOW,
    )
    decision = AdmissionDecision(
        decision_id=decision_id, tenant_id=tenant_id, candidate_id=candidate_id,
        candidate_version=1, candidate_digest=candidate.candidate_digest,
        disposition=AdmissionDisposition.ACCEPTED, reason_codes=("sealed_fixture",),
        decided_by="p2-evaluator", decided_at=NOW + timedelta(seconds=1),
        admitted_model_id=model_id, admitted_version_id=version_id,
    )
    digest = ModelVersion.compute_semantic_digest(
        proposition=candidate.proposition, natural=candidate.natural,
        evidence=candidate.proposed_evidence, scope=candidate.proposed_scope,
    )
    version = ModelVersion(
        version_id=version_id, model_id=model_id, version=1, tenant_id=tenant_id,
        admission_decision_id=decision_id, source_candidate_id=candidate_id,
        source_candidate_version=1, natural=candidate.natural,
        proposition=candidate.proposition, evidence=candidate.proposed_evidence,
        scope=candidate.proposed_scope, created_at=NOW + timedelta(seconds=2),
        semantic_digest=digest,
    )
    return AdmitModelCommand(
        command_id=uuid4(), idempotency_key=f"p2-admit:{candidate_id}",
        tenant_id=tenant_id, candidate=candidate, decision=decision,
        version=version, issued_at=NOW + timedelta(seconds=3),
    )


def _advance(receipt: Any, prior: ModelVersion, transition: ModelTruthTransition, ordinal: int) -> AdvanceModelHeadCommand:
    lifecycle = transition.resulting_lifecycle
    version = prior.model_copy(update={
        "version_id": uuid4(), "version": prior.version + 1,
        "lifecycle": lifecycle, "created_at": prior.created_at + timedelta(minutes=ordinal + 1),
    })
    return AdvanceModelHeadCommand(
        command_id=uuid4(), idempotency_key=f"p2-{transition.value}:{receipt.model_id}:{ordinal}",
        tenant_id=receipt.tenant_id,
        expectation=ModelHeadExpectation(
            tenant_id=receipt.tenant_id, model_id=receipt.model_id,
            expected_version_id=receipt.version_id, expected_version=receipt.version,
            expected_semantic_digest=receipt.semantic_digest,
            expected_lifecycle=receipt.lifecycle,
        ), next_version=version, transition=transition,
        reason_codes=("sealed_fixture",), issued_at=version.created_at + timedelta(seconds=1),
    )


async def _snapshot(conn: Any, tenant_id: UUID) -> dict[str, Any]:
    rows = await conn.fetch(
        "SELECT id, truth_version_id, truth_version, truth_semantic_digest, truth_lifecycle FROM accepted_current_models WHERE tenant_id=$1 ORDER BY id",
        tenant_id,
    )
    return {"accepted_models": [dict(row) for row in rows]}


class P2TruthKernelEvaluator:
    """Execute the sealed population in a caller-owned rollback transaction."""

    def __init__(self, conn: Any, tenant_id: UUID, *, concurrency_dsn: str | None = None) -> None:
        self.conn, self.tenant_id = conn, tenant_id
        self.concurrency_dsn = concurrency_dsn
        self.service = TruthKernelService(storage=AsyncpgTruthKernelStorage())
        self.relation_service = RelationTruthKernel(AsyncpgRelationKernelStorage())
        self.receipts: list[dict[str, Any]] = []
        self.snapshots: list[dict[str, Any]] = []
        self.latencies: list[float] = []
        self._admitted: list[tuple[Any, AdmitModelCommand]] = []
        self._relations: list[tuple[Any, RelationCandidate, AdmitRelationCommand]] = []
        self.duplicate_attempts = 0
        self.duplicates_absorbed = 0
        self._fault_retry_probe: Any | None = None
        self._perfect_confidence_relation_explanations: list[bool] = []

    async def run(self) -> dict[str, Any]:
        population = build_p2_population()
        await self.conn.execute("INSERT INTO tenants (id, name) VALUES ($1, $2)", self.tenant_id, "p2-truth-kernel-evaluator")
        observations: dict[str, P2CaseObservation] = {}
        for case in population.cases:
            before = await _snapshot(self.conn, self.tenant_id)
            before_digest = stable_digest(before)
            started = time.perf_counter()
            try:
                # Each probe gets a savepoint.  A deliberately rejected SQL
                # mutation must not poison the evaluator's caller-owned
                # transaction and hide all evidence that follows it.
                async with self.conn.transaction():
                    observation = await self._case(case, before_digest)
            except Exception as error:  # evaluator failures are evidence, not suite aborts
                observation = P2CaseObservation(
                    case.case_id, "observed", None,
                    tuple((gate, False) for gate in case.expected_invariants),
                    before_digest=before_digest,
                    violation_codes=(f"evaluator_case_error:{type(error).__name__}:{error}",),
                )
            self.latencies.append((time.perf_counter() - started) * 1000)
            after = await _snapshot(self.conn, self.tenant_id)
            observations[case.case_id] = observation.model_copy(update={"after_digest": stable_digest(after)}) if hasattr(observation, "model_copy") else P2CaseObservation(
                observation.case_id, observation.status, observation.observed_disposition,
                observation.invariant_checks, observation.before_digest, stable_digest(after),
                observation.command_receipt_id, observation.violation_codes,
            )
            self.snapshots.append({"case_id": case.case_id, "before": before, "after": after})

        race_started = time.perf_counter()
        race_observations = await self._races(population.races)
        race_elapsed_ms = (time.perf_counter() - race_started) * 1000
        report = build_p2_exit_artifact(population=population, case_observations=observations, execution_status="complete")
        expected = {case.case_id: case.expected_disposition for case in population.cases}
        report["hard_gates"] = {
            gate: asdict(evaluate_gate(gate, eligible_case_ids=[c.case_id for c in population.cases if gate in c.expected_invariants], observations=observations, expected_dispositions=expected))
            for gate in P2_GATE_IDS
        }
        report["command_receipts"] = _jsonable(self.receipts)
        report["truth_snapshots"] = _jsonable(self.snapshots)
        report["case_results"] = [asdict(observations[c.case_id]) for c in population.cases]
        report["race_results"] = [
            {**asdict(item), "conforms": race_conforms(item, race.expected_outcome)}
            for race, item in zip(population.races, race_observations, strict=True)
        ]
        report["continuous_metrics"].update({
            "evidence_lineage_coverage": self._rate(observations, ("accepted_atomic", "accepted_synthesis"), "HG-05"),
            "scope_precision": self._rate(observations, ("accepted_atomic", "accepted_synthesis", "entity_type_conflict"), "HG-06"),
            "relation_joint_accuracy": self._rate(observations, ("business_relation",), "HG-09"),
            "semantic_duplicate_absorption": (
                self.duplicates_absorbed / self.duplicate_attempts
                if self.duplicate_attempts
                else None
            ),
            "active_unexplained_perfect_confidence_relation_rate": (
                1.0
                - sum(self._perfect_confidence_relation_explanations)
                / len(self._perfect_confidence_relation_explanations)
                if self._perfect_confidence_relation_explanations
                else 0.0
            ),
            "active_wrapper_contamination": 0.0 if all(dict(observations[c.case_id].invariant_checks).get("HG-04") for c in population.family("wrapper_control")) else 1.0,
            "lifecycle_transition_latency_ms": sum(self.latencies) / len(self.latencies) if self.latencies else None,
        })
        missing_cases = [item.case_id for item in observations.values() if item.status != "observed"]
        missing_races = [item.scenario_id for item in race_observations if item.status != "observed"]
        report["missing_evidence"] = [f"case:{item}" for item in missing_cases] + [f"race:{item}" for item in missing_races]
        repo_root = Path(__file__).resolve().parents[3]
        reader_report = scan_reader_cutover(
            repo_root,
            repo_root
            / "docs/plans/epistemic-repair/p2/reader-authority-manifest-v1.json",
        )
        report["remaining_compatibility_debt"] = [
            "relation fixtures require the relation-kernel executable evaluator adapter",
            "five-projection fault injection has no public production fault point",
            "derived writer rejection needs capability-enforced component identities",
            *[
                f"uncovered consequential reader: {module}"
                for module in reader_report.remaining_debt
            ],
        ]
        report["reader_cutover_coverage"] = reader_report.coverage
        report["continuous_metric_thresholds"] = {
            "semantic_duplicate_absorption": 0.90,
        }
        report["phase_exit_ready"] = (
            not report["missing_evidence"]
            and all(gate["status"] == "pass" for gate in report["hard_gates"].values())
            and all(item["conforms"] for item in report["race_results"])
            and report["continuous_metrics"]["semantic_duplicate_absorption"]
            >= 0.90
            and report["reader_cutover_coverage"] == 1.0
        )
        report["artifact_content_digest"] = stable_digest({k: v for k, v in report.items() if k not in {"generated_at", "artifact_content_digest"}})
        case_by_id = {case.case_id: case for case in population.cases}
        gate_members = {
            gate: [{
                "member_id": observation.case_id,
                "conforms": bool(
                    observation.status == "observed"
                    and dict(observation.invariant_checks).get(gate) is True
                    and not observation.violation_codes
                    and observation.observed_disposition
                    == case_by_id[observation.case_id].expected_disposition
                ),
                "raw_source_digest": stable_digest(observation),
            } for observation in observations.values()
            if gate in case_by_id[observation.case_id].expected_invariants]
            for gate in P2_GATE_IDS
        }

        def case_metric_members(
            metric: str, families: tuple[str, ...], gate: str, *, invert: bool = False,
        ) -> list[dict[str, Any]]:
            members = [{
                "member_id": f"{metric}:{item.case_id}",
                "numerator": int(
                    item.status == "observed"
                    and dict(item.invariant_checks).get(gate) is True
                    and not item.violation_codes
                ),
                "denominator": 1,
                "raw_source_digest": stable_digest(item),
            } for item in observations.values()
            if case_by_id[item.case_id].family in families]
            if invert:
                for member in members:
                    member["numerator"] = 1 - member["numerator"]
            return members

        metric_members = {
            "active_unexplained_perfect_confidence_relation_rate": [{
                "member_id": f"perfect-confidence-relation:{index}",
                "numerator": int(not explained), "denominator": 1,
                "raw_source_digest": stable_digest({
                    "ordinal": index, "explained": explained,
                }),
            } for index, explained in enumerate(
                self._perfect_confidence_relation_explanations, start=1
            )] or [{
                "member_id": "perfect-confidence-relation:none-observed",
                "numerator": 0, "denominator": 1,
                "raw_source_digest": stable_digest({"observed_count": 0}),
            }],
            "active_wrapper_contamination": case_metric_members(
                "active_wrapper_contamination", ("wrapper_control",), "HG-04",
                invert=True,
            ),
            "background_repair_latency_ms": [{
                "member_id": "race-suite-wall-time",
                "numerator": race_elapsed_ms,
                "denominator": max(1, len(race_observations)),
                "raw_source_digest": stable_digest({
                    "race_results": race_observations,
                    "elapsed_ms": race_elapsed_ms,
                }),
            }],
            "evidence_lineage_coverage": case_metric_members(
                "evidence_lineage_coverage",
                ("accepted_atomic", "accepted_synthesis"), "HG-05",
            ),
            "lifecycle_transition_latency_ms": [{
                "member_id": f"case:{case.case_id}:latency",
                "numerator": elapsed_ms, "denominator": 1,
                "raw_source_digest": stable_digest({
                    "case_result": observations[case.case_id],
                    "elapsed_ms": elapsed_ms,
                }),
            } for case, elapsed_ms in zip(population.cases, self.latencies, strict=True)],
            "relation_joint_accuracy": case_metric_members(
                "relation_joint_accuracy", ("business_relation",), "HG-09",
            ),
            "scope_precision": case_metric_members(
                "scope_precision",
                ("accepted_atomic", "accepted_synthesis", "entity_type_conflict"),
                "HG-06",
            ),
            "semantic_duplicate_absorption": [{
                "member_id": f"semantic_duplicate_absorption:{item.case_id}",
                "numerator": int(
                    item.status == "observed" and not item.violation_codes
                ),
                "denominator": 1,
                "raw_source_digest": stable_digest(item),
            } for item in observations.values()
            if case_by_id[item.case_id].family == "semantic_duplicate"],
        }
        return attach_p9_member_evidence(
            report, phase="p2", gate_members=gate_members,
            metric_members=metric_members,
            run_provenance=git_run_provenance(repo_root),
        )

    async def _case(self, case: P2Case, before_digest: str) -> P2CaseObservation:
        family = case.family
        if family in {"accepted_atomic", "accepted_synthesis"}:
            if family == "accepted_synthesis" and not self._admitted:
                return P2CaseObservation(case.case_id, "missing", violation_codes=("synthesis_source_model_missing",))
            source_version_id = str(self._admitted[-1][0].version_id) if family == "accepted_synthesis" else None
            command = _admission(self.tenant_id, len(self._admitted) + 1, kind=TruthCandidateKind.SYNTHESIS if family == "accepted_synthesis" else TruthCandidateKind.ATOMIC_CLAIM, evidence_kind=TruthEvidenceKind.MODEL_VERSION if family == "accepted_synthesis" else TruthEvidenceKind.OBSERVATION, evidence_id=source_version_id)
            receipt = await self.service.admit(tx=self.conn, command=command)
            self._admitted.append((receipt, command)); self.receipts.append(asdict(receipt))
            row = await self.conn.fetchrow("SELECT * FROM accepted_current_models WHERE tenant_id=$1 AND id=$2", self.tenant_id, receipt.model_id)
            evidence_count = await self.conn.fetchval("SELECT count(*) FROM model_truth_evidence_references WHERE tenant_id=$1 AND model_version_id=$2", self.tenant_id, receipt.version_id)
            scope_count = await self.conn.fetchval("SELECT count(*) FROM model_truth_scope_bindings WHERE tenant_id=$1 AND model_version_id=$2", self.tenant_id, receipt.version_id)
            checks = {gate: row is not None for gate in case.expected_invariants}
            checks["HG-05"] = bool(row and evidence_count >= 1)
            checks["HG-06"] = bool(row and scope_count >= 1)
            checks["HG-07"] = bool(row and row["truth_semantic_digest"] == command.version.semantic_digest)
            return P2CaseObservation(case.case_id, "observed", "accept", tuple(sorted(checks.items())), before_digest, command_receipt_id=str(receipt.command_id))
        if family == "semantic_duplicate":
            if not self._admitted:
                return P2CaseObservation(
                    case.case_id,
                    "missing",
                    violation_codes=("duplicate_source_model_missing",),
                )
            source_receipt, source_command = self._admitted[0]
            ordinal = self.duplicate_attempts + 1
            duplicate = source_command.model_copy(
                update={
                    "command_id": uuid4(),
                    "idempotency_key": f"p2-semantic-duplicate:{ordinal}",
                    "issued_at": source_command.issued_at
                    + timedelta(seconds=ordinal),
                }
            )
            before_versions = await self.conn.fetchval(
                "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1",
                self.tenant_id,
            )
            receipt = await self.service.admit(tx=self.conn, command=duplicate)
            after_versions = await self.conn.fetchval(
                "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1",
                self.tenant_id,
            )
            self.duplicate_attempts += 1
            absorbed = (
                receipt.outcome == "absorbed_duplicate"
                and receipt.version_id == source_receipt.version_id
                and before_versions == after_versions
            )
            self.duplicates_absorbed += int(absorbed)
            self.receipts.append(asdict(receipt))
            return P2CaseObservation(
                case.case_id,
                "observed",
                "accept",
                before_digest=before_digest,
                command_receipt_id=str(receipt.command_id),
                violation_codes=()
                if absorbed
                else ("exact_semantic_duplicate_not_absorbed",),
            )
        if family == "nonaccepted_admission":
            command = _admission(self.tenant_id, 100 + len(self.receipts))
            disposition = AdmissionDisposition(case.fact("admission_state")) if case.fact("admission_state") in {"rejected", "needs_review"} else AdmissionDisposition.NEEDS_REVIEW
            decision = command.decision.model_copy(update={"disposition": disposition, "admitted_model_id": None, "admitted_version_id": None})
            rejected = False
            try: AdmitModelCommand(**{**command.model_dump(), "decision": decision})
            except ValidationError: rejected = True
            count = await self.conn.fetchval("SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND id=$2", self.tenant_id, command.version.model_id)
            ok = rejected and count == 0
            return P2CaseObservation(case.case_id, "observed", "remain_noncanonical", (("HG-04", ok),), before_digest, violation_codes=() if ok else ("nonaccepted_model_leaked",))
        if family == "wrapper_control":
            command = _admission(self.tenant_id, 200 + len(self.receipts))
            kind = TruthCandidateKind.CONTROL_LANGUAGE if "control" in (case.fact("wrapper_kind") or "") or "prompt" in (case.fact("wrapper_kind") or "") else TruthCandidateKind.PROCESSING_WRAPPER
            candidate = command.candidate.model_copy(update={"kind": kind})
            rejected = False
            try: AdmitModelCommand(**{**command.model_dump(), "candidate": candidate})
            except ValidationError: rejected = True
            return P2CaseObservation(case.case_id, "observed", "remain_noncanonical", (("HG-04", rejected),), before_digest, violation_codes=() if rejected else ("wrapper_admitted",))
        if family == "entity_type_conflict":
            command = _admission(self.tenant_id, 300 + len(self.receipts)); binding = command.candidate.proposed_scope[0]
            proposed = case.fact("proposed_type")
            proposed_kind = ScopeSubjectKind.ORGANIZATION if proposed == "company" else ScopeSubjectKind(proposed)
            conflict = binding.model_copy(update={"subject_kind": proposed_kind})
            rejected = False
            try: TruthCandidate(**{**command.candidate.model_dump(), "proposed_scope": (binding, conflict)})
            except ValidationError: rejected = True
            return P2CaseObservation(case.case_id, "observed", "reject", (("HG-06", rejected),), before_digest, violation_codes=() if rejected else ("conflicting_scope_type_admitted",))
        if family == "representation_divergence":
            command = _admission(self.tenant_id, 400 + len(self.receipts)); rejected = False
            divergent = command.version.model_dump(mode="python")
            divergent["natural"] = command.version.natural + " divergent"
            try: ModelVersion.model_validate(divergent)
            except ValidationError: rejected = True
            return P2CaseObservation(case.case_id, "observed", "reject", (("HG-07", rejected),), before_digest, violation_codes=() if rejected else ("representation_divergence_admitted",))
        if family in {"falsification", "valid_supersession"}:
            base = _admission(self.tenant_id, 500 + len(self.receipts)); admitted = await self.service.admit(tx=self.conn, command=base)
            transition = ModelTruthTransition.FALSIFY if family == "falsification" else ModelTruthTransition.SUPERSEDE
            receipt = await self.service.advance(tx=self.conn, command=_advance(admitted, base.version, transition, len(self.receipts)))
            self.receipts.extend((asdict(admitted), asdict(receipt)))
            active = await self.conn.fetchval("SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND id=$2", self.tenant_id, receipt.model_id)
            events = await self.conn.fetchval("SELECT count(*) FROM model_truth_lifecycle_events WHERE tenant_id=$1 AND model_id=$2", self.tenant_id, receipt.model_id)
            ok = active == 0 and events == 1
            return P2CaseObservation(case.case_id, "observed", "accept", (("HG-08", ok),), before_digest, command_receipt_id=str(receipt.command_id), violation_codes=() if ok else ("terminal_model_not_fenced",))
        if family == "invalid_supersession":
            base = _admission(self.tenant_id, 600 + len(self.receipts)); admitted = await self.service.admit(tx=self.conn, command=base)
            terminal = await self.service.advance(tx=self.conn, command=_advance(admitted, base.version, ModelTruthTransition.FALSIFY, len(self.receipts)))
            rejected = False
            try:
                stale = _advance(admitted, base.version, ModelTruthTransition.SUPERSEDE, len(self.receipts) + 1)
                await self.service.advance(tx=self.conn, command=stale)
            except (InvariantViolation, ValidationError): rejected = True
            self.receipts.extend((asdict(admitted), asdict(terminal)))
            return P2CaseObservation(case.case_id, "observed", "reject", (("HG-08", rejected),), before_digest, violation_codes=() if rejected else ("invalid_supersession_applied",))
        if family == "business_relation":
            if len(self._admitted) < 2:
                return P2CaseObservation(case.case_id, "missing", violation_codes=("relation_endpoint_models_missing",))
            left, right = self._admitted[0], self._admitted[1]
            left_receipt, left_command = left
            right_receipt, _ = right
            evidence_receipt, evidence_command = self._admitted[2] if len(self._admitted) > 2 else left
            kind_text = case.fact("relation_kind") or "unknown_relation_kind"
            shape = case.fact("shape") or ""
            known = RelationKind(kind_text) if kind_text in RelationKind._value2member_map_ else None
            roles = ROLE_SCHEMA[known] if known else ("source", "target")
            if shape == "wrong_role": roles = ("wrong_source", "wrong_target")
            target_model_id, target_version_id = right_receipt.model_id, right_receipt.version_id
            if shape == "wrong_endpoint": target_version_id = uuid4()
            participants = (
                RelationParticipant(model_id=left_receipt.model_id, model_version_id=left_receipt.version_id, role=roles[0], ordinal=0),
                RelationParticipant(model_id=target_model_id, model_version_id=target_version_id, role=roles[1], ordinal=1),
            )
            assertion = None
            if known:
                reverse = shape in {"reverse_direction", "reciprocal_invalidity"}
                assertion = DirectionAssertion(
                    kind=known,
                    source_model_version_id=target_version_id if reverse else left_receipt.version_id,
                    target_model_version_id=left_receipt.version_id if reverse else target_version_id,
                    polarity=-1 if shape == "self_negating_rationale" else 1,
                )
            evidence_ref = evidence_command.version.evidence[0]
            candidate = RelationCandidate(
                candidate_relation_id=uuid4(), tenant_id=self.tenant_id,
                proposed_kind=kind_text, participants=participants,
                rationale="The typed source has the declared effect on the typed target.",
                assertion=assertion,
                evidence=(RelationEvidence(
                    evidence_reference_id=evidence_ref.reference_id,
                    model_version_id=evidence_receipt.version_id,
                    evidence_digest=evidence_ref.evidence_digest,
                    polarity=1, weight=0.8,
                ),), created_at=NOW,
            )
            await self.conn.execute(
                "INSERT INTO relation_instances (id, tenant_id, relation_kind, status, participant_binding_status, write_policy) VALUES ($1,$2,$3,'candidate','bound','candidate')",
                candidate.candidate_relation_id, self.tenant_id, kind_text,
            )
            command = AdmitRelationCommand(uuid4(), f"p2-relation:{candidate.candidate_relation_id}", candidate, uuid4(), uuid4(), NOW + timedelta(hours=1))
            receipt = await self.relation_service.admit(tx=self.conn, command=command)
            self.receipts.append(asdict(receipt))
            if receipt.disposition is RelationDisposition.ACCEPTED:
                self._relations.append((receipt, candidate, command))
                # A perfect evidence projection is only explained when its
                # signed, version-bound evidence and human-readable rationale
                # are both present. Track this at admission time because later
                # lifecycle races intentionally remove relations from the
                # accepted-current view.
                confidence = evidence_confidence(candidate.evidence)
                if confidence == 1.0:
                    self._perfect_confidence_relation_explanations.append(
                        bool(candidate.rationale.strip() and candidate.evidence)
                    )
            canonical = await self.conn.fetchval("SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1 AND id=$2", self.tenant_id, candidate.candidate_relation_id)
            actual = "accept" if receipt.disposition is RelationDisposition.ACCEPTED else "remain_noncanonical"
            ok = actual == case.expected_disposition and ((actual == "accept" and canonical == 1) or (actual != "accept" and canonical == 0))
            return P2CaseObservation(case.case_id, "observed", actual, (("HG-09", ok),), before_digest, command_receipt_id=str(receipt.command_id), violation_codes=() if ok else ("relation_disposition_or_read_mismatch",))
        if family == "evidence_idempotence" and self._relations:
            receipt, candidate, command = self._relations[0]
            before_count = await self.conn.fetchval(
                "SELECT count(*) FROM relation_truth_evidence WHERE tenant_id=$1 AND relation_version_id=$2",
                self.tenant_id, receipt.relation_version_id,
            )
            replays = [await self.relation_service.admit(tx=self.conn, command=command) for _ in range(5)]
            after_count = await self.conn.fetchval(
                "SELECT count(*) FROM relation_truth_evidence WHERE tenant_id=$1 AND relation_version_id=$2",
                self.tenant_id, receipt.relation_version_id,
            )
            ok = all(item == receipt for item in replays) and before_count == after_count
            return P2CaseObservation(case.case_id, "observed", "accept", (("HG-09", ok),), before_digest, command_receipt_id=str(receipt.command_id), violation_codes=() if ok else ("duplicate_relation_evidence_changed_truth",))
        if family == "retrieval_stability":
            base = _admission(self.tenant_id, 700); admitted = await self.service.admit(tx=self.conn, command=base)
            first = await _snapshot(self.conn, self.tenant_id)
            for _ in range(100):
                await self.conn.fetchrow("SELECT * FROM accepted_current_models WHERE tenant_id=$1 AND id=$2", self.tenant_id, admitted.model_id)
            ok = stable_digest(first) == stable_digest(await _snapshot(self.conn, self.tenant_id))
            return P2CaseObservation(case.case_id, "observed", "accept", (("HG-07", ok), ("HG-10", ok)), before_digest, violation_codes=() if ok else ("retrieval_mutated_truth",))
        if family == "derived_direct_write":
            if not self._admitted:
                return P2CaseObservation(
                    case.case_id,
                    "missing",
                    violation_codes=("accepted_model_missing_for_writer_probe",),
                )
            component = case.fact("component") or "unknown"
            probe = await probe_derived_writer_rejection(
                self.conn,
                tenant_id=self.tenant_id,
                model_id=self._admitted[0][0].model_id,
                component=component,
            )
            return P2CaseObservation(
                case.case_id,
                "observed",
                "reject" if probe.rejected else "accept",
                (("HG-10", probe.conforms),),
                before_digest,
                violation_codes=(
                    ()
                    if probe.conforms
                    else (f"derived_writer_not_rejected:{component}",)
                ),
            )
        if family == "projection_idempotence":
            if not self._relations:
                return P2CaseObservation(
                    case.case_id,
                    "missing",
                    violation_codes=("accepted_relation_missing_for_projection_probe",),
                )
            _, candidate, _ = self._relations[0]
            repeat_count = int(case.fact("repeat_count") or "5")
            probe = await probe_projection_idempotence(
                self.conn,
                tenant_id=self.tenant_id,
                relation_id=candidate.candidate_relation_id,
                source_model_id=candidate.participants[0].model_id,
                target_model_id=candidate.participants[1].model_id,
                repeat_count=repeat_count,
            )
            return P2CaseObservation(
                case.case_id,
                "observed",
                "accept",
                (("HG-09", probe.conforms), ("HG-10", probe.conforms)),
                before_digest,
                violation_codes=(
                    ()
                    if probe.conforms
                    else ("projection_replay_changed_semantic_state",)
                ),
            )
        if family == "command_idempotence":
            base = _admission(self.tenant_id, 800); first = await self.service.admit(tx=self.conn, command=base); second = await self.service.admit(tx=self.conn, command=base)
            count = await self.conn.fetchval("SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1 AND model_id=$2", self.tenant_id, first.model_id)
            ok = first == second and count == 1
            return P2CaseObservation(case.case_id, "observed", "accept", (("HG-08", ok),), before_digest, command_receipt_id=str(first.command_id), violation_codes=() if ok else ("command_replay_extra_effect",))
        return P2CaseObservation(case.case_id, "missing", None, tuple((gate, False) for gate in case.expected_invariants), before_digest, violation_codes=(f"unsupported_runtime_probe:{family}",))

    async def _races(self, races: tuple[Any, ...]) -> list[P2RaceObservation]:
        results: list[P2RaceObservation] = []
        for race in races:
            before = await _snapshot(self.conn, self.tenant_id)
            before_digest = stable_digest(before)
            if race.operation in {
                "falsify_model_with_five_projections",
                "retry_falsify_model_with_five_projections",
            }:
                if self._fault_retry_probe is None:
                    base = _admission(self.tenant_id, 900)
                    admitted = await self.service.admit(tx=self.conn, command=base)
                    command = _advance(
                        admitted, base.version, ModelTruthTransition.FALSIFY, 900
                    )
                    self._fault_retry_probe = await probe_fault_rollback_and_retry(
                        self.conn, command=command
                    )
                probe = self._fault_retry_probe
                rollback_case = race.operation == "falsify_model_with_five_projections"
                conforms = probe.rollback_conforms if rollback_case else probe.retry_conforms
                results.append(P2RaceObservation(
                    race.scenario_id,
                    "observed",
                    race.expected_outcome if conforms else "lifecycle_fault_probe_mismatch",
                    before_digest,
                    stable_digest(await _snapshot(self.conn, self.tenant_id)),
                    lifecycle_event_count=(
                        probe.rollback_lifecycle_event_count
                        if rollback_case else probe.lifecycle_event_count
                    ),
                    repair_obligation_count=(
                        probe.rollback_repair_obligation_count
                        if rollback_case else probe.repair_obligation_count
                    ),
                    violation_codes=() if conforms else probe.violation_codes,
                ))
                continue
            if race.operation == "concurrent_confirm_and_falsify_same_expected_version":
                if not self.concurrency_dsn:
                    results.append(P2RaceObservation(
                        race.scenario_id, "missing", before_digest=before_digest,
                        after_digest=stable_digest(await _snapshot(self.conn, self.tenant_id)),
                        violation_codes=("concurrency_dsn_not_supplied",),
                    ))
                    continue
                isolated_tenant = uuid4()
                probe = await probe_concurrent_transitions(
                    self.concurrency_dsn, tenant_id=isolated_tenant,
                    model_id=uuid4(),
                )
                results.append(P2RaceObservation(
                    race.scenario_id, "observed",
                    race.expected_outcome if probe.conforms else "concurrent_transition_mismatch",
                    before_digest,
                    stable_digest(await _snapshot(self.conn, self.tenant_id)),
                    lifecycle_event_count=probe.lifecycle_event_count,
                    violation_codes=probe.violation_codes,
                ))
                continue
            if race.operation == "falsify_nonparticipant_relation_evidence" and self._relations:
                receipt, candidate, _ = self._relations[0]
                invalidated = candidate.evidence[0].model_version_id
                expected_affected = await self.conn.fetchval(
                    """
                    SELECT count(*)
                    FROM relation_truth_heads h
                    WHERE h.tenant_id=$1 AND h.lifecycle='active'
                      AND EXISTS (
                        SELECT 1 FROM relation_truth_evidence e
                        WHERE e.tenant_id=h.tenant_id
                          AND e.relation_version_id=h.relation_version_id
                          AND e.model_version_id=$2
                      )
                    """,
                    self.tenant_id, invalidated,
                )
                evidence_model = next((item for item in self._admitted if item[0].version_id == invalidated), None)
                if evidence_model is None:
                    results.append(P2RaceObservation(race.scenario_id, "missing", violation_codes=("nonparticipant_evidence_model_missing",)))
                    continue
                evidence_receipt, evidence_command = evidence_model
                await self.service.advance(
                    tx=self.conn,
                    command=_advance(evidence_receipt, evidence_command.version, ModelTruthTransition.FALSIFY, 998),
                )
                affected = await self.relation_service.invalidate_evidence(
                    tx=self.conn, tenant_id=self.tenant_id,
                    invalidated_model_version_id=invalidated,
                    cause_code="MODEL_FALSIFIED", occurred_at=NOW + timedelta(days=1),
                )
                # Replay proves uniqueness of the version-bound obligation.
                replay = await self.relation_service.invalidate_evidence(
                    tx=self.conn, tenant_id=self.tenant_id,
                    invalidated_model_version_id=invalidated,
                    cause_code="MODEL_FALSIFIED", occurred_at=NOW + timedelta(days=1),
                )
                visible = await self.conn.fetchval(
                    """
                    SELECT count(*) FROM accepted_current_relations r
                    WHERE r.tenant_id=$1
                      AND EXISTS (
                        SELECT 1 FROM relation_truth_evidence e
                        WHERE e.tenant_id=r.tenant_id
                          AND e.relation_version_id=r.truth_relation_version_id
                          AND e.model_version_id=$2
                      )
                    """,
                    self.tenant_id, invalidated,
                )
                obligations = await self.conn.fetchval(
                    "SELECT count(*) FROM truth_repair_obligations WHERE tenant_id=$1 AND invalidated_model_version_id=$2 AND cause_code='MODEL_FALSIFIED'",
                    self.tenant_id, invalidated,
                )
                ok = (
                    expected_affected > 0
                    and len(affected) == expected_affected
                    and replay == affected
                    and visible == 0
                    and obligations == expected_affected
                )
                results.append(P2RaceObservation(
                    race.scenario_id, "observed",
                    race.expected_outcome if ok else "relation_evidence_fence_mismatch",
                    before_digest, stable_digest(await _snapshot(self.conn, self.tenant_id)),
                    repair_obligation_count=obligations,
                    violation_codes=() if ok else ("relation_evidence_invalidation_not_atomic_or_idempotent",),
                ))
                continue
            if race.operation == "supersede_relation_participant" and len(self._relations) >= 2:
                _, candidate, _ = self._relations[1]
                endpoint = next((item for item in self._admitted if item[0].version_id == candidate.participants[0].model_version_id), None)
                if endpoint is not None:
                    admitted, command = endpoint
                    terminal = await self.service.advance(tx=self.conn, command=_advance(admitted, command.version, ModelTruthTransition.SUPERSEDE, 999))
                    visible = await self.conn.fetchval(
                        "SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1 AND id=$2",
                        self.tenant_id, candidate.candidate_relation_id,
                    )
                    participant_version = await self.conn.fetchval(
                        "SELECT model_version_id FROM relation_truth_participants WHERE tenant_id=$1 AND relation_version_id=$2 AND role=$3",
                        self.tenant_id, self._relations[1][0].relation_version_id,
                        candidate.participants[0].role,
                    )
                    ok = visible == 0 and participant_version == candidate.participants[0].model_version_id
                    results.append(P2RaceObservation(
                        race.scenario_id, "observed",
                        race.expected_outcome if ok else "automatic_endpoint_rebinding_or_visibility_leak",
                        before_digest, stable_digest(await _snapshot(self.conn, self.tenant_id)),
                        lifecycle_event_count=await self.conn.fetchval(
                            "SELECT count(*) FROM model_truth_lifecycle_events WHERE tenant_id=$1 AND command_id=$2",
                            self.tenant_id, terminal.command_id,
                        ), violation_codes=() if ok else ("relation_endpoint_rebound_or_remained_visible",),
                    ))
                    continue
            # The public API has no five-projection fault point and a single
            # supplied connection cannot honestly establish concurrent CAS.
            results.append(P2RaceObservation(
                race.scenario_id, "missing", before_digest=before_digest,
                after_digest=stable_digest(await _snapshot(self.conn, self.tenant_id)),
                violation_codes=(f"unsupported_runtime_probe:{race.operation}",),
            ))
        return results

    @staticmethod
    def _rate(observations: dict[str, P2CaseObservation], families: tuple[str, ...], gate: str) -> float | None:
        items = [v for k, v in observations.items() if any(f"p2-{family}-" in k for family in families) and v.status == "observed"]
        return sum(dict(item.invariant_checks).get(gate) is True for item in items) / len(items) if items else None


async def run_p2_truth_kernel(
    conn: Any, *, tenant_id: UUID | None = None, concurrency_dsn: str | None = None
) -> dict[str, Any]:
    return await P2TruthKernelEvaluator(
        conn, tenant_id or uuid4(), concurrency_dsn=concurrency_dsn
    ).run()


__all__ = ["P2TruthKernelEvaluator", "run_p2_truth_kernel"]
