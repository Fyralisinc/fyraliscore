"""Continuous evaluation of consequential agency and outcome integrity."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.agency import (
    Attribution,
    AttributionCommand,
    AuthorizationDecision,
    AuthorizationDecisionCommand,
    AuthorizationDisposition,
    ConsequentialProposal,
    ConsequentialProposalFate,
    ConsequentialProposalRegistrationCommand,
    ConsequentialProposalReviewCommand,
    EpisodeStageFate,
    EpisodeUpdateCommand,
    InterventionEpisode,
    InterventionSpec,
    Outcome,
    OutcomeRecordingCommand,
    Prediction,
    PredictionRegistrationCommand,
    ResidualClass,
    Settlement,
    SettlementCommand,
    SettlementDisposition,
)
from lib.contracts.kernel import canonical_sha256
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantRunEvidence,
    MetricObservation,
)


class _AgencyEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class AgencyEvaluationScope(_AgencyEvaluationModel):
    tenant_id: UUID
    start: datetime
    end: datetime
    run_id: str = Field(min_length=1)

    @field_validator("start", "end")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def interval_is_forward(self) -> Self:
        if self.end <= self.start:
            raise ValueError("agency evaluation end must follow start")
        return self


class AgencyEvaluationState(_AgencyEvaluationModel):
    scope: AgencyEvaluationScope
    proposal_count: int = Field(ge=0)
    proposal_fate_counts: dict[str, int]
    intervention_spec_count: int = Field(ge=0)
    valid_proposal_spec_count: int = Field(ge=0)
    proposal_spec_atomicity_rate: float = Field(ge=0.0, le=1.0)
    prediction_count: int = Field(ge=0)
    valid_preregistered_prediction_count: int = Field(ge=0)
    prediction_preregistration_rate: float = Field(ge=0.0, le=1.0)
    immutable_table_count: int = Field(ge=0)
    guarded_immutable_table_count: int = Field(ge=0)
    immutable_storage_guard_rate: float = Field(ge=0.0, le=1.0)
    authorization_count: int = Field(ge=0)
    exact_authorization_count: int = Field(ge=0)
    authorization_exactness_rate: float = Field(ge=0.0, le=1.0)
    outcome_count: int = Field(ge=0)
    independent_outcome_count: int = Field(ge=0)
    outcome_independence_rate: float = Field(ge=0.0, le=1.0)
    due_prediction_count: int = Field(ge=0)
    terminal_due_prediction_count: int = Field(ge=0)
    due_prediction_terminalization_rate: float = Field(ge=0.0, le=1.0)
    settlement_count: int = Field(ge=0)
    comparable_settlement_count: int = Field(ge=0)
    settlement_comparability_rate: float = Field(ge=0.0, le=1.0)
    residual_conformant_count: int = Field(ge=0)
    residual_conformance_rate: float = Field(ge=0.0, le=1.0)
    attribution_count: int = Field(ge=0)
    conservative_attribution_count: int = Field(ge=0)
    conservative_attribution_rate: float = Field(ge=0.0, le=1.0)
    episode_count: int = Field(ge=0)
    valid_episode_manifest_count: int = Field(ge=0)
    episode_manifest_integrity_rate: float = Field(ge=0.0, le=1.0)
    spec_continuous_episode_count: int = Field(ge=0)
    spec_continuity_rate: float = Field(ge=0.0, le=1.0)
    command_count: int = Field(ge=0)
    reconstructable_command_count: int = Field(ge=0)
    command_reconstructability_rate: float = Field(ge=0.0, le=1.0)
    command_event_coverage: float = Field(ge=0.0, le=1.0)
    command_outbox_coverage: float = Field(ge=0.0, le=1.0)
    mean_recorded_brier_loss: float | None = Field(default=None, ge=0.0)
    incident_counts: dict[str, int]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


async def evaluate_agency_state(
    conn: asyncpg.Connection,
    *,
    scope: AgencyEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> AgencyEvaluationState:
    proposals = await conn.fetch(
        """
        SELECT p.*, s.spec, s.spec_digest AS stored_spec_digest
        FROM consequential_proposals p
        LEFT JOIN consequential_intervention_specs s
          ON s.tenant_id = p.tenant_id AND s.spec_id = p.intervention_spec_id
        WHERE p.tenant_id = $1
          AND p.created_at >= $2 AND p.created_at < $3
        ORDER BY p.created_at, p.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    specs = await conn.fetch(
        """
        SELECT s.*,
               (SELECT count(*) FROM consequential_proposals p
                 WHERE p.tenant_id = s.tenant_id
                   AND p.intervention_spec_id = s.spec_id) AS proposal_count
        FROM consequential_intervention_specs s
        WHERE s.tenant_id = $1
          AND s.created_at >= $2 AND s.created_at < $3
        ORDER BY s.created_at, s.spec_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    predictions = await conn.fetch(
        """
        SELECT * FROM consequential_predictions
        WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
        ORDER BY created_at, id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    authorizations = await conn.fetch(
        """
        SELECT a.*, p.current_fate, p.intervention_spec_id, s.spec
        FROM consequential_authorization_decisions a
        JOIN consequential_proposals p
          ON p.tenant_id = a.tenant_id AND p.id = a.proposal_id
        JOIN consequential_intervention_specs s
          ON s.tenant_id = p.tenant_id AND s.spec_id = p.intervention_spec_id
        WHERE a.tenant_id = $1 AND a.decided_at >= $2 AND a.decided_at < $3
        ORDER BY a.decided_at, a.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    outcomes = await conn.fetch(
        """
        SELECT * FROM consequential_outcomes
        WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
        ORDER BY created_at, id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    settlements = await conn.fetch(
        """
        SELECT s.*, p.prediction, p.prediction_digest, p.created_at AS prediction_created_at,
               p.metric_definition AS prediction_metric, p.preregistered_at,
               p.evidence_cutoff, p.intervention_spec_digest,
               o.outcome, o.created_at AS outcome_created_at,
               o.metric_definition AS outcome_metric, o.observed_at, o.valid_time
        FROM consequential_settlements s
        JOIN consequential_predictions p ON p.id = s.prediction_id
        LEFT JOIN consequential_outcomes o ON o.id = s.outcome_id
        WHERE s.tenant_id = $1 AND s.settled_at >= $2 AND s.settled_at < $3
        ORDER BY s.settled_at, s.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    attributions = await conn.fetch(
        """
        SELECT a.*, s.settlement
        FROM consequential_attributions a
        JOIN consequential_settlements s ON s.id = a.settlement_id
        WHERE a.tenant_id = $1 AND a.created_at >= $2 AND a.created_at < $3
        ORDER BY a.created_at, a.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    episodes = await conn.fetch(
        """
        SELECT h.*, v.episode, v.episode_digest
        FROM intervention_episode_heads h
        JOIN intervention_episode_versions v
          ON v.tenant_id = h.tenant_id AND v.episode_id = h.episode_id
         AND v.aggregate_version = h.current_version
        WHERE h.tenant_id = $1
          AND h.created_at < $3 AND h.updated_at >= $2
        ORDER BY h.episode_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    commands = await conn.fetch(
        """
        SELECT r.*,
               (SELECT count(*) FROM agency_canonical_events e
                 WHERE e.command_result_id = r.id) AS event_count,
               (SELECT count(*)
                  FROM agency_canonical_events e
                  JOIN agency_outbox_records o ON o.event_id = e.id
                 WHERE e.command_result_id = r.id) AS outbox_count
        FROM agency_command_results r
        WHERE r.tenant_id = $1 AND r.created_at >= $2 AND r.created_at < $3
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    guarded_tables = await conn.fetch(
        """
        SELECT DISTINCT c.relname AS table_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND NOT t.tgisinternal
          AND t.tgname LIKE 'reject_consequential%_mutation'
        """
    )
    return analyze_agency_rows(
        scope=scope,
        proposals=proposals,
        specs=specs,
        predictions=predictions,
        authorizations=authorizations,
        outcomes=outcomes,
        settlements=settlements,
        attributions=attributions,
        episodes=episodes,
        commands=commands,
        guarded_tables={row["table_name"] for row in guarded_tables},
        artifact_refs=artifact_refs,
    )


def analyze_agency_rows(
    *,
    scope: AgencyEvaluationScope,
    proposals: Sequence[Mapping[str, Any]],
    specs: Sequence[Mapping[str, Any]],
    predictions: Sequence[Mapping[str, Any]],
    authorizations: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    settlements: Sequence[Mapping[str, Any]],
    attributions: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    guarded_tables: set[str],
    artifact_refs: tuple[str, ...],
) -> AgencyEvaluationState:
    proposal_fates = Counter(str(row["current_fate"]) for row in proposals)
    proposal_by_id: dict[UUID, ConsequentialProposal] = {}
    valid_proposal_specs = 0
    proposal_spec_failures = 0
    for row in proposals:
        try:
            proposal = ConsequentialProposal.model_validate(_json(row["proposal"]))
            spec = InterventionSpec.model_validate(_json(row["spec"]))
            valid = (
                proposal.proposal_id == row["id"]
                and proposal.proposal_digest == row["proposal_digest"]
                and proposal.intervention_spec == spec
                and spec.spec_digest == row["stored_spec_digest"]
                and spec.spec_digest == row["intervention_spec_digest"]
                and spec.spec_id == row["intervention_spec_id"]
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            proposal = None
        valid_proposal_specs += int(valid)
        proposal_spec_failures += int(not valid)
        if valid and proposal is not None:
            proposal_by_id[proposal.proposal_id] = proposal
    orphan_specs = sum(int(row.get("proposal_count") or 0) == 0 for row in specs)

    valid_predictions = 0
    prediction_failures = 0
    prediction_by_id: dict[UUID, Prediction] = {}
    for row in predictions:
        try:
            prediction = Prediction.model_validate(_json(row["prediction"]))
            valid = (
                prediction.prediction_id == row["id"]
                and prediction.prediction_digest == row["prediction_digest"]
                and prediction.evidence_cutoff <= prediction.preregistered_at
                and prediction.preregistered_at <= prediction.forecast_window_start
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            prediction = None
        valid_predictions += int(valid)
        prediction_failures += int(not valid)
        if valid and prediction is not None:
            prediction_by_id[prediction.prediction_id] = prediction

    exact_authorizations = 0
    authorization_failures = 0
    for row in authorizations:
        try:
            decision = AuthorizationDecision.model_validate(_json(row["decision"]))
            proposal = proposal_by_id[decision.proposal_id]
            spec = InterventionSpec.model_validate(_json(row["spec"]))
            target = spec.target_referent
            target_ref = f"referent:{target.referent_id}:v{target.referent_version}"
            expected_fields = {f"parameters.{name}" for name in spec.parameters}
            exact = (
                decision.decision_digest == row["decision_digest"]
                and decision.proposal_digest == proposal.proposal_digest
                and decision.intervention_spec_digest == spec.spec_digest
                and row["current_fate"]
                == ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION.value
            )
            if decision.disposition is AuthorizationDisposition.AUTHORIZED:
                exact = exact and (
                    spec.operation in decision.exact_operations
                    and target_ref in decision.exact_target_refs
                    and expected_fields <= decision.exact_field_paths
                )
        except (TypeError, ValueError, KeyError):
            exact = False
        exact_authorizations += int(exact)
        authorization_failures += int(not exact)

    independent_outcomes = 0
    outcome_failures = 0
    outcome_by_id: dict[UUID, Outcome] = {}
    for row in outcomes:
        try:
            outcome = Outcome.model_validate(_json(row["outcome"]))
            independent = (
                outcome.outcome_id == row["id"]
                and outcome.outcome_digest == row["outcome_digest"]
                and outcome.independent_of_execution_claim
                and bool(row["independent_of_execution_claim"])
            )
        except (TypeError, ValueError, KeyError):
            independent = False
            outcome = None
        independent_outcomes += int(independent)
        outcome_failures += int(not independent)
        if independent and outcome is not None:
            outcome_by_id[outcome.outcome_id] = outcome

    comparable_settlements = 0
    residual_conformant = 0
    settlement_failures = 0
    residual_failures = 0
    settlement_by_id: dict[UUID, Settlement] = {}
    brier_losses: list[float] = []
    for row in settlements:
        try:
            settlement = Settlement.model_validate(_json(row["settlement"]))
            prediction = Prediction.model_validate(_json(row["prediction"]))
            outcome = (
                Outcome.model_validate(_json(row["outcome"]))
                if row.get("outcome") is not None
                else None
            )
            comparable = (
                settlement.prediction_id == prediction.prediction_id
                and settlement.settlement_digest == row["settlement_digest"]
            )
            if settlement.disposition is SettlementDisposition.SETTLED:
                comparable = comparable and bool(
                    outcome
                    and settlement.outcome_id == outcome.outcome_id
                    and prediction.episode_id == outcome.episode_id
                    and prediction.metric_definition == outcome.metric_definition
                    and row["outcome_created_at"] > row["prediction_created_at"]
                    and outcome.observed_at >= prediction.preregistered_at
                    and outcome.valid_time >= prediction.evidence_cutoff
                )
            residual_ok = (
                not settlement.residual_distribution
                or abs(sum(settlement.residual_distribution.values()) - 1.0) <= 1e-6
            )
            value = (settlement.comparison_result or {}).get("brier_loss")
            if isinstance(value, (int, float)) and value >= 0:
                brier_losses.append(float(value))
        except (TypeError, ValueError, KeyError):
            comparable = False
            residual_ok = False
            settlement = None
        comparable_settlements += int(comparable)
        residual_conformant += int(residual_ok)
        settlement_failures += int(not comparable)
        residual_failures += int(not residual_ok)
        if comparable and settlement is not None:
            settlement_by_id[settlement.settlement_id] = settlement

    conservative_attributions = 0
    attribution_failures = 0
    for row in attributions:
        try:
            attribution = Attribution.model_validate(_json(row["attribution"]))
            settlement = settlement_by_id[row["settlement_id"]]
            nonidentifiable_mass = sum(
                float(settlement.residual_distribution.get(item, 0.0))
                for item in (ResidualClass.CONFOUNDING, ResidualClass.NON_IDENTIFIABLE)
            )
            conservative = (
                attribution.attribution_digest == row["attribution_digest"]
                and attribution.episode_id == row["episode_id"]
                and str(row["settlement_id"]) in attribution.evidence_refs
                and (nonidentifiable_mass < 0.5 or attribution.withheld_credit)
            )
        except (TypeError, ValueError, KeyError):
            conservative = False
        conservative_attributions += int(conservative)
        attribution_failures += int(not conservative)

    semantic_refs, spec_by_ref = _semantic_object_indexes(
        proposals=proposals,
        predictions=predictions,
        authorizations=authorizations,
        outcomes=outcomes,
        settlements=settlements,
        attributions=attributions,
    )
    valid_manifests = 0
    spec_continuous = 0
    manifest_failures = 0
    spec_failures = 0
    required_stages = {
        "proposal",
        "prediction",
        "authorization",
        "outcome",
        "settlement",
        "attribution",
    }
    writer_by_stage = {
        "proposal": "ProposalAppender",
        "prediction": "PredictionWriter",
        "authorization": "AuthorizationApplier",
        "outcome": "OutcomeRecorder",
        "settlement": "SettlementApplier",
        "attribution": "AttributionApplier",
    }
    for row in episodes:
        try:
            episode = InterventionEpisode.model_validate(_json(row["episode"]))
            links = {item.stage: item for item in episode.stage_links}
            manifest_ok = (
                episode.episode_id == row["episode_id"]
                and episode.episode_digest == row["episode_digest"]
                and required_stages <= links.keys()
            )
            for stage in required_stages:
                link = links.get(stage)
                if link is None:
                    manifest_ok = False
                elif link.fate is EpisodeStageFate.PRESENT:
                    manifest_ok = manifest_ok and (
                        link.writer_id == writer_by_stage[stage]
                        and link.object_ref in semantic_refs[stage]
                    )
            applicable_digests = {
                digest
                for stage, link in links.items()
                if link.fate is EpisodeStageFate.PRESENT
                and (digest := spec_by_ref.get(stage, {}).get(link.object_ref or ""))
            }
            if episode.intervention_spec_digest is not None:
                applicable_digests.add(episode.intervention_spec_digest)
            continuity_ok = len(applicable_digests) <= 1
        except (TypeError, ValueError, KeyError):
            manifest_ok = False
            continuity_ok = False
        valid_manifests += int(manifest_ok)
        spec_continuous += int(continuity_ok)
        manifest_failures += int(not manifest_ok)
        spec_failures += int(not continuity_ok)

    reconstructable = 0
    event_covered = 0
    outbox_covered = 0
    command_failures = 0
    for row in commands:
        valid = _command_reconstructable(row)
        reconstructable += int(valid)
        command_failures += int(not valid)
        event_covered += int(int(row.get("event_count") or 0) == 1)
        outbox_covered += int(int(row.get("outbox_count") or 0) == 1)

    immutable_tables = {
        "consequential_intervention_specs",
        "consequential_predictions",
        "consequential_authorization_decisions",
        "consequential_outcomes",
        "consequential_settlements",
        "consequential_attributions",
    }
    guarded = len(immutable_tables & guarded_tables)
    due_predictions = sum(
        row["forecast_window_end"] <= scope.end for row in predictions
    )
    settled_prediction_ids = {row["prediction_id"] for row in settlements}
    terminal_due = sum(
        row["forecast_window_end"] <= scope.end and row["id"] in settled_prediction_ids
        for row in predictions
    )
    incident_counts = {
        "orphan_intervention_spec": orphan_specs,
        "proposal_spec_mismatch": proposal_spec_failures,
        "invalid_or_late_prediction_registration": prediction_failures,
        "unguarded_immutable_agency_table": len(immutable_tables) - guarded,
        "authorization_scope_or_digest_mismatch": authorization_failures,
        "outcome_not_independent": outcome_failures,
        "settlement_comparability_or_postdiction": settlement_failures,
        "invalid_residual_distribution": residual_failures,
        "unjustified_causal_credit": attribution_failures,
        "incomplete_or_invalid_episode_manifest": manifest_failures,
        "intervention_spec_discontinuity": spec_failures,
        "unreconstructable_agency_command": command_failures,
        "agency_command_without_event": len(commands) - event_covered,
        "agency_command_without_outbox": len(commands) - outbox_covered,
        "due_prediction_without_terminal_settlement": due_predictions - terminal_due,
    }
    incident_counts = {
        key: value for key, value in incident_counts.items() if value > 0
    }
    return AgencyEvaluationState(
        scope=scope,
        proposal_count=len(proposals),
        proposal_fate_counts=dict(sorted(proposal_fates.items())),
        intervention_spec_count=len(specs),
        valid_proposal_spec_count=valid_proposal_specs,
        proposal_spec_atomicity_rate=_ratio(
            valid_proposal_specs, max(len(proposals), len(specs))
        ),
        prediction_count=len(predictions),
        valid_preregistered_prediction_count=valid_predictions,
        prediction_preregistration_rate=_ratio(valid_predictions, len(predictions)),
        immutable_table_count=len(immutable_tables),
        guarded_immutable_table_count=guarded,
        immutable_storage_guard_rate=_ratio(guarded, len(immutable_tables)),
        authorization_count=len(authorizations),
        exact_authorization_count=exact_authorizations,
        authorization_exactness_rate=_ratio(exact_authorizations, len(authorizations)),
        outcome_count=len(outcomes),
        independent_outcome_count=independent_outcomes,
        outcome_independence_rate=_ratio(independent_outcomes, len(outcomes)),
        due_prediction_count=due_predictions,
        terminal_due_prediction_count=terminal_due,
        due_prediction_terminalization_rate=_ratio(terminal_due, due_predictions),
        settlement_count=len(settlements),
        comparable_settlement_count=comparable_settlements,
        settlement_comparability_rate=_ratio(comparable_settlements, len(settlements)),
        residual_conformant_count=residual_conformant,
        residual_conformance_rate=_ratio(residual_conformant, len(settlements)),
        attribution_count=len(attributions),
        conservative_attribution_count=conservative_attributions,
        conservative_attribution_rate=_ratio(
            conservative_attributions, len(attributions)
        ),
        episode_count=len(episodes),
        valid_episode_manifest_count=valid_manifests,
        episode_manifest_integrity_rate=_ratio(valid_manifests, len(episodes)),
        spec_continuous_episode_count=spec_continuous,
        spec_continuity_rate=_ratio(spec_continuous, len(episodes)),
        command_count=len(commands),
        reconstructable_command_count=reconstructable,
        command_reconstructability_rate=_ratio(reconstructable, len(commands)),
        command_event_coverage=_ratio(event_covered, len(commands)),
        command_outbox_coverage=_ratio(outbox_covered, len(commands)),
        mean_recorded_brier_loss=(
            sum(brier_losses) / len(brier_losses) if brier_losses else None
        ),
        incident_counts=incident_counts,
        uncertainty=(
            "This E3 component population proves protocol mechanics, not intervention effectiveness or forecast calibration across worlds.",
            "Canonical Outcome independence is asserted and source-linked here; source-system measurement validity needs simulator-oracle or customer evidence.",
            "Causal attribution quality cannot be proven from one observational episode; E5 controlled worlds and confounder attacks remain required.",
            "External-effect fencing, workflow/task state, provider receipts, compensation and reconciliation are outside this slice.",
            "Legacy Forecasts, Model predictions and SAGE feedback are not yet governed producers/consumers of this canonical protocol.",
        ),
        artifact_refs=artifact_refs,
    )


def build_agency_invariant_evidence(
    state: AgencyEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    definitions = {
        "INV-09": (
            "inv.prediction_preregistration",
            state.valid_preregistered_prediction_count,
            state.prediction_count,
            {"invalid_or_late_prediction_registration"},
        ),
        "INV-10": (
            "inv.execution_outcome_separation",
            state.independent_outcome_count,
            state.outcome_count,
            {"outcome_not_independent", "settlement_comparability_or_postdiction"},
        ),
        "INV-11": (
            "inv.residual_attribution",
            min(state.residual_conformant_count, state.conservative_attribution_count),
            max(state.settlement_count, state.attribution_count),
            {"invalid_residual_distribution", "unjustified_causal_credit"},
        ),
        "INV-16": (
            "inv.reconstructability",
            state.reconstructable_command_count,
            state.command_count,
            {
                "unreconstructable_agency_command",
                "agency_command_without_event",
                "agency_command_without_outbox",
            },
        ),
        "INV-22": (
            "inv.spec_continuity",
            min(state.valid_proposal_spec_count, state.spec_continuous_episode_count),
            max(state.proposal_count, state.episode_count),
            {
                "orphan_intervention_spec",
                "proposal_spec_mismatch",
                "intervention_spec_discontinuity",
                "authorization_scope_or_digest_mismatch",
            },
        ),
    }
    rows = []
    for invariant_id, (
        metric_id,
        numerator,
        denominator_value,
        names,
    ) in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        violations = sum(state.incident_counts.get(name, 0) for name in names)
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:agency",
            denominator_version="consequential-agency-denominator-v1",
            population_definition_version="canonical-agency-object-command-v1",
            query_or_manifest_hash=canonical_sha256(
                {
                    "scope": state.scope.model_dump(mode="json"),
                    "invariant": invariant_id,
                }
            ),
            source_or_oracle_population=denominator_value,
            production_accepted=denominator_value,
            eligible=denominator_value,
            attempted_or_committed=denominator_value,
            terminal_fates={"covered": min(numerator, denominator_value)},
            nonterminal_fates={"uncovered": max(0, denominator_value - numerator)},
            report_cutoff=state.scope.end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="consequential_agency",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "postdiction" in name or "credit" in name else 4,
                summary=f"Observed {state.incident_counts[name]} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name in sorted(names)
            if state.incident_counts.get(name, 0)
        )
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(
                    {
                        "object_event_and_result_ids",
                        "authority_context",
                        "proposal_spec_authority_work_lease_effect_receipt_and_episode_versions",
                        "prediction_commit_position_cutoff_and_hash",
                        "prediction",
                        "settlement",
                        "attribution",
                    }
                ),
                executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="consequential-agency-runtime-v1",
                        raw_numerator=float(numerator),
                        raw_denominator=float(denominator_value),
                        point_estimate=(
                            numerator / denominator_value if denominator_value else None
                        ),
                        violation_count=violations,
                        severity_mass=float(violations),
                        artifact_refs=state.artifact_refs,
                    ),
                ),
                incidents=incidents,
                achieved_evidence_tier=EvidenceTier.E3,
                denominator=denominator,
                uncertainty=state.uncertainty,
                blind_spots=state.uncertainty,
                artifact_refs=state.artifact_refs,
            )
        )
    return tuple(rows)


def render_agency_markdown(state: AgencyEvaluationState) -> str:
    lines = [
        f"# Consequential agency evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        f"- Proposal/spec atomicity: **{state.valid_proposal_spec_count}/{max(state.proposal_count, state.intervention_spec_count)} ({state.proposal_spec_atomicity_rate:.1%})**",
        f"- Prediction preregistration: **{state.valid_preregistered_prediction_count}/{state.prediction_count} ({state.prediction_preregistration_rate:.1%})**",
        f"- Exact authorization: **{state.exact_authorization_count}/{state.authorization_count} ({state.authorization_exactness_rate:.1%})**",
        f"- Independent outcomes: **{state.independent_outcome_count}/{state.outcome_count} ({state.outcome_independence_rate:.1%})**",
        f"- Due prediction terminalization: **{state.terminal_due_prediction_count}/{state.due_prediction_count} ({state.due_prediction_terminalization_rate:.1%})**",
        f"- Settlement comparability: **{state.comparable_settlement_count}/{state.settlement_count} ({state.settlement_comparability_rate:.1%})**",
        f"- Conservative attribution: **{state.conservative_attribution_count}/{state.attribution_count} ({state.conservative_attribution_rate:.1%})**",
        f"- Episode manifest integrity: **{state.valid_episode_manifest_count}/{state.episode_count} ({state.episode_manifest_integrity_rate:.1%})**",
        f"- Spec continuity: **{state.spec_continuous_episode_count}/{state.episode_count} ({state.spec_continuity_rate:.1%})**",
        f"- Command reconstructability: **{state.reconstructable_command_count}/{state.command_count} ({state.command_reconstructability_rate:.1%})**",
        f"- Append-only storage guards: **{state.guarded_immutable_table_count}/{state.immutable_table_count} ({state.immutable_storage_guard_rate:.1%})**",
        "",
        "## Proposal fates",
        "",
        *(f"- {name}: {count}" for name, count in state.proposal_fate_counts.items()),
        "",
        "## Incidents",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.incident_counts.items())
            if state.incident_counts
            else ("- none observed in this scope",)
        ),
        "",
        "## Proof limits",
        "",
        *(f"- {item}" for item in state.uncertainty),
        "",
    ]
    return "\n".join(lines)


def _semantic_object_indexes(
    *,
    proposals: Sequence[Mapping[str, Any]],
    predictions: Sequence[Mapping[str, Any]],
    authorizations: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    settlements: Sequence[Mapping[str, Any]],
    attributions: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, set[str]], dict[str, dict[str, str | None]]]:
    groups = {
        "proposal": proposals,
        "prediction": predictions,
        "authorization": authorizations,
        "outcome": outcomes,
        "settlement": settlements,
        "attribution": attributions,
    }
    refs: dict[str, set[str]] = defaultdict(set)
    digests: dict[str, dict[str, str | None]] = defaultdict(dict)
    for stage, rows in groups.items():
        for row in rows:
            ref = f"{stage}:{row['id']}"
            refs[stage].add(ref)
            digests[stage][ref] = row.get("intervention_spec_digest")
    return refs, digests


def _command_reconstructable(row: Mapping[str, Any]) -> bool:
    model_by_kind = {
        "register_consequential_proposal": ConsequentialProposalRegistrationCommand,
        "review_consequential_proposal": ConsequentialProposalReviewCommand,
        "update_intervention_episode": EpisodeUpdateCommand,
        "register_prediction": PredictionRegistrationCommand,
        "apply_authorization_decision": AuthorizationDecisionCommand,
        "record_independent_outcome": OutcomeRecordingCommand,
        "settle_prediction": SettlementCommand,
        "apply_attribution": AttributionCommand,
    }
    try:
        model = model_by_kind[str(row["command_kind"])]
        command = model.model_validate(_json(row["command"]))
        valid = (
            command.request_digest == row["request_digest"]
            and command.context.processing_authority.fingerprint
            == row["processing_authority_fingerprint"]
            and command.context.writer_scope_epoch.scope_id == row["writer_scope_id"]
            and command.context.writer_scope_epoch.epoch == row["writer_epoch"]
        )
        if isinstance(command, ConsequentialProposalReviewCommand):
            valid = valid and (
                command.review.authority.fingerprint
                == row["consumption_authority_fingerprint"]
            )
        if isinstance(command, AuthorizationDecisionCommand):
            valid = valid and (
                command.decision.authority.fingerprint
                == row["consumption_authority_fingerprint"]
            )
        return valid
    except (KeyError, TypeError, ValueError):
        return False


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _ratio(numerator: int, denominator: int) -> float:
    return numerator / denominator if denominator else 1.0


__all__ = [
    "AgencyEvaluationScope",
    "AgencyEvaluationState",
    "analyze_agency_rows",
    "build_agency_invariant_evidence",
    "evaluate_agency_state",
    "render_agency_markdown",
]
