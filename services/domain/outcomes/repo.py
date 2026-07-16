"""Named transactional writers for consequential agency and learning evidence.

Each class owns exactly one semantic object.  The episode coordinator links
objects but never writes their state.  OutcomeRecorder accepts independent
measurements only; execution/task completion belongs to other ledgers.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.agency import (
    AttributionCommand,
    AuthorizationDecisionCommand,
    AuthorizationDisposition,
    ConsequentialProposalFate,
    EpisodeStageFate,
    EpisodeUpdateCommand,
    InterventionEpisode,
    OutcomeRecordingCommand,
    PredictionKind,
    PredictionRegistrationCommand,
    ResidualClass,
    SettlementCommand,
    SettlementDisposition,
)
from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.ids import uuid7
from services.domain.agency_protocol import (
    AgencyCommitResult,
    AgencyProtocolIds,
    ensure_live_context,
    insert_protocol_event_and_outbox,
    insert_protocol_result,
    prior_protocol_result,
)


class EpisodeCoordinator:
    """Own only the versioned stage-link manifest for an intervention episode."""

    async def apply(
        self,
        *,
        conn: asyncpg.Connection,
        command: EpisodeUpdateCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        now = now or datetime.now(timezone.utc)
        context = command.context
        episode = command.episode
        ensure_live_context(context, now=now)
        prior_result = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="EpisodeCoordinator",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior_result is not None:
            return prior_result
        head = await conn.fetchrow(
            """
            SELECT * FROM intervention_episode_heads
            WHERE tenant_id = $1 AND episode_id = $2
            FOR UPDATE
            """,
            context.tenant_id,
            episode.episode_id,
        )
        current_version = int(head["current_version"]) if head else 0
        if current_version != command.expected_version:
            raise InvariantViolation(
                "INTERVENTION_EPISODE_CAS",
                "episode expected version does not match current head",
                expected_version=command.expected_version,
                current_version=current_version,
            )
        prior_episode = None
        if head is not None:
            prior_payload = await conn.fetchval(
                """
                SELECT episode FROM intervention_episode_versions
                WHERE tenant_id = $1 AND episode_id = $2 AND aggregate_version = $3
                """,
                context.tenant_id,
                episode.episode_id,
                current_version,
            )
            prior_episode = InterventionEpisode.model_validate(_json(prior_payload))
            self._validate_manifest_successor(prior_episode, episode)
        elif episode.created_at != episode.updated_at:
            raise InvariantViolation(
                "INTERVENTION_EPISODE_CREATE_TIME",
                "new episode must begin with equal create and update times",
            )
        await self._validate_stage_links(conn=conn, episode=episode)
        next_version = current_version + 1
        ids = AgencyProtocolIds.new()
        result = {
            "episode_id": str(episode.episode_id),
            "episode_version": next_version,
            "episode_digest": episode.episode_digest,
            "intervention_spec_digest": episode.intervention_spec_digest,
            "stage_count": len(episode.stage_links),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="EpisodeCoordinator",
            command_kind="update_intervention_episode",
            command=command,
            request_digest=command.request_digest,
            object_type="intervention_episode",
            object_id=episode.episode_id,
            object_version=next_version,
            result=result,
        )
        if head is None:
            await conn.execute(
                """
                INSERT INTO intervention_episode_heads (
                    tenant_id, episode_id, episode_kind, current_version,
                    current_episode_digest, intervention_spec_digest,
                    created_at, updated_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                context.tenant_id,
                episode.episode_id,
                episode.kind,
                next_version,
                episode.episode_digest,
                episode.intervention_spec_digest,
                episode.created_at,
                episode.updated_at,
            )
        else:
            updated = await conn.execute(
                """
                UPDATE intervention_episode_heads
                SET current_version = $3, current_episode_digest = $4,
                    intervention_spec_digest = $5, updated_at = $6
                WHERE tenant_id = $1 AND episode_id = $2 AND current_version = $7
                """,
                context.tenant_id,
                episode.episode_id,
                next_version,
                episode.episode_digest,
                episode.intervention_spec_digest,
                episode.updated_at,
                current_version,
            )
            if updated != "UPDATE 1":
                raise InvariantViolation(
                    "INTERVENTION_EPISODE_CAS",
                    "episode head changed during manifest commit",
                )
        await conn.execute(
            """
            INSERT INTO intervention_episode_versions (
                id, tenant_id, episode_id, aggregate_version,
                episode_digest, episode, command_result_id
            ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            """,
            uuid7(),
            context.tenant_id,
            episode.episode_id,
            next_version,
            episode.episode_digest,
            json.dumps(episode.model_dump(mode="json")),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="EpisodeCoordinator",
            object_type="intervention_episode",
            object_id=episode.episode_id,
            object_version=next_version,
            semantic_transition=(
                "episode_registered" if head is None else "manifest_advanced"
            ),
            event_payload=result,
            intervention_spec_digest=episode.intervention_spec_digest,
            destination_operation="agency.episode.updated",
        )

    @staticmethod
    def _validate_manifest_successor(
        prior: InterventionEpisode,
        successor: InterventionEpisode,
    ) -> None:
        if successor.kind != prior.kind or successor.created_at != prior.created_at:
            raise InvariantViolation(
                "INTERVENTION_EPISODE_IDENTITY",
                "episode kind or creation identity cannot change",
            )
        if successor.updated_at < prior.updated_at:
            raise InvariantViolation(
                "INTERVENTION_EPISODE_TIME",
                "episode update time cannot move backward",
            )
        if (
            prior.intervention_spec_digest is not None
            and successor.intervention_spec_digest != prior.intervention_spec_digest
        ):
            raise InvariantViolation(
                "INTERVENTION_SPEC_CONTINUITY",
                "episode cannot replace its immutable InterventionSpec digest",
            )
        previous = {link.stage: link for link in prior.stage_links}
        current = {link.stage: link for link in successor.stage_links}
        if not previous.keys() <= current.keys():
            raise InvariantViolation(
                "INTERVENTION_EPISODE_STAGE_LOSS",
                "episode update cannot silently remove a prior stage fate",
            )
        for stage, old in previous.items():
            new = current[stage]
            if old.fate is EpisodeStageFate.PRESENT and new != old:
                raise InvariantViolation(
                    "INTERVENTION_EPISODE_LINK_IMMUTABILITY",
                    f"present episode stage {stage} cannot be replaced",
                )

    @staticmethod
    async def _validate_stage_links(
        *, conn: asyncpg.Connection, episode: InterventionEpisode
    ) -> None:
        table_by_stage = {
            "proposal": ("consequential_proposals", "ProposalAppender"),
            "prediction": ("consequential_predictions", "PredictionWriter"),
            "authorization": (
                "consequential_authorization_decisions",
                "AuthorizationApplier",
            ),
            "outcome": ("consequential_outcomes", "OutcomeRecorder"),
            "settlement": ("consequential_settlements", "SettlementApplier"),
            "attribution": ("consequential_attributions", "AttributionApplier"),
        }
        for link in episode.stage_links:
            if (
                link.fate is not EpisodeStageFate.PRESENT
                or link.stage not in table_by_stage
            ):
                continue
            table, writer = table_by_stage[link.stage]
            if link.writer_id != writer:
                raise InvariantViolation(
                    "INTERVENTION_EPISODE_WRITER",
                    f"episode stage {link.stage} names the wrong semantic writer",
                )
            object_id = _object_ref_uuid(link.object_ref or "")
            row = await conn.fetchrow(
                f"SELECT episode_id FROM {table} WHERE tenant_id = $1 AND id = $2",
                episode.tenant_id,
                object_id,
            )
            if row is None or row["episode_id"] != episode.episode_id:
                raise InvariantViolation(
                    "INTERVENTION_EPISODE_SOURCE_LINK",
                    f"episode stage {link.stage} does not resolve to its source object",
                )


class PredictionWriter:
    async def register(
        self,
        *,
        conn: asyncpg.Connection,
        command: PredictionRegistrationCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        now = now or datetime.now(timezone.utc)
        context = command.context
        prediction = command.prediction
        ensure_live_context(context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="PredictionWriter",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        episode = await conn.fetchrow(
            """
            SELECT * FROM intervention_episode_heads
            WHERE tenant_id = $1 AND episode_id = $2
            FOR KEY SHARE
            """,
            context.tenant_id,
            prediction.episode_id,
        )
        if episode is None or episode["created_at"] > prediction.preregistered_at:
            raise InvariantViolation(
                "PREDICTION_EPISODE_ORDER",
                "episode identity must exist before prediction preregistration",
            )
        spec_digest = prediction.intervention_spec_digest
        if prediction.kind is PredictionKind.INTERVENTION_EFFECT:
            spec = await conn.fetchrow(
                """
                SELECT spec, episode_id FROM consequential_intervention_specs
                WHERE tenant_id = $1 AND spec_digest = $2
                FOR KEY SHARE
                """,
                context.tenant_id,
                spec_digest,
            )
            if spec is None or spec["episode_id"] != prediction.episode_id:
                raise InvariantViolation(
                    "PREDICTION_INTERVENTION_SPEC",
                    "intervention-effect prediction requires its episode's registered spec",
                )
            spec_payload = _json(spec["spec"])
            if (
                spec_payload["outcome_metric"] != prediction.metric_definition
                or datetime.fromisoformat(spec_payload["outcome_window_start"])
                != prediction.forecast_window_start
                or datetime.fromisoformat(spec_payload["outcome_window_end"])
                != prediction.forecast_window_end
            ):
                raise InvariantViolation(
                    "PREDICTION_MEASUREMENT_CONTRACT",
                    "prediction metric/window differs from its InterventionSpec",
                )
        leaked_outcome = await conn.fetchval(
            """
            SELECT count(*) FROM consequential_outcomes
            WHERE tenant_id = $1 AND episode_id = $2
              AND metric_definition = $3 AND observed_at <= $4
            """,
            context.tenant_id,
            prediction.episode_id,
            prediction.metric_definition,
            prediction.preregistered_at,
        )
        if leaked_outcome:
            raise InvariantViolation(
                "PREDICTION_OUTCOME_LEAKAGE",
                "prediction was registered after canonical outcome visibility",
            )
        existing = await conn.fetchrow(
            """
            SELECT id FROM consequential_predictions
            WHERE tenant_id = $1 AND (id = $2 OR prediction_digest = $3)
            """,
            context.tenant_id,
            prediction.prediction_id,
            prediction.prediction_digest,
        )
        if existing is not None:
            raise InvariantViolation(
                "PREDICTION_IMMUTABILITY",
                "prediction identity or digest already exists under another command",
            )
        ids = AgencyProtocolIds.new()
        result = {
            "prediction_id": str(prediction.prediction_id),
            "episode_id": str(prediction.episode_id),
            "prediction_digest": prediction.prediction_digest,
            "intervention_spec_digest": spec_digest,
            "preregistered_at": prediction.preregistered_at.isoformat(),
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="PredictionWriter",
            command_kind="register_prediction",
            command=command,
            request_digest=command.request_digest,
            object_type="prediction",
            object_id=prediction.prediction_id,
            object_version=1,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO consequential_predictions (
                id, tenant_id, episode_id, prediction_kind,
                prediction_digest, intervention_spec_digest,
                metric_definition, evidence_cutoff, forecast_window_start,
                forecast_window_end, preregistered_at, prediction,
                command_result_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12::jsonb, $13
            )
            """,
            prediction.prediction_id,
            context.tenant_id,
            prediction.episode_id,
            prediction.kind.value,
            prediction.prediction_digest,
            spec_digest,
            prediction.metric_definition,
            prediction.evidence_cutoff,
            prediction.forecast_window_start,
            prediction.forecast_window_end,
            prediction.preregistered_at,
            json.dumps(prediction.model_dump(mode="json")),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="PredictionWriter",
            object_type="prediction",
            object_id=prediction.prediction_id,
            object_version=1,
            semantic_transition="prediction_preregistered",
            event_payload=result,
            intervention_spec_digest=spec_digest,
            destination_operation="agency.prediction.registered",
        )


class AuthorizationApplier:
    async def apply(
        self,
        *,
        conn: asyncpg.Connection,
        command: AuthorizationDecisionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        now = now or datetime.now(timezone.utc)
        context = command.context
        decision = command.decision
        ensure_live_context(context, now=now)
        if (
            decision.disposition is AuthorizationDisposition.AUTHORIZED
            and not decision.authority.is_live(now)
        ):
            raise InvariantViolation(
                "AUTHORIZATION_AUTHORITY_EXPIRED",
                "authorization authority expired before decision commit",
            )
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="AuthorizationApplier",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        proposal = await conn.fetchrow(
            """
            SELECT p.*, s.spec
            FROM consequential_proposals p
            JOIN consequential_intervention_specs s
              ON s.tenant_id = p.tenant_id
             AND s.spec_id = p.intervention_spec_id
            WHERE p.tenant_id = $1 AND p.id = $2
            FOR KEY SHARE OF p, s
            """,
            context.tenant_id,
            decision.proposal_id,
        )
        if proposal is None:
            raise ValidationError(
                "consequential proposal not found",
                proposal_id=str(decision.proposal_id),
            )
        if (
            proposal["current_fate"]
            != ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION.value
            or proposal["proposal_digest"] != decision.proposal_digest
            or proposal["intervention_spec_digest"] != decision.intervention_spec_digest
        ):
            raise InvariantViolation(
                "AUTHORIZATION_EXACT_PROPOSAL",
                "authorization does not bind the exact accepted proposal and spec",
            )
        spec = _json(proposal["spec"])
        target = spec["target_referent"]
        target_ref = f"referent:{target['referent_id']}:v{target['referent_version']}"
        expected_fields = {f"parameters.{name}" for name in spec["parameters"]}
        if decision.disposition is AuthorizationDisposition.AUTHORIZED and (
            spec["operation"] not in decision.exact_operations
            or target_ref not in decision.exact_target_refs
            or not expected_fields <= decision.exact_field_paths
        ):
            raise InvariantViolation(
                "AUTHORIZATION_EXACT_SCOPE",
                "authorization omits the exact operation, target, or parameter fields",
            )
        existing = await conn.fetchrow(
            """
            SELECT id FROM consequential_authorization_decisions
            WHERE tenant_id = $1 AND (id = $2 OR decision_digest = $3)
            """,
            context.tenant_id,
            decision.decision_id,
            decision.decision_digest,
        )
        if existing is not None:
            raise InvariantViolation(
                "AUTHORIZATION_IMMUTABILITY",
                "authorization identity or digest already exists under another command",
            )
        ids = AgencyProtocolIds.new()
        result = {
            "decision_id": str(decision.decision_id),
            "proposal_id": str(decision.proposal_id),
            "decision_digest": decision.decision_digest,
            "intervention_spec_digest": decision.intervention_spec_digest,
            "disposition": decision.disposition.value,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="AuthorizationApplier",
            command_kind="apply_authorization_decision",
            command=command,
            request_digest=command.request_digest,
            object_type="authorization_decision",
            object_id=decision.decision_id,
            object_version=1,
            result=result,
            consumption_authority_fingerprint=decision.authority.fingerprint,
        )
        await conn.execute(
            """
            INSERT INTO consequential_authorization_decisions (
                id, tenant_id, episode_id, proposal_id, proposal_version, proposal_digest,
                intervention_spec_digest, decision_digest, disposition,
                authority_fingerprint, use_budget, attempt_budget, decision,
                command_result_id, decided_at, expires_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13::jsonb, $14, $15, $16
            )
            """,
            decision.decision_id,
            context.tenant_id,
            proposal["episode_id"],
            decision.proposal_id,
            proposal["proposal_version"],
            decision.proposal_digest,
            decision.intervention_spec_digest,
            decision.decision_digest,
            decision.disposition.value,
            decision.authority.fingerprint,
            decision.use_budget,
            decision.attempt_budget,
            json.dumps(decision.model_dump(mode="json")),
            ids.command_result_id,
            decision.decided_at,
            decision.expires_at,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="AuthorizationApplier",
            object_type="authorization_decision",
            object_id=decision.decision_id,
            object_version=1,
            semantic_transition=f"authorization_{decision.disposition.value}",
            event_payload=result,
            intervention_spec_digest=decision.intervention_spec_digest,
            destination_operation="agency.authorization.decided",
        )


class OutcomeRecorder:
    async def record(
        self,
        *,
        conn: asyncpg.Connection,
        command: OutcomeRecordingCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        now = now or datetime.now(timezone.utc)
        context = command.context
        outcome = command.outcome
        ensure_live_context(context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="OutcomeRecorder",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        episode = await conn.fetchrow(
            """
            SELECT episode_id FROM intervention_episode_heads
            WHERE tenant_id = $1 AND episode_id = $2
            FOR KEY SHARE
            """,
            context.tenant_id,
            outcome.episode_id,
        )
        if episode is None:
            raise InvariantViolation(
                "OUTCOME_EPISODE_REQUIRED",
                "Outcome requires a registered intervention episode",
            )
        existing = await conn.fetchrow(
            """
            SELECT id FROM consequential_outcomes
            WHERE tenant_id = $1 AND (id = $2 OR outcome_digest = $3)
            """,
            context.tenant_id,
            outcome.outcome_id,
            outcome.outcome_digest,
        )
        if existing is not None:
            raise InvariantViolation(
                "OUTCOME_IMMUTABILITY",
                "Outcome identity or digest already exists under another command",
            )
        ids = AgencyProtocolIds.new()
        result = {
            "outcome_id": str(outcome.outcome_id),
            "episode_id": str(outcome.episode_id),
            "outcome_digest": outcome.outcome_digest,
            "metric_definition": outcome.metric_definition,
            "independent_of_execution_claim": True,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="OutcomeRecorder",
            command_kind="record_independent_outcome",
            command=command,
            request_digest=command.request_digest,
            object_type="outcome",
            object_id=outcome.outcome_id,
            object_version=1,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO consequential_outcomes (
                id, tenant_id, episode_id, outcome_digest, metric_definition,
                observed_at, valid_time, independent_of_execution_claim,
                measurement_quality, outcome, command_result_id
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11
            )
            """,
            outcome.outcome_id,
            context.tenant_id,
            outcome.episode_id,
            outcome.outcome_digest,
            outcome.metric_definition,
            outcome.observed_at,
            outcome.valid_time,
            outcome.independent_of_execution_claim,
            outcome.measurement_quality,
            json.dumps(outcome.model_dump(mode="json")),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="OutcomeRecorder",
            object_type="outcome",
            object_id=outcome.outcome_id,
            object_version=1,
            semantic_transition="independent_outcome_recorded",
            event_payload=result,
            intervention_spec_digest=None,
            destination_operation="agency.outcome.recorded",
        )


class SettlementApplier:
    async def apply(
        self,
        *,
        conn: asyncpg.Connection,
        command: SettlementCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        now = now or datetime.now(timezone.utc)
        context = command.context
        settlement = command.settlement
        ensure_live_context(context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="SettlementApplier",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        prediction = await conn.fetchrow(
            """
            SELECT * FROM consequential_predictions
            WHERE tenant_id = $1 AND id = $2
            FOR KEY SHARE
            """,
            context.tenant_id,
            settlement.prediction_id,
        )
        if prediction is None:
            raise ValidationError(
                "consequential prediction not found",
                prediction_id=str(settlement.prediction_id),
            )
        if settlement.settled_at < prediction["forecast_window_start"]:
            raise InvariantViolation(
                "SETTLEMENT_TOO_EARLY",
                "prediction cannot settle before its forecast window begins",
            )
        outcome = None
        if settlement.outcome_id is not None:
            outcome = await conn.fetchrow(
                """
                SELECT * FROM consequential_outcomes
                WHERE tenant_id = $1 AND id = $2
                FOR KEY SHARE
                """,
                context.tenant_id,
                settlement.outcome_id,
            )
            if outcome is None:
                raise ValidationError(
                    "consequential Outcome not found",
                    outcome_id=str(settlement.outcome_id),
                )
            if (
                outcome["episode_id"] != prediction["episode_id"]
                or outcome["metric_definition"] != prediction["metric_definition"]
            ):
                raise InvariantViolation(
                    "SETTLEMENT_COMPARABILITY",
                    "Outcome episode or metric differs from Prediction",
                )
            if (
                outcome["created_at"] <= prediction["created_at"]
                or outcome["observed_at"] < prediction["preregistered_at"]
                or outcome["valid_time"] < prediction["evidence_cutoff"]
            ):
                raise InvariantViolation(
                    "SETTLEMENT_POSTDICTION",
                    "Outcome was visible or valid before the preregistered prediction boundary",
                )
        if settlement.disposition is SettlementDisposition.SETTLED and outcome is None:
            raise InvariantViolation(
                "SETTLEMENT_OUTCOME_REQUIRED",
                "settled prediction requires its independent Outcome",
            )
        existing = await conn.fetchrow(
            """
            SELECT id FROM consequential_settlements
            WHERE tenant_id = $1 AND (id = $2 OR prediction_id = $3)
            """,
            context.tenant_id,
            settlement.settlement_id,
            settlement.prediction_id,
        )
        if existing is not None:
            raise InvariantViolation(
                "SETTLEMENT_TERMINAL",
                "prediction already has an immutable terminal settlement",
            )
        episode_id = prediction["episode_id"]
        spec_digest = prediction["intervention_spec_digest"]
        ids = AgencyProtocolIds.new()
        result = {
            "settlement_id": str(settlement.settlement_id),
            "prediction_id": str(settlement.prediction_id),
            "outcome_id": str(settlement.outcome_id) if settlement.outcome_id else None,
            "episode_id": str(episode_id),
            "settlement_digest": settlement.settlement_digest,
            "disposition": settlement.disposition.value,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="SettlementApplier",
            command_kind="settle_prediction",
            command=command,
            request_digest=command.request_digest,
            object_type="settlement",
            object_id=settlement.settlement_id,
            object_version=1,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO consequential_settlements (
                id, tenant_id, episode_id, prediction_id, outcome_id,
                settlement_digest, disposition, settlement,
                command_result_id, settled_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10)
            """,
            settlement.settlement_id,
            context.tenant_id,
            episode_id,
            settlement.prediction_id,
            settlement.outcome_id,
            settlement.settlement_digest,
            settlement.disposition.value,
            json.dumps(settlement.model_dump(mode="json")),
            ids.command_result_id,
            settlement.settled_at,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="SettlementApplier",
            object_type="settlement",
            object_id=settlement.settlement_id,
            object_version=1,
            semantic_transition=f"prediction_{settlement.disposition.value}",
            event_payload=result,
            intervention_spec_digest=spec_digest,
            destination_operation="agency.settlement.applied",
        )


class AttributionApplier:
    async def apply(
        self,
        *,
        conn: asyncpg.Connection,
        command: AttributionCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        now = now or datetime.now(timezone.utc)
        context = command.context
        attribution = command.attribution
        ensure_live_context(context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="AttributionApplier",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        settlement = await conn.fetchrow(
            """
            SELECT s.*, p.intervention_spec_digest
            FROM consequential_settlements s
            JOIN consequential_predictions p ON p.id = s.prediction_id
            WHERE s.tenant_id = $1 AND s.id = $2
            FOR KEY SHARE OF s, p
            """,
            context.tenant_id,
            command.settlement_id,
        )
        if settlement is None:
            raise ValidationError(
                "consequential Settlement not found",
                settlement_id=str(command.settlement_id),
            )
        if settlement["disposition"] != SettlementDisposition.SETTLED.value:
            raise InvariantViolation(
                "ATTRIBUTION_UNSETTLED",
                "causal attribution requires a settled prediction/outcome comparison",
            )
        if attribution.episode_id != settlement["episode_id"]:
            raise InvariantViolation(
                "ATTRIBUTION_EPISODE",
                "Attribution episode differs from Settlement episode",
            )
        if str(command.settlement_id) not in attribution.evidence_refs:
            raise InvariantViolation(
                "ATTRIBUTION_EVIDENCE",
                "Attribution must cite the exact Settlement",
            )
        settlement_payload = _json(settlement["settlement"])
        residual = settlement_payload.get("residual_distribution") or {}
        nonidentifiable_mass = sum(
            float(residual.get(key, 0.0))
            for key in (
                ResidualClass.CONFOUNDING.value,
                ResidualClass.NON_IDENTIFIABLE.value,
            )
        )
        if nonidentifiable_mass >= 0.5 and not attribution.withheld_credit:
            raise InvariantViolation(
                "ATTRIBUTION_NON_IDENTIFIABLE",
                "majority confounding/non-identifiability requires withholding credit",
            )
        existing = await conn.fetchrow(
            """
            SELECT id FROM consequential_attributions
            WHERE tenant_id = $1
              AND (id = $2 OR (settlement_id = $3 AND subject_ref = $4))
            """,
            context.tenant_id,
            attribution.attribution_id,
            command.settlement_id,
            attribution.subject_ref,
        )
        if existing is not None:
            raise InvariantViolation(
                "ATTRIBUTION_IMMUTABILITY",
                "Settlement subject already has an immutable Attribution",
            )
        ids = AgencyProtocolIds.new()
        result = {
            "attribution_id": str(attribution.attribution_id),
            "settlement_id": str(command.settlement_id),
            "episode_id": str(attribution.episode_id),
            "attribution_digest": attribution.attribution_digest,
            "withheld_credit": attribution.withheld_credit,
            "causal_confidence": attribution.causal_confidence,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="AttributionApplier",
            command_kind="apply_attribution",
            command=command,
            request_digest=command.request_digest,
            object_type="attribution",
            object_id=attribution.attribution_id,
            object_version=1,
            result=result,
        )
        await conn.execute(
            """
            INSERT INTO consequential_attributions (
                id, tenant_id, episode_id, settlement_id,
                attribution_digest, subject_ref, causal_confidence,
                withheld_credit, attribution, command_result_id
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
            """,
            attribution.attribution_id,
            context.tenant_id,
            attribution.episode_id,
            command.settlement_id,
            attribution.attribution_digest,
            attribution.subject_ref,
            attribution.causal_confidence,
            attribution.withheld_credit,
            json.dumps(attribution.model_dump(mode="json")),
            ids.command_result_id,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="AttributionApplier",
            object_type="attribution",
            object_id=attribution.attribution_id,
            object_version=1,
            semantic_transition=(
                "causal_credit_withheld"
                if attribution.withheld_credit
                else "causal_credit_attributed"
            ),
            event_payload=result,
            intervention_spec_digest=settlement["intervention_spec_digest"],
            destination_operation="agency.attribution.applied",
        )


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    return dict(value)


def _object_ref_uuid(value: str) -> UUID:
    try:
        return UUID(value.rsplit(":", 1)[-1])
    except ValueError as exc:
        raise InvariantViolation(
            "INTERVENTION_EPISODE_OBJECT_REF",
            "known episode stage object reference must end in a UUID",
            object_ref=value,
        ) from exc


__all__ = [
    "AttributionApplier",
    "AuthorizationApplier",
    "EpisodeCoordinator",
    "OutcomeRecorder",
    "PredictionWriter",
    "SettlementApplier",
]
