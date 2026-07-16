"""Transactional writers for interpreted proposals and constituted intent.

`ProposalAppender` owns proposal identity and review fate. `IntentApplier` is
the only adapter allowed to turn an exact accepted/direct command into an Act
aggregate mutation. Both require a caller-owned transaction so legacy aggregate
state, constitutional lineage, command result, event, and outbox commit or roll
back together.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable
from uuid import UUID

import asyncpg

from lib.contracts.agency import (
    ConsequentialProposalFate,
    ConsequentialProposalRegistrationCommand,
    ConsequentialProposalReviewCommand,
    ConstitutiveIntentAuthorityBasisKind,
    ExactProposalAcceptance,
    IntentMutation,
    IntentOperation,
    IntentProposalFate,
    InterpretedIntentProposal,
    TypedConstitutiveIntentCommand,
)
from lib.contracts.kernel import canonical_sha256
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


@dataclass(frozen=True)
class AppliedIntentMutation:
    aggregate_id: UUID
    result_kind: str
    result_payload: dict[str, Any]


@dataclass(frozen=True)
class IntentApplyResult:
    command_result_id: UUID
    aggregate_id: UUID
    aggregate_version: int
    result_kind: str
    duplicate: bool = False


MutationApplier = Callable[
    [asyncpg.Connection, IntentMutation], Awaitable[AppliedIntentMutation]
]

_LEGACY_INTENT_TABLES = {
    "goal": "goals",
    "decision": "decisions",
    "commitment": "commitments",
}


_PROPOSAL_TRANSITIONS: dict[IntentProposalFate, frozenset[IntentProposalFate]] = {
    IntentProposalFate.OPEN: frozenset(
        {
            IntentProposalFate.DEFERRED,
            IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION,
            IntentProposalFate.REJECTED,
            IntentProposalFate.EXPIRED,
            IntentProposalFate.SUPERSEDED,
        }
    ),
    IntentProposalFate.DEFERRED: frozenset(
        {
            IntentProposalFate.OPEN,
            IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION,
            IntentProposalFate.REJECTED,
            IntentProposalFate.EXPIRED,
            IntentProposalFate.SUPERSEDED,
        }
    ),
    IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION: frozenset(
        {IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION}
    ),
    IntentProposalFate.REJECTED: frozenset({IntentProposalFate.REJECTED}),
    IntentProposalFate.EXPIRED: frozenset({IntentProposalFate.EXPIRED}),
    IntentProposalFate.SUPERSEDED: frozenset({IntentProposalFate.SUPERSEDED}),
}


class ProposalAppender:
    async def append_consequential(
        self,
        *,
        conn: asyncpg.Connection,
        command: ConsequentialProposalRegistrationCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        """Atomically register one action Proposal and its immutable spec."""

        now = now or datetime.now(timezone.utc)
        context = command.context
        proposal = command.proposal
        ensure_live_context(context, now=now)
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="ProposalAppender",
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
            proposal.episode_id,
        )
        if episode is None:
            raise InvariantViolation(
                "AGENCY_EPISODE_REQUIRED",
                "consequential proposal requires a registered episode identity",
            )
        existing_proposal = await conn.fetchrow(
            """
            SELECT id, proposal_digest FROM consequential_proposals
            WHERE tenant_id = $1 AND (id = $2 OR proposal_digest = $3)
            FOR UPDATE
            """,
            context.tenant_id,
            proposal.proposal_id,
            proposal.proposal_digest,
        )
        if existing_proposal is not None:
            raise InvariantViolation(
                "CONSEQUENTIAL_PROPOSAL_IDENTITY",
                "proposal identity or digest already exists under another command",
            )
        existing_spec = await conn.fetchrow(
            """
            SELECT spec_id, spec_digest, spec
            FROM consequential_intervention_specs
            WHERE tenant_id = $1 AND (spec_id = $2 OR spec_digest = $3)
            FOR KEY SHARE
            """,
            context.tenant_id,
            proposal.intervention_spec.spec_id,
            proposal.intervention_spec_digest,
        )
        spec_was_registered = existing_spec is None
        if existing_spec is not None:
            existing_payload = self._json(existing_spec["spec"])
            if (
                existing_spec["spec_id"] != proposal.intervention_spec.spec_id
                or existing_spec["spec_digest"] != proposal.intervention_spec_digest
                or existing_payload
                != proposal.intervention_spec.model_dump(mode="json")
            ):
                raise InvariantViolation(
                    "INTERVENTION_SPEC_IMMUTABILITY",
                    "InterventionSpec identity or digest was reused for different content",
                )

        ids = AgencyProtocolIds.new()
        result = {
            "proposal_id": str(proposal.proposal_id),
            "proposal_version": proposal.proposal_version,
            "proposal_digest": proposal.proposal_digest,
            "intervention_spec_id": str(proposal.intervention_spec.spec_id),
            "intervention_spec_digest": proposal.intervention_spec_digest,
            "spec_was_registered": spec_was_registered,
            "fate": ConsequentialProposalFate.OPEN.value,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="ProposalAppender",
            command_kind="register_consequential_proposal",
            command=command,
            request_digest=command.request_digest,
            object_type="consequential_proposal",
            object_id=proposal.proposal_id,
            object_version=1,
            result=result,
        )
        if spec_was_registered:
            await conn.execute(
                """
                INSERT INTO consequential_intervention_specs (
                    tenant_id, spec_id, spec_digest, episode_id,
                    registered_by_proposal_id, registered_by_proposal_version,
                    spec, command_result_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                """,
                context.tenant_id,
                proposal.intervention_spec.spec_id,
                proposal.intervention_spec_digest,
                proposal.episode_id,
                proposal.proposal_id,
                proposal.proposal_version,
                json.dumps(proposal.intervention_spec.model_dump(mode="json")),
                ids.command_result_id,
            )
        await conn.execute(
            """
            INSERT INTO consequential_proposals (
                id, tenant_id, proposal_version, proposal_digest, episode_id,
                intervention_spec_id, intervention_spec_digest, proposal,
                current_fate_version, current_fate, review_due_at,
                command_result_id, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::jsonb,
                1, 'open', $9, $10, $11
            )
            """,
            proposal.proposal_id,
            context.tenant_id,
            proposal.proposal_version,
            proposal.proposal_digest,
            proposal.episode_id,
            proposal.intervention_spec.spec_id,
            proposal.intervention_spec_digest,
            json.dumps(proposal.model_dump(mode="json")),
            proposal.review_due_at,
            ids.command_result_id,
            proposal.created_at,
        )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="ProposalAppender",
            object_type="consequential_proposal",
            object_id=proposal.proposal_id,
            object_version=1,
            semantic_transition="registered_open_with_intervention_spec",
            event_payload=result,
            intervention_spec_digest=proposal.intervention_spec_digest,
            destination_operation="agency.proposal.registered",
        )

    async def review_consequential(
        self,
        *,
        conn: asyncpg.Connection,
        command: ConsequentialProposalReviewCommand,
        now: datetime | None = None,
    ) -> AgencyCommitResult:
        """Apply one exact capable review without granting execution itself."""

        now = now or datetime.now(timezone.utc)
        context = command.context
        review = command.review
        ensure_live_context(context, now=now)
        if not review.authority.is_live(now):
            raise InvariantViolation(
                "CONSEQUENTIAL_PROPOSAL_REVIEW_AUTHORITY",
                "proposal review authority expired before commit",
            )
        prior = await prior_protocol_result(
            conn=conn,
            tenant_id=context.tenant_id,
            writer_id="ProposalAppender",
            idempotency_key=context.idempotency_key,
            request_digest=command.request_digest,
        )
        if prior is not None:
            return prior
        row = await conn.fetchrow(
            """
            SELECT * FROM consequential_proposals
            WHERE tenant_id = $1 AND id = $2
            FOR UPDATE
            """,
            context.tenant_id,
            review.proposal_id,
        )
        if row is None:
            raise ValidationError(
                "consequential proposal not found",
                proposal_id=str(review.proposal_id),
            )
        exact = (
            int(row["proposal_version"]) == review.proposal_version
            and row["proposal_digest"] == review.proposal_digest
            and row["intervention_spec_digest"] == review.intervention_spec_digest
            and row["current_fate"] == review.from_fate.value
        )
        if not exact:
            raise InvariantViolation(
                "CONSEQUENTIAL_PROPOSAL_EXACT_REVIEW",
                "review does not bind the exact current proposal/spec/fate",
            )
        if (
            review.to_fate is ConsequentialProposalFate.ACCEPTED_FOR_AUTHORIZATION
            and now >= row["review_due_at"]
        ):
            raise InvariantViolation(
                "CONSEQUENTIAL_PROPOSAL_EXPIRED",
                "proposal cannot be accepted after its review deadline",
            )
        if (
            review.to_fate is ConsequentialProposalFate.EXPIRED
            and now < row["review_due_at"]
        ):
            raise InvariantViolation(
                "CONSEQUENTIAL_PROPOSAL_PREMATURE_EXPIRY",
                "proposal cannot expire before its review deadline",
            )
        from_version = int(row["current_fate_version"])
        to_version = from_version + 1
        ids = AgencyProtocolIds.new()
        result = {
            "proposal_id": str(review.proposal_id),
            "proposal_version": review.proposal_version,
            "proposal_fate_version": to_version,
            "from_fate": review.from_fate.value,
            "to_fate": review.to_fate.value,
            "state": review.to_fate.value,
            "current_fate": review.to_fate.value,
            "proposal_digest": review.proposal_digest,
            "intervention_spec_digest": review.intervention_spec_digest,
        }
        await insert_protocol_result(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="ProposalAppender",
            command_kind="review_consequential_proposal",
            command=command,
            request_digest=command.request_digest,
            object_type="consequential_proposal",
            object_id=review.proposal_id,
            object_version=to_version,
            result=result,
            consumption_authority_fingerprint=review.authority.fingerprint,
        )
        await conn.execute(
            """
            INSERT INTO consequential_proposal_reviews (
                id, tenant_id, proposal_id, proposal_version,
                proposal_digest, intervention_spec_digest,
                from_fate_version, to_fate_version, from_fate, to_fate,
                principal_or_policy_ref, consumption_authority_fingerprint,
                review, command_result_id, decided_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13::jsonb, $14, $15
            )
            """,
            review.review_id,
            context.tenant_id,
            review.proposal_id,
            review.proposal_version,
            review.proposal_digest,
            review.intervention_spec_digest,
            from_version,
            to_version,
            review.from_fate.value,
            review.to_fate.value,
            review.principal_or_policy_ref,
            review.authority.fingerprint,
            json.dumps(review.model_dump(mode="json")),
            ids.command_result_id,
            review.decided_at,
        )
        updated = await conn.execute(
            """
            UPDATE consequential_proposals
            SET current_fate_version = $3, current_fate = $4, updated_at = now()
            WHERE tenant_id = $1 AND id = $2 AND current_fate_version = $5
            """,
            context.tenant_id,
            review.proposal_id,
            to_version,
            review.to_fate.value,
            from_version,
        )
        if updated != "UPDATE 1":
            raise InvariantViolation(
                "CONSEQUENTIAL_PROPOSAL_CAS",
                "proposal fate changed during exact review",
            )
        return await insert_protocol_event_and_outbox(
            conn=conn,
            ids=ids,
            context=context,
            writer_id="ProposalAppender",
            object_type="consequential_proposal",
            object_id=review.proposal_id,
            object_version=to_version,
            semantic_transition=f"{review.from_fate.value}_to_{review.to_fate.value}",
            event_payload=result,
            intervention_spec_digest=review.intervention_spec_digest,
            destination_operation="agency.proposal.reviewed",
        )

    async def append(
        self,
        *,
        conn: asyncpg.Connection,
        proposal: InterpretedIntentProposal,
        semantic_idempotency_key: str,
        actor_or_service_ref: str,
    ) -> tuple[InterpretedIntentProposal, bool]:
        if not semantic_idempotency_key.strip():
            raise ValidationError("proposal semantic idempotency key is required")
        proposal_payload = proposal.model_dump(mode="json")
        proposal_digest = canonical_sha256(proposal_payload)
        inserted = await conn.fetchrow(
            """
            INSERT INTO intent_proposals (
                id, tenant_id, proposal_version, semantic_idempotency_key,
                object_kind, operation, target_aggregate_id,
                normalized_payload_digest, proposal_digest,
                source_assertion_refs, grounding_dependency_refs,
                processing_authority_fingerprint, proposal, fate,
                review_due_at, created_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13::jsonb, $14, $15, $16
            )
            ON CONFLICT (tenant_id, semantic_idempotency_key) DO NOTHING
            RETURNING *
            """,
            proposal.proposal_id,
            proposal.tenant_id,
            proposal.proposal_version,
            semantic_idempotency_key,
            proposal.normalized_mutation.object_kind.value,
            proposal.normalized_mutation.operation.value,
            proposal.normalized_mutation.target_aggregate_id,
            proposal.normalized_payload_digest,
            proposal_digest,
            list(proposal.source_assertion_refs),
            list(proposal.grounding_dependency_refs),
            proposal.processing_authority_fingerprint,
            json.dumps(proposal_payload),
            proposal.fate.value,
            proposal.review_due_at,
            proposal.created_at,
        )
        if inserted is None:
            existing = await conn.fetchrow(
                """
                SELECT * FROM intent_proposals
                WHERE tenant_id = $1 AND semantic_idempotency_key = $2
                """,
                proposal.tenant_id,
                semantic_idempotency_key,
            )
            if existing is None:
                raise InvariantViolation(
                    "INTENT_PROPOSAL_IDEMPOTENCY",
                    "proposal conflict disappeared during idempotency check",
                )
            existing_payload = self._json(existing["proposal"])
            if self._semantic_proposal_digest(
                existing_payload
            ) != self._semantic_proposal_digest(proposal_payload):
                raise InvariantViolation(
                    "INTENT_PROPOSAL_IDEMPOTENCY",
                    "one proposal idempotency key was reused for different content",
                    semantic_idempotency_key=semantic_idempotency_key,
                )
            return self._hydrate(existing), False

        await self._append_fate_event(
            conn=conn,
            tenant_id=proposal.tenant_id,
            proposal_id=proposal.proposal_id,
            proposal_version=proposal.proposal_version,
            from_fate=None,
            to_fate=proposal.fate,
            reason_class="proposal_registered",
            reason="interpreted direction registered for governed review",
            actor_or_service_ref=actor_or_service_ref,
        )
        return proposal, True

    async def get(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        proposal_id: UUID,
        for_update: bool = False,
    ) -> InterpretedIntentProposal | None:
        lock = " FOR UPDATE" if for_update else ""
        row = await conn.fetchrow(
            f"""
            SELECT * FROM intent_proposals
            WHERE tenant_id = $1 AND id = $2
            {lock}
            """,
            tenant_id,
            proposal_id,
        )
        return self._hydrate(row) if row else None

    async def transition(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        proposal_id: UUID,
        to_fate: IntentProposalFate,
        reason_class: str,
        reason: str,
        actor_or_service_ref: str,
    ) -> InterpretedIntentProposal:
        current = await self.get(
            conn=conn,
            tenant_id=tenant_id,
            proposal_id=proposal_id,
            for_update=True,
        )
        if current is None:
            raise ValidationError(
                "intent proposal not found", proposal_id=str(proposal_id)
            )
        if to_fate not in _PROPOSAL_TRANSITIONS[current.fate]:
            raise InvariantViolation(
                "INTENT_PROPOSAL_STATE",
                "illegal intent proposal fate transition",
                from_fate=current.fate.value,
                to_fate=to_fate.value,
            )
        if current.fate is to_fate:
            return current
        await conn.execute(
            """
            UPDATE intent_proposals
            SET fate = $3, updated_at = now()
            WHERE tenant_id = $1 AND id = $2
            """,
            tenant_id,
            proposal_id,
            to_fate.value,
        )
        await self._append_fate_event(
            conn=conn,
            tenant_id=tenant_id,
            proposal_id=proposal_id,
            proposal_version=current.proposal_version,
            from_fate=current.fate,
            to_fate=to_fate,
            reason_class=reason_class,
            reason=reason,
            actor_or_service_ref=actor_or_service_ref,
        )
        return current.model_copy(update={"fate": to_fate})

    async def accept_exact(
        self,
        *,
        conn: asyncpg.Connection,
        acceptance: ExactProposalAcceptance,
    ) -> ExactProposalAcceptance:
        proposal = await self.get(
            conn=conn,
            tenant_id=acceptance.tenant_id,
            proposal_id=acceptance.proposal_id,
            for_update=True,
        )
        if proposal is None:
            raise ValidationError(
                "intent proposal not found", proposal_id=str(acceptance.proposal_id)
            )
        if datetime.now(timezone.utc) >= proposal.review_due_at:
            await self.transition(
                conn=conn,
                tenant_id=acceptance.tenant_id,
                proposal_id=acceptance.proposal_id,
                to_fate=IntentProposalFate.EXPIRED,
                reason_class="review_deadline_elapsed",
                reason="proposal expired before exact acceptance",
                actor_or_service_ref=acceptance.principal_id,
            )
            raise ValidationError("intent proposal expired before acceptance")
        if not acceptance.accepts(proposal):
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE",
                "acceptance does not bind the exact open proposal and payload",
            )
        inserted = await conn.fetchrow(
            """
            INSERT INTO intent_exact_acceptances (
                id, tenant_id, proposal_id, proposal_version,
                proposal_digest, normalized_payload_digest, principal_id,
                capability_ref, authority_fingerprint, acceptance,
                accepted_at, expires_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12
            )
            ON CONFLICT (tenant_id, proposal_id, proposal_version) DO NOTHING
            RETURNING id
            """,
            acceptance.acceptance_id,
            acceptance.tenant_id,
            acceptance.proposal_id,
            acceptance.proposal_version,
            acceptance.proposal_digest,
            acceptance.normalized_payload_digest,
            acceptance.principal_id,
            acceptance.capability_ref,
            acceptance.authority.fingerprint,
            json.dumps(acceptance.model_dump(mode="json")),
            acceptance.accepted_at,
            acceptance.expires_at,
        )
        if inserted is None:
            existing = await conn.fetchrow(
                """
                SELECT acceptance FROM intent_exact_acceptances
                WHERE tenant_id = $1 AND proposal_id = $2 AND proposal_version = $3
                """,
                acceptance.tenant_id,
                acceptance.proposal_id,
                acceptance.proposal_version,
            )
            existing_acceptance = ExactProposalAcceptance.model_validate(
                self._json(existing["acceptance"])
            )
            if existing_acceptance != acceptance:
                raise InvariantViolation(
                    "INTENT_EXACT_ACCEPTANCE",
                    "proposal already has a different immutable acceptance",
                )
            return existing_acceptance
        await self.transition(
            conn=conn,
            tenant_id=acceptance.tenant_id,
            proposal_id=acceptance.proposal_id,
            to_fate=IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION,
            reason_class="exact_principal_acceptance",
            reason="principal accepted exact proposal version and payload digest",
            actor_or_service_ref=acceptance.principal_id,
        )
        return acceptance

    async def _append_fate_event(
        self,
        *,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        proposal_id: UUID,
        proposal_version: int,
        from_fate: IntentProposalFate | None,
        to_fate: IntentProposalFate,
        reason_class: str,
        reason: str,
        actor_or_service_ref: str,
    ) -> None:
        event_payload = {
            "proposal_id": str(proposal_id),
            "proposal_version": proposal_version,
            "from_fate": from_fate.value if from_fate else None,
            "to_fate": to_fate.value,
            "reason_class": reason_class,
            "reason": reason,
            "actor_or_service_ref": actor_or_service_ref,
        }
        await conn.execute(
            """
            INSERT INTO intent_proposal_fate_events (
                id, tenant_id, proposal_id, proposal_version, from_fate,
                to_fate, reason_class, reason, actor_or_service_ref, event_digest
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (
                tenant_id, proposal_id, proposal_version, event_digest
            ) DO NOTHING
            """,
            uuid7(),
            tenant_id,
            proposal_id,
            proposal_version,
            from_fate.value if from_fate else None,
            to_fate.value,
            reason_class,
            reason,
            actor_or_service_ref,
            canonical_sha256(event_payload),
        )

    @classmethod
    def _hydrate(cls, row: asyncpg.Record) -> InterpretedIntentProposal:
        payload = cls._json(row["proposal"])
        payload["fate"] = row["fate"]
        return InterpretedIntentProposal.model_validate(payload)

    @staticmethod
    def _json(value: Any) -> Any:
        return json.loads(value) if isinstance(value, str) else value

    @staticmethod
    def _semantic_proposal_digest(payload: dict[str, Any]) -> str:
        semantic = dict(payload)
        semantic.pop("proposal_id", None)
        return canonical_sha256(semantic)


async def ensure_legacy_intent_baseline(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    object_kind: str,
    aggregate_id: UUID,
) -> int:
    """Capture one honest version-1 cutover head for a pre-protocol Act."""

    existing = await conn.fetchval(
        """
        SELECT current_version
        FROM intent_aggregate_heads
        WHERE tenant_id = $1 AND object_kind = $2 AND aggregate_id = $3
        FOR UPDATE
        """,
        tenant_id,
        object_kind,
        aggregate_id,
    )
    if existing is not None:
        return int(existing)
    table = _LEGACY_INTENT_TABLES.get(object_kind)
    if table is None:
        raise ValidationError(
            "legacy intent cutover is unsupported for this object kind",
            object_kind=object_kind,
        )
    snapshot = await conn.fetchval(
        f"""
        SELECT to_jsonb(row_value)
        FROM {table} AS row_value
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        tenant_id,
        aggregate_id,
    )
    if snapshot is None:
        raise ValidationError(
            "legacy intent target not found",
            object_kind=object_kind,
            aggregate_id=str(aggregate_id),
        )
    snapshot = ProposalAppender._json(snapshot)
    digest = canonical_sha256(snapshot)
    await conn.execute(
        """
        INSERT INTO intent_legacy_baselines (
            tenant_id, object_kind, aggregate_id, baseline_payload_digest,
            source_table, source_snapshot
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        ON CONFLICT (tenant_id, object_kind, aggregate_id) DO NOTHING
        """,
        tenant_id,
        object_kind,
        aggregate_id,
        digest,
        table,
        json.dumps(snapshot),
    )
    await conn.execute(
        """
        INSERT INTO intent_aggregate_heads (
            tenant_id, object_kind, aggregate_id, current_version,
            current_payload_digest, current_fate
        ) VALUES ($1, $2, $3, 1, $4, 'current')
        ON CONFLICT (tenant_id, object_kind, aggregate_id) DO NOTHING
        """,
        tenant_id,
        object_kind,
        aggregate_id,
        digest,
    )
    version = await conn.fetchval(
        """
        SELECT current_version
        FROM intent_aggregate_heads
        WHERE tenant_id = $1 AND object_kind = $2 AND aggregate_id = $3
        FOR UPDATE
        """,
        tenant_id,
        object_kind,
        aggregate_id,
    )
    if version is None:
        raise InvariantViolation(
            "INTENT_LEGACY_BASELINE",
            "legacy intent baseline failed to establish a head",
        )
    return int(version)


class IntentApplier:
    """Validate and atomically record one exact constitutive mutation."""

    async def apply(
        self,
        *,
        conn: asyncpg.Connection,
        command: TypedConstitutiveIntentCommand,
        mutation_applier: MutationApplier,
        now: datetime | None = None,
    ) -> IntentApplyResult:
        now = now or datetime.now(timezone.utc)
        self._validate_live_command(command=command, now=now)

        prior = await conn.fetchrow(
            """
            SELECT * FROM intent_command_results
            WHERE tenant_id = $1 AND semantic_idempotency_key = $2
            FOR UPDATE
            """,
            command.tenant_id,
            command.idempotency_key,
        )
        if prior is not None:
            if prior["request_digest"] != command.request_digest:
                raise InvariantViolation(
                    "INTENT_COMMAND_IDEMPOTENCY",
                    "one intent idempotency key was reused for a different request",
                )
            if prior["status"] not in {"applied", "duplicate"}:
                raise InvariantViolation(
                    "INTENT_COMMAND_PRIOR_FATE",
                    "prior intent command did not produce a reusable applied result",
                    prior_status=prior["status"],
                )
            return IntentApplyResult(
                command_result_id=prior["id"],
                aggregate_id=prior["aggregate_id"],
                aggregate_version=prior["aggregate_version"],
                result_kind=self._json(prior["result"])["result_kind"],
                duplicate=True,
            )

        acceptance_id = await self._validate_acceptance(
            conn=conn, command=command, now=now
        )
        await self._validate_grounding(conn=conn, command=command, now=now)
        expected_next_version = await self._expected_next_version(
            conn=conn, command=command
        )

        applied = await mutation_applier(conn, command.mutation)
        if command.mutation.operation is not IntentOperation.CREATE:
            if applied.aggregate_id != command.mutation.target_aggregate_id:
                raise InvariantViolation(
                    "INTENT_TARGET_CONTINUITY",
                    "mutation adapter changed the authorized target aggregate",
                )

        result_id = uuid7()
        result_payload = {
            "result_kind": applied.result_kind,
            "result_payload": applied.result_payload,
            "aggregate_id": str(applied.aggregate_id),
            "aggregate_version": expected_next_version,
        }
        await conn.execute(
            """
            INSERT INTO intent_command_results (
                id, tenant_id, command_id, semantic_idempotency_key,
                request_digest, mutation_payload_digest, object_kind,
                operation, status, proposal_acceptance_id, authority_basis,
                survival_policy, grounding_dependencies, writer_scope_id,
                writer_epoch, aggregate_id, aggregate_version, result,
                command, processing_authority_fingerprint,
                consumption_authority_fingerprint, authority_capture_status
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, 'applied', $9,
                $10::jsonb, $11::jsonb, $12::jsonb, $13, $14, $15, $16,
                $17::jsonb, $18::jsonb, $19, $20, 'complete'
            )
            """,
            result_id,
            command.tenant_id,
            command.command_id,
            command.idempotency_key,
            command.request_digest,
            command.declared_payload_digest,
            command.mutation.object_kind.value,
            command.mutation.operation.value,
            acceptance_id,
            json.dumps(command.authority_basis.model_dump(mode="json")),
            json.dumps(command.survival_policy.model_dump(mode="json")),
            json.dumps(
                [
                    item.model_dump(mode="json")
                    for item in command.grounding_dependencies
                ]
            ),
            command.writer_scope_epoch.scope_id,
            command.writer_scope_epoch.epoch,
            applied.aggregate_id,
            expected_next_version,
            json.dumps(result_payload),
            json.dumps(command.model_dump(mode="json")),
            command.processing_authority.fingerprint,
            command.consumption_authority.fingerprint,
        )
        await conn.execute(
            """
            INSERT INTO intent_versions (
                id, tenant_id, object_kind, aggregate_id, aggregate_version,
                operation, mutation_payload_digest, command_result_id,
                proposal_acceptance_id, authority_basis_snapshot,
                survival_policy, grounding_dependencies, effective_at, fate
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb,
                $11::jsonb, $12::jsonb, $13, 'current'
            )
            """,
            uuid7(),
            command.tenant_id,
            command.mutation.object_kind.value,
            applied.aggregate_id,
            expected_next_version,
            command.mutation.operation.value,
            command.declared_payload_digest,
            result_id,
            acceptance_id,
            json.dumps(command.authority_basis.model_dump(mode="json")),
            json.dumps(command.survival_policy.model_dump(mode="json")),
            json.dumps(
                [
                    item.model_dump(mode="json")
                    for item in command.grounding_dependencies
                ]
            ),
            command.mutation.effective_at,
        )
        await conn.execute(
            """
            INSERT INTO intent_aggregate_heads (
                tenant_id, object_kind, aggregate_id, current_version,
                current_payload_digest, current_fate
            ) VALUES ($1, $2, $3, $4, $5, 'current')
            ON CONFLICT (tenant_id, object_kind, aggregate_id) DO UPDATE SET
                current_version = EXCLUDED.current_version,
                current_payload_digest = EXCLUDED.current_payload_digest,
                current_fate = EXCLUDED.current_fate,
                updated_at = now()
            WHERE intent_aggregate_heads.current_version = EXCLUDED.current_version - 1
            """,
            command.tenant_id,
            command.mutation.object_kind.value,
            applied.aggregate_id,
            expected_next_version,
            command.declared_payload_digest,
        )
        head_version = await conn.fetchval(
            """
            SELECT current_version FROM intent_aggregate_heads
            WHERE tenant_id = $1 AND object_kind = $2 AND aggregate_id = $3
            """,
            command.tenant_id,
            command.mutation.object_kind.value,
            applied.aggregate_id,
        )
        if head_version != expected_next_version:
            raise InvariantViolation(
                "INTENT_AGGREGATE_CAS",
                "intent aggregate head did not advance exactly one version",
            )

        event_id = uuid7()
        event_payload = {
            "command_result_id": str(result_id),
            "object_kind": command.mutation.object_kind.value,
            "aggregate_id": str(applied.aggregate_id),
            "aggregate_version": expected_next_version,
            "semantic_transition": command.mutation.operation.value,
            "mutation_payload_digest": command.declared_payload_digest,
        }
        await conn.execute(
            """
            INSERT INTO intent_canonical_events (
                id, tenant_id, command_result_id, object_kind, aggregate_id,
                aggregate_version, semantic_transition, event_payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            """,
            event_id,
            command.tenant_id,
            result_id,
            command.mutation.object_kind.value,
            applied.aggregate_id,
            expected_next_version,
            command.mutation.operation.value,
            json.dumps(event_payload),
        )
        outbox_payload = event_payload | {"event_id": str(event_id)}
        await conn.execute(
            """
            INSERT INTO intent_outbox_records (
                id, tenant_id, event_id, destination_operation, payload_hash,
                payload, deadline, attempt_budget
            ) VALUES ($1, $2, $3, 'trace_and_repair', $4, $5::jsonb, $6, 8)
            """,
            uuid7(),
            command.tenant_id,
            event_id,
            canonical_sha256(outbox_payload),
            json.dumps(outbox_payload),
            now + timedelta(days=7),
        )
        return IntentApplyResult(
            command_result_id=result_id,
            aggregate_id=applied.aggregate_id,
            aggregate_version=expected_next_version,
            result_kind=applied.result_kind,
        )

    @staticmethod
    def _validate_live_command(
        *, command: TypedConstitutiveIntentCommand, now: datetime
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValidationError("intent apply time must be timezone-aware")
        if now >= command.expires_at:
            raise ValidationError("intent command expired")
        scope = command.writer_scope_epoch
        if not scope.permits(
            writer_owner="IntentApplier",
            epoch=scope.epoch,
            tenant_id=command.tenant_id,
            semantic_responsibility="intent",
            source_partition=scope.source_partition,
        ):
            raise InvariantViolation(
                "INTENT_WRITER_SCOPE",
                "command is not addressed to the live IntentApplier scope",
            )
        object_kind = command.mutation.object_kind.value
        operation = command.mutation.operation.value
        for authority in (command.processing_authority, command.consumption_authority):
            if authority.operation != operation:
                raise InvariantViolation(
                    "INTENT_AUTHORITY_OPERATION",
                    "authority operation does not match intent operation",
                )
            if not authority.object_types.permits(object_kind):
                raise InvariantViolation(
                    "INTENT_AUTHORITY_OBJECT",
                    "authority does not permit this intent object kind",
                )
            if not authority.is_live(now):
                raise InvariantViolation(
                    "INTENT_AUTHORITY_LIVE",
                    "intent authority is no longer live at apply time",
                )
        if command.mutation.target_aggregate_id is not None:
            target = str(command.mutation.target_aggregate_id)
            if not command.consumption_authority.object_ids.permits(target):
                raise InvariantViolation(
                    "INTENT_AUTHORITY_TARGET",
                    "consumption authority does not permit the exact target",
                )

    @staticmethod
    async def _validate_acceptance(
        *,
        conn: asyncpg.Connection,
        command: TypedConstitutiveIntentCommand,
        now: datetime,
    ) -> UUID | None:
        if command.proposal_acceptance_ref is None:
            return None
        if (
            command.authority_basis.kind
            is not ConstitutiveIntentAuthorityBasisKind.EXPLICIT_PRINCIPAL
        ):
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE",
                "accepted interpreted proposals require an explicit-principal basis",
            )
        try:
            acceptance_id = UUID(command.proposal_acceptance_ref)
        except ValueError as exc:
            raise ValidationError(
                "proposal acceptance reference must be a UUID"
            ) from exc
        row = await conn.fetchrow(
            """
            SELECT a.*, p.fate, p.proposal_digest AS current_proposal_digest,
                   p.normalized_payload_digest AS current_payload_digest
            FROM intent_exact_acceptances a
            JOIN intent_proposals p
              ON p.tenant_id = a.tenant_id
             AND p.id = a.proposal_id
             AND p.proposal_version = a.proposal_version
            WHERE a.tenant_id = $1 AND a.id = $2
            FOR UPDATE OF p
            """,
            command.tenant_id,
            acceptance_id,
        )
        if row is None:
            raise ValidationError("exact proposal acceptance not found")
        if row["fate"] != IntentProposalFate.ACCEPTED_FOR_AUTHORIZATION.value:
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE",
                "proposal is not in its exact accepted terminal fate",
            )
        if row["proposal_digest"] != row["current_proposal_digest"]:
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE", "proposal digest changed"
            )
        if row["normalized_payload_digest"] != command.declared_payload_digest:
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE", "acceptance binds a different intent payload"
            )
        if row["expires_at"] <= now:
            raise ValidationError("exact proposal acceptance expired")
        if row["principal_id"] != command.authority_basis.principal_or_actor_id:
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE", "acceptance principal changed before apply"
            )
        if row["capability_ref"] != command.authority_basis.capability_or_grant_ref:
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE", "acceptance capability changed before apply"
            )
        if row["authority_fingerprint"] != command.consumption_authority.fingerprint:
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE", "acceptance authority does not match command"
            )
        if (
            command.authority_basis.acknowledged_payload_digest
            != row["normalized_payload_digest"]
        ):
            raise InvariantViolation(
                "INTENT_EXACT_ACCEPTANCE",
                "constitutive basis does not bind acceptance digest",
            )
        return acceptance_id

    @staticmethod
    async def _validate_grounding(
        *,
        conn: asyncpg.Connection,
        command: TypedConstitutiveIntentCommand,
        now: datetime,
    ) -> None:
        for dependency in command.grounding_dependencies:
            try:
                decision_id = UUID(dependency.grounding_admission_decision_id)
            except ValueError as exc:
                raise ValidationError(
                    "grounding admission reference must be a UUID"
                ) from exc
            row = await conn.fetchrow(
                """
                SELECT decision_version, purpose, risk_tier, disposition,
                       selected_referent, expires_at
                FROM grounding_admission_decisions
                WHERE tenant_id = $1 AND id = $2
                """,
                command.tenant_id,
                decision_id,
            )
            if row is None:
                raise ValidationError(
                    "intent grounding admission not found",
                    semantic_role=dependency.semantic_role,
                )
            if (
                row["decision_version"] != dependency.grounding_admission_version
                or row["purpose"] != "intent_mutation"
                or row["risk_tier"] != dependency.risk_tier
                or row["disposition"] != "single_referent"
                or row["expires_at"] <= now
                or IntentApplier._json(row["selected_referent"])
                != dependency.selected_referent.model_dump(mode="json")
            ):
                raise InvariantViolation(
                    "INTENT_GROUNDING_ADMISSION",
                    "grounding admission is stale, mismatched, or insufficient for intent",
                    semantic_role=dependency.semantic_role,
                )

    @staticmethod
    async def _expected_next_version(
        *, conn: asyncpg.Connection, command: TypedConstitutiveIntentCommand
    ) -> int:
        mutation = command.mutation
        if mutation.operation is IntentOperation.CREATE:
            return 1
        row = await conn.fetchrow(
            """
            SELECT current_version FROM intent_aggregate_heads
            WHERE tenant_id = $1 AND object_kind = $2 AND aggregate_id = $3
            FOR UPDATE
            """,
            command.tenant_id,
            mutation.object_kind.value,
            mutation.target_aggregate_id,
        )
        if row is None:
            raise InvariantViolation(
                "INTENT_AGGREGATE_VERSION",
                "non-create intent target has no governed version head",
            )
        if mutation.expected_target_version != row["current_version"]:
            raise InvariantViolation(
                "INTENT_AGGREGATE_VERSION",
                "intent target version changed before apply",
                expected=mutation.expected_target_version,
                current=row["current_version"],
            )
        return int(row["current_version"]) + 1

    @staticmethod
    def _json(value: Any) -> Any:
        return json.loads(value) if isinstance(value, str) else value


__all__ = [
    "AppliedIntentMutation",
    "IntentApplyResult",
    "IntentApplier",
    "MutationApplier",
    "ProposalAppender",
    "ensure_legacy_intent_baseline",
]
