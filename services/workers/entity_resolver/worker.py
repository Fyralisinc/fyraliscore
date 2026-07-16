"""services/workers/entity_resolver/worker.py — resolver loop.

BUILD-PLAN §3 Prompt 2.B item 2, ARCHITECTURE §15.

Process (single unresolved phrase):
    1. Load an as-known source-structured context. Slack context is bounded by
       actual channel and reply/edit topology before temporal alternatives;
       prior Fyralis Models are never identity corroboration.
    2. Construct a tenant-authorized closed candidate population and ask the
       model to select one candidate ID or return no known referent.
    3. Mechanically reject any model-invented ID/ref.
    4. Atomically append context snapshot, candidate request/set, evidence-
       relative assessment, consumer-specific admission, and total fate.
    5. Route material ambiguity to review. The resolver never writes aliases,
       rewrites the source Observation, or emits its conclusion as evidence.

Input sources
-------------

Two modes, selectable at construction time:
    - LISTEN mode (default when asyncpg.Pool is given): SUBSCRIBE
      to `observations_new` channel; on each wakeup, scan the
      referenced observation for unresolved phrases emitted by ingestion.
    - POLL mode: a scheduled `process_pending()` walks the last N
      observations since a watermark. Used by the end-to-end test
      fixture + by operators manually re-running resolution.

Both modes call `process_observation()`, which is the unit of work.
Tests invoke `process_observation()` directly.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg
import structlog

from lib.contracts import EntityMentionDetectionFate
from lib.llm.provider import (
    LLMError,
    LLMParseError,
    LLMProvider,
    LLMRateLimitError,
    LLMTimeoutError,
)
from lib.shared.ids import uuid7
from services.domain.triggers import enqueue_trigger
from services.domain.clarifications import open_clarification_request
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.domain.entity_grounding import (
    ContextObservationInput,
    EntityGroundingRepo,
    GroundingCandidateInput,
    GroundingEpisode,
    build_grounding_episode,
)
from services.domain.source_semantics.processor import GroundedBeliefProcessor
from services.workers.entity_resolver.context import (
    ResolverContext,
    build_context,
)
from pydantic import BaseModel, Field


_log = structlog.get_logger(__name__)


# Confidence thresholds per prompt. Making them instance-level so tests
# can override without re-instantiating every module.
HIGH_CONFIDENCE = 0.8
REVIEW_MIN = 0.5

# Types whose late-resolution "materially changes context" and triggers
# a T1 re-enqueue per ARCHITECTURE §15.
_TRIGGER_REENQUEUE_TYPES = frozenset(("customer", "commitment", "goal"))


# =====================================================================
# LLM schema (Pydantic) — what the resolver prompt returns.
# =====================================================================

class EntityResolution(BaseModel):
    """What the resolver LLM returns per phrase.

    canonical_ref is a JSON object like {"type": "...", "id": "..."}
    or None when the phrase does not resolve to a known entity.
    """

    candidate_id: str | None = None
    canonical_ref: dict[str, Any] | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""


# =====================================================================
# Rate limit — per-tenant token bucket
# =====================================================================

ResolverDecision = Literal[
    "resolved",
    "review",
    "unresolved",
    "retryable",
    "rate_limited",
]


@dataclass
class _Bucket:
    capacity: int
    tokens: float
    refilled_at: float

    def take(self, now: float, refill_per_s: float) -> bool:
        elapsed = now - self.refilled_at
        self.tokens = min(self.capacity, self.tokens + elapsed * refill_per_s)
        self.refilled_at = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True
        return False


class ResolverLLMBudget:
    """Token bucket: budget per minute, per tenant.

    Env:
        ENTITY_RESOLVER_LLM_BUDGET_PER_MIN (default: 30)

    Thread-safety: the worker is single-asyncio-task; no lock needed.
    """

    def __init__(self, per_minute: int = 30):
        self.per_minute = per_minute
        self.refill_per_s = per_minute / 60.0
        self._buckets: dict[UUID, _Bucket] = {}

    def check_and_consume(self, tenant_id: UUID) -> bool:
        """Returns True if a token was consumed; False if rate-limited."""
        now = time.monotonic()
        b = self._buckets.get(tenant_id)
        if b is None:
            b = _Bucket(
                capacity=self.per_minute,
                tokens=float(self.per_minute),
                refilled_at=now,
            )
            self._buckets[tenant_id] = b
        return b.take(now, self.refill_per_s)


# =====================================================================
# The worker
# =====================================================================

class EntityResolverWorker:
    """Runs in two modes — LISTEN or POLL. See module docstring."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        llm: LLMProvider,
        alias_repo: EntityAliasRepo,
        grounding_repo: EntityGroundingRepo | None = None,
        grounded_belief_processor: GroundedBeliefProcessor | None = None,
        budget: ResolverLLMBudget | None = None,
        high_confidence: float = HIGH_CONFIDENCE,
        review_min: float = REVIEW_MIN,
        logger: Any | None = None,
    ) -> None:
        self._pool = pool
        self._llm = llm
        # Retained in the constructor during the semantic strangulation window
        # so existing launch wiring remains compatible. Resolver model output
        # is no longer permitted to call this registry writer.
        del alias_repo
        self._grounding_repo = grounding_repo or EntityGroundingRepo(pool)
        self._grounded_belief_processor = (
            grounded_belief_processor or GroundedBeliefProcessor()
        )
        self._budget = budget or ResolverLLMBudget()
        self._high = high_confidence
        self._review = review_min
        self._log = logger or _log
        self._retry_after: dict[UUID, float] = {}
        # Observation -> number of requeues (for backoff).
        self._requeue_count: dict[UUID, int] = {}

    # -----------------------------------------------------------------
    # Unit of work: process one Observation's unresolved phrases.
    # -----------------------------------------------------------------

    async def process_observation(
        self,
        observation_id: UUID,
        tenant_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[tuple[str, ResolverDecision]]:
        """Process every unresolved phrase attached to an observation.

        Returns a list of (phrase, decision) pairs for observability.
        """
        phrases = await self._load_unresolved_phrases(
            observation_id, tenant_id, conn=conn
        )
        if not phrases:
            return []

        results: list[tuple[str, ResolverDecision]] = []
        for phrase in phrases:
            try:
                decision = await self._process_phrase(
                    phrase=phrase,
                    observation_id=observation_id,
                    tenant_id=tenant_id,
                    conn=conn,
                )
            except LLMRateLimitError:
                self._log.warning(
                    "entity_resolver.phrase_rate_limited",
                    phrase=phrase,
                    observation_id=str(observation_id),
                )
                self._bump_requeue(observation_id)
                await self._record_retryable_fate(
                    tenant_id=tenant_id,
                    observation_id=observation_id,
                    phrase=phrase,
                    failure_class="provider_rate_limited",
                    failure_reason="LLM provider rate limit",
                    conn=conn,
                )
                decision = "rate_limited"
            except LLMTimeoutError:
                self._log.warning(
                    "entity_resolver.phrase_timeout",
                    phrase=phrase,
                    observation_id=str(observation_id),
                )
                self._bump_requeue(observation_id)
                await self._record_retryable_fate(
                    tenant_id=tenant_id,
                    observation_id=observation_id,
                    phrase=phrase,
                    failure_class="provider_timeout",
                    failure_reason="LLM provider timeout",
                    conn=conn,
                )
                decision = "rate_limited"   # treat as requeue
            except LLMParseError as e:
                # Provider-local retries are exhausted, but the durable work
                # item remains open. A transient model-format failure is not a
                # terminal entity fate.
                self._log.error(
                    "entity_resolver.phrase_llm_parse_exhausted",
                    phrase=phrase,
                    observation_id=str(observation_id),
                    error=str(e),
                )
                self._bump_requeue(observation_id)
                await self._record_retryable_fate(
                    tenant_id=tenant_id,
                    observation_id=observation_id,
                    phrase=phrase,
                    failure_class="provider_parse_exhausted",
                    failure_reason=str(e),
                    conn=conn,
                )
                decision = "retryable"
            results.append((phrase, decision))
        return results

    # -----------------------------------------------------------------
    # Core resolution for a single phrase.
    # -----------------------------------------------------------------

    async def _process_phrase(
        self,
        *,
        phrase: str,
        observation_id: UUID,
        tenant_id: UUID,
        conn: asyncpg.Connection | None,
    ) -> ResolverDecision:
        # Build the exact authorized context and mention fate before deciding
        # whether this opportunity is eligible to consume model budget.
        target = conn if conn is not None else self._pool
        ctx = await build_context(
            pool=target,
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
        )
        if (
            ctx.context_selection_command is None
            or ctx.context_selection_outcome is None
            or ctx.mention_detection_command is None
        ):
            raise ValueError("resolver context is missing its pre-model grounding state")

        detection = ctx.mention_detection_command.detection
        if detection.fate is not EntityMentionDetectionFate.DETECTED:
            await self._grounding_repo.append_rejected_mention(
                context_command=ctx.context_selection_command,
                context_outcome=ctx.context_selection_outcome,
                mention_detection_command=ctx.mention_detection_command,
                tenant_id=tenant_id,
                source_observation_id=observation_id,
                phrase=phrase,
                conn=conn,
            )
            self._log.info(
                "entity_resolver.mention_rejected",
                phrase=phrase,
                tenant_id=str(tenant_id),
                observation_id=str(observation_id),
                detection_id=str(detection.detection_id),
                fate=detection.fate.value,
                reason_codes=list(detection.reason_codes),
                llm_invoked=False,
            )
            return "unresolved"

        # Only source-anchored mentions are eligible to consume LLM budget.
        if not self._budget.check_and_consume(tenant_id):
            self._bump_requeue(observation_id)
            await self._record_retryable_fate(
                tenant_id=tenant_id,
                observation_id=observation_id,
                phrase=phrase,
                failure_class="local_budget_exhausted",
                failure_reason="per-tenant resolver LLM budget exhausted",
                conn=conn,
            )
            self._log.warning(
                "entity_resolver.rate_limited",
                phrase=phrase,
                tenant_id=str(tenant_id),
                observation_id=str(observation_id),
            )
            return "rate_limited"

        # Invoke LLM, then convert its choice into an evidence-relative
        # assessment over the exact authorized candidate set.
        resolution = await self._invoke_llm(ctx)
        episode = self._build_grounding_episode(ctx=ctx, resolution=resolution)
        if conn is not None:
            async with conn.transaction():
                return await self._commit_episode_and_route(
                    conn,
                    ctx=ctx,
                    resolution=resolution,
                    episode=episode,
                    phrase=phrase,
                    observation_id=observation_id,
                    tenant_id=tenant_id,
                )
        async with self._pool.acquire() as owned, owned.transaction():
            return await self._commit_episode_and_route(
                owned,
                ctx=ctx,
                resolution=resolution,
                episode=episode,
                phrase=phrase,
                observation_id=observation_id,
                tenant_id=tenant_id,
            )

    async def _commit_episode_and_route(
        self,
        conn: asyncpg.Connection,
        *,
        ctx: ResolverContext,
        resolution: EntityResolution,
        episode: GroundingEpisode,
        phrase: str,
        observation_id: UUID,
        tenant_id: UUID,
    ) -> ResolverDecision:
        """Atomically finish grounding and the first admitted-belief lane."""

        trace_id = await self._grounding_repo.append_episode(
            episode=episode,
            tenant_id=tenant_id,
            source_observation_id=observation_id,
            phrase=phrase,
            conn=conn,
        )

        if episode.current_fate == "resolved_for_consumer":
            selected_ref = episode.admitted_canonical_ref or {}
            await self._process_grounded_belief(
                conn,
                tenant_id=tenant_id,
                observation_id=observation_id,
                grounding_trace_id=trace_id,
            )
            if selected_ref.get("type") in _TRIGGER_REENQUEUE_TYPES:
                await self._maybe_enqueue_trigger(
                    observation_id=observation_id,
                    tenant_id=tenant_id,
                    entity_ref=selected_ref,
                    conn=conn,
                    grounding_episode=episode,
                )
            self._log.info(
                "entity_resolver.resolved",
                phrase=phrase,
                canonical_ref=selected_ref,
                confidence=resolution.confidence,
                observation_id=str(observation_id),
                decision="resolved",
                identity_registry_mutated=False,
                source_observation_mutated=False,
            )
            return "resolved"

        if episode.current_fate == "review":
            await self._enqueue_review(
                ctx=ctx,
                resolution=resolution,
                episode=episode,
                conn=conn,
            )
            self._log.info(
                "entity_resolver.review",
                phrase=phrase,
                canonical_ref=episode.assessed_canonical_ref,
                confidence=resolution.confidence,
                observation_id=str(observation_id),
                decision="review",
            )
            return "review"

        self._log.info(
            "entity_resolver.unresolved",
            phrase=phrase,
            canonical_ref=resolution.canonical_ref,
            confidence=resolution.confidence,
            observation_id=str(observation_id),
            decision="unresolved",
            fate=episode.current_fate,
            reason_codes=list(episode.admission.reason_codes),
        )
        return "unresolved"

    async def _process_grounded_belief(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        grounding_trace_id: UUID,
    ) -> None:
        """Run the thin semantic lane when ingestion supplied an embedding."""

        row = await conn.fetchrow(
            """
            SELECT embedding, embedding_pending
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            FOR KEY SHARE
            """,
            tenant_id,
            observation_id,
        )
        if row is None:
            raise ValueError("grounded belief source observation is missing")
        if row["embedding_pending"] or row["embedding"] is None:
            self._log.warning(
                "entity_resolver.grounded_belief_deferred_no_embedding",
                tenant_id=str(tenant_id),
                observation_id=str(observation_id),
                grounding_trace_id=str(grounding_trace_id),
            )
            return
        await self._grounded_belief_processor.process_trace(
            conn,
            tenant_id=tenant_id,
            grounding_trace_id=grounding_trace_id,
            embedding=[float(value) for value in row["embedding"]],
        )

    def _build_grounding_episode(
        self,
        *,
        ctx: ResolverContext,
        resolution: EntityResolution,
    ) -> GroundingEpisode:
        if ctx.evidence_cutoff is None:
            raise ValueError("resolver context is missing its evidence cutoff")
        if (
            ctx.context_selection_command is None
            or ctx.context_selection_outcome is None
            or ctx.mention_detection_command is None
        ):
            raise ValueError("resolver context is missing its pre-LLM grounding state")
        context_observations = tuple(
            ContextObservationInput(
                observation_id=item.id,
                occurred_at=item.occurred_at,
                source_channel=item.source_channel,
                source_space=ctx.source_space,
                inclusion_layer=item.inclusion_layer,
                inclusion_reasons=tuple(item.inclusion_reasons),
            )
            for item in ctx.recent_observations
        )
        return build_grounding_episode(
            tenant_id=ctx.tenant_id,
            observation_id=ctx.observation_id,
            phrase=ctx.phrase,
            occurred_at=ctx.evidence_cutoff,
            source_channel=ctx.source_channel,
            source_space=ctx.source_space,
            topology_incomplete=ctx.topology_incomplete,
            boundary_hypotheses=tuple(ctx.boundary_hypotheses),
            context_observations=context_observations,
            selection_dependency_refs=tuple(ctx.selection_dependencies),
            candidates=self._candidate_inputs(ctx),
            model_candidate_id=resolution.candidate_id,
            model_canonical_ref=resolution.canonical_ref,
            model_confidence=resolution.confidence,
            model_reasoning=resolution.reasoning,
            high_confidence=self._high,
            review_min=self._review,
            prepared_context_command=ctx.context_selection_command,
            prepared_context_outcome=ctx.context_selection_outcome,
            prepared_mention_detection_command=ctx.mention_detection_command,
        )

    @staticmethod
    def _candidate_inputs(ctx: ResolverContext) -> tuple[GroundingCandidateInput, ...]:
        candidates: list[GroundingCandidateInput] = []
        for ref in ctx.source_entities_mentioned:
            if isinstance(ref, dict):
                candidates.append(
                    GroundingCandidateInput(
                        canonical_ref=ref,
                        candidate_source="source_mentions",
                        positive_evidence_refs=(
                            f"observation:{ctx.observation_id}:entities-mentioned",
                        ),
                    )
                )
        for item in ctx.recent_aliases:
            alias_ref = f"entity-alias:{item.alias_id}"
            candidates.append(
                GroundingCandidateInput(
                    canonical_ref=item.resolved_entity_ref,
                    candidate_source="tenant_aliases",
                    positive_evidence_refs=(alias_ref,),
                    independent_identity_evidence_refs=(
                        (item.identity_basis_ref,)
                        if (
                            item.identity_basis_ref
                            and item.source in {"manual", "ingestion"}
                            and item.identity_basis_class
                            in {"source_authoritative", "independently_adjudicated"}
                        )
                        else ()
                    ),
                )
            )
        for item in ctx.known_entity_candidates:
            alias_ref = f"entity-alias:{item.alias_id}"
            candidates.append(
                GroundingCandidateInput(
                    canonical_ref=item.resolved_entity_ref,
                    candidate_source="tenant_aliases",
                    positive_evidence_refs=(alias_ref,),
                    independent_identity_evidence_refs=(
                        (item.identity_basis_ref,)
                        if (
                            item.identity_basis_ref
                            and item.source in {"manual", "ingestion"}
                            and item.identity_basis_class
                            in {"source_authoritative", "independently_adjudicated"}
                        )
                        else ()
                    ),
                )
            )
        return tuple(candidates)

    # -----------------------------------------------------------------
    # LLM invocation with translation of SDK-layer errors.
    # -----------------------------------------------------------------

    async def _invoke_llm(self, ctx: ResolverContext) -> EntityResolution:
        system = (
            "You are an entity resolver for an organizational "
            "intelligence system. Given a phrase that appeared in a "
            "message, determine what canonical entity it refers to. "
            "Select only a candidate_id already present in "
            "source_entities_mentioned, prior_alias_matches, or "
            "known_entity_candidates. Do not invent IDs or entities. "
            "When the source text defines an alias, for example "
            "'<entity name> as <phrase>', resolve the phrase to that "
            "mentioned entity and copy its exact candidate_id. "
            "Return candidate_id and its exact canonical_ref, or set both "
            "to null when no listed known entity is justified. "
            "Confidence is in [0,1]. "
            "Return ONLY the JSON object with no prose."
        )
        user = (
            f"Context (JSON):\n{ctx.to_prompt_blob()}\n\n"
            f"Phrase to resolve: {ctx.phrase!r}"
        )
        try:
            return await self._llm.structured(
                system=system,
                user=user,
                schema=EntityResolution,
                temperature=0.0,
                max_tokens=512,
            )
        except (asyncio.TimeoutError, TimeoutError) as e:
            raise LLMTimeoutError(
                "entity resolver LLM call timed out",
                phrase=ctx.phrase,
            ) from e
        except LLMError:
            raise
        except Exception as e:
            # Anthropic / OpenAI client errors carry distinct types;
            # we pattern-match the class name to stay provider-agnostic.
            name = e.__class__.__name__
            if "RateLimit" in name or "429" in name:
                raise LLMRateLimitError(
                    "entity resolver rate-limited by provider",
                    phrase=ctx.phrase,
                ) from e
            if "Timeout" in name:
                raise LLMTimeoutError(
                    "entity resolver LLM call timed out",
                    phrase=ctx.phrase,
                ) from e
            raise

    async def _enqueue_review(
        self,
        *,
        ctx: ResolverContext,
        resolution: EntityResolution,
        episode: GroundingEpisode,
        conn: asyncpg.Connection | None,
    ) -> None:
        """Project a durable review admission into the human-work queue."""
        candidates = [
            {
                "candidate_id": episode.selected_candidate_id,
                "canonical_ref": episode.assessed_canonical_ref,
                "confidence": resolution.confidence,
                "reasoning": resolution.reasoning,
                "assessment_id": episode.assessment.assessment_id,
                "assessment_version": episode.assessment.assessment_version,
            }
        ]
        row_id = uuid7()
        await self._execute(
            conn,
            """
            INSERT INTO entity_review_queue (
                id, tenant_id, phrase, source_observation_id,
                candidates, created_at
            ) VALUES (
                $1, $2, $3, $4, $5::jsonb, now()
            )
            """,
            row_id,
            ctx.tenant_id,
            ctx.phrase,
            ctx.observation_id,
            json.dumps(candidates),
        )
        await self._open_entity_resolution_clarification(
            ctx=ctx,
            canonical_ref=episode.assessed_canonical_ref,
            review_id=row_id,
            candidates=candidates,
            conn=conn,
        )

    async def _open_entity_resolution_clarification(
        self,
        *,
        ctx: ResolverContext,
        canonical_ref: dict[str, Any] | None,
        review_id: UUID,
        candidates: list[dict[str, Any]],
        conn: asyncpg.Connection | None,
    ) -> None:
        ref = canonical_ref or {}
        label = f"{ref.get('type')}:{ref.get('id')}" if ref else "no candidate"
        options = [
            {
                "id": "accept_candidate",
                "label": f"Resolve to {label}",
                "value": {
                    "action": "accept_candidate",
                    "canonical_ref": canonical_ref,
                },
            },
            {
                "id": "not_same_entity",
                "label": "Not this entity",
                "value": {"action": "reject_candidate"},
            },
            {
                "id": "needs_new_entity",
                "label": "Create a new entity",
                "value": {"action": "create_new_entity"},
                "requires": ["entity_type"],
            },
        ]
        try:
            if conn is not None:
                await self._write_entity_resolution_clarification(
                    conn,
                    ctx=ctx,
                    ref=ref,
                    review_id=review_id,
                    candidates=candidates,
                    options=options,
                )
            else:
                async with self._pool.acquire() as owned:
                    await self._write_entity_resolution_clarification(
                        owned,
                        ctx=ctx,
                        ref=ref,
                        review_id=review_id,
                        candidates=candidates,
                        options=options,
                    )
        except Exception:
            self._log.warning(
                "entity_resolver.clarification_open_failed",
                phrase=ctx.phrase,
                observation_id=str(ctx.observation_id),
            )

    async def _write_entity_resolution_clarification(
        self,
        conn: asyncpg.Connection,
        *,
        ctx: ResolverContext,
        ref: dict[str, Any],
        review_id: UUID,
        candidates: list[dict[str, Any]],
        options: list[dict[str, Any]],
    ) -> None:
        await open_clarification_request(
            conn,
            tenant_id=ctx.tenant_id,
            kind="entity_resolution",
            priority=(
                "high"
                if ref.get("type") in {"customer", "actor", "person", "organization"}
                else "normal"
            ),
            question=f"What does '{ctx.phrase}' refer to?",
            explanation=(
                "The resolver found a plausible match but not enough evidence "
                "to safely write a canonical alias without user judgment."
            ),
            object_kind="entity_review",
            object_id=review_id,
            source_observation_id=ctx.observation_id,
            options=options,
            payload={
                "phrase": ctx.phrase,
                "candidates": candidates,
                "source": "entity_resolver",
            },
        )

    # -----------------------------------------------------------------
    # Helpers: durable work visibility and lineage-carrying trigger enqueue.
    # -----------------------------------------------------------------

    async def _load_unresolved_phrases(
        self,
        observation_id: UUID,
        tenant_id: UUID,
        *,
        conn: asyncpg.Connection | None,
    ) -> list[str]:
        """Load unresolved phrases from observation content.

        Ingestion stores actual-content candidates at top-level
        `content._unresolved_phrases`. Older fixtures and notes used
        `content.metadata._unresolved_phrases` (or `_metadata`). Read
        all of them defensively and preserve first-seen order. A terminal
        grounding trace, rather than source-payload mutation, marks a phrase
        handled for this processing generation.
        """
        row = await self._fetchrow(
            conn,
            """
            SELECT
              o.content,
              COALESCE(
                (
                  SELECT array_agg(gt.phrase)
                  FROM grounding_traces gt
                  WHERE gt.tenant_id = o.tenant_id
                    AND gt.source_observation_id = o.id
                ),
                ARRAY[]::text[]
              ) AS terminal_phrases,
              COALESCE(
                (
                  SELECT jsonb_object_agg(
                    w.phrase,
                    jsonb_build_object(
                      'status', w.status,
                      'next_attempt_at', w.next_attempt_at
                    )
                  )
                  FROM entity_grounding_work_items w
                  WHERE w.tenant_id = o.tenant_id
                    AND w.source_observation_id = o.id
                    AND w.processing_generation = 1
                ),
                '{}'::jsonb
              ) AS work_fates
            FROM observations o
            WHERE o.id = $1 AND o.tenant_id = $2
            """,
            observation_id,
            tenant_id,
        )
        if row is None:
            return []
        content = row["content"]
        if isinstance(content, (bytes, bytearray)):
            content = content.decode()
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                return []
        if not isinstance(content, dict):
            return []
        terminal_phrases = {
            item.strip()
            for item in row["terminal_phrases"]
            if isinstance(item, str) and item.strip()
        }
        work_fates = row["work_fates"] or {}
        if isinstance(work_fates, str):
            work_fates = json.loads(work_fates)
        now = datetime.now(timezone.utc)

        def ready(phrase: str) -> bool:
            fate = work_fates.get(phrase) if isinstance(work_fates, dict) else None
            if not isinstance(fate, dict):
                return True
            if fate.get("status") != "retry_scheduled":
                return False
            raw_next = fate.get("next_attempt_at")
            if not isinstance(raw_next, str):
                return True
            try:
                return datetime.fromisoformat(raw_next.replace("Z", "+00:00")) <= now
            except ValueError:
                return True

        return [
            phrase
            for phrase in _extract_unresolved_phrases(content)
            if phrase not in terminal_phrases and ready(phrase)
        ]

    async def _maybe_enqueue_trigger(
        self,
        *,
        observation_id: UUID,
        tenant_id: UUID,
        entity_ref: dict[str, Any],
        conn: asyncpg.Connection | None,
        grounding_episode: GroundingEpisode,
    ) -> None:
        """Try to enqueue T1 on think_trigger_queue if the table exists.

        Deviation docs: prompt lets us pick "try/except" OR "check
        pg_class". Picking pg_class check: it's a single fast query
        and avoids logging the error as noise when 2-A's 0004 is
        present.
        """
        exists = await self._fetchval(
            conn,
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = 'think_trigger_queue'
                  AND c.relkind IN ('r', 'p')
            )
            """,
        )
        if not exists:
            self._log.warning(
                "entity_resolver.trigger_skipped_no_table",
                entity_ref=entity_ref,
                observation_id=str(observation_id),
            )
            return
        try:
            if conn is not None:
                await enqueue_trigger(
                    conn,
                    tenant_id=tenant_id,
                    trigger_kind="T1",
                    trigger_subkind="entity_resolved_late",
                    observation_id=observation_id,
                    payload={
                        "entity_ref": entity_ref,
                        "seed_entity_ids": [entity_ref],
                        "resolution_assessment_ref": {
                            "id": grounding_episode.assessment.assessment_id,
                            "version": grounding_episode.assessment.assessment_version,
                        },
                        "grounding_admission_ref": {
                            "id": grounding_episode.admission.decision_id,
                            "version": grounding_episode.admission.decision_version,
                            "expires_at": grounding_episode.admission.expires_at.isoformat(),
                        },
                    },
                )
            else:
                async with self._pool.acquire() as owned:
                    await enqueue_trigger(
                        owned,
                        tenant_id=tenant_id,
                        trigger_kind="T1",
                        trigger_subkind="entity_resolved_late",
                        observation_id=observation_id,
                        payload={
                            "entity_ref": entity_ref,
                            "seed_entity_ids": [entity_ref],
                            "resolution_assessment_ref": {
                                "id": grounding_episode.assessment.assessment_id,
                                "version": grounding_episode.assessment.assessment_version,
                            },
                            "grounding_admission_ref": {
                                "id": grounding_episode.admission.decision_id,
                                "version": grounding_episode.admission.decision_version,
                                "expires_at": grounding_episode.admission.expires_at.isoformat(),
                            },
                        },
                    )
        except asyncpg.exceptions.UndefinedTableError:
            self._log.warning(
                "entity_resolver.trigger_skipped_no_table",
                entity_ref=entity_ref,
                observation_id=str(observation_id),
            )

    # -----------------------------------------------------------------
    # Connection shims.
    # -----------------------------------------------------------------

    async def _execute(
        self,
        conn: asyncpg.Connection | None,
        sql: str,
        *args: Any,
    ) -> str:
        if conn is not None:
            return await conn.execute(sql, *args)
        async with self._pool.acquire() as c:
            return await c.execute(sql, *args)

    async def _fetchrow(
        self,
        conn: asyncpg.Connection | None,
        sql: str,
        *args: Any,
    ) -> Any:
        if conn is not None:
            return await conn.fetchrow(sql, *args)
        async with self._pool.acquire() as c:
            return await c.fetchrow(sql, *args)

    async def _fetchval(
        self,
        conn: asyncpg.Connection | None,
        sql: str,
        *args: Any,
    ) -> Any:
        if conn is not None:
            return await conn.fetchval(sql, *args)
        async with self._pool.acquire() as c:
            return await c.fetchval(sql, *args)

    def _bump_requeue(self, observation_id: UUID) -> int:
        """Track how many times a given observation has been
        requeued — used to hand out exponential backoffs to callers
        that retry."""
        n = self._requeue_count.get(observation_id, 0) + 1
        self._requeue_count[observation_id] = n
        return n

    async def _record_retryable_fate(
        self,
        *,
        tenant_id: UUID,
        observation_id: UUID,
        phrase: str,
        failure_class: str,
        failure_reason: str,
        conn: asyncpg.Connection | None,
    ) -> None:
        now = datetime.now(timezone.utc)
        await self._grounding_repo.record_retryable_fate(
            tenant_id=tenant_id,
            source_observation_id=observation_id,
            phrase=phrase,
            failure_class=failure_class,
            failure_reason=failure_reason,
            next_attempt_at=now + timedelta(seconds=self.requeue_delay_s(observation_id)),
            conn=conn,
        )

    def requeue_delay_s(self, observation_id: UUID) -> float:
        """Exponential backoff cap at 60s."""
        n = self._requeue_count.get(observation_id, 0)
        return min(60.0, (2 ** n) * 1.0)

    # -----------------------------------------------------------------
    # Poll mode — scans recent observations with unresolved phrases.
    # -----------------------------------------------------------------

    async def process_pending(
        self,
        *,
        limit: int = 50,
        since_ms: int | None = None,
    ) -> int:
        """Scan the `limit` most recent observations that still have
        unresolved phrases and process each one. Returns count of
        observations processed.

        `since_ms` is an optional epoch-ms watermark; when None, scan
        everything with non-empty `_unresolved_phrases` regardless of
        age.

        Uses a single connection for bounded polling. Durable grounding fates
        make terminal phrases invisible without mutating Observation content.
        """
        processed = 0
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT o.id, o.tenant_id
                FROM observations o
                WHERE (
                    CASE
                        WHEN jsonb_typeof(
                            o.content -> '_unresolved_phrases'
                        ) = 'array'
                        THEN jsonb_array_length(
                            o.content -> '_unresolved_phrases'
                        )
                        ELSE 0
                    END
                    +
                    CASE
                        WHEN jsonb_typeof(
                            o.content -> 'metadata' -> '_unresolved_phrases'
                        ) = 'array'
                        THEN jsonb_array_length(
                            o.content -> 'metadata' -> '_unresolved_phrases'
                        )
                        ELSE 0
                    END
                    +
                    CASE
                        WHEN jsonb_typeof(
                            o.content -> '_metadata' -> '_unresolved_phrases'
                        ) = 'array'
                        THEN jsonb_array_length(
                            o.content -> '_metadata' -> '_unresolved_phrases'
                        )
                        ELSE 0
                    END
                ) > 0
                ORDER BY o.occurred_at DESC
                LIMIT $1
                """,
                limit,
            )
            for r in rows:
                decisions = await self.process_observation(
                    r["id"], r["tenant_id"], conn=conn
                )
                if decisions:
                    processed += 1
        return processed


def _extract_unresolved_phrases(content: dict[str, Any]) -> list[str]:
    phrases: list[str] = []
    seen: set[str] = set()

    def add(raw: Any) -> None:
        if not isinstance(raw, list):
            return
        for phrase in raw:
            if not isinstance(phrase, str):
                continue
            cleaned = phrase.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            phrases.append(cleaned)

    add(content.get("_unresolved_phrases"))
    for metadata_key in ("metadata", "_metadata"):
        metadata = content.get(metadata_key)
        if isinstance(metadata, dict):
            add(metadata.get("_unresolved_phrases"))
    return phrases


__all__ = [
    "EntityResolution",
    "EntityResolverWorker",
    "LLMRateLimitError",
    "LLMTimeoutError",
    "ResolverDecision",
    "ResolverLLMBudget",
]
