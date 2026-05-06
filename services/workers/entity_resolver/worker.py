"""services/workers/entity_resolver/worker.py — resolver loop.

BUILD-PLAN §3 Prompt 2.B item 2, ARCHITECTURE §15.

Process (single unresolved phrase):
    1. Load context (20 recent obs in same channel + recent active
       Models + prior alias hits) via services.workers.entity_resolver.context.
    2. Call LLMProvider.structured(schema=EntityResolution, ...)
       with an agent-y system prompt.
    3. Apply per-tenant token-bucket rate limit before each LLM call.
       If the tenant is over budget, SKIP this phrase; the next cycle
       picks it up.
    4. Apply the decision:
        - confidence >  0.8 AND canonical_ref is not null →
            insert_alias, record_usage, append to
            observations.entities_mentioned, emit state_change, and
            (defensively) enqueue a T1 trigger if the type is in
            {customer, commitment, goal} and think_trigger_queue exists.
        - 0.5 ≤ confidence ≤ 0.8 → write to entity_review_queue.
        - confidence < 0.5 OR canonical_ref null → log + drop.
    5. Structured log every decision (phrase, canonical_ref,
       confidence, observation_id, decision).

Input sources
-------------

Two modes, selectable at construction time:
    - LISTEN mode (default when asyncpg.Pool is given): SUBSCRIBE
      to `observations_new` channel; on each wakeup, scan the
      referenced observation for `content.metadata._unresolved_phrases`.
    - POLL mode: a scheduled `process_pending()` walks the last N
      observations since a watermark. Used by the end-to-end test
      fixture + by operators manually re-running resolution.

Both modes call `process_observation()`, which is the unit of work.
Tests invoke `process_observation()` directly.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMError, LLMParseError, LLMProvider
from lib.shared.ids import uuid7
from services.entity_aliases.repo import EntityAliasRepo
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

    canonical_ref: dict[str, Any] | None = None
    confidence: float = Field(ge=0.0, le=1.0)
    reasoning: str = ""


# =====================================================================
# Rate limit — per-tenant token bucket
# =====================================================================

ResolverDecision = Literal["resolved", "review", "dropped", "rate_limited"]


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
# Exceptions the resolver translates from LLM SDK layers.
# =====================================================================

class LLMRateLimitError(LLMError):
    """Raised when the LLM provider returns a 429. Re-queue with backoff."""

    default_code = "llm_rate_limited"


class LLMTimeoutError(LLMError):
    """Raised when the LLM provider times out. Re-queue with backoff."""

    default_code = "llm_timeout"


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
        budget: ResolverLLMBudget | None = None,
        high_confidence: float = HIGH_CONFIDENCE,
        review_min: float = REVIEW_MIN,
        logger: Any | None = None,
    ) -> None:
        self._pool = pool
        self._llm = llm
        self._alias_repo = alias_repo
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
                decision = "rate_limited"
            except LLMTimeoutError:
                self._log.warning(
                    "entity_resolver.phrase_timeout",
                    phrase=phrase,
                    observation_id=str(observation_id),
                )
                self._bump_requeue(observation_id)
                decision = "rate_limited"   # treat as requeue
            except LLMParseError as e:
                # Retries already exhausted inside LLMProvider.
                self._log.error(
                    "entity_resolver.phrase_llm_parse_exhausted",
                    phrase=phrase,
                    observation_id=str(observation_id),
                    error=str(e),
                )
                decision = "dropped"
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
        # Rate limit BEFORE the LLM call.
        if not self._budget.check_and_consume(tenant_id):
            self._log.warning(
                "entity_resolver.rate_limited",
                phrase=phrase,
                tenant_id=str(tenant_id),
                observation_id=str(observation_id),
            )
            return "rate_limited"

        # Build context.
        target = conn if conn is not None else self._pool
        ctx = await build_context(
            pool=target,
            tenant_id=tenant_id,
            observation_id=observation_id,
            phrase=phrase,
        )

        # Invoke LLM.
        resolution = await self._invoke_llm(ctx)

        # Decide what to do.
        if (
            resolution.canonical_ref is not None
            and resolution.confidence > self._high
        ):
            await self._apply_resolved(
                ctx=ctx,
                resolution=resolution,
                conn=conn,
            )
            self._log.info(
                "entity_resolver.resolved",
                phrase=phrase,
                canonical_ref=resolution.canonical_ref,
                confidence=resolution.confidence,
                observation_id=str(observation_id),
                decision="resolved",
            )
            return "resolved"

        if (
            resolution.canonical_ref is not None
            and self._review <= resolution.confidence <= self._high
        ):
            await self._enqueue_review(
                ctx=ctx,
                resolution=resolution,
                conn=conn,
            )
            self._log.info(
                "entity_resolver.review",
                phrase=phrase,
                canonical_ref=resolution.canonical_ref,
                confidence=resolution.confidence,
                observation_id=str(observation_id),
                decision="review",
            )
            return "review"

        self._log.info(
            "entity_resolver.dropped",
            phrase=phrase,
            canonical_ref=resolution.canonical_ref,
            confidence=resolution.confidence,
            observation_id=str(observation_id),
            decision="dropped",
        )
        return "dropped"

    # -----------------------------------------------------------------
    # LLM invocation with translation of SDK-layer errors.
    # -----------------------------------------------------------------

    async def _invoke_llm(self, ctx: ResolverContext) -> EntityResolution:
        system = (
            "You are an entity resolver for an organizational "
            "intelligence system. Given a phrase that appeared in a "
            "message, determine what canonical entity it refers to. "
            "Return canonical_ref as a JSON object like "
            '{"type": "<entity-kind>", "id": "<stable-id>"} or null '
            "when the phrase does not refer to any known entity. "
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

    # -----------------------------------------------------------------
    # Apply a resolved phrase — alias insert, obs append, trigger.
    # -----------------------------------------------------------------

    async def _apply_resolved(
        self,
        *,
        ctx: ResolverContext,
        resolution: EntityResolution,
        conn: asyncpg.Connection | None,
    ) -> None:
        assert resolution.canonical_ref is not None
        ref = resolution.canonical_ref

        # 1. Insert alias.
        alias = await self._alias_repo.insert_alias(
            phrase=ctx.phrase,
            resolved_entity_ref=ref,
            source="resolver_worker",
            confidence=resolution.confidence,
            tenant_id=ctx.tenant_id,
            source_event_id=ctx.observation_id,
        )
        # 2. Record usage (bumps confirmed_count).
        with contextlib.suppress(Exception):
            await self._alias_repo.record_usage(alias.id)

        # 3. Append to observation entities_mentioned.
        await self._append_entities_mentioned(
            observation_id=ctx.observation_id,
            tenant_id=ctx.tenant_id,
            entity_ref=ref,
            conn=conn,
        )

        # 4. Emit a state_change observation (kind=state_change).
        await self._emit_state_change(
            observation_id=ctx.observation_id,
            tenant_id=ctx.tenant_id,
            phrase=ctx.phrase,
            entity_ref=ref,
            confidence=resolution.confidence,
            conn=conn,
        )

        # 5. Defensive T1 enqueue when the type is material.
        if ref.get("type") in _TRIGGER_REENQUEUE_TYPES:
            await self._maybe_enqueue_trigger(
                observation_id=ctx.observation_id,
                tenant_id=ctx.tenant_id,
                entity_ref=ref,
                conn=conn,
            )

    async def _enqueue_review(
        self,
        *,
        ctx: ResolverContext,
        resolution: EntityResolution,
        conn: asyncpg.Connection | None,
    ) -> None:
        """Write a row to entity_review_queue.

        Candidates: a list with the single LLM candidate. If future
        resolvers surface multiple candidates, this array grows.
        """
        candidates = [
            {
                "canonical_ref": resolution.canonical_ref,
                "confidence": resolution.confidence,
                "reasoning": resolution.reasoning,
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

    # -----------------------------------------------------------------
    # Helpers: load phrases, append entities_mentioned, state_change,
    # conditional trigger enqueue.
    # -----------------------------------------------------------------

    async def _load_unresolved_phrases(
        self,
        observation_id: UUID,
        tenant_id: UUID,
        *,
        conn: asyncpg.Connection | None,
    ) -> list[str]:
        """Load observation.content.metadata._unresolved_phrases.

        This is the shape Agent 2-A's ingestion core is expected to
        produce per Prompt 2.A. The resolver reads it defensively:
        missing metadata or wrong types return [] rather than crashing.
        """
        row = await self._fetchrow(
            conn,
            """
            SELECT content FROM observations
            WHERE id = $1 AND tenant_id = $2
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
        metadata = content.get("metadata") or content.get("_metadata") or {}
        if not isinstance(metadata, dict):
            return []
        phrases = metadata.get("_unresolved_phrases")
        if not isinstance(phrases, list):
            return []
        return [p for p in phrases if isinstance(p, str) and p.strip()]

    async def _append_entities_mentioned(
        self,
        *,
        observation_id: UUID,
        tenant_id: UUID,
        entity_ref: dict[str, Any],
        conn: asyncpg.Connection | None,
    ) -> None:
        """Idempotently append `entity_ref` to observations.entities_mentioned.

        Uses JSONB `||` + a containment check to avoid duplicates.
        """
        await self._execute(
            conn,
            """
            UPDATE observations
            SET entities_mentioned = (
                CASE
                    WHEN entities_mentioned @> $3::jsonb THEN entities_mentioned
                    ELSE COALESCE(entities_mentioned, '[]'::jsonb) || $3::jsonb
                END
            )
            WHERE id = $1 AND tenant_id = $2
            """,
            observation_id,
            tenant_id,
            json.dumps([entity_ref]),
        )

    async def _emit_state_change(
        self,
        *,
        observation_id: UUID,
        tenant_id: UUID,
        phrase: str,
        entity_ref: dict[str, Any],
        confidence: float,
        conn: asyncpg.Connection | None,
    ) -> None:
        """Insert a state_change Observation recording the late resolution.

        Fields:
            cause_id    = the original observation id
            kind        = 'state_change'
            trust_tier  = 'authoritative' (the resolver's own attestation)
            source_channel = 'internal:state_change'
            content     = {phrase, entity_ref, confidence, kind:
                           'entity_late_resolution'}
            content_text = f"phrase '...' resolved to type=... id=..."
        """
        obs_id = uuid7()
        content = {
            "_state_change_kind": "entity_late_resolution",
            "phrase": phrase,
            "entity_ref": entity_ref,
            "confidence": confidence,
            "source_observation_id": str(observation_id),
        }
        content_text = (
            f"phrase {phrase!r} resolved to type={entity_ref.get('type')} "
            f"id={entity_ref.get('id')} (conf={confidence:.2f})"
        )
        await self._execute(
            conn,
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                content, content_text, trust_tier, cause_id
            ) VALUES (
                $1, $2, now(), 'state_change', 'internal:state_change',
                $3::jsonb, $4, 'authoritative', $5
            )
            """,
            obs_id,
            tenant_id,
            json.dumps(content),
            content_text,
            observation_id,
        )

    async def _maybe_enqueue_trigger(
        self,
        *,
        observation_id: UUID,
        tenant_id: UUID,
        entity_ref: dict[str, Any],
        conn: asyncpg.Connection | None,
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
        trigger_id = uuid7()
        try:
            await self._execute(
                conn,
                """
                INSERT INTO think_trigger_queue (
                    id, tenant_id, trigger_kind, trigger_subkind,
                    observation_id, payload
                ) VALUES (
                    $1, $2, 'T1', 'entity_resolved_late', $3, $4::jsonb
                )
                """,
                trigger_id,
                tenant_id,
                observation_id,
                json.dumps({"entity_ref": entity_ref}),
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

        Uses a single connection so the `_append_entities_mentioned`
        update and the new state_change insert share a transaction.
        """
        processed = 0
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT o.id, o.tenant_id
                FROM observations o
                WHERE jsonb_array_length(
                        COALESCE(
                            o.content -> 'metadata' -> '_unresolved_phrases',
                            '[]'::jsonb
                        )
                      ) > 0
                ORDER BY o.occurred_at DESC
                LIMIT $1
                """,
                limit,
            )
            for r in rows:
                await self.process_observation(
                    r["id"], r["tenant_id"], conn=conn
                )
                processed += 1
        return processed


__all__ = [
    "EntityResolution",
    "EntityResolverWorker",
    "LLMRateLimitError",
    "LLMTimeoutError",
    "ResolverDecision",
    "ResolverLLMBudget",
]
