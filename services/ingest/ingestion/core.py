"""services/ingest/ingestion/core.py — UniformIngestPath.

BUILD-PLAN §3 Prompt 2.A steps 1-7:

    1. Handler extract → ObservationDraft with content_text, content,
       source_actor_ref, external_id, occurred_at, entities_hint.
    2. Pre-assign observation_id = uuid7().
    3. Resolve actor via ActorRepo.resolve_by_source_actor_ref. Unknown
       → actor_id = None + queue entry.
    4. Fast-path entity extraction via EntityAliasRepo.fast_path_resolve
       on tokenized phrases from content_text. Populate
       entities_mentioned. Unresolved phrases → queue.
    5. Compute embedding via OllamaClient.embed(content_text). On Ollama
       error (post retries) → embedding_pending=True.
    6. Inside a tx: ObservationRepository.insert(ObservationCreate(...)).
       Dedup + post-commit NOTIFY handled by the repo.
    7. Enqueue T1 trigger for Think in think_trigger_queue.

ARCHITECTURE §14 — trust assignment is lifted from CHANNEL_TRUST_MAP
in the handler; core does not override unless the handler explicitly
sets a different tier (e.g. GitHub "comment" vs "merge" — Wave 2-B's
concern, not ours).

Queue design — BUILD-PLAN allows the agent to pick:
- Unresolved entity phrases: stored in observations.content under the
  reserved key `_unresolved_phrases` (list[str]). The Wave 2-B entity
  resolver worker LISTENs on `observations_new` and reads that key
  to decide what to LLM-resolve. This avoids creating another table
  Wave 2-A doesn't own.
- Unresolved actor references: the observation is inserted with
  actor_id=NULL; the core records a marker in content._unresolved_actor_ref
  = "<channel>:<ref>". Same rationale.
- T1 trigger: migration 0004 think_trigger_queue (documented in
  SCHEMA-QUESTION.md Q4 partial resolution).
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import (
    EMBEDDING_DIM,
    OllamaClient,
    OllamaDimensionMismatch,
    OllamaError,
)
from lib.entity_mention_detection import extract_bootstrap_mention_opportunities
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationCreate, ObservationRow
from services.domain.actors.repo import ActorRepo
from services.domain.clarifications import open_clarification_request
from services.domain.entity_aliases.repo import EntityAliasRepo, normalize_phrase
from services.domain.triggers import ensure_event_arrival_trigger
from services.ingest.ingestion.handlers import (
    ObservationDraft,
    get_handler,
)
from services.ingest.ingestion.payload_validation import (
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    validate_ingest_json_payload,
)
from services.ingest.ingestion.kafka.topics import INGESTION_SOURCES
from services.domain.observations.events import emit_pending_notifications, notify_scope
from services.domain.observations.partitions import ensure_partition_for_occurred_at
from services.domain.observations.repo import ObservationRepository


# Phrase extraction: a tiny tokenizer that yields 1- to 3-word runs of
# alphanumerics + hyphens. Not linguistic — the fast path does exact
# lookups against known aliases, so precision > recall here. Wave 2-B
# entity resolver worker handles the long tail with LLM help.
_TOKEN_RE = re.compile(
    r"[A-Za-z][A-Za-z0-9\-]{1,}(?:['’]s)?"
)
_MAX_MENTION_OPPORTUNITIES = 50


def _dedup_lock_key(source_channel: str, external_id: str) -> int:
    digest = hashlib.sha256(
        f"{source_channel}\0{external_id}".encode("utf-8")
    ).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


def candidate_phrases(text: str, *, max_phrases: int = 50) -> list[str]:
    """Generate candidate phrases (1-, 2-, and 3-grams) for fast-path
    entity lookup.

    - Only alpha starters; skips tokens with no letters to drop stray
      numeric / timestamp-like chunks.
    - Deterministic, case-preserving order; normalization happens
      inside EntityAliasRepo.fast_path_resolve.
    - Capped at `max_phrases` so pathological long text doesn't
      explode the fan-out. 50 is generous for typical Slack chatter.
    """
    if not text:
        return []
    tokens = [m.group(0) for m in _TOKEN_RE.finditer(text)]
    phrases: list[str] = []
    seen: set[str] = set()
    for i, tok in enumerate(tokens):
        for n in (1, 2, 3):
            if i + n > len(tokens):
                break
            gram = " ".join(tokens[i : i + n])
            norm = normalize_phrase(gram)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            phrases.append(gram)
            if len(phrases) >= max_phrases:
                return phrases
    return phrases


@dataclass
class IngestResult:
    """Return value of `ingest()`."""

    observation: ObservationRow
    deduped: bool  # True when the inserted row was actually an existing row
    trigger_queue_id: UUID | None  # think_trigger_queue id, or None on dedup


@dataclass(frozen=True)
class _ActorResolution:
    actor_id: UUID | None
    unresolved_actor_ref: str | None


@dataclass(frozen=True)
class _EntityResolution:
    entities_mentioned: list[dict[str, Any]]
    unresolved_phrases: list[str]


@dataclass(frozen=True)
class _EmbeddingResult:
    embedding: list[float] | None
    pending: bool


@dataclass(frozen=True)
class _ObservationPreparation:
    obs_id: UUID
    obs_create: ObservationCreate
    embedding: _EmbeddingResult
    summary_pending: bool


def _document_summary_threshold_chars() -> int:
    try:
        return int(os.environ.get("INGEST_DOCUMENT_SUMMARY_THRESHOLD_CHARS", "8192"))
    except ValueError:
        return 8192


def _document_summary_source_channels() -> set[str]:
    raw = os.environ.get(
        "INGEST_DOCUMENT_SUMMARY_CHANNELS",
        "google_drive:file,notion:object,fireflies:transcript",
    )
    return {part.strip() for part in raw.split(",") if part.strip()}


def _summary_metadata(content: dict[str, Any]) -> dict[str, Any]:
    current = content.get("summarization")
    return current if isinstance(current, dict) else {}


def _draft_has_pending_summary(draft: ObservationDraft) -> bool:
    return _summary_metadata(draft.content).get("status") == "pending"


def _document_title(draft: ObservationDraft) -> str:
    for key in ("name", "title", "file_name"):
        value = draft.content.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return draft.external_id or draft.source_channel


def _prepare_document_summarization(
    draft: ObservationDraft,
    *,
    raw_s3_key: str | None,
    ingress_kind: str | None,
) -> bool:
    """Mutate a large document draft into a pending-summary observation."""
    if _draft_has_pending_summary(draft):
        return True
    content = draft.content
    if _summary_metadata(content).get("status") == "complete":
        return False
    if draft.source_channel not in _document_summary_source_channels() and not bool(
        content.get("is_document")
    ):
        return False
    source_text = draft.content_text or ""
    if len(source_text) < _document_summary_threshold_chars():
        return False

    summary: dict[str, Any] = {
        "status": "pending",
        "reason": "large_document",
        "original_chars": len(source_text),
        "raw_s3_key": raw_s3_key,
        "ingress_kind": ingress_kind,
        "source_channel": draft.source_channel,
        "model": None,
    }
    if raw_s3_key is None:
        # Inline fallback: no raw-tier pointer exists, so retain the full text
        # temporarily in JSONB and remove it once the summary lands.
        summary["source_text"] = source_text
    content["summarization"] = summary
    if raw_s3_key is not None:
        content["raw_s3_key"] = raw_s3_key
    draft.content_text = f"Document '{_document_title(draft)}' is queued for summarization."
    return True


async def ingest(
    channel: str,
    raw_payload: dict[str, Any],
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    request_headers: dict[str, str] | None = None,
    enqueue_trigger: bool = True,
    embedding_producer: Any | None = None,
    summarization_producer: Any | None = None,
) -> IngestResult:
    """Run the UniformIngestPath for `channel` + `raw_payload`.

    Raises:
    - HandlerNotFound when `channel` is not registered.
    - ValidationError when the handler rejects the payload.
    - PayloadTooLarge if the JSON-encoded raw payload exceeds 1 MB.
    - SlackSignatureError (and similar) when signature verification
      fails at the Gateway layer (signature check happens BEFORE this
      function is called — core assumes the payload is pre-verified).

    Idempotent: two calls with the same (source_channel, external_id)
    return the same observation row, `deduped=True` on the second call.

    M3.2: when `embedding_producer` is provided AND the inline
    embedding step left the row at `embedding_pending=TRUE`, publishes
    an envelope to `ingestion.embedding` so the M3.2 embedding worker
    can retry the Ollama call asynchronously. The publish is
    best-effort and CANNOT fail the ingest — if Kafka is down, the
    M3.3 backlog drainer (which scans Postgres directly for
    `embedding_pending=TRUE`) will eventually pick up the row.
    """
    if not isinstance(raw_payload, dict):
        raise ValidationError("raw_payload must be a JSON object")
    validate_ingest_json_payload(raw_payload, channel=channel)

    request_headers = request_headers or {}

    # ---- step 1: handler extract -------------------------------------
    handler = get_handler(channel)
    draft = await handler(raw_payload, request_headers)
    if draft.source_channel != channel:
        # Defensive — handlers are trusted but a typo would cause a
        # trust-tier mismatch downstream.
        raise ValidationError(
            f"handler returned source_channel={draft.source_channel!r} "
            f"but was registered for {channel!r}"
        )

    # Steps 2-7 are shared with the M5.2 writer's full-mode path,
    # which consumes NormalizedEnvelope (handler already ran in
    # the normalizer) and would re-handle the payload otherwise.
    #
    # Reactive per-month partition self-heal (INLINE path only — the writer
    # wraps `ingest_from_draft` with its own ticket-#44 self-heal, so we must
    # NOT bake one into the shared function). A backfilled / arbitrary-age
    # observation whose occurred_at month has no partition makes the INSERT
    # inside `ingest_from_draft` raise an UNNAMED CheckViolationError, which
    # rolls the per-envelope transaction back (releasing its `observations`
    # lock). Only THEN do we create the covering month — on a separate pooled
    # connection, after the lock is gone, so the partition DDL's ACCESS
    # EXCLUSIVE can't deadlock against our own open transaction — and retry
    # once. Out-of-guardrail dates (>~10y) are not healed; the error
    # propagates, mirroring how the writer DLQs such timestamps.
    #
    # TODO(partitioning): if deep/unpredictable historical backfill becomes
    # common, evaluate pg_partman to own forward+backward monthly partition
    # management instead of this per-path self-heal. See
    # services/domain/observations/partitions.py and CODEBASE-MANAGEMENT.md.
    for _attempt in range(2):
        try:
            return await ingest_from_draft(
                channel=channel,
                draft=draft,
                pool=pool,
                tenant_id=tenant_id,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                enqueue_trigger=enqueue_trigger,
                embedding_producer=embedding_producer,
                summarization_producer=summarization_producer,
            )
        except asyncpg.exceptions.CheckViolationError as exc:
            # Unnamed CheckViolation on the range-partitioned insert == missing
            # partition. Heal + retry once; anything else (named constraint,
            # out-of-guardrail date, already retried) propagates.
            if _attempt == 0 and exc.constraint_name is None:
                healed = await ensure_partition_for_occurred_at(
                    pool, draft.occurred_at
                )
                if healed in ("ensured", "exists", "cached"):
                    continue
            raise
    # Unreachable: the loop either returns or re-raises.
    raise AssertionError("ingest_from_draft retry loop fell through")


async def ingest_from_draft(
    *,
    channel: str,
    draft: ObservationDraft,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    enqueue_trigger: bool = True,
    embedding_producer: Any | None = None,
    summarization_producer: Any | None = None,
    raw_s3_key: str | None = None,
    ingress_kind: str | None = None,
) -> IngestResult:
    """Persist an already-built draft through the shared ingest path."""
    if draft.source_channel != channel:
        raise ValidationError(
            f"draft.source_channel={draft.source_channel!r} does not match "
            f"channel={channel!r}"
        )

    preparation = await _prepare_observation_for_ingest(
        channel=channel,
        draft=draft,
        pool=pool,
        tenant_id=tenant_id,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        summarization_producer=summarization_producer,
        raw_s3_key=raw_s3_key,
        ingress_kind=ingress_kind,
    )

    result = await _insert_observation_and_maybe_enqueue_trigger(
        pool=pool,
        draft=draft,
        obs_create=preparation.obs_create,
        embedding=preparation.embedding,
        enqueue_trigger=(
            enqueue_trigger
            and not preparation.summary_pending
            and not _requires_entity_grounding(preparation.obs_create)
        ),
        obs_id=preparation.obs_id,
        tenant_id=tenant_id,
    )
    await _publish_embedding_request_if_needed(
        producer=embedding_producer,
        tenant_id=tenant_id,
        draft=draft,
        row=result.observation,
        skip=preparation.summary_pending,
    )
    await _publish_summarization_request_if_needed(
        producer=summarization_producer,
        tenant_id=tenant_id,
        draft=draft,
        row=result.observation,
        deduped=result.deduped,
    )
    return result


def _requires_entity_grounding(observation: ObservationCreate) -> bool:
    if not str(observation.source_channel).startswith("slack:"):
        return False
    content = observation.content
    if not isinstance(content, dict):
        return False
    unresolved = content.get("_unresolved_phrases")
    return isinstance(unresolved, list) and bool(unresolved)


async def _prepare_observation_for_ingest(
    *,
    channel: str,
    draft: ObservationDraft,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    actor_repo: ActorRepo | None,
    alias_repo: EntityAliasRepo | None,
    embedder: OllamaClient | None,
    summarization_producer: Any | None,
    raw_s3_key: str | None,
    ingress_kind: str | None,
) -> _ObservationPreparation:
    from services.ingest.ingestion.enrichers import run_enrichers

    await run_enrichers(channel, draft, pool=pool, tenant_id=tenant_id)
    summary_pending = (
        summarization_producer is not None
        and _prepare_document_summarization(
            draft,
            raw_s3_key=raw_s3_key,
            ingress_kind=ingress_kind,
        )
    )
    obs_id = uuid7()
    actor = await _resolve_actor(draft, actor_repo)
    entities = await _resolve_entities(draft, alias_repo, tenant_id)
    embedding = (
        _EmbeddingResult(embedding=None, pending=True)
        if summary_pending
        else await _compute_embedding(embedder, draft.content_text)
    )
    return _ObservationPreparation(
        obs_id=obs_id,
        obs_create=_build_observation_create(
            obs_id=obs_id,
            tenant_id=tenant_id,
            draft=draft,
            actor=actor,
            entities=entities,
        ),
        embedding=embedding,
        summary_pending=summary_pending,
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _resolve_actor(
    draft: ObservationDraft,
    actor_repo: ActorRepo | None,
) -> _ActorResolution:
    if not draft.source_actor_ref or actor_repo is None:
        return _ActorResolution(actor_id=None, unresolved_actor_ref=None)
    ref = draft.source_actor_ref
    if ":" not in ref:
        ref = f"{draft.source_channel}:{ref}"
    try:
        resolved_actor_id = await actor_repo.resolve_by_source_actor_ref(ref)
    except ValidationError:
        resolved_actor_id = None
    return _ActorResolution(
        actor_id=resolved_actor_id,
        unresolved_actor_ref=ref if resolved_actor_id is None else None,
    )


async def _resolve_entities(
    draft: ObservationDraft,
    alias_repo: EntityAliasRepo | None,
    tenant_id: UUID,
) -> _EntityResolution:
    entities_mentioned: list[dict[str, Any]] = list(draft.entities_hint)
    unresolved_phrases: list[str] = []
    seen_opportunities: set[str] = set()

    def add_opportunity(phrase: str) -> None:
        normalized = normalize_phrase(phrase)
        if (
            not normalized
            or normalized in seen_opportunities
            or len(unresolved_phrases) >= _MAX_MENTION_OPPORTUNITIES
        ):
            return
        seen_opportunities.add(normalized)
        unresolved_phrases.append(phrase)

    for phrase in draft.unresolved_phrases:
        add_opportunity(phrase)
    for phrase in extract_bootstrap_mention_opportunities(
        draft.content_text,
        max_opportunities=_MAX_MENTION_OPPORTUNITIES,
    ):
        add_opportunity(phrase)
    if alias_repo is None or not draft.content_text:
        return _EntityResolution(entities_mentioned, unresolved_phrases)

    seen_ref_keys = {json.dumps(e, sort_keys=True) for e in entities_mentioned}
    phrases = candidate_phrases(draft.content_text)
    resolved_by_norm = await alias_repo.fast_path_resolve_many(phrases, tenant_id)
    for phrase in phrases:
        ref = resolved_by_norm.get(normalize_phrase(phrase))
        if ref is not None:
            key = json.dumps(ref, sort_keys=True)
            if key not in seen_ref_keys:
                seen_ref_keys.add(key)
                entities_mentioned.append(ref)
            # A known identity is a candidate seed. It does not suppress the
            # exact source surface's independent mention-detection fate.
            add_opportunity(phrase)
    return _EntityResolution(entities_mentioned, unresolved_phrases)


async def _compute_embedding(
    embedder: OllamaClient | None,
    content_text: str,
) -> _EmbeddingResult:
    if embedder is None or not content_text:
        return _EmbeddingResult(embedding=None, pending=True)
    try:
        return _EmbeddingResult(
            embedding=await embedder.embed(content_text),
            pending=False,
        )
    except (OllamaError, OllamaDimensionMismatch):
        return _EmbeddingResult(embedding=None, pending=True)


def _extract_cause_id(content: dict[str, Any]) -> UUID | None:
    cause_id_str = content.pop("_cause_event_id", None)
    if cause_id_str is None:
        return None
    try:
        return UUID(str(cause_id_str))
    except ValueError:
        return None


def _build_observation_create(
    *,
    obs_id: UUID,
    tenant_id: UUID,
    draft: ObservationDraft,
    actor: _ActorResolution,
    entities: _EntityResolution,
) -> ObservationCreate:
    content = dict(draft.content)
    if actor.unresolved_actor_ref is not None:
        content["_unresolved_actor_ref"] = actor.unresolved_actor_ref
    if entities.unresolved_phrases:
        content["_unresolved_phrases"] = entities.unresolved_phrases
    cause_id = _extract_cause_id(content)
    return ObservationCreate(
        id=obs_id,
        tenant_id=tenant_id,
        occurred_at=draft.occurred_at,
        kind=draft.kind,  # type: ignore[arg-type]
        source_channel=draft.source_channel,
        source_actor_ref=draft.source_actor_ref,
        actor_id=actor.actor_id,
        content=content,
        content_text=draft.content_text,
        trust_tier=draft.trust_tier,  # type: ignore[arg-type]
        external_id=draft.external_id,
        cause_id=cause_id,
        entities_mentioned=entities.entities_mentioned,
    )


async def _lock_and_find_existing_observation(
    conn: asyncpg.Connection,
    draft: ObservationDraft,
) -> asyncpg.Record | None:
    if draft.external_id is None:
        return None
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1)",
        _dedup_lock_key(draft.source_channel, draft.external_id),
    )
    return await conn.fetchrow(
        """
        SELECT id FROM observations
        WHERE source_channel = $1 AND external_id = $2
        LIMIT 1
        """,
        draft.source_channel,
        draft.external_id,
    )


async def _enqueue_event_arrival_trigger(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    draft: ObservationDraft,
    row: ObservationRow,
) -> UUID:
    return await ensure_event_arrival_trigger(
        conn,
        tenant_id=tenant_id,
        observation_id=row.id,
        payload={
            "source_channel": draft.source_channel,
            "kind": row.kind,
            "trust_tier": row.trust_tier,
            "seed_occurred_at": row.occurred_at.isoformat(),
            "seed_natural_text": (row.content_text or "")[:2000],
            "scope_actors": [str(row.actor_id)] if row.actor_id else [],
        },
    )


async def _insert_observation_and_maybe_enqueue_trigger(
    *,
    pool: asyncpg.Pool,
    draft: ObservationDraft,
    obs_create: ObservationCreate,
    embedding: _EmbeddingResult,
    enqueue_trigger: bool,
    obs_id: UUID,
    tenant_id: UUID,
) -> IngestResult:
    repo = ObservationRepository(
        pool,
        embedder=_PrecomputedEmbedder(embedding.embedding, embedding.pending),
    )
    trigger_queue_id: UUID | None = None

    with notify_scope() as scope:
        async with pool.acquire() as conn:
            async with conn.transaction():
                existing = await _lock_and_find_existing_observation(conn, draft)
                if existing is not None:
                    deduped_row = await repo.insert(obs_create, conn=conn)
                    if enqueue_trigger:
                        trigger_queue_id = await _enqueue_event_arrival_trigger(
                            conn,
                            tenant_id=tenant_id,
                            draft=draft,
                            row=deduped_row,
                        )
                    return IngestResult(
                        observation=deduped_row,
                        deduped=True,
                        trigger_queue_id=trigger_queue_id,
                    )
                row = await repo.insert(obs_create, conn=conn)
                if draft.external_id is not None and row.id != obs_id:
                    if enqueue_trigger:
                        trigger_queue_id = await _enqueue_event_arrival_trigger(
                            conn,
                            tenant_id=tenant_id,
                            draft=draft,
                            row=row,
                        )
                    return IngestResult(
                        observation=row,
                        deduped=True,
                        trigger_queue_id=trigger_queue_id,
                    )
                await _maybe_open_actor_identity_clarification(
                    conn,
                    tenant_id=tenant_id,
                    draft=draft,
                    row=row,
                    unresolved_actor_ref=(
                        obs_create.content.get("_unresolved_actor_ref")
                        if isinstance(obs_create.content, dict)
                        else None
                    ),
                )
                if enqueue_trigger:
                    trigger_queue_id = await _enqueue_event_arrival_trigger(
                        conn,
                        tenant_id=tenant_id,
                        draft=draft,
                        row=row,
                    )
        if scope.events:
            await emit_pending_notifications(pool, scope.events)

    return IngestResult(
        observation=row,
        deduped=False,
        trigger_queue_id=trigger_queue_id,
    )


async def _maybe_open_actor_identity_clarification(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    draft: ObservationDraft,
    row: ObservationRow,
    unresolved_actor_ref: Any,
) -> None:
    if row.actor_id is not None or not unresolved_actor_ref:
        return
    ref = str(unresolved_actor_ref).strip()
    if not ref:
        return
    question = (
        f"Who is '{ref}'? Is this an existing actor alias, a new actor, "
        "or not a person?"
    )
    options = [
        {
            "id": "same_as_existing_actor",
            "label": "Same as existing actor",
            "value": {
                "action": "map_to_existing_actor",
                "source_actor_ref": ref,
                "actor_id": None,
            },
        },
        {
            "id": "new_internal_actor",
            "label": "New internal actor",
            "value": {
                "action": "create_internal_actor",
                "source_actor_ref": ref,
            },
        },
        {
            "id": "new_external_actor",
            "label": "New external actor",
            "value": {
                "action": "create_external_actor",
                "source_actor_ref": ref,
            },
        },
        {
            "id": "not_an_actor",
            "label": "Not an actor",
            "value": {
                "action": "ignore_actor_ref",
                "source_actor_ref": ref,
            },
        },
    ]
    try:
        await open_clarification_request(
            conn,
            tenant_id=tenant_id,
            kind="actor_identity",
            priority=_actor_ref_priority(ref),
            question=question,
            explanation=(
                "Ingestion saw a source-specific actor reference but could not "
                "map it to a canonical actor. Leaving it unresolved prevents "
                "actor-scoped models and cross-source behavior patterns."
            ),
            object_kind="source_actor_ref",
            object_key=ref,
            source_observation_id=row.id,
            options=options,
            payload={
                "source_actor_ref": ref,
                "source_channel": draft.source_channel,
                "content_text": (row.content_text or "")[:500],
                "source": "ingestion_actor_resolution",
            },
        )
    except Exception:
        # Clarification capture should not block ingestion. Operators can still
        # reconstruct unresolved refs from observations.content.
        return


def _actor_ref_priority(ref: str) -> str:
    lowered = ref.casefold()
    if "@" in lowered and not any(
        marker in lowered
        for marker in ("noreply", "no-reply", "bot@", "dependabot")
    ):
        return "high"
    if lowered.startswith(("slack:", "signal:", "telegram:", "github:")):
        return "normal"
    return "low"


async def _publish_embedding_request_if_needed(
    *,
    producer: Any | None,
    tenant_id: UUID,
    draft: ObservationDraft,
    row: ObservationRow,
    skip: bool = False,
) -> None:
    if skip or producer is None or not row.embedding_pending:
        return
    family = draft.source_channel.split(":", 1)[0]
    if family not in INGESTION_SOURCES:
        return
    from services.ingest.ingestion.embedding.publish import (
        publish_embedding_request,
    )

    await publish_embedding_request(
        producer=producer,
        tenant_id=tenant_id,
        source=family,
        observation_id=row.id,
    )


async def _publish_summarization_request_if_needed(
    *,
    producer: Any | None,
    tenant_id: UUID,
    draft: ObservationDraft,
    row: ObservationRow,
    deduped: bool,
) -> None:
    if deduped or producer is None or not _draft_has_pending_summary(draft):
        return
    family = draft.source_channel.split(":", 1)[0]
    if family not in INGESTION_SOURCES:
        return
    from services.ingest.ingestion.summarization.publish import (
        publish_summarization_request,
    )

    summary = _summary_metadata(draft.content)
    raw_key = summary.get("raw_s3_key")
    await publish_summarization_request(
        producer=producer,
        tenant_id=tenant_id,
        source=family,
        observation_id=row.id,
        raw_s3_key=raw_key if isinstance(raw_key, str) else None,
        ingress_kind=summary.get("ingress_kind"),  # type: ignore[arg-type]
    )


class _PrecomputedEmbedder:
    """Embedder shim that returns a pre-computed embedding to
    `ObservationRepository.insert` so the repo doesn't call Ollama a
    second time. When `pending=True`, we fabricate an error so the
    repo's fallback branch sets embedding_pending=TRUE as expected.
    """

    class _C:
        expected_dim = EMBEDDING_DIM

    def __init__(self, embedding: list[float] | None, pending: bool) -> None:
        self._embedding = embedding
        self._pending = pending
        self.config = self._C()

    async def embed(self, text: str) -> list[float]:
        if self._pending or self._embedding is None:
            raise OllamaError("precomputed embedder marked pending")
        return self._embedding


__all__ = [
    "candidate_phrases",
    "ingest",
    "ingest_from_draft",
    "IngestResult",
    "MAX_PAYLOAD_BYTES",
    "PayloadTooLarge",
]
