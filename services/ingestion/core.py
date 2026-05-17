"""services/ingestion/core.py — UniformIngestPath.

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

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.embeddings.ollama import (
    EMBEDDING_DIM,
    OllamaClient,
    OllamaDimensionMismatch,
    OllamaError,
)
from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationCreate, ObservationRow
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo, normalize_phrase
from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    HandlerNotFound,
    ObservationDraft,
    get_handler,
)
from services.observations.events import emit_pending_notifications, notify_scope
from services.observations.repo import ObservationRepository


MAX_PAYLOAD_BYTES = 1 * 1024 * 1024  # 1 MB per BUILD-PLAN tests


class PayloadTooLarge(CompanyOSError):
    default_code = "payload_too_large"


# Phrase extraction: a tiny tokenizer that yields 1- to 3-word runs of
# alphanumerics + hyphens. Not linguistic — the fast path does exact
# lookups against known aliases, so precision > recall here. Wave 2-B
# entity resolver worker handles the long tail with LLM help.
_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9\-]{1,}")


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
    # Oversize check (1 MB). Using json.dumps gives us a stable byte
    # budget independent of the original HTTP transport encoding.
    encoded = json.dumps(raw_payload, default=str)
    if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise PayloadTooLarge(
            f"payload size > {MAX_PAYLOAD_BYTES} bytes",
            channel=channel,
            size=len(encoded),
        )
    # Reject NUL bytes in string fields. Postgres TEXT columns cannot
    # store \x00 and the resulting UntranslatableCharacterError is not a
    # structured 4xx shape we want leaking to callers. `json.dumps`
    # escapes NUL as "\\u0000", so scan the original dict recursively.
    if _contains_nul(raw_payload):
        raise ValidationError(
            "payload contains NUL byte (0x00) which cannot be stored",
            channel=channel,
        )

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

    # ---- step 2: pre-assign UUID v7 ----------------------------------
    obs_id = uuid7()

    # ---- step 3: actor resolution ------------------------------------
    resolved_actor_id: UUID | None = None
    unresolved_actor_ref: str | None = None
    if draft.source_actor_ref and actor_repo is not None:
        # source_actor_ref is expected as "<channel>:<ref>" (Slack
        # handler already formats it that way). Defensive fallback for
        # other channels: prepend `source_channel:` when no colon
        # appears.
        ref = draft.source_actor_ref
        if ":" not in ref:
            ref = f"{draft.source_channel}:{ref}"
        try:
            resolved_actor_id = await actor_repo.resolve_by_source_actor_ref(
                ref
            )
        except ValidationError:
            resolved_actor_id = None
        if resolved_actor_id is None:
            unresolved_actor_ref = ref

    # ---- step 4: fast-path entity extraction -------------------------
    entities_mentioned: list[dict[str, Any]] = list(draft.entities_hint)
    unresolved_phrases: list[str] = list(draft.unresolved_phrases)
    if alias_repo is not None and draft.content_text:
        seen_ref_keys = {
            json.dumps(e, sort_keys=True) for e in entities_mentioned
        }
        for phrase in candidate_phrases(draft.content_text):
            ref = await alias_repo.fast_path_resolve(phrase, tenant_id)
            if ref is not None:
                key = json.dumps(ref, sort_keys=True)
                if key not in seen_ref_keys:
                    seen_ref_keys.add(key)
                    entities_mentioned.append(ref)
            # Heuristic — only queue phrases that look like entity
            # references: capitalized or containing a hyphen. Prevents
            # common words (verbs, prepositions) from polluting the
            # resolver-worker queue.
            elif _looks_like_entity(phrase) and phrase not in unresolved_phrases:
                unresolved_phrases.append(phrase)

    # ---- step 5: compute embedding -----------------------------------
    embedding: list[float] | None = None
    embedding_pending = True
    if embedder is not None and draft.content_text:
        try:
            embedding = await embedder.embed(draft.content_text)
            embedding_pending = False
        except (OllamaError, OllamaDimensionMismatch):
            embedding = None
            embedding_pending = True

    # ---- build content ------------------------------------------------
    content = dict(draft.content)
    if unresolved_actor_ref is not None:
        content["_unresolved_actor_ref"] = unresolved_actor_ref
    if unresolved_phrases:
        # Reserved key for the Wave 2-B entity resolver worker. Stored
        # inside content (not entities_mentioned) so it doesn't pollute
        # the GIN index used for structured lookups.
        content["_unresolved_phrases"] = unresolved_phrases

    # cause_event_id may have been hoisted into content by the system
    # handler. Lift it into the ObservationCreate.cause_id column.
    cause_id_str = content.pop("_cause_event_id", None)
    cause_id: UUID | None = None
    if cause_id_str is not None:
        try:
            cause_id = UUID(str(cause_id_str))
        except ValueError:
            cause_id = None

    obs_create = ObservationCreate(
        id=obs_id,
        tenant_id=tenant_id,
        occurred_at=draft.occurred_at,
        kind=draft.kind,  # type: ignore[arg-type]
        source_channel=draft.source_channel,
        source_actor_ref=draft.source_actor_ref,
        actor_id=resolved_actor_id,
        content=content,
        content_text=draft.content_text,
        trust_tier=draft.trust_tier,  # type: ignore[arg-type]
        external_id=draft.external_id,
        cause_id=cause_id,
        entities_mentioned=entities_mentioned,
    )

    # ---- step 6: INSERT in transaction + post-commit NOTIFY ----------
    # We use the repo that is aware of embedding_pending fallback; but
    # the repo recomputes the embedding itself. Since we already did
    # that above (and captured the error path), we use a direct-write
    # path via repo.insert, overriding the repo's embedder by passing
    # one that returns our vector. Simpler: inject embedder that
    # returns what we computed.
    repo = ObservationRepository(pool, embedder=_PrecomputedEmbedder(
        embedding, embedding_pending
    ))
    trigger_queue_id: UUID | None = None
    existing_by_extid: ObservationRow | None = None

    with notify_scope() as scope:
        async with pool.acquire() as conn:
            async with conn.transaction():
                # Pre-check for dedup so we can tell the caller whether
                # the returned row was a re-insert or a fresh one.
                if draft.external_id is not None:
                    existing = await conn.fetchrow(
                        """
                        SELECT id FROM observations
                        WHERE source_channel = $1 AND external_id = $2
                        LIMIT 1
                        """,
                        draft.source_channel,
                        draft.external_id,
                    )
                    if existing is not None:
                        # Let the repo return the existing row — do not
                        # run the T1 enqueue (the observation is already
                        # known to Think).
                        deduped_row = await repo.insert(obs_create, conn=conn)
                        return IngestResult(
                            observation=deduped_row,
                            deduped=True,
                            trigger_queue_id=None,
                        )
                row = await repo.insert(obs_create, conn=conn)
                # ---- step 7: enqueue T1 trigger -----------------------
                if enqueue_trigger:
                    trigger_queue_id = uuid7()
                    await conn.execute(
                        """
                        INSERT INTO think_trigger_queue (
                            id, tenant_id, trigger_kind, trigger_subkind,
                            observation_id, model_id, payload
                        ) VALUES (
                            $1, $2, 'T1', 'event_arrival', $3, NULL, $4::jsonb
                        )
                        """,
                        trigger_queue_id,
                        tenant_id,
                        row.id,
                        json.dumps(
                            {
                                "source_channel": draft.source_channel,
                                "kind": row.kind,
                                "trust_tier": row.trust_tier,
                                "seed_occurred_at": row.occurred_at.isoformat(),
                                "seed_natural_text": (row.content_text or "")[:2000],
                                "scope_actors": (
                                    [str(row.actor_id)] if row.actor_id else []
                                ),
                            },
                            default=str,
                        ),
                    )
        # Transaction committed — flush NOTIFY.
        if scope.events:
            await emit_pending_notifications(pool, scope.events)

    # M3.2: publish embedding-needed signal if inline embedding
    # didn't land. The publish is post-commit (the observation is
    # durable) and best-effort (failure is logged but never raised
    # — backlog drainer is the safety net for Kafka outages).
    #
    # Source-family extraction: source_channel is granular
    # ("slack:message", "github:pr", "internal:state_change"); the
    # embedding envelope's source enum is the four-source family
    # (LLD §1). Non-source-family channels (e.g. internal:*) skip
    # the publish — they have no embedding worker contract.
    if (
        embedding_producer is not None
        and row.embedding_pending
    ):
        from services.ingestion.embedding.publish import (
            publish_embedding_request,
        )
        family = draft.source_channel.split(":", 1)[0]
        if family in ("slack", "github", "discord", "gmail"):
            await publish_embedding_request(
                producer=embedding_producer,
                tenant_id=tenant_id,
                source=family,
                observation_id=row.id,
            )

    return IngestResult(observation=row, deduped=False, trigger_queue_id=trigger_queue_id)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _contains_nul(obj: Any) -> bool:
    """Recursively check a JSON-shaped value for NUL bytes in strings."""
    if isinstance(obj, str):
        return "\x00" in obj
    if isinstance(obj, dict):
        return any(
            (isinstance(k, str) and "\x00" in k) or _contains_nul(v)
            for k, v in obj.items()
        )
    if isinstance(obj, list):
        return any(_contains_nul(v) for v in obj)
    return False


def _looks_like_entity(phrase: str) -> bool:
    """Heuristic: phrase has a capital letter or contains a hyphen.

    This intentionally errs on the side of enqueueing fewer common
    words for the resolver worker. Wave 2-B can refine the rule or
    move to a POS tagger — the queue key is stable either way.
    """
    if not phrase:
        return False
    if "-" in phrase:
        return True
    return any(c.isupper() for c in phrase)


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
    "IngestResult",
    "MAX_PAYLOAD_BYTES",
    "PayloadTooLarge",
]
