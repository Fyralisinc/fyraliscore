"""services/observations/state_change.py — shared emitter for
`kind='state_change'` observations.

BUILD-PLAN.md §2 Prompt 1.A item 3:
    "state_change.py — helper for other services to emit state_change
     observations in their transactions. Signature:
     emit_state_change(tx, kind, entity_id, cause_event_id=None,
     metadata=None)."

Spec §1 "Process" / ARCHITECTURE §7 "State-change Observation
emission":
    Internal mutations (Model archived, Commitment moved to
    doneverified, Resource acquired, ...) emit a `state_change`
    Observation inside the same transaction as the mutation. This
    creates the audit trail: every `*_event_id` FK on Models / Acts /
    Resources points to a real Observation row.

Why a helper rather than a direct INSERT in each service:
- Uniform shape. Every state_change has the same content structure
  `{"entity_id": ..., "entity_kind": ..., "metadata": {...}}`, the
  same source_channel `internal:state_change`, the same
  trust_tier `authoritative` (it's a system-of-record event).
- Single code path for cause_id threading. Cascade Observation A →
  state_change B → state_change C works if every emitter sets
  cause_event_id consistently. Centralising this prevents drift.
- Post-commit NOTIFY is scheduled via `events.schedule_notify` so
  subscribers wake up once the mutation is durable.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from .events import NewObservationEvent, schedule_notify


STATE_CHANGE_CHANNEL = "internal:state_change"
STATE_CHANGE_TRUST_TIER = "authoritative"


async def emit_state_change(
    tx: asyncpg.Connection,
    *,
    kind: str,
    entity_id: UUID,
    tenant_id: UUID,
    cause_event_id: UUID | None = None,
    metadata: dict[str, Any] | None = None,
    actor_id: UUID | None = None,
    occurred_at: datetime | None = None,
    entity_kind: str | None = None,
) -> UUID:
    """
    Insert a `kind='state_change'` observation inside the caller's
    transaction. Returns the new observation's UUID (v7).

    Parameters:
    - `tx` — asyncpg Connection that is already inside a transaction.
      Caller owns the surrounding `async with tx.transaction(): ...`
      (e.g. via lib.shared.db.transaction()).
    - `kind` — the content discriminator describing the lifecycle
      event, e.g. 'model_archived', 'commitment_doneverified',
      'resource_deployed'. NOT the observation `kind` column (that is
      always 'state_change').
    - `entity_id` — UUID of the entity being mutated.
    - `tenant_id` — UUID of the tenant owning the entity.
    - `cause_event_id` — Optional UUID of the Observation that caused
      this mutation. Drives cascade chain reconstruction via the
      `cause_id` FK.
    - `metadata` — Optional JSON-serialisable dict of extra context.
    - `actor_id` — Optional UUID of the actor who caused the mutation
      (e.g. the Nexus-attested agent that archived the Model).
    - `occurred_at` — Optional override for the event time. Defaults
      to `now()`.
    - `entity_kind` — Optional string tag (e.g. 'model', 'commitment')
      captured inside `content` for downstream filtering without
      needing a separate column.

    After the caller's transaction commits, a NOTIFY is fired by the
    outer notify_scope. Callers who want no notification should not
    enter a notify_scope (schedule_notify is a no-op outside one).
    """
    occurred_at = occurred_at or datetime.now(timezone.utc)
    obs_id = uuid7()
    content: dict[str, Any] = {
        "entity_id": str(entity_id),
        "state_change_kind": kind,
    }
    if entity_kind is not None:
        content["entity_kind"] = entity_kind
    if metadata is not None:
        content["metadata"] = metadata

    content_text = render_state_change_text(kind, entity_id, entity_kind, metadata)

    await tx.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            source_actor_ref, actor_id,
            content, content_text,
            embedding, embedding_pending,
            trust_tier, external_id, cause_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, 'state_change', $4,
            NULL, $5,
            $6::jsonb, $7,
            NULL, FALSE,
            $8, NULL, $9, '[]'::jsonb
        )
        """,
        obs_id,
        tenant_id,
        occurred_at,
        STATE_CHANGE_CHANNEL,
        actor_id,
        _jsonb(content),
        content_text,
        STATE_CHANGE_TRUST_TIER,
        cause_event_id,
    )

    schedule_notify(
        NewObservationEvent(
            id=obs_id,
            kind="state_change",
            tenant_id=tenant_id,
            source_channel=STATE_CHANGE_CHANNEL,
        )
    )
    return obs_id


def render_state_change_text(
    kind: str,
    entity_id: UUID,
    entity_kind: str | None,
    metadata: dict[str, Any] | None,
) -> str:
    """
    Synthesize a short natural-language content_text so the
    state_change observation is legible in UI and embeddable if we
    ever decide to index it. Intentionally deterministic — no LLM.

    Per ARCHITECTURE-FINAL.md §7 "State-change Observation emission"
    (and AUDIT-REVIEW-1-FIXES FU6), this is the canonical helper that
    `emit_state_changes` uses to populate `content_text`. Contract:

      * ≤ 200 chars; no newlines.
      * Deterministic in (kind, entity_id, entity_kind, metadata).
      * Suitable for embedding and for UI rendering.
      * Must never return empty — `content_text` is NOT NULL in the
        Observations schema.
    """
    subject = f"{entity_kind or 'entity'} {entity_id}"
    if metadata:
        extras = ", ".join(
            f"{k}={v}" for k, v in sorted(metadata.items()) if _is_scalar(v)
        )
        if extras:
            return f"state change: {kind} on {subject} ({extras})"
    return f"state change: {kind} on {subject}"


# Back-compat alias for any legacy caller.
_build_content_text = render_state_change_text


def _is_scalar(v: Any) -> bool:
    return isinstance(v, (str, int, float, bool, type(None)))


def _jsonb(value: Any) -> str:
    import json as _json
    return _json.dumps(value, sort_keys=True, default=str)


__all__ = [
    "STATE_CHANGE_CHANNEL",
    "STATE_CHANGE_TRUST_TIER",
    "emit_state_change",
    "render_state_change_text",
]
