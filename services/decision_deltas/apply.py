"""
services/decision_deltas/apply.py — accept-and-apply side effects.

When the CEO accepts a Decision Delta, the system:

  1. Transitions the delta row to status='accepted', stamps accepted_at
     / accepted_by.
  2. Walks consequence_preview.{creates, updates, archives, notifies}
     and dispatches each side effect. v1 only wires the "primary"
     target node update (target_node_kind / target_node_id) — the
     other consequence_preview categories are recorded in the audit
     row but not yet auto-applied. Future PRs will extend the
     dispatcher.
  3. Emits a topology_events row tagged with kind='drift' and
     payload.cause='decision_delta_accepted' so the ledger / Today
     SSE stream observes the acceptance. This keeps decision-delta
     acceptances visible inside the same surface that already shows
     phase events.
  4. Calls a notification stub (no-op in v1) for each
     consequence_preview.notifies entry.

Returns (updated_view, events_dict) so the router can hand back what
actually fired.

Caller owns the transaction.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.ids import uuid7

logger = logging.getLogger(__name__)


class DeltaApplyError(CompanyOSError):
    default_code = "decision_delta_apply_error"


# ---------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------


async def apply_acceptance(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    delta_id: UUID,
    user_id: UUID,
) -> tuple[Any, dict[str, Any]]:
    """Accept a delta and run its consequence_preview side effects.

    Returns (DecisionDeltaView, triggered_events_dict). The
    triggered_events_dict has keys:
      - target_updated: bool — whether the primary node update fired
      - target_event_id: UUID | None — topology_events row id (if any)
      - notifications_dispatched: int — how many notifies stubs ran
      - notes: list[str] — informational messages (skipped consequences)
    """
    # Late import — apply is called from repo and we need to avoid an
    # import cycle on module load.
    from services.decision_deltas import repo as dd_repo

    current = await dd_repo.get_delta(
        conn, tenant_id=tenant_id, delta_id=delta_id,
    )
    if current is None:
        raise dd_repo.DeltaNotFoundError(
            f"decision delta {delta_id} not found",
            delta_id=str(delta_id),
        )
    if current.status == "accepted":
        # Idempotent accept: short-circuit but report nothing fired.
        return current, {
            "target_updated": False,
            "target_event_id": None,
            "notifications_dispatched": 0,
            "notes": ["already_accepted"],
        }
    if not dd_repo._is_allowed_transition(current.status, "accepted"):
        raise dd_repo.InvalidStatusTransitionError(
            f"cannot accept from status={current.status}",
            from_status=current.status, to_status="accepted",
        )

    notes: list[str] = []

    # --- 1. Primary target update (when present + safe) ---------------
    target_updated = False
    target_event_id: UUID | None = None
    if current.target_node_kind and current.target_node_id:
        applied = await _apply_target_update(
            conn=conn,
            tenant_id=tenant_id,
            target_kind=current.target_node_kind,
            target_id=current.target_node_id,
            suggested_update=current.suggested_update or {},
            notes=notes,
        )
        target_updated = applied
    else:
        notes.append("no_target_node")

    # --- 2. Transition to accepted (UPDATE) ---------------------------
    await conn.execute(
        """
        UPDATE decision_deltas
        SET status = 'accepted',
            accepted_at = $2,
            accepted_by = $3
        WHERE id = $1 AND tenant_id = $4
        """,
        delta_id,
        datetime.now(timezone.utc),
        user_id,
        tenant_id,
    )

    # --- 3. Emit a ledger-visible event -------------------------------
    # We piggyback on topology_events so the Ledger surface and Today
    # SSE stream can render decision-delta acceptances without a new
    # event table. kind='drift' is the closest fit (state changed but
    # the underlying neighborhood may still be the same); the
    # discriminator lives in payload.event_kind.
    target_event_id = await _emit_acceptance_event(
        conn=conn,
        tenant_id=tenant_id,
        delta_id=delta_id,
        user_id=user_id,
        target_kind=current.target_node_kind,
        target_id=current.target_node_id,
        consequence_preview=current.consequence_preview or {},
    )

    # --- 4. Notification stubs ----------------------------------------
    notifies = (current.consequence_preview or {}).get("notifies") or []
    dispatched = 0
    if isinstance(notifies, list):
        for entry in notifies:
            try:
                await _stub_notify(
                    tenant_id=tenant_id,
                    delta_id=delta_id,
                    entry=entry,
                )
                dispatched += 1
            except Exception as e:  # noqa: BLE001
                # Notify failure is non-fatal for v1 — log and continue.
                notes.append(f"notify_failed:{e}")

    refreshed = await dd_repo.get_delta(
        conn, tenant_id=tenant_id, delta_id=delta_id,
    )
    if refreshed is None:  # pragma: no cover
        raise DeltaApplyError(
            "delta disappeared after accept",
            delta_id=str(delta_id),
        )
    return refreshed, {
        "target_updated": target_updated,
        "target_event_id": (
            str(target_event_id) if target_event_id else None
        ),
        "notifications_dispatched": dispatched,
        "notes": notes,
    }


# ---------------------------------------------------------------------
# Primary target update — small dispatcher
# ---------------------------------------------------------------------


async def _apply_target_update(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    target_kind: str,
    target_id: UUID,
    suggested_update: dict[str, Any],
    notes: list[str],
) -> bool:
    """Apply the suggested_update to the target node.

    v1 supports a single shape:
      - target_kind in {customer, resource}: update the underlying
        `resources` row's metadata to merge in suggested_update.value
        (when present). `customer` and `resource` share the
        `resources` table in this codebase.
      - other kinds: NOT YET WIRED. We record a note and skip the
        update — the delta is still marked accepted because the
        intent is preserved via the audit event. Future PRs add
        commitment/goal/decision dispatchers.
    """
    if not suggested_update:
        notes.append("no_suggested_update")
        return False

    if target_kind in ("customer", "resource"):
        # Try to update resources.metadata. Best-effort — if the row
        # doesn't exist we record a note rather than failing the
        # entire accept (the delta may target a node that was just
        # archived; user accepted in good faith).
        new_value = (
            suggested_update.get("value")
            or suggested_update.get("label")
        )
        if new_value is None:
            notes.append("suggested_update_missing_value")
            return False
        # Probe the table first.
        exists = await conn.fetchval(
            "SELECT 1 FROM resources "
            "WHERE id = $1 AND tenant_id = $2",
            target_id, tenant_id,
        )
        if not exists:
            notes.append(f"target_resource_missing:{target_id}")
            return False
        # Merge into metadata JSONB. We don't have a strict shape,
        # so we stash the accepted state under a stable key.
        await conn.execute(
            """
            UPDATE resources
            SET metadata = COALESCE(metadata, '{}'::jsonb)
                         || jsonb_build_object(
                              'decision_delta_state',
                              $2::jsonb
                            )
            WHERE id = $1 AND tenant_id = $3
            """,
            target_id,
            json.dumps({
                "value": new_value,
                "applied_at": datetime.now(timezone.utc).isoformat(),
            }),
            tenant_id,
        )
        return True

    notes.append(f"target_kind_not_wired:{target_kind}")
    return False


# ---------------------------------------------------------------------
# Audit / ledger emission
# ---------------------------------------------------------------------


async def _emit_acceptance_event(
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    delta_id: UUID,
    user_id: UUID,
    target_kind: str | None,
    target_id: UUID | None,
    consequence_preview: dict[str, Any],
) -> UUID | None:
    """Insert a topology_events row marking the acceptance.

    Why topology_events: the Ledger surface (services/today + ledger
    UI) already polls topology_events for "what changed". Using the
    existing pipe avoids fanning new tables to every consumer for a
    Phase-1 ship. payload.event_kind keeps the discriminator explicit
    so consumers can branch.
    """
    event_id = uuid7()
    payload = {
        "event_kind": "decision_delta_accepted",
        "delta_id": str(delta_id),
        "accepted_by": str(user_id),
        "target_kind": target_kind,
        "target_id": str(target_id) if target_id else None,
        "consequence_preview": consequence_preview,
    }
    # We don't have neighborhood context for a delta; kind='drift' is
    # the only existing kind that doesn't carry strict member
    # semantics. member_model_ids is NOT NULL so we pass an empty
    # array.
    try:
        await conn.execute(
            """
            INSERT INTO topology_events (
              id, tenant_id, kind, neighborhood_id,
              predecessor_neighborhood_ids, sibling_neighborhood_ids,
              member_model_ids, magnitude, named_signature, payload
            )
            VALUES (
              $1, $2, 'drift', NULL,
              NULL, NULL,
              $3::uuid[], NULL, $4, $5::jsonb
            )
            """,
            event_id,
            tenant_id,
            [],  # member_model_ids
            f"decision_delta:{delta_id}",
            json.dumps(payload, default=str),
        )
        return event_id
    except Exception as e:  # noqa: BLE001 — ledger write is best-effort
        logger.warning(
            "decision_delta_event_emit_failed delta_id=%s err=%s",
            delta_id, e,
        )
        return None


# ---------------------------------------------------------------------
# Notification stub
# ---------------------------------------------------------------------


async def _stub_notify(
    *,
    tenant_id: UUID,
    delta_id: UUID,
    entry: Any,
) -> None:
    """Placeholder notifier — Phase 1 just logs. A future PR will wire
    this to the actual Slack/email dispatchers."""
    logger.info(
        "decision_delta_notify_stub tenant=%s delta=%s entry=%r",
        tenant_id, delta_id, entry,
    )


__all__ = [
    "DeltaApplyError",
    "apply_acceptance",
]
