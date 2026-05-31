"""Trigger emission for missing-transition anomalies.

When the detector ([services/dynamics/detectors.py](services/dynamics/detectors.py))
finds a substrate state-jump discontinuity, the Think pipeline needs a
durable T3 trigger so the next Think cycle picks it up, calls the
imputer, and lands a hypothesis Model. This module is the thin layer
that translates lightweight `DynamicSignal` envelopes into rich
T3:missing_transition trigger queue rows.

Design constraints:

1. **Idempotency.** Multiple Think runs against the same Model will
   produce the same discontinuity signal on every pass until the
   hypothesis Model is ratified (and the discontinuity resolved).
   Without dedup we'd enqueue duplicate triggers each cycle and the
   reconciler would have to merge after the fact. We avoid that here
   by skipping enqueue when an unprocessed T3:missing_transition for
   the same `(tenant_id, model_id, prev_event_id)` already sits in the
   queue.

2. **Caller-owned transaction.** Every helper takes a conn already
   inside a transaction. We never commit ourselves — the caller (e.g.
   [services/think/reason.py](services/think/reason.py)) decides when
   the queue rows become visible to other Think workers.

3. **Enriched payload.** The DynamicSignal carries only cause_ids
   (observation UUIDs). The handler needs the audit `event_id` BIGINTs
   for bracketing. We re-query
   `fetch_missing_transition_discontinuity` to obtain them rather
   than fattening DynamicSignal with substrate-internal columns.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Iterable
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from .detectors import (
    DynamicSignal,
    fetch_missing_transition_discontinuity,
)


T3_MISSING_TRANSITION_SUBKIND: str = "missing_transition"


async def emit_missing_transition_triggers(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    signals: Iterable[DynamicSignal],
    reference_time: datetime | None = None,
    lookback: timedelta = timedelta(days=30),
) -> list[UUID]:
    """For each missing_transition signal, enqueue a T3 trigger if one
    isn't already pending. Returns the list of newly-enqueued trigger
    UUIDs in order of emission.

    Non-missing_transition signals are silently skipped — the function
    is safe to pass the full signal list from `detect_dynamic_signals`.

    The `lookback` window is used for two purposes: (a) bounding the
    `fetch_missing_transition_discontinuity` audit-events scan, and
    (b) the conceptual horizon beyond which discontinuities are
    considered "settled" by user behavior and no longer worth surfacing.
    """
    ref = reference_time or datetime.now(timezone.utc)
    since = ref - lookback
    out: list[UUID] = []
    seen_keys: set[tuple[UUID, int | None]] = set()

    for signal in signals:
        if signal.dynamic_kind != "missing_transition":
            continue
        if not signal.subject_model_ids:
            continue
        model_id = signal.subject_model_ids[0]

        disc = await fetch_missing_transition_discontinuity(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            since=since,
        )
        if disc is None:
            # Detector saw a discontinuity but the enriched fetcher
            # couldn't find one within the lookback — typically a race
            # with concurrent writes that filled the gap. Skip silently.
            continue

        dedup_key = (model_id, disc.prev_event_id)
        if dedup_key in seen_keys:
            # Same model, same gap appeared twice in this signal batch —
            # only enqueue once.
            continue
        seen_keys.add(dedup_key)

        # Dedup against the persistent queue. We hold the caller's
        # transaction, so concurrent emitters in OTHER transactions
        # could race past this check — duplicate enqueue is acceptable
        # because the reconciler dedupes the resulting hypothesis
        # Models on cosine similarity (and `applied_triggers`
        # idempotency catches re-runs of the same trigger row).
        already_pending = await conn.fetchval(
            """
            SELECT 1 FROM think_trigger_queue
            WHERE tenant_id = $1
              AND trigger_kind = 'T3'
              AND trigger_subkind = $2
              AND model_id = $3
              AND (payload -> 'region_spec' ->> 'prev_event_id')::bigint = $4
              AND completed_at IS NULL
            LIMIT 1
            """,
            tenant_id,
            T3_MISSING_TRANSITION_SUBKIND,
            model_id,
            disc.prev_event_id,
        )
        if already_pending is not None:
            continue

        trig_id = uuid7()
        payload = {
            "seed_model_ids": [str(model_id)],
            "seed_entity_ids": [{"type": "model", "id": str(model_id)}],
            "region_spec": {
                "anomaly_kind": "missing_transition",
                "prev_event_id": disc.prev_event_id,
                "next_event_id": disc.next_event_id,
                "differing_fields": list(disc.differing_fields),
                "gap_seconds": disc.gap_seconds,
                "prev_event_occurred_at": (
                    disc.prev_event_occurred_at.isoformat()
                ),
                "next_event_occurred_at": (
                    disc.next_event_occurred_at.isoformat()
                ),
            },
            "trigger_id": str(trig_id),
        }
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
              id, tenant_id, trigger_kind, trigger_subkind,
              observation_id, model_id, payload
            ) VALUES ($1, $2, 'T3', $3, NULL, $4, $5::jsonb)
            """,
            trig_id,
            tenant_id,
            T3_MISSING_TRANSITION_SUBKIND,
            model_id,
            json.dumps(payload, default=str),
        )
        out.append(trig_id)

    return out


__all__ = [
    "T3_MISSING_TRANSITION_SUBKIND",
    "emit_missing_transition_triggers",
]
