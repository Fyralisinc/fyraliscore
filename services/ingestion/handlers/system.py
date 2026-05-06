"""services/ingestion/handlers/system.py — internal:* channel handlers.

BUILD-PLAN §3 Prompt 2.A:
    "system.py: Minimal handler for internal:state_change,
     internal:anomaly, internal:prediction_resolution (used by other
     services to emit observations)."

Trust tier: `authoritative` per §14 CHANNEL_TRUST_MAP (internal:*
channels are the system itself emitting via its own path).

No signature verification. These handlers are invoked via in-process
calls (or HTTP with service-token auth handled at the Gateway layer);
either way the payload is trusted structurally and the only job is to
shape it into an `ObservationDraft`.

Payload shape (strict):
    {
        "content_text": str,            # required
        "content": dict[str, Any],      # required
        "tenant_id": str (UUID),        # required
        "occurred_at": str (ISO-8601),  # optional; defaults to now()
        "cause_event_id": str (UUID),   # optional
        "external_id": str,             # optional
        "source_actor_ref": str,        # optional
        "entities_hint": list[dict],    # optional
    }
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


async def _handle_internal(
    channel: str,
    payload: dict[str, Any],
    headers: dict[str, str],
    kind: str = "state_change",
) -> ObservationDraft:
    """Shared core for all internal:* channels.

    Validates required fields, normalizes occurred_at, enforces the
    trust_tier from the CHANNEL_TRUST_MAP. Never trusts the payload's
    `trust_tier` field — internal channels are always authoritative.
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "payload must be a JSON object",
            channel=channel,
        )
    content_text = payload.get("content_text")
    content = payload.get("content")
    if not isinstance(content_text, str) or not content_text.strip():
        raise ValidationError(
            "content_text is required and must be a non-empty string",
            channel=channel,
            field="content_text",
        )
    if not isinstance(content, dict):
        raise ValidationError(
            "content is required and must be a JSON object",
            channel=channel,
            field="content",
        )

    occurred_at_raw = payload.get("occurred_at")
    if occurred_at_raw is None:
        occurred_at = datetime.now(timezone.utc)
    else:
        try:
            occurred_at = datetime.fromisoformat(str(occurred_at_raw))
        except ValueError as e:
            raise ValidationError(
                f"occurred_at is not ISO-8601: {occurred_at_raw!r}",
                channel=channel,
                field="occurred_at",
            ) from e
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)

    external_id = payload.get("external_id")
    source_actor_ref = payload.get("source_actor_ref")
    entities_hint = payload.get("entities_hint") or []
    if not isinstance(entities_hint, list):
        raise ValidationError(
            "entities_hint must be a list",
            channel=channel,
            field="entities_hint",
        )

    # Copy cause_event_id into content for downstream threading; the
    # ingestion core also hoists this to the ObservationCreate.cause_id
    # column if present (via content["_cause_event_id"]).
    cause_event_id = payload.get("cause_event_id")
    content_copy = dict(content)
    if cause_event_id is not None:
        content_copy["_cause_event_id"] = str(cause_event_id)

    trust_tier = CHANNEL_TRUST_MAP[channel]

    return ObservationDraft(
        source_channel=channel,
        content_text=content_text,
        content=content_copy,
        occurred_at=occurred_at,
        trust_tier=trust_tier,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=source_actor_ref,
        external_id=external_id,
        entities_hint=list(entities_hint),
        raw_payload=None,  # in-process — no need to stash
    )


@register("internal:state_change")
async def handle_state_change(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """internal:state_change — trust_tier=authoritative, kind=state_change."""
    return await _handle_internal(
        "internal:state_change", payload, headers, kind="state_change"
    )


@register("internal:anomaly")
async def handle_anomaly(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """internal:anomaly — kind=anomaly_flagged."""
    return await _handle_internal(
        "internal:anomaly", payload, headers, kind="anomaly_flagged"
    )


@register("internal:prediction_resolution")
async def handle_prediction_resolution(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """internal:prediction_resolution — kind=prediction_resolution."""
    return await _handle_internal(
        "internal:prediction_resolution",
        payload,
        headers,
        kind="prediction_resolution",
    )


__all__ = [
    "handle_state_change",
    "handle_anomaly",
    "handle_prediction_resolution",
]
