"""services/ingestion/handlers/stripe.py — Stripe webhook handler.

Stripe sends signed webhooks with a top-level envelope:

    {
        "id":        "evt_1NoOe2...",
        "type":      "invoice.paid" | "customer.subscription.updated" | ...,
        "created":   <unix_seconds>,
        "data":      {"object": {...}},
        "account":   "acct_..."   # present only on Connect events
    }

Signature verification happens in the webhook router
([services/webhooks/signatures/stripe.py](../../webhooks/signatures/stripe.py));
this handler shapes the verified payload into an ObservationDraft.

`external_id` is Stripe's event id (`evt_...`), which Stripe documents
as unique-per-event and is the canonical dedup key for replay-safety.

Trust tier: `authoritative` (per CHANNEL_TRUST_MAP). Stripe events are
system-of-record financial state changes.
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


_CHANNEL = "stripe:webhook"


def _summary(event_type: str | None, data: dict[str, Any]) -> str:
    obj = data.get("object") if isinstance(data, dict) else None
    bits: list[str] = [f"stripe:{event_type or 'event'}"]
    if isinstance(obj, dict):
        # Friendly hints — id / amount / status — without assuming a
        # specific Stripe object shape.
        for key in ("id", "amount", "amount_due", "amount_paid", "status"):
            v = obj.get(key)
            if v is not None:
                bits.append(f"{key}={v}")
    return " ".join(bits)


@register(_CHANNEL)
async def handle_stripe_webhook(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "stripe payload must be a JSON object", channel=_CHANNEL
        )
    event_id = payload.get("id")
    event_type = payload.get("type")
    if not isinstance(event_type, str):
        raise ValidationError(
            "stripe payload missing string 'type'", channel=_CHANNEL
        )

    created = payload.get("created")
    if isinstance(created, (int, float)):
        occurred_at = datetime.fromtimestamp(int(created), tz=timezone.utc)
    else:
        occurred_at = datetime.now(tz=timezone.utc)

    data = payload.get("data") or {}
    summary = _summary(event_type, data)

    entities_hint: list[dict[str, Any]] = []
    obj = data.get("object") if isinstance(data, dict) else None
    if isinstance(obj, dict):
        for kind_key, type_label in (
            ("customer", "stripe_customer"),
            ("subscription", "stripe_subscription"),
            ("invoice", "stripe_invoice"),
            ("payment_intent", "stripe_payment_intent"),
        ):
            v = obj.get(kind_key)
            if isinstance(v, str):
                entities_hint.append({"type": type_label, "id": v})
        # The object itself usually has an `id` that names the entity
        # the event is about (subscription event → subscription id).
        own_id = obj.get("id")
        own_kind = obj.get("object")  # e.g. "subscription", "invoice"
        if isinstance(own_id, str) and isinstance(own_kind, str):
            entities_hint.append(
                {"type": f"stripe_{own_kind}", "id": own_id}
            )

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=summary,
        content={
            "event_type": event_type,
            "event_id": event_id,
            "account": payload.get("account"),
            "data": data,
        },
        occurred_at=occurred_at,
        trust_tier=CHANNEL_TRUST_MAP[_CHANNEL],  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,  # Stripe events are system-originated
        external_id=event_id if isinstance(event_id, str) else None,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


__all__ = ["handle_stripe_webhook"]
