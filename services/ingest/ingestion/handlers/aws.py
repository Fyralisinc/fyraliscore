"""services/ingest/ingestion/handlers/aws.py — AWS CloudTrail event handler (IN-AWS).

ONE channel (AWS is a single-channel source, like Jira — both the time-window
backfill AND the live poll feed `aws:event`):

  - `aws:event` — CloudTrail management events from `CloudTrail:LookupEvents`
    (BACKFILL/POLL) AND the live SQS/EventBridge POLL. Records arrive tagged
    `_fyralis_record_type="event"` (set by the fetcher / the poll edge). A plain
    management event -> kind="signal"; a CloudWatch alarm-state-change event
    (carries an alarm `newState`/`alarmName`) -> kind="state_change", mirroring
    Grafana's annotation-vs-statechange discrimination.
        external_id: aws:{account_id}:{region}:event:{event_id}   (IMMUTABLE)

The channel is `authoritative`: AWS CloudTrail is the system of record for its
own account's control-plane + alarm-state history.

external_id is IMMUTABLE (a CloudTrail `eventId` is globally unique and stable —
no mutation dimension to version), so a re-fetched or re-polled event dedups to
one observation (the cross-path dedup invariant the backfill+poll edges share).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


_CHANNEL_EVENT = "aws:event"
_TRUST = "authoritative"


# ---------------------------------------------------------------------
# external_id constructor
# ---------------------------------------------------------------------

def aws_event(account_id: str, region: str, event_id: str) -> str:
    """`aws:{account_id}:{region}:event:{event_id}` — IMMUTABLE.

    A CloudTrail `eventId` is globally unique + stable, so the key is just a
    namespaced id (no version suffix). Namespaced by (account_id, region) so two
    tenants' / two regions' events never collide in the global
    UNIQUE(source_channel, external_id, occurred_at).

    NOTE: a matching `idempotency.aws_event(account_id, region, event_id)`
    constructor belongs in `services/ingest/ingestion/idempotency/__init__.py`;
    that module is owned by the wiring phase, so the constructor is defined here
    for now (see notes). Keep the two byte-identical when promoting.
    """
    return f"aws:{account_id}:{region}:event:{event_id}"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _from_ms(value: Any) -> datetime | None:
    """epoch milliseconds -> aware datetime."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and value > 0:
        try:
            return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return None
    return None


def _parse_iso(value: Any) -> datetime | None:
    """Parse an RFC3339 timestamp (CloudTrail `eventTime`, e.g.
    `2026-06-02T10:30:00Z`)."""
    if not isinstance(value, str) or not value:
        return None
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _occurred_at(event: dict[str, Any]) -> datetime:
    """eventTime as epoch ms (synthetic/normalized) OR RFC3339 (real API)."""
    raw = event.get("eventTime")
    dt = _from_ms(raw)
    if dt is not None:
        return dt
    dt = _parse_iso(raw)
    return dt or _utcnow()


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _namespace(payload: dict[str, Any]) -> tuple[str, str]:
    """external_id namespace (account_id, region). Backfill/poll records carry
    `_fyralis_account_id` / `_fyralis_region`; a real CloudTrail element also
    carries `recipientAccountId` / `awsRegion` as a fallback."""
    account = payload.get("_fyralis_account_id")
    if not (isinstance(account, str) and account):
        account = payload.get("recipientAccountId")
        account = account if isinstance(account, str) and account else "unknown"
    region = payload.get("_fyralis_region")
    if not (isinstance(region, str) and region):
        region = payload.get("awsRegion")
        region = region if isinstance(region, str) and region else "unknown"
    return account, region


def _alarm_state(event: dict[str, Any]) -> tuple[str | None, str | None, str | None]:
    """Extract a CloudWatch alarm transition if present.

    Returns (alarm_name, new_state, prev_state). A CloudTrail event for a
    CloudWatch alarm state change carries these (mirroring Grafana's
    newState/prevState); a plain management event carries none.
    """
    alarm_name = event.get("alarmName")
    alarm_name = alarm_name if isinstance(alarm_name, str) and alarm_name else None
    new_state = event.get("newState")
    new_state = new_state if isinstance(new_state, str) and new_state else None
    prev_state = event.get("prevState")
    prev_state = prev_state if isinstance(prev_state, str) and prev_state else None
    return alarm_name, new_state, prev_state


# ---------------------------------------------------------------------
# Event channel (backfill / poll)
# ---------------------------------------------------------------------

def _event_draft(event: dict[str, Any], account_id: str, region: str) -> ObservationDraft:
    event_id = event.get("eventId")
    if event_id is None:
        raise ValidationError("aws event missing eventId", channel=_CHANNEL_EVENT)
    event_id = str(event_id)

    occurred = _occurred_at(event)
    external_id = aws_event(account_id, region, event_id)

    event_name = event.get("eventName") if isinstance(event.get("eventName"), str) else ""
    event_source = event.get("eventSource") if isinstance(event.get("eventSource"), str) else ""
    alarm_name, new_state, prev_state = _alarm_state(event)
    # A CloudWatch alarm-state-change event carries an alarm name + newState;
    # treat it as a state_change. A plain management event is a signal.
    is_alarm_state = bool(alarm_name) or bool(new_state)
    object_type = "alarm_state_change" if is_alarm_state else "management_event"

    if is_alarm_state:
        transition = f"{prev_state or '∅'} → {new_state or '∅'}"
        headline = alarm_name or event_name or "alarm"
        content_text = f"[aws alarm] {headline}: {transition}"
    else:
        label = event_name or "event"
        src = event_source.split(".", 1)[0] if event_source else ""
        content_text = f"[aws] {src + ':' if src else ''}{label}"

    # Actor: the IAM principal that performed the management action. A CloudTrail
    # event carries `userIdentity` (arn / type / userName); a machine-generated
    # alarm-state event may be actorless.
    user_identity = event.get("userIdentity") if isinstance(event.get("userIdentity"), dict) else {}
    arn = user_identity.get("arn") if isinstance(user_identity.get("arn"), str) else None
    user_name = user_identity.get("userName") if isinstance(user_identity.get("userName"), str) else None
    principal_id = (
        user_identity.get("principalId") if isinstance(user_identity.get("principalId"), str) else None
    )
    actor_ref: str | None = None
    if arn:
        actor_ref = f"aws:iam:{arn}"
    elif principal_id:
        actor_ref = f"aws:iam:{principal_id}"

    entities: list[dict[str, Any]] = [
        {"type": "aws_account", "id": account_id},
        {"type": "aws_region", "id": region},
    ]
    if event_source:
        entities.append({"type": "aws_service", "id": event_source})
    if alarm_name:
        entities.append({"type": "aws_alarm", "id": alarm_name})
    if actor_ref and (user_name or arn):
        entities.append({
            "type": "aws_principal",
            "id": arn or principal_id or "principal",
            "display_name": user_name or arn,
            "role": "actor",
        })

    content: dict[str, Any] = {
        "object_type": object_type,
        "event_id": event_id,
        "event_name": event_name,
        "event_source": event_source,
        "account_id": account_id,
        "region": region,
        "alarm_name": alarm_name,
        "new_state": new_state,
        "prev_state": prev_state,
        "event_time": event.get("eventTime"),
        "user_identity": user_identity,
        # Full CloudTrail JSON preserved for audit / downstream enrichment.
        "cloud_trail_event": event.get("cloudTrailEvent"),
    }

    return ObservationDraft(
        source_channel=_CHANNEL_EVENT,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change" if is_alarm_state else "signal",
        source_actor_ref=actor_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=event,
    )


@register(_CHANNEL_EVENT)
async def handle_aws_event(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "aws event payload must be a JSON object", channel=_CHANNEL_EVENT,
        )
    account_id, region = _namespace(payload)
    return _event_draft(payload, account_id, region)


CHANNEL_TRUST_MAP.setdefault(_CHANNEL_EVENT, _TRUST)


__all__ = ["handle_aws_event", "aws_event"]
