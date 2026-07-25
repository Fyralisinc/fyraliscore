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

import json
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion.handlers import (
    ObservationDraft,
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


def _dual(event: dict[str, Any], camel: str, pascal: str) -> Any:
    """Read a CloudTrail event field tolerant of BOTH key casings.

    Provider Lab / normalized records emit camelCase (`eventId`,
    `eventTime`, …); a REAL botocore LookupEvents element is PascalCase
    (`EventId`, `EventTime`, …). camelCase is read first so the existing
    synthetic path is unchanged; PascalCase is the additive fallback.
    """
    v = event.get(camel)
    if v is not None:
        return v
    return event.get(pascal)


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
    """eventTime as epoch ms (synthetic/normalized), RFC3339 string (captured
    real API), OR a tz-aware `datetime` (botocore LookupEvents returns one).

    Reads camelCase `eventTime` first (synthetic fallback), then PascalCase
    `EventTime` (real botocore shape)."""
    raw = _dual(event, "eventTime", "EventTime")
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
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
    alarm_name = _dual(event, "alarmName", "AlarmName")
    alarm_name = alarm_name if isinstance(alarm_name, str) and alarm_name else None
    new_state = _dual(event, "newState", "NewState")
    new_state = new_state if isinstance(new_state, str) and new_state else None
    prev_state = _dual(event, "prevState", "PrevState")
    prev_state = prev_state if isinstance(prev_state, str) and prev_state else None
    return alarm_name, new_state, prev_state


# ---------------------------------------------------------------------
# Event channel (backfill / poll)
# ---------------------------------------------------------------------

def _cloud_trail_event(event: dict[str, Any]) -> Any:
    """The full CloudTrail JSON for the event.

    The REAL botocore LookupEvents element carries `CloudTrailEvent` as a JSON
    STRING (PascalCase) that must be `json.loads`'d; the synthetic / normalized
    record carries `cloudTrailEvent` already as a dict. We preserve a structured
    dict downstream regardless: parse the string, fall back to the raw value on
    bad JSON (so a malformed real payload is still auditable)."""
    raw = _dual(event, "cloudTrailEvent", "CloudTrailEvent")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            return raw
    return raw


def _resources(event: dict[str, Any]) -> list[dict[str, Any]]:
    """Real botocore `Resources` list of `{ResourceType, ResourceName}` (or the
    synthetic camelCase `resources`). Each becomes an `aws_resource` entity."""
    raw = _dual(event, "resources", "Resources")
    if not isinstance(raw, list):
        return []
    out: list[dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        rtype = _dual(r, "resourceType", "ResourceType")
        rname = _dual(r, "resourceName", "ResourceName")
        rtype = rtype if isinstance(rtype, str) and rtype else None
        rname = rname if isinstance(rname, str) and rname else None
        if rtype or rname:
            out.append({"resource_type": rtype, "resource_name": rname})
    return out


def _event_draft(event: dict[str, Any], account_id: str, region: str) -> ObservationDraft:
    event_id = _dual(event, "eventId", "EventId")
    if event_id is None:
        raise ValidationError("aws event missing eventId", channel=_CHANNEL_EVENT)
    event_id = str(event_id)

    occurred = _occurred_at(event)
    external_id = aws_event(account_id, region, event_id)

    raw_event_name = _dual(event, "eventName", "EventName")
    event_name = raw_event_name if isinstance(raw_event_name, str) else ""
    raw_event_source = _dual(event, "eventSource", "EventSource")
    event_source = raw_event_source if isinstance(raw_event_source, str) else ""
    cloud_trail_event = _cloud_trail_event(event)
    resources = _resources(event)
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
    #
    # Synthetic / normalized records carry `userIdentity` as a top-level dict. In
    # the REAL botocore LookupEvents element there is NO top-level userIdentity:
    # the principal lives INSIDE the parsed `CloudTrailEvent` JSON, and the
    # element exposes a top-level `Username` string. Resolve additively: prefer a
    # top-level dict, else fall back to the parsed CloudTrailEvent's userIdentity.
    user_identity = event.get("userIdentity") if isinstance(event.get("userIdentity"), dict) else {}
    if not user_identity and isinstance(cloud_trail_event, dict):
        nested = cloud_trail_event.get("userIdentity")
        if isinstance(nested, dict):
            user_identity = nested
    arn = user_identity.get("arn") if isinstance(user_identity.get("arn"), str) else None
    user_name = user_identity.get("userName") if isinstance(user_identity.get("userName"), str) else None
    principal_id = (
        user_identity.get("principalId") if isinstance(user_identity.get("principalId"), str) else None
    )
    # Top-level `Username` (real botocore element) backstops the display name.
    top_username = _dual(event, "username", "Username")
    if not user_name and isinstance(top_username, str) and top_username:
        user_name = top_username
    actor_ref: str | None = None
    if arn:
        actor_ref = f"aws:iam:{arn}"
    elif principal_id:
        actor_ref = f"aws:iam:{principal_id}"
    elif user_name:
        # Real element with only a top-level `Username` (no resolvable arn).
        actor_ref = f"aws:iam:{user_name}"

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
            "id": arn or principal_id or user_name or "principal",
            "display_name": user_name or arn,
            "role": "actor",
        })
    for res in resources:
        entities.append({
            "type": "aws_resource",
            "id": res.get("resource_name") or res.get("resource_type") or "resource",
            "resource_type": res.get("resource_type"),
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
        # ISO string of the resolved occurred_at — stable across the epoch-ms
        # (synthetic), ISO-string, and botocore-datetime input shapes.
        "event_time": occurred.isoformat(),
        "user_identity": user_identity,
        "resources": resources,
        # Full CloudTrail JSON preserved for audit / downstream enrichment. For a
        # real botocore element this is the json.loads'd dict (the wire form is a
        # JSON STRING); for the synthetic record it is the dict as-emitted.
        "cloud_trail_event": cloud_trail_event,
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


async def handle_aws_event(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "aws event payload must be a JSON object", channel=_CHANNEL_EVENT,
        )
    account_id, region = _namespace(payload)
    return _event_draft(payload, account_id, region)




__all__ = ["handle_aws_event", "aws_event"]
