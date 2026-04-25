"""services/ingestion/handlers/slack.py — Slack webhook handler.

BUILD-PLAN §3 Prompt 2.A (minimum viable Phase 1):
    "slack.py:
      - Signature verification against SLACK_SIGNING_SECRET.
      - Payload → ObservationDraft with content_text = message text,
        source_actor_ref = user id (format 'slack:U...'),
        external_id = '{channel}:{ts}' for dedup,
        occurred_at = parsed Slack ts,
        entities_hint = parsed @mentions + #channels + URLs.
      - Trust tier: attested_agent per §14 CHANNEL_TRUST_MAP
        for slack:message."

Signature protocol (Slack docs, standard v0):
  basestring = f"v0:{timestamp}:{body}"
  sig = f"v0={hex(hmac_sha256(secret, basestring))}"
  compare against X-Slack-Signature header (constant-time).
  Reject if timestamp is > 5 minutes old (replay protection).
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import CompanyOSError, ValidationError

from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    HandlerError,
    ObservationDraft,
    register,
)


# Slack replay window per docs: 5 minutes. Callers can tune via env.
_DEFAULT_MAX_AGE_S = 300
_MAX_AGE_S = int(os.environ.get("SLACK_MAX_TIMESTAMP_AGE_S", _DEFAULT_MAX_AGE_S))

# @mention pattern: <@U01ABC> or <@U01ABC|alice>
_MENTION_RE = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")
# #channel pattern: <#C01ENG> or <#C01ENG|general>
_CHANNEL_MENTION_RE = re.compile(r"<#([A-Z0-9]+)(?:\|[^>]+)?>")
# URL pattern: <https://example.com> or <https://example.com|label>
_URL_RE = re.compile(r"<(https?://[^|>]+)(?:\|[^>]+)?>")


class SlackSignatureError(CompanyOSError):
    default_code = "slack_signature_invalid"


def _constant_time_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def verify_slack_signature(
    body: bytes,
    timestamp: str,
    signature: str,
    secret: str,
    *,
    max_age_s: int = _MAX_AGE_S,
    now: float | None = None,
) -> None:
    """Raise `SlackSignatureError` if the signature is invalid, the
    timestamp is missing, or the payload is older than `max_age_s`.

    Follows Slack's standard v0 HMAC-SHA256 protocol. Constant-time
    compare prevents timing oracles.
    """
    if not signature or not timestamp or not secret:
        raise SlackSignatureError(
            "missing signature, timestamp, or secret",
            have_signature=bool(signature),
            have_timestamp=bool(timestamp),
            have_secret=bool(secret),
        )
    try:
        ts_int = int(timestamp)
    except ValueError as e:
        raise SlackSignatureError(
            f"X-Slack-Request-Timestamp not integer: {timestamp!r}"
        ) from e
    now_s = int(now if now is not None else time.time())
    if abs(now_s - ts_int) > max_age_s:
        raise SlackSignatureError(
            "slack timestamp too old (replay protection)",
            timestamp=ts_int,
            now=now_s,
            max_age_s=max_age_s,
        )
    basestring = f"v0:{timestamp}:{body.decode('utf-8')}".encode("utf-8")
    mac = hmac.new(secret.encode("utf-8"), basestring, hashlib.sha256)
    expected = "v0=" + mac.hexdigest()
    if not _constant_time_eq(expected, signature):
        raise SlackSignatureError("slack signature mismatch")


def parse_slack_ts(ts: str) -> datetime:
    """Parse a Slack message timestamp ("1234567890.123456") to UTC dt.

    Slack uses fractional-second epoch strings. Microsecond precision
    is fine for ingest ordering.
    """
    try:
        secs = float(ts)
    except ValueError as e:
        raise ValidationError(
            f"slack ts not a float: {ts!r}", field="ts"
        ) from e
    return datetime.fromtimestamp(secs, tz=timezone.utc)


def extract_entities_from_text(text: str) -> tuple[list[dict[str, Any]], list[str]]:
    """Return (entities_hint, unresolved_phrases) parsed from a Slack
    message body.

    entities_hint: dicts suitable for observations.entities_mentioned —
      - {"type": "slack_user", "id": "U01ABC"} for @mentions
      - {"type": "slack_channel", "id": "C01ENG"} for #channel refs
      - {"type": "url", "id": "https://..."} for URLs
    unresolved_phrases: kept empty for now — Agent 2-B's entity resolver
      will harvest these from the full text; we only extract structural
      hints at this layer.
    """
    entities: list[dict[str, Any]] = []
    for m in _MENTION_RE.finditer(text):
        entities.append({"type": "slack_user", "id": m.group(1)})
    for m in _CHANNEL_MENTION_RE.finditer(text):
        entities.append({"type": "slack_channel", "id": m.group(1)})
    for m in _URL_RE.finditer(text):
        entities.append({"type": "url", "id": m.group(1)})
    # De-dupe while preserving order.
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for e in entities:
        key = (e["type"], e["id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(e)
    return deduped, []


def _extract_event(payload: dict[str, Any]) -> dict[str, Any]:
    """Slack Events API wraps the real event under `event`. Support both
    the wrapped and the unwrapped shape so direct webhook forwards also
    work in tests.
    """
    if "event" in payload and isinstance(payload["event"], dict):
        return payload["event"]
    return payload


@register("slack:message")
async def handle_slack_message(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """Parse a Slack message webhook payload into an ObservationDraft.

    Expects signature verification to have been performed by the caller
    (ingestion core invokes `verify_slack_signature` before dispatching
    to the handler — handlers receive pre-verified payloads so that
    signature logic stays centralised).
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "slack payload must be a JSON object",
            channel="slack:message",
        )

    event = _extract_event(payload)
    text = event.get("text")
    if not isinstance(text, str):
        # Slack system events (channel_join, message_deleted, etc) may
        # omit text; reject with 400 so the sender knows it was
        # unprocessable. Wave 2-B can add handlers for the subtypes
        # that matter.
        raise ValidationError(
            "slack event missing 'text' field (handler supports message events only)",
            channel="slack:message",
            event_type=event.get("type"),
            subtype=event.get("subtype"),
        )
    ts = event.get("ts") or event.get("event_ts")
    channel_id = event.get("channel") or event.get("channel_id")
    user_id = event.get("user") or event.get("user_id")

    if not ts or not isinstance(ts, str):
        raise ValidationError(
            "slack event missing 'ts' string", channel="slack:message"
        )
    if not channel_id or not isinstance(channel_id, str):
        raise ValidationError(
            "slack event missing 'channel' string", channel="slack:message"
        )

    occurred_at = parse_slack_ts(ts)
    entities, _ = extract_entities_from_text(text)

    source_actor_ref = f"slack:{user_id}" if user_id else None
    external_id = f"{channel_id}:{ts}"

    content = {
        "channel": channel_id,
        "ts": ts,
        "user": user_id,
        "text": text,
        "team": event.get("team") or payload.get("team_id"),
        "event_type": event.get("type"),
    }

    return ObservationDraft(
        source_channel="slack:message",
        content_text=text,
        content=content,
        occurred_at=occurred_at,
        trust_tier=CHANNEL_TRUST_MAP["slack:message"],  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=source_actor_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


__all__ = [
    "SlackSignatureError",
    "verify_slack_signature",
    "parse_slack_ts",
    "extract_entities_from_text",
    "handle_slack_message",
]
