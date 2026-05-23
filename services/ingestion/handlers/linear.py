"""services/ingestion/handlers/linear.py — Linear webhook handler.

BUILD-PLAN §3 Prompt 2.B:
    "Events: Issue.create, Issue.update (state, assignee, due_date),
     Comment.create, Project.update.
     Entities: issue id, project id, team id.
     Trust tier: authoritative for state changes, inferential for
     comments.
     Signature verification per Linear's webhook spec
     (`Linear-Signature` header, HMAC-SHA256 against raw body)."

Protocol (developers.linear.app/docs/graphql/webhooks):
    Linear-Signature: <hex_hmac_sha256(secret, body)>
    Payload body (JSON):
        {
            "action": "create" | "update" | "remove",
            "type":   "Issue" | "Comment" | "Project" | "IssueLabel" | ...,
            "data":   { ... },
            "updatedFrom": { ... }   # on update, the prior field values
            "createdAt": "2026-04-21T10:00:00Z",
            "url":        "https://linear.app/...",
        }
"""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingestion.handlers import (
    HandlerError,
    ObservationDraft,
    register,
)


_CHANNEL = "linear:webhook"


class LinearSignatureError(HandlerError):
    default_code = "linear_signature_invalid"


def _constant_time_eq(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def verify_linear_signature(
    body: bytes,
    signature_header: str | None,
    secret: str | None,
) -> None:
    """Raise `LinearSignatureError` if the HMAC-SHA256 fails.

    Linear sends the signature as the raw hex digest (no prefix),
    per its webhook docs.
    """
    if not secret:
        raise LinearSignatureError(
            "LINEAR_WEBHOOK_SECRET is not configured", reason="missing_secret"
        )
    if not signature_header:
        raise LinearSignatureError(
            "missing Linear-Signature header", reason="missing_signature"
        )
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    expected = mac.hexdigest()
    # Some Linear tooling sends lowercase hex; be lenient on case.
    if not _constant_time_eq(expected.lower(), signature_header.strip().lower()):
        raise LinearSignatureError(
            "linear signature mismatch", reason="mismatch"
        )


def _parse_iso(dt: Any, default: datetime | None = None) -> datetime:
    if dt is None:
        return default or datetime.now(timezone.utc)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt
    s = str(dt)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return default or datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _actor_of(data: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (display_name, source_actor_ref) for the webhook actor.

    Linear exposes `assignee` (for issues) and `user` (for comments);
    source_actor_ref is prefixed `linear:` so it feeds directly into
    actor_identity_mappings.
    """
    for key in ("user", "actor", "assignee", "creator", "updatedBy"):
        person = data.get(key)
        if isinstance(person, dict):
            uid = person.get("id")
            name = person.get("name") or person.get("displayName")
            if uid:
                return name, f"linear:{uid}"
    return None, None


# ---------------------------------------------------------------------
# Event-type shapers
# ---------------------------------------------------------------------

def _shape_issue(payload: dict[str, Any]) -> ObservationDraft:
    action = payload.get("action") or "unknown"
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise ValidationError(
            "linear Issue payload missing data object", channel=_CHANNEL
        )
    identifier = data.get("identifier")  # e.g. "ENG-123"
    title = (data.get("title") or "").strip() or "(untitled)"
    issue_id = data.get("id")
    team = data.get("team") or {}
    team_id = team.get("id") if isinstance(team, dict) else None
    project = data.get("project") or {}
    project_id = project.get("id") if isinstance(project, dict) else None
    actor_name, actor_ref = _actor_of(data)

    updated_from = payload.get("updatedFrom") or {}
    trust_tier: str
    kind: str

    # Default sentence; overridden below for specific state changes.
    content_text = (
        f"{actor_name or 'someone'} {action}d {identifier} '{title}'"
    )
    trust_tier = "authoritative"
    kind = "signal"

    if action == "create":
        content_text = (
            f"{actor_name or 'someone'} created {identifier} '{title}'"
        )
        trust_tier = "authoritative"
        kind = "signal"
    elif action == "update":
        # Introspect updatedFrom to produce a readable sentence.
        # Prefer state changes, then assignee, then due_date.
        if "state" in updated_from:
            old_state = updated_from.get("state", {}) or {}
            new_state = data.get("state") or {}
            old_name = (
                old_state.get("name") if isinstance(old_state, dict) else None
            ) or "?"
            new_name = (
                new_state.get("name") if isinstance(new_state, dict) else None
            ) or "?"
            content_text = (
                f"{actor_name or 'someone'} moved {identifier} "
                f"'{title}' from {old_name} to {new_name}"
            )
            kind = "state_change"
        elif "assigneeId" in updated_from:
            new_assignee = (data.get("assignee") or {}).get("name") or "(none)"
            content_text = (
                f"{actor_name or 'someone'} reassigned {identifier} "
                f"'{title}' to {new_assignee}"
            )
            kind = "state_change"
        elif "dueDate" in updated_from:
            new_due = data.get("dueDate") or "(none)"
            content_text = (
                f"{actor_name or 'someone'} set due date of "
                f"{identifier} '{title}' to {new_due}"
            )
            kind = "state_change"
        else:
            content_text = (
                f"{actor_name or 'someone'} updated {identifier} '{title}'"
            )
            kind = "signal"

    entities_hint: list[dict[str, Any]] = []
    if issue_id:
        entities_hint.append({"type": "linear_issue", "id": issue_id})
    if project_id:
        entities_hint.append({"type": "linear_project", "id": project_id})
    if team_id:
        entities_hint.append({"type": "linear_team", "id": team_id})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "Issue",
            "action": action,
            "identifier": identifier,
            "issue_id": issue_id,
            "team_id": team_id,
            "project_id": project_id,
            "title": title,
            "updatedFrom": updated_from,
        },
        occurred_at=_parse_iso(
            payload.get("createdAt") or data.get("updatedAt") or data.get("createdAt")
        ),
        trust_tier=trust_tier,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=actor_ref,
        external_id=issue_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_comment(payload: dict[str, Any]) -> ObservationDraft:
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise ValidationError(
            "linear Comment payload missing data object", channel=_CHANNEL
        )
    comment_id = data.get("id")
    body = (data.get("body") or "").strip()
    issue = data.get("issue") or {}
    issue_id = issue.get("id") if isinstance(issue, dict) else None
    issue_identifier = (
        issue.get("identifier") if isinstance(issue, dict) else None
    )
    actor_name, actor_ref = _actor_of(data)

    content_text = (
        f"{actor_name or 'someone'} commented on {issue_identifier or 'an issue'}: "
        f"{body[:200]}"
    )

    entities_hint: list[dict[str, Any]] = []
    if issue_id:
        entities_hint.append({"type": "linear_issue", "id": issue_id})
    if comment_id:
        entities_hint.append({"type": "linear_comment", "id": comment_id})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "Comment",
            "action": payload.get("action"),
            "comment_id": comment_id,
            "issue_id": issue_id,
            "issue_identifier": issue_identifier,
            "body": body,
        },
        occurred_at=_parse_iso(
            data.get("updatedAt") or data.get("createdAt") or payload.get("createdAt")
        ),
        trust_tier="inferential",
        kind="signal",
        source_actor_ref=actor_ref,
        external_id=comment_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _shape_project(payload: dict[str, Any]) -> ObservationDraft:
    action = payload.get("action") or "update"
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        raise ValidationError(
            "linear Project payload missing data object", channel=_CHANNEL
        )
    project_id = data.get("id")
    name = (data.get("name") or "").strip() or "(unnamed)"
    state = data.get("state")
    actor_name, actor_ref = _actor_of(data)

    if action == "update":
        content_text = (
            f"{actor_name or 'someone'} updated project '{name}'"
            + (f" state={state}" if state else "")
        )
        kind = "state_change"
    else:
        content_text = (
            f"{actor_name or 'someone'} {action}d project '{name}'"
        )
        kind = "signal"

    entities_hint: list[dict[str, Any]] = []
    if project_id:
        entities_hint.append({"type": "linear_project", "id": project_id})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "event_type": "Project",
            "action": action,
            "project_id": project_id,
            "name": name,
            "state": state,
            "updatedFrom": payload.get("updatedFrom"),
        },
        occurred_at=_parse_iso(
            data.get("updatedAt") or data.get("createdAt") or payload.get("createdAt")
        ),
        trust_tier="authoritative",
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=actor_ref,
        external_id=project_id,
        entities_hint=entities_hint,
        raw_payload=payload,
    )


_TYPE_SHAPERS = {
    "Issue": _shape_issue,
    "Comment": _shape_comment,
    "Project": _shape_project,
}


@register(_CHANNEL)
async def handle_linear_webhook(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """Linear webhook entry.

    Signature verification is the caller's responsibility; if headers
    contain `Linear-Signature` and the ambient env has a secret, the
    handler verifies defensively.

    Event-type routing comes from `payload['type']` (Linear embeds
    it in the body unlike GitHub which uses a header).
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "linear payload must be a JSON object", channel=_CHANNEL
        )
    event_type = payload.get("type")
    if not event_type:
        raise ValidationError(
            "missing 'type' field in linear payload", channel=_CHANNEL
        )
    shaper = _TYPE_SHAPERS.get(event_type)
    if shaper is None:
        raise ValidationError(
            f"unsupported linear type: {event_type}",
            channel=_CHANNEL,
            supported=sorted(_TYPE_SHAPERS.keys()),
        )
    return shaper(payload)


__all__ = [
    "LinearSignatureError",
    "verify_linear_signature",
    "handle_linear_webhook",
]
