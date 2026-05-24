"""services/ingestion/handlers/jira.py — Jira issue/transition/comment handler (IN-17).

ONE channel `jira:issue` (decision D3, mirrors github:webhook's one-channel/
many-event-types shape). The handler is a pure function (no DB / network) and
branches on the input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"issue","transition","comment"} (set by the fetcher's per-issue fan-out).
  - LIVE WEBHOOK: the raw Jira webhook body carries `webhookEvent`
    (e.g. "jira:issue_updated", "comment_created"); the handler maps it onto the
    same three record builders so a webhook-delivered change and its backfill
    twin dedup.

Signal mapping (the reasoning value, per the design doc):
  - issue snapshot               -> kind="signal", object_type="issue"
  - changelog STATUS transition  -> kind="state_change" (flow/velocity signal)
  - other field changes          -> kind="signal", object_type="transition"
  - comment                      -> kind="signal", object_type="comment"

external_id — VERSIONED for the MUTABLE entities (the observations repo dedups
on (source_channel, external_id) IGNORING occurred_at; per the IN-15 lesson a
re-edit must land as a NEW observation, not silently dedup):
  - issue:      jira:{site}:issue:{issue_id}:{updated}
  - comment:    jira:{site}:comment:{comment_id}:{updated}
  - transition: jira:{site}:transition:{issue_id}:{history_id}   (immutable)

Trust posture: Jira is the system of record for work-tracking -> `authoritative`.
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


_CHANNEL = "jira:issue"
_TRUST = "authoritative"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    """Parse a Jira ISO-8601 timestamp. Jira returns offsets WITHOUT a colon
    (e.g. `2026-05-24T10:30:00.000+0000`), which `fromisoformat` rejects on
    older Pythons — normalise to `+00:00`."""
    if not isinstance(value, str) or not value:
        return None
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif len(s) >= 5 and s[-5] in "+-" and s[-3] != ":":
        s = s[:-2] + ":" + s[-2:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _adf_to_text(node: Any) -> str:
    """Flatten an Atlassian Document Format (ADF) body to plain text. Comment
    and description bodies are ADF docs in the v3 API; older shapes / our mock
    may send a plain string."""
    if isinstance(node, str):
        return node
    if not isinstance(node, dict):
        return ""
    parts: list[str] = []
    if node.get("type") == "text" and isinstance(node.get("text"), str):
        parts.append(node["text"])
    content = node.get("content")
    if isinstance(content, list):
        for child in content:
            child_text = _adf_to_text(child)
            if child_text:
                parts.append(child_text)
    # Paragraph / hardBreak separators keep the text legible.
    sep = "\n" if node.get("type") in ("paragraph", "heading") else " "
    return sep.join(p for p in parts if p).strip()


def _account_ref(actor: Any) -> tuple[str | None, dict[str, Any] | None]:
    """(`source_actor_ref`, entity_hint) for a Jira user object."""
    if not isinstance(actor, dict):
        return None, None
    account_id = actor.get("accountId")
    email = actor.get("emailAddress")
    name = actor.get("displayName")
    # Prefer email (resolvable across sources); fall back to the Jira accountId.
    if isinstance(email, str) and email:
        ref = f"email:{email.lower()}"
        hint = {"type": "email_address", "id": email.lower(), "role": "actor"}
        if name:
            hint["display_name"] = name
        return ref, hint
    if isinstance(account_id, str) and account_id:
        hint = {"type": "jira_account", "id": account_id, "role": "actor"}
        if name:
            hint["display_name"] = name
        return f"jira:account:{account_id}", hint
    return None, None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------
# Per-record-type draft builders (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _issue_draft(issue: dict[str, Any], site: str) -> ObservationDraft:
    issue_id = str(issue.get("id") or "")
    key = issue.get("key") or issue_id
    fields = issue.get("fields") or {}
    if not issue_id:
        raise ValidationError("jira issue missing id", channel=_CHANNEL)

    updated = fields.get("updated") or fields.get("created")
    occurred = _parse_iso(updated) or _utcnow()
    external_id = f"jira:{site}:issue:{issue_id}:{updated or 'none'}"

    summary = fields.get("summary") or "(no summary)"
    status = ((fields.get("status") or {}).get("name")) if isinstance(fields.get("status"), dict) else None
    issuetype = ((fields.get("issuetype") or {}).get("name")) if isinstance(fields.get("issuetype"), dict) else None
    priority = ((fields.get("priority") or {}).get("name")) if isinstance(fields.get("priority"), dict) else None
    resolution = ((fields.get("resolution") or {}).get("name")) if isinstance(fields.get("resolution"), dict) else None
    assignee_ref, assignee_hint = _account_ref(fields.get("assignee"))
    reporter_ref, reporter_hint = _account_ref(fields.get("reporter") or fields.get("creator"))

    content_text = f"[{key}] {summary}"
    meta = " · ".join(p for p in (issuetype, status, priority) if p)
    if meta:
        content_text += f" — {meta}"

    entities: list[dict[str, Any]] = [{"type": "jira_issue", "id": str(key)}]
    project = fields.get("project") or {}
    if isinstance(project, dict) and project.get("key"):
        entities.append({"type": "jira_project", "id": project["key"]})
    if assignee_hint:
        assignee_hint = {**assignee_hint, "role": "assignee"}
        entities.append(assignee_hint)
    if reporter_hint:
        reporter_hint = {**reporter_hint, "role": "reporter"}
        entities.append(reporter_hint)

    content: dict[str, Any] = {
        "object_type": "issue",
        "issue_id": issue_id,
        "issue_key": key,
        "summary": summary,
        "status": status,
        "issue_type": issuetype,
        "priority": priority,
        "resolution": resolution,
        "labels": fields.get("labels") or [],
        "project_key": project.get("key") if isinstance(project, dict) else None,
        "assignee": assignee_ref,
        "reporter": reporter_ref,
        "created": fields.get("created"),
        "updated": updated,
        # Common default custom fields (story points / sprint); absent is fine.
        "story_points": fields.get("customfield_10016"),
        "sprint": fields.get("customfield_10020"),
        "description": _truncate(_adf_to_text(fields.get("description"))),
    }

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=reporter_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=issue,
    )


_STATE_CHANGE_FIELDS = frozenset({"status", "resolution"})


def _transition_draft(
    history: dict[str, Any], issue_id: str, issue_key: Any, site: str,
) -> ObservationDraft:
    history_id = str(history.get("id") or "")
    if not issue_id or not history_id:
        raise ValidationError(
            "jira transition missing issue_id/history_id", channel=_CHANNEL,
        )
    items = history.get("items")
    items = [i for i in items if isinstance(i, dict)] if isinstance(items, list) else []
    occurred = _parse_iso(history.get("created")) or _utcnow()
    author_ref, author_hint = _account_ref(history.get("author"))
    key = issue_key or issue_id

    changed_fields = [str(i.get("field")) for i in items if i.get("field")]
    is_state_change = any(f in _STATE_CHANGE_FIELDS for f in changed_fields)
    external_id = f"jira:{site}:transition:{issue_id}:{history_id}"

    # Human-legible synthesis — lead with the status transition when present.
    phrases: list[str] = []
    for item in items:
        field = item.get("field")
        frm = item.get("fromString")
        to = item.get("toString")
        phrases.append(f"{field}: {frm or '∅'} → {to or '∅'}")
    who = (author_hint or {}).get("display_name") or author_ref or "someone"
    content_text = f"[{key}] {who} changed " + "; ".join(phrases) if phrases else f"[{key}] updated"

    entities: list[dict[str, Any]] = [{"type": "jira_issue", "id": str(key)}]
    if author_hint:
        entities.append(author_hint)

    content: dict[str, Any] = {
        "object_type": "transition",
        "issue_id": issue_id,
        "issue_key": key,
        "history_id": history_id,
        "changed_fields": changed_fields,
        "items": items,
        "author": author_ref,
        "created": history.get("created"),
    }

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change" if is_state_change else "signal",
        source_actor_ref=author_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=history,
    )


def _comment_draft(
    comment: dict[str, Any], issue_id: str, issue_key: Any, site: str,
) -> ObservationDraft:
    comment_id = str(comment.get("id") or "")
    if not issue_id or not comment_id:
        raise ValidationError(
            "jira comment missing issue_id/comment_id", channel=_CHANNEL,
        )
    updated = comment.get("updated") or comment.get("created")
    occurred = _parse_iso(updated) or _utcnow()
    author_ref, author_hint = _account_ref(comment.get("author") or comment.get("updateAuthor"))
    key = issue_key or issue_id
    body_text = _adf_to_text(comment.get("body"))
    external_id = f"jira:{site}:comment:{comment_id}:{updated or 'none'}"

    who = (author_hint or {}).get("display_name") or author_ref or "someone"
    content_text = f"[{key}] {who} commented: {_truncate(body_text)}"

    entities: list[dict[str, Any]] = [{"type": "jira_issue", "id": str(key)}]
    if author_hint:
        entities.append(author_hint)

    content: dict[str, Any] = {
        "object_type": "comment",
        "issue_id": issue_id,
        "issue_key": key,
        "comment_id": comment_id,
        "author": author_ref,
        "body": body_text,
        "created": comment.get("created"),
        "updated": updated,
    }

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=author_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=comment,
    )


def _site_of(payload: dict[str, Any]) -> str:
    """external_id site namespace. Backfill/poll records carry `_fyralis_site`;
    a live webhook carries it in the issue's `self` URL host (fallback)."""
    site = payload.get("_fyralis_site")
    if isinstance(site, str) and site:
        return site
    issue = payload.get("issue") or {}
    self_url = issue.get("self") if isinstance(issue, dict) else None
    if isinstance(self_url, str) and self_url:
        return self_url.replace("https://", "").replace("http://", "").split("/")[0]
    return "unknown"


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_jira_issue(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("jira payload must be a JSON object", channel=_CHANNEL)

    site = _site_of(payload)

    # --- LIVE WEBHOOK path (raw Jira webhook body) ---
    webhook_event = payload.get("webhookEvent")
    if isinstance(webhook_event, str) and webhook_event:
        issue = payload.get("issue") or {}
        issue_id = str(issue.get("id") or "") if isinstance(issue, dict) else ""
        issue_key = issue.get("key") if isinstance(issue, dict) else None

        if webhook_event.startswith("comment_"):
            comment = payload.get("comment") or {}
            if isinstance(comment, dict) and comment.get("id"):
                return _comment_draft(comment, issue_id, issue_key, site)
            raise ValidationError(
                f"jira {webhook_event} missing comment", channel=_CHANNEL,
            )

        if webhook_event.startswith("jira:issue"):
            # A status/resolution transition is the high-value signal — emit it
            # as a state_change when the changelog carries one; otherwise emit
            # the issue snapshot. (Comments come as separate comment_* events.)
            changelog = payload.get("changelog")
            if isinstance(changelog, dict) and changelog.get("items"):
                history = {
                    "id": changelog.get("id"),
                    "items": changelog.get("items"),
                    "author": payload.get("user"),
                    "created": (issue.get("fields") or {}).get("updated")
                    if isinstance(issue, dict) else None,
                }
                items = [i for i in changelog["items"] if isinstance(i, dict)]
                if any((i.get("field") in _STATE_CHANGE_FIELDS) for i in items):
                    return _transition_draft(history, issue_id, issue_key, site)
            if isinstance(issue, dict) and issue.get("id"):
                return _issue_draft(issue, site)

        raise ValidationError(
            f"unsupported jira webhook event {webhook_event!r}", channel=_CHANNEL,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if record_type == "transition":
        return _transition_draft(
            payload.get("history") or {},
            str(payload.get("_fyralis_issue_id") or ""),
            payload.get("_fyralis_issue_key"),
            site,
        )
    if record_type == "comment":
        return _comment_draft(
            payload.get("comment") or {},
            str(payload.get("_fyralis_issue_id") or ""),
            payload.get("_fyralis_issue_key"),
            site,
        )
    if record_type == "issue" or "fields" in payload:
        return _issue_draft(payload, site)

    raise ValidationError(
        "jira payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_jira_issue"]
