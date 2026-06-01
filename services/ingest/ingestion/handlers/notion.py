"""services/ingest/ingestion/handlers/notion.py — Notion object handler (IN-14).

ONE channel `notion:object` (decision D3). The handler branches on the
Notion object's native `object` field — "page" | "block" | "comment" —
exactly as the GitHub handler branches on `X-GitHub-Event`, assigning
per-record `kind` and `content.object_type`. There is no separate
`notion:page_content` channel: the normalizer routes
`(notion, backfill|poll) → notion:object` (channel_mapping), and a single
source+ingress pair cannot fan out to multiple registered channels.

Trust posture (§VI): all Notion objects are human-authored via an
authenticated integration — `attested_agent`, the same tier as
`slack:message`. Notion declares *intent*; it does not verify *reality*
(that is github merges / stripe events). A DB row carrying a `status`
property is emitted as `kind="state_change"` (a tracked workflow item),
otherwise `kind="signal"`.
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


_CHANNEL = "notion:object"
_TRUST = "attested_agent"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt: Any) -> datetime:
    if not isinstance(dt, str):
        return _utcnow()
    s = dt[:-1] + "+00:00" if dt.endswith("Z") else dt
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return _utcnow()
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _rich_text_to_plain(rich: Any) -> str:
    """Join a Notion rich_text array into plain text."""
    if not isinstance(rich, list):
        return ""
    out = []
    for span in rich:
        if isinstance(span, dict):
            txt = span.get("plain_text")
            if isinstance(txt, str):
                out.append(txt)
    return "".join(out)


def _page_title(properties: dict[str, Any]) -> str:
    """Extract a page's title from its `title`-typed property."""
    if not isinstance(properties, dict):
        return "(untitled)"
    for prop in properties.values():
        if isinstance(prop, dict) and prop.get("type") == "title":
            title = _rich_text_to_plain(prop.get("title"))
            if title:
                return title
    return "(untitled)"


def _actor_ref(obj: dict[str, Any], key: str) -> str | None:
    """`notion:{user_id}` from a `created_by`/`last_edited_by` object."""
    who = obj.get(key)
    if isinstance(who, dict):
        uid = who.get("id")
        if isinstance(uid, str) and uid:
            return f"notion:{uid}"
    return None


def _mentions(rich: Any) -> list[dict[str, Any]]:
    """Entity hints from `mention` spans in a rich_text array."""
    hints: list[dict[str, Any]] = []
    if not isinstance(rich, list):
        return hints
    for span in rich:
        if not isinstance(span, dict) or span.get("type") != "mention":
            continue
        mention = span.get("mention") or {}
        mtype = mention.get("type") if isinstance(mention, dict) else None
        if mtype == "user":
            uid = (mention.get("user") or {}).get("id")
            if isinstance(uid, str):
                hints.append({"type": "notion_user", "id": uid})
        elif mtype == "page":
            pid = (mention.get("page") or {}).get("id")
            if isinstance(pid, str):
                hints.append({"type": "notion_page", "id": pid})
    return hints


# ---------------------------------------------------------------------
# Per-object shapers
# ---------------------------------------------------------------------

def _shape_page(obj: dict[str, Any]) -> ObservationDraft:
    page_id = obj.get("id")
    if not isinstance(page_id, str):
        raise ValidationError("notion page missing id", channel=_CHANNEL)
    props = obj.get("properties") or {}
    title = _page_title(props)
    parent = obj.get("parent") or {}
    in_database = isinstance(parent, dict) and parent.get("type") == "database_id"
    db_id = parent.get("database_id") if in_database else None

    # A row in a database that carries a status/select workflow property is
    # a tracked work item → its arrival/change is a state_change.
    has_status = any(
        isinstance(p, dict) and p.get("type") in ("status", "select")
        for p in props.values()
    )
    kind = "state_change" if (in_database and has_status) else "signal"

    where = f"database {db_id}" if in_database else "workspace"
    content_text = f"Notion page '{title}' in {where}"

    entities: list[dict[str, Any]] = [{"type": "notion_page", "id": page_id}]
    if db_id:
        entities.append({"type": "notion_database", "id": db_id})
    # Relation properties encode page↔page edges.
    for name, prop in props.items() if isinstance(props, dict) else []:
        if isinstance(prop, dict) and prop.get("type") == "relation":
            for rel in prop.get("relation") or []:
                rid = rel.get("id") if isinstance(rel, dict) else None
                if isinstance(rid, str):
                    entities.append({"type": "notion_page", "id": rid, "relation": name})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": "page",
            "page_id": page_id,
            "title": title,
            "in_database": in_database,
            "database_id": db_id,
            "url": obj.get("url"),
            "properties": props,
            "workspace_id": obj.get("_fyralis_workspace_id"),
        },
        occurred_at=_parse_iso(obj.get("last_edited_time") or obj.get("created_time")),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=_actor_ref(obj, "last_edited_by"),
        external_id=f"notion:page:{page_id}",
        entities_hint=entities,
        raw_payload=obj,
    )


def _shape_block(obj: dict[str, Any]) -> ObservationDraft:
    block_id = obj.get("id")
    if not isinstance(block_id, str):
        raise ValidationError("notion block missing id", channel=_CHANNEL)
    block_type = obj.get("type") or "unknown"
    inner = obj.get(block_type) if isinstance(obj.get(block_type), dict) else {}
    text = _rich_text_to_plain(inner.get("rich_text"))
    content_text = f"Notion {block_type}: {text[:200]}" if text else f"Notion {block_type} block"

    content: dict[str, Any] = {
        "object_type": "block",
        "block_id": block_id,
        "block_type": block_type,
        "text": text,
        "workspace_id": obj.get("_fyralis_workspace_id"),
    }
    truncated = obj.get("_fyralis_truncated")
    if truncated:
        content["_truncated"] = truncated

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=_parse_iso(obj.get("last_edited_time") or obj.get("created_time")),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=_actor_ref(obj, "last_edited_by"),
        external_id=f"notion:block:{block_id}",
        entities_hint=_mentions(inner.get("rich_text")),
        raw_payload=obj,
    )


def _shape_comment(obj: dict[str, Any]) -> ObservationDraft:
    comment_id = obj.get("id")
    if not isinstance(comment_id, str):
        raise ValidationError("notion comment missing id", channel=_CHANNEL)
    text = _rich_text_to_plain(obj.get("rich_text"))
    parent = obj.get("parent") or {}
    parent_id = (
        parent.get("page_id") or parent.get("block_id")
        if isinstance(parent, dict) else None
    )
    content_text = f"Notion comment: {text[:200]}" if text else "Notion comment"

    entities: list[dict[str, Any]] = list(_mentions(obj.get("rich_text")))
    if isinstance(parent_id, str):
        entities.append({"type": "notion_page", "id": parent_id})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "object_type": "comment",
            "comment_id": comment_id,
            "text": text,
            "parent_id": parent_id,
            "discussion_id": obj.get("discussion_id"),
            "workspace_id": obj.get("_fyralis_workspace_id"),
        },
        occurred_at=_parse_iso(obj.get("created_time")),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=_actor_ref(obj, "created_by"),
        external_id=f"notion:comment:{comment_id}",
        entities_hint=entities,
        raw_payload=obj,
    )


_OBJECT_SHAPERS = {
    "page": _shape_page,
    "block": _shape_block,
    "comment": _shape_comment,
}


@register(_CHANNEL)
async def handle_notion_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """Entry point. Branches on the Notion object's native `object` field."""
    if not isinstance(payload, dict):
        raise ValidationError("notion payload must be a JSON object", channel=_CHANNEL)
    object_type = payload.get("object")
    shaper = _OBJECT_SHAPERS.get(object_type)
    if shaper is None:
        raise ValidationError(
            f"unsupported notion object type: {object_type}",
            channel=_CHANNEL,
            supported=sorted(_OBJECT_SHAPERS.keys()),
        )
    return shaper(payload)


# Single channel (D3): every Notion observation carries
# source_channel="notion:object". Object granularity lives in
# content.object_type + kind. Register the trust default so any code that
# looks up source_channel → trust finds it (the handler also sets it
# explicitly per draft).
CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_notion_object"]
