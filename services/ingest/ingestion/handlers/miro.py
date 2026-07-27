"""Normalize fetcher-tagged Miro board items.

The poll-only source has one channel, ``miro:item``. Backfill and reconciliation
records must be tagged by the fetcher with ``_fyralis_record_type == "item"``,
``_fyralis_org_id``, and ``_fyralis_board_id``. The handler is a pure function
and produces exactly one signal observation per call.

The external id is versioned for the mutable item entity (the observations repo
dedups on (source_channel, external_id) IGNORING occurred_at; an edit must land
as a NEW observation, not silently dedup):
  - item: miro:{org_id}:item:{item_id}:{version}

The org id namespaces the key so two tenants' identical board/item ids stay
distinct under the global UNIQUE(source_channel, external_id, occurred_at).

Trust posture: Miro is the system of record for its boards -> `authoritative`.

TODO(human): confirm Miro item version field. The handler keys the external_id
on `item.modifiedAt` (Miro's last-modified timestamp) as the version
discriminator, falling back to `version`/`createdAt`. If Miro exposes a
monotonic version counter on items, prefer it (it is a cleaner discriminator
than a timestamp).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lib.shared.errors import ValidationError

from services.ingest.ingestion import idempotency
from services.ingest.ingestion.handlers import (
    ObservationDraft,
)


_CHANNEL = "miro:item"
_TRUST = "authoritative"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
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


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _item_version(item: dict[str, Any]) -> str:
    """The mutation discriminator that versions the external_id.

    Prefers Miro's monotonic version counter when present, else the
    last-modified timestamp, else the created timestamp; `none` when absent so
    a versionless item still keys deterministically."""
    ver = (
        item.get("version")
        or item.get("modifiedAt")
        or item.get("updatedAt")
        or item.get("createdAt")
    )
    return str(ver) if ver else "none"


def _item_text(item: dict[str, Any]) -> str:
    """The human-readable content carried by the item (sticky-note / text /
    card title), defaulting to the item type when the item carries no text."""
    data = item.get("data")
    if isinstance(data, dict):
        for key in ("content", "text", "title"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    for key in ("content", "text", "title"):
        val = item.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return ""


def _item_extras(item: dict[str, Any]) -> dict[str, Any]:
    """The richer Miro item fields beyond the core text/type. Only present
    (non-None) keys are returned so `content` stays lean."""
    raw: dict[str, Any] = {
        "created_by": _actor_id(item.get("createdBy")),
        "modified_by": _actor_id(item.get("modifiedBy")),
        "parent_id": _nested_id(item.get("parent")),
        "position": item.get("position"),
        "geometry": item.get("geometry"),
        "style": item.get("style"),
        "created_at": item.get("createdAt"),
        "modified_at": item.get("modifiedAt"),
    }
    return {k: v for k, v in raw.items() if v is not None}


def _actor_id(actor: Any) -> Any:
    if isinstance(actor, dict):
        return actor.get("id") or actor.get("name")
    return actor


def _nested_id(obj: Any) -> Any:
    if isinstance(obj, dict):
        return obj.get("id")
    return obj


# ---------------------------------------------------------------------
# Draft builder
# ---------------------------------------------------------------------

def _item_draft(
    item: dict[str, Any], org_id: str, board_id: str,
) -> ObservationDraft:
    item_id = str(item.get("id") or "")
    if not org_id or not item_id:
        raise ValidationError(
            "miro item missing org_id/id", channel=_CHANNEL,
        )
    item_type = str(item.get("type") or "item")
    version = _item_version(item)
    occurred = (
        _parse_iso(item.get("modifiedAt"))
        or _parse_iso(item.get("updatedAt"))
        or _parse_iso(item.get("createdAt"))
        or _utcnow()
    )
    external_id = idempotency.miro_item(org_id, item_id, version)

    text = _item_text(item)
    if text:
        content_text = f"{item_type} updated: {text}"
    else:
        content_text = f"{item_type} updated on board {board_id}"

    entities: list[dict[str, Any]] = [
        {"type": "miro_board", "id": board_id},
    ]
    created_by = _actor_id(item.get("createdBy"))
    if isinstance(created_by, str) and created_by:
        entities.append({"type": "person", "id": created_by, "role": "author"})

    content: dict[str, Any] = {
        "object_type": "item",
        "org_id": org_id,
        "board_id": board_id,
        "item_id": item_id,
        "item_type": item_type,
        "text": text or None,
    }
    content.update(_item_extras(item))
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=item,
    )


def _org_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the exact install scope from a fetcher-tagged record."""
    oid = payload.get("_fyralis_org_id")
    if isinstance(oid, str) and oid:
        return oid
    if isinstance(obj, dict):
        cand = obj.get("org_id")
        if isinstance(cand, str) and cand:
            return cand
    return ""


def _board_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the board id from a fetcher-tagged record."""
    bid = payload.get("_fyralis_board_id")
    if isinstance(bid, str) and bid:
        return bid
    if isinstance(obj, dict):
        cand = obj.get("boardId") or obj.get("board_id")
        if isinstance(cand, str) and cand:
            return cand
        board = obj.get("board")
        if isinstance(board, dict) and isinstance(board.get("id"), str):
            return board["id"]
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

async def handle_miro_item(
    payload: dict[str, Any], _headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("miro payload must be a JSON object", channel=_CHANNEL)

    record_type = payload.get("_fyralis_record_type")
    if record_type != "item":
        raise ValidationError(
            "miro payload must be a tagged backfill/poll item record",
            channel=_CHANNEL,
        )
    item = payload.get("item")
    if not isinstance(item, dict):
        raise ValidationError(
            "miro tagged item record missing item",
            channel=_CHANNEL,
        )
    return _item_draft(
        item, _org_id_of(payload, item), _board_id_of(payload, item),
    )

__all__ = ["handle_miro_item"]
