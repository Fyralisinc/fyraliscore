"""services/ingest/ingestion/handlers/miro.py — Miro board-item handler.

ONE channel `miro:item` (mirrors brex:transaction / github:webhook's
one-channel/many-record-types shape). The handler is a pure function (no DB /
network) and branches on the input shape to produce exactly ONE observation per
call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    == "item" (set by the fetcher's per-board fan-out), plus `_fyralis_org_id`
    (the install-namespacing org id) and `_fyralis_board_id`.
  - LIVE WEBHOOK: the raw Miro webhook body carries an `event`
    (e.g. "board_item.created", "board_item.updated", "board_item.deleted");
    the handler maps it onto the same item builder so a webhook-delivered change
    and its backfill twin dedup.

Signal mapping (the reasoning value):
  - item created/updated (present)  -> kind="signal"
  - item deleted/removed            -> kind="state_change" (the board-state
                                       signal: an item was removed)

external_id — VERSIONED for the MUTABLE item entity (the observations repo
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
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    register,
)


_CHANNEL = "miro:item"
_TRUST = "authoritative"

# Webhook event suffixes that represent a removal (a board-state change).
_DELETE_EVENTS = frozenset({"deleted", "removed"})


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
# Per-record-type draft builders (shared by backfill + webhook paths)
# ---------------------------------------------------------------------

def _item_draft(
    item: dict[str, Any], org_id: str, board_id: str, *, deleted: bool = False,
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
    verb = "removed" if deleted else "updated"
    if text:
        content_text = f"{item_type} {verb}: {text}"
    else:
        content_text = f"{item_type} {verb} on board {board_id}"

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
        "deleted": deleted,
    }
    content.update(_item_extras(item))
    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="state_change" if deleted else "signal",
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=item,
    )


def _org_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the org id for a webhook/backfill record."""
    oid = payload.get("_fyralis_org_id") or payload.get("orgId") or payload.get("organizationId")
    if isinstance(oid, str) and oid:
        return oid
    if isinstance(obj, dict):
        cand = obj.get("orgId") or obj.get("org_id")
        if isinstance(cand, str) and cand:
            return cand
    return ""


def _board_id_of(payload: dict[str, Any], obj: dict[str, Any] | None) -> str:
    """Resolve the board id for a webhook/backfill record."""
    bid = payload.get("_fyralis_board_id") or payload.get("boardId")
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

@register(_CHANNEL)
async def handle_miro_item(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("miro payload must be a JSON object", channel=_CHANNEL)

    # --- LIVE WEBHOOK path (raw Miro webhook body) ---
    event_type = payload.get("event") or payload.get("type")
    if isinstance(event_type, str) and event_type:
        if event_type.startswith(("board_item.", "item.")):
            item = payload.get("item") or payload.get("data") or {}
            if isinstance(item, dict) and item.get("id"):
                deleted = event_type.rsplit(".", 1)[-1] in _DELETE_EVENTS
                return _item_draft(
                    item,
                    _org_id_of(payload, item),
                    _board_id_of(payload, item),
                    deleted=deleted,
                )
            raise ValidationError(
                f"miro {event_type} missing item", channel=_CHANNEL,
            )
        raise ValidationError(
            f"unsupported miro webhook event {event_type!r}", channel=_CHANNEL,
        )

    # --- BACKFILL / POLL path (fetcher-tagged records) ---
    record_type = payload.get("_fyralis_record_type")
    if record_type == "item" or "item" in payload:
        item = payload.get("item") or {}
        return _item_draft(
            item, _org_id_of(payload, item), _board_id_of(payload, item),
        )

    raise ValidationError(
        "miro payload is neither a webhook event nor a tagged record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_miro_item"]
