"""services/ingest/ingestion/handlers/linkedin.py — LinkedIn people/recruiting handler.

ONE channel `linkedin:object` (mirrors carta:object's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"share","social_action","follower_stat"} (set by the fetcher or the poll
    dispatcher).
  - LIVE POLL: the poll dispatcher (`integrations/linkedin/poll.py`) emits the
    SAME fetcher-shaped tagged record, so a polled change and its backfill twin
    dedup.

LinkedIn is POLL-ONLY (no webhook), so there is no webhook-envelope branch — the
live edge re-uses the backfill record shape exactly.

external_id — DISCRIMINATED by entity_kind, NOT versioned by a sync token (the
observations repo dedups on (source_channel, external_id) IGNORING occurred_at):
  - linkedin:{org}:{kind}:{id}
LinkedIn organization objects are append/stat-shaped (a share is published once,
a follower-stat snapshot has a window-keyed id), so a fresh id per object is the
natural dedup key; the entity_kind discriminator keeps multi-entity fixtures with
the same id from ever colliding.

Trust posture: LinkedIn is the organization system of record for its own
shares/stats -> `authoritative`.
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


_CHANNEL = "linkedin:object"
_TRUST = "authoritative"

# Map a LinkedIn record_type (backfill/poll) to a canonical kind. Per the
# cross-agent CONTRACT the entity types are share | social_action | follower_stat.
_ENTITY_NORMALISE = {
    "share": "share",
    "social_action": "social_action",
    "follower_stat": "follower_stat",
}

# Organization lifecycle states that constitute a state_change (vs an open
# signal). LinkedIn shares can be edited/deleted; stats are snapshots.
_STATE_CHANGE_STATUSES = {
    "deleted", "removed", "archived", "edited", "expired",
}


def linkedin_entity(
    organization_urn: str, entity_kind: str, entity_id: str,
) -> str:
    """`linkedin:{org}:{kind}:{id}` — DISCRIMINATED by entity_kind so multi-entity
    fixtures sharing an id never collide. NOT versioned by a sync token (LinkedIn
    organization objects are append/stat-shaped).

    TODO(human): during the wiring phase move this constructor to
        `services/ingest/ingestion/idempotency/__init__.py` as
        `linkedin_entity(organization_urn, entity_kind, entity_id)` (the canonical
        home, mirroring `carta_entity`) and import it here. That module is a
        SHARED file this phase must not edit, so the format lives here for now —
        the format string MUST stay byte-identical across the move.
    """
    return f"linkedin:{organization_urn}:{entity_kind}:{entity_id}"


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


def _last_updated(entity: dict[str, Any]) -> str | None:
    meta = entity.get("MetaData") or entity.get("Metadata") or {}
    if isinstance(meta, dict):
        v = meta.get("LastUpdatedTime")
        return v if isinstance(v, str) else None
    return None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _author(entity: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """(actor_ref, entity_hint) for the author/member on the LinkedIn object."""
    ref = entity.get("AuthorRef") or entity.get("MemberRef")
    if isinstance(ref, dict):
        name = ref.get("name")
        rid = ref.get("value")
        hint: dict[str, Any] = {"type": "person", "role": "author"}
        if name:
            hint["id"] = name
        elif rid:
            hint["id"] = str(rid)
        actor = f"linkedin:member:{rid}" if rid else None
        return actor, (hint if hint.get("id") else None)
    return None, None


def _label(entity_kind: str, entity: dict[str, Any]) -> str:
    """Human reference like 'Share UGC-12' / 'Follower Stat 2026-06'."""
    doc = entity.get("DocNumber") or entity.get("Id") or "?"
    nice = entity_kind.replace("_", " ").title()
    return f"{nice} {doc}"


def _classify(entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for a LinkedIn object.

    Objects whose `Status` indicates a lifecycle transition (deleted / archived /
    edited / …) are `state_change`; everything else is an open `signal`."""
    status = entity.get("Status")
    status_word = str(status).strip().lower() if status else "active"
    if status_word in _STATE_CHANGE_STATUSES:
        return "state_change", status_word
    return "signal", status_word


def _entity_extras(entity: dict[str, Any]) -> dict[str, Any]:
    """The richer LinkedIn organization fields beyond the header. Only present
    keys are returned so `content` stays lean.

    TODO(human): confirm the real LinkedIn organization field names. These map
    the placeholder fixture shape; the entitled REST surface exposes
    likeCount/commentCount/shareCount under socialActions and
    organicFollowerCount/paidFollowerCount under followerStatistics.
    """
    extras: dict[str, Any] = {}
    for src, dst in (
        ("Commentary", "commentary"),
        ("Text", "text"),
        ("LikeCount", "like_count"),
        ("CommentCount", "comment_count"),
        ("ShareCount", "share_count"),
        ("ClickCount", "click_count"),
        ("ImpressionCount", "impression_count"),
        ("EngagementRate", "engagement_rate"),
        ("FollowerCount", "follower_count"),
        ("OrganicFollowerCount", "organic_follower_count"),
        ("PaidFollowerCount", "paid_follower_count"),
        ("PublishedAt", "published_at"),
        ("TimeRange", "time_range"),
    ):
        if entity.get(src) is not None:
            extras[dst] = entity.get(src)
    return extras


def _entity_draft(
    entity_kind: str, entity: dict[str, Any], organization_urn: str,
) -> ObservationDraft:
    entity_id = str(entity.get("Id") or "")
    if not organization_urn or not entity_id:
        raise ValidationError(
            "linkedin entity missing organization_urn/Id", channel=_CHANNEL,
        )
    external_id = linkedin_entity(organization_urn, entity_kind, entity_id)

    updated = _last_updated(entity)
    occurred = _parse_iso(updated) or _utcnow()
    kind, status_word = _classify(entity)
    actor_ref, author_hint = _author(entity)

    label = _label(entity_kind, entity)
    who = (author_hint or {}).get("id")
    parts = [label]
    if who:
        parts.append(f"· {who}")
    amount = (
        entity.get("ImpressionCount")
        or entity.get("LikeCount")
        or entity.get("FollowerCount")
    )
    if amount is not None:
        parts.append(f"· {amount}")
    parts.append(f"· {status_word}")
    content_text = " ".join(str(p) for p in parts)

    entities: list[dict[str, Any]] = [
        {"type": "linkedin_object", "id": f"{entity_kind}:{entity_id}"},
    ]
    if author_hint:
        entities.append(author_hint)

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "organization_urn": organization_urn,
        "entity_id": entity_id,
        "doc_number": entity.get("DocNumber"),
        "status": status_word,
        "author": (entity.get("AuthorRef") or {}).get("name")
        if isinstance(entity.get("AuthorRef"), dict) else None,
        "last_updated": updated,
    }
    content.update(_entity_extras(entity))

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=actor_ref,
        external_id=external_id,
        entities_hint=entities,
        raw_payload=entity,
    )


def _org_of(payload: dict[str, Any]) -> str:
    rid = payload.get("_fyralis_org_urn") or payload.get("organizationUrn")
    if isinstance(rid, str) and rid:
        return rid
    return ""


# ---------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------

@register(_CHANNEL)
async def handle_linkedin_object(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError("linkedin payload must be a JSON object", channel=_CHANNEL)

    # --- BACKFILL / POLL path (fetcher- or poll-tagged records) ---
    # LinkedIn is poll-only; the live edge re-uses the SAME tagged record shape,
    # so there is one branch for both.
    record_type = payload.get("_fyralis_record_type")
    if isinstance(record_type, str) and record_type:
        entity_kind = _ENTITY_NORMALISE.get(record_type.lower())
        if entity_kind is None:
            raise ValidationError(
                f"unsupported linkedin record_type {record_type!r}",
                channel=_CHANNEL,
            )
        entity = payload.get("entity") or {}
        return _entity_draft(entity_kind, entity, _org_of(payload))

    raise ValidationError(
        "linkedin payload is not a tagged organization record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_linkedin_object", "linkedin_entity"]
