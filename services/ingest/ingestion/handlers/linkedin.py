"""services/ingest/ingestion/handlers/linkedin.py — LinkedIn organization handler.

ONE channel `linkedin:object` (mirrors carta:object's one-channel/many-record-types
shape). The handler is a pure function (no DB / network) and branches on the
input shape to produce exactly ONE observation per call:

  - BACKFILL / POLL: records arrive tagged with a private `_fyralis_record_type`
    ∈ {"post","share_statistics","follower_statistics"} (set by the fetcher or
    the poll dispatcher), with `entity` = the raw Community-Management element.
  - LIVE POLL: the poll dispatcher (`integrations/linkedin/poll.py`) emits the
    SAME fetcher-shaped tagged record, so a polled change and its backfill twin
    dedup.

LinkedIn is POLL-ONLY (no webhook), so there is no webhook-envelope branch — the
live edge re-uses the backfill record shape exactly.

Wire shapes (Community Management API; epoch-millis timestamps):
  - post: `{id: "urn:li:share:N"|"urn:li:ugcPost:N", author: "urn:li:…",
    commentary, lifecycleState, visibility, createdAt, lastModifiedAt,
    publishedAt, lifecycleStateInfo: {isEditedByAuthor}}`.
  - share_statistics: `{organizationalEntity, timeRange: {start, end},
    totalShareStatistics: {clickCount, likeCount, commentCount, shareCount,
    impressionCount, uniqueImpressionsCount, engagement}}`.
  - follower_statistics: `{organizationalEntity, timeRange: {start, end},
    followerGains: {organicFollowerGain, paidFollowerGain}}`.

external_id — DISCRIMINATED by entity_kind, NOT versioned by a sync token (the
observations repo dedups on (source_channel, external_id) IGNORING occurred_at):
  - linkedin:{org}:post:{post URN}                      (a post is created once;
    edits re-list the same id and dedup)
  - linkedin:{org}:share_statistics:{timeRange.start}   (snapshot, versioned by
  - linkedin:{org}:follower_statistics:{timeRange.start} its time bucket — the
    read path always requests time-bound statistics, so the epoch-millis bucket
    start is the deterministic id; re-polling a bucket dedups, a new bucket is a
    new observation)

Trust posture: LinkedIn is the organization system of record for its own
posts/statistics -> `authoritative`.
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

# Map a LinkedIn record_type (backfill/poll) to a canonical kind, keyed to the
# real read surface: posts finder + the two organizationalEntity statistics.
_ENTITY_NORMALISE = {
    "post": "post",
    "share_statistics": "share_statistics",
    "follower_statistics": "follower_statistics",
}

# Post lifecycle states that constitute a state_change (vs an open signal).
# lifecycleState ∈ DRAFT | PUBLISHED | PUBLISH_REQUESTED | PUBLISH_FAILED;
# an author edit is flagged via lifecycleStateInfo.isEditedByAuthor.
_STATE_CHANGE_LIFECYCLES = {"publish_failed"}


def linkedin_entity(
    organization_urn: str, entity_kind: str, entity_id: str,
) -> str:
    """`linkedin:{org}:{kind}:{id}` — DISCRIMINATED by entity_kind so streams
    sharing an id never collide. Posts use the post URN as id; statistics use
    the `timeRange.start` epoch-millis bucket (snapshot versioning).

    TODO(human): during a wiring phase move this constructor to
        `services/ingest/ingestion/idempotency/__init__.py` as
        `linkedin_entity(organization_urn, entity_kind, entity_id)` (the
        canonical home, mirroring `carta_entity`) and import it here. The
        format string MUST stay byte-identical across the move.
    """
    return f"linkedin:{organization_urn}:{entity_kind}:{entity_id}"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_epoch_ms(value: Any) -> datetime | None:
    """LinkedIn timestamps are epoch-millis integers (createdAt /
    lastModifiedAt / timeRange.start|end)."""
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _time_range(entity: dict[str, Any]) -> dict[str, Any] | None:
    tr = entity.get("timeRange")
    return tr if isinstance(tr, dict) else None


def _truncate(text: str, limit: int = 600) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _author(entity: dict[str, Any]) -> tuple[str | None, dict[str, Any] | None]:
    """(actor_ref, entity_hint) for the author URN on a LinkedIn post
    (`urn:li:organization:N` or `urn:li:person:…`)."""
    author = entity.get("author")
    if isinstance(author, str) and author:
        hint_type = "person" if ":person:" in author else "organization"
        hint = {"type": hint_type, "role": "author", "id": author}
        return f"linkedin:author:{author}", hint
    return None, None


def _short_id(entity_id: str) -> str:
    """The trailing URN segment for human-readable labels
    (`urn:li:share:123` -> `123`)."""
    return entity_id.rpartition(":")[2] or entity_id


def _classify_post(entity: dict[str, Any]) -> tuple[str, str]:
    """Return (kind, status_word) for a post. PUBLISH_FAILED and author edits
    are lifecycle transitions (`state_change`); everything else is an open
    `signal`."""
    lifecycle = str(entity.get("lifecycleState") or "PUBLISHED").strip().lower()
    info = entity.get("lifecycleStateInfo")
    edited = isinstance(info, dict) and bool(info.get("isEditedByAuthor"))
    if lifecycle in _STATE_CHANGE_LIFECYCLES:
        return "state_change", lifecycle
    if edited:
        return "state_change", "edited"
    return "signal", lifecycle


def _post_draft(
    entity: dict[str, Any], organization_urn: str,
) -> ObservationDraft:
    entity_id = str(entity.get("id") or "")
    if not organization_urn or not entity_id:
        raise ValidationError(
            "linkedin post missing organization_urn/id", channel=_CHANNEL,
        )
    external_id = linkedin_entity(organization_urn, "post", entity_id)

    modified_ms = entity.get("lastModifiedAt") or entity.get("createdAt")
    occurred = _parse_epoch_ms(modified_ms) or _utcnow()
    kind, status_word = _classify_post(entity)
    actor_ref, author_hint = _author(entity)

    commentary = entity.get("commentary")
    parts = [f"Post {_short_id(entity_id)}"]
    if isinstance(commentary, str) and commentary.strip():
        parts.append(f"· {commentary.strip()}")
    if author_hint:
        parts.append(f"· {author_hint['id']}")
    parts.append(f"· {status_word}")
    content_text = " ".join(str(p) for p in parts)

    entities: list[dict[str, Any]] = [
        {"type": "linkedin_object", "id": f"post:{entity_id}"},
    ]
    if author_hint:
        entities.append(author_hint)

    content: dict[str, Any] = {
        "object_type": "post",
        "organization_urn": organization_urn,
        "entity_id": entity_id,
        "status": status_word,
        "author": entity.get("author"),
        "commentary": commentary,
        "lifecycle_state": entity.get("lifecycleState"),
        "visibility": entity.get("visibility"),
        "created_at_ms": entity.get("createdAt"),
        "last_modified_at_ms": entity.get("lastModifiedAt"),
        "published_at_ms": entity.get("publishedAt"),
    }
    content = {k: v for k, v in content.items() if v is not None}

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


# Statistic counters lifted into `content` (present keys only, snake_cased).
_SHARE_STAT_FIELDS = (
    ("clickCount", "click_count"),
    ("likeCount", "like_count"),
    ("commentCount", "comment_count"),
    ("shareCount", "share_count"),
    ("impressionCount", "impression_count"),
    ("uniqueImpressionsCount", "unique_impressions_count"),
    ("engagement", "engagement"),
)
_FOLLOWER_GAIN_FIELDS = (
    ("organicFollowerGain", "organic_follower_gain"),
    ("paidFollowerGain", "paid_follower_gain"),
)


def _statistics_draft(
    entity_kind: str, entity: dict[str, Any], organization_urn: str,
) -> ObservationDraft:
    time_range = _time_range(entity)
    bucket_start = time_range.get("start") if time_range else None
    if not organization_urn or bucket_start is None:
        raise ValidationError(
            "linkedin statistics element missing organization_urn/timeRange.start",
            channel=_CHANNEL,
        )
    entity_id = str(int(bucket_start))
    external_id = linkedin_entity(organization_urn, entity_kind, entity_id)

    bucket_end = time_range.get("end") if time_range else None
    occurred = (
        _parse_epoch_ms(bucket_end) or _parse_epoch_ms(bucket_start) or _utcnow()
    )

    if entity_kind == "share_statistics":
        counters = entity.get("totalShareStatistics")
        fields = _SHARE_STAT_FIELDS
        headline_key = "impressionCount"
    else:
        counters = entity.get("followerGains")
        fields = _FOLLOWER_GAIN_FIELDS
        headline_key = "organicFollowerGain"
    counters = counters if isinstance(counters, dict) else {}

    stats: dict[str, Any] = {}
    for src, dst in fields:
        if counters.get(src) is not None:
            stats[dst] = counters.get(src)

    day = occurred.date().isoformat()
    nice = entity_kind.replace("_", " ").title()
    parts = [f"{nice} {day}"]
    headline = counters.get(headline_key)
    if headline is not None:
        parts.append(f"· {headline}")
    parts.append("· snapshot")
    content_text = " ".join(str(p) for p in parts)

    content: dict[str, Any] = {
        "object_type": entity_kind,
        "organization_urn": organization_urn,
        "entity_id": entity_id,
        "status": "snapshot",
        "organizational_entity": entity.get("organizationalEntity"),
        "time_range": time_range,
    }
    content.update(stats)
    content = {k: v for k, v in content.items() if v is not None}

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_truncate(content_text),
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",  # type: ignore[arg-type]
        source_actor_ref=None,
        external_id=external_id,
        entities_hint=[
            {"type": "linkedin_object", "id": f"{entity_kind}:{entity_id}"},
        ],
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
        if entity_kind == "post":
            return _post_draft(entity, _org_of(payload))
        return _statistics_draft(entity_kind, entity, _org_of(payload))

    raise ValidationError(
        "linkedin payload is not a tagged organization record",
        channel=_CHANNEL,
    )


CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_linkedin_object", "linkedin_entity"]
