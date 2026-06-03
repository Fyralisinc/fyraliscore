"""services/ingest/ingestion/handlers/google_drive.py — Drive handler (IN-16).

The Drive fetcher emits THREE record types. The normalizer routes the whole
source through ONE channel (`google_drive:file`) and `core.ingest` requires the
draft's `source_channel` to equal that routing channel — so all three share the
`google_drive:file` source_channel and are distinguished by `content.object_type`
+ a distinct versioned `external_id` namespace:

  - `file`     object_type=file      external_id `gdrive:{file_id}:{version}`
  - `comment`  object_type=comment   external_id `gdrive-comment:{file_id}:{comment_id}:{modifiedTime}`
  - `revision` object_type=revision  external_id `gdrive-revision:{file_id}:{revision_id}`

The single registered handler branches on the injected `_fyralis_record_type`.
For a `file`: `kind=state_change` when removed/trashed (a document left the
workspace), else `signal`. A resolved comment is a `state_change`; a revision is
always a `signal` (an edit in the file's timeline).

Records arrive shaped by the backfill/poll fetcher: the RAW Drive v3 object plus
injected private keys — `_fyralis_drive_id`, `_fyralis_drive_kind`,
`_fyralis_owner_email`, `_fyralis_removed`, `_fyralis_change_time`,
`_fyralis_extracted_text` (Doc/Sheet/Slide/PDF/text body, exported by the
fetcher so the handler stays a pure function with no I/O), and for comment /
revision records `_fyralis_file_id` + `_fyralis_file_name`.

Trust posture (D4): Drive is the system of record for file existence + metadata
— `authoritative`.

external_id — VERSIONED (mutable-source landmine): the observations repo dedups
on `(source_channel, external_id)` IGNORING occurred_at. Drive files MUTATE
constantly (rename, edit, move, re-share, trash). Drive's `version` field is a
monotonic counter that bumps on every metadata/content change, so:

    gdrive:{file_id}:{version}                 # a normal file state
    gdrive:{file_id}:removed:{change_time}     # a removed/lost-access change

This means:
  - identical re-fetches (backfill twin == poll twin) collapse to one
    observation (same version);
  - an edit (version bumps) lands a NEW observation so the activity signal +
    fresh content stay current;
  - a trash/removal lands a NEW observation with kind=state_change.

content_text embeds BOTH a legible activity line AND the extracted body so the
semantic layer can reason over what the document says, not just that it changed.
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


_CHANNEL = "google_drive:file"
# Used only as ValidationError labels for the comment/revision builders; the
# observations themselves carry source_channel=_CHANNEL (see module docstring).
_COMMENT_CHANNEL = "google_drive:comment"
_REVISION_CHANNEL = "google_drive:revision"
_TRUST = "authoritative"

# Pretty labels for the common Google-native mime types.
_MIME_LABEL = {
    "application/vnd.google-apps.document": "Google Doc",
    "application/vnd.google-apps.spreadsheet": "Google Sheet",
    "application/vnd.google-apps.presentation": "Google Slides",
    "application/vnd.google-apps.folder": "folder",
    "application/pdf": "PDF",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt: Any) -> datetime | None:
    if not isinstance(dt, str) or not dt:
        return None
    s = dt[:-1] + "+00:00" if dt.endswith("Z") else dt
    try:
        parsed = datetime.fromisoformat(s)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _email(node: Any) -> str | None:
    if isinstance(node, dict):
        e = node.get("emailAddress")
        if isinstance(e, str) and e:
            return e.lower()
    return None


def _user_ref(node: Any) -> tuple[str | None, str | None]:
    """A Drive `User` (comment/revision author) → (email, display_name). Drive
    often omits emailAddress on comment authors for privacy, leaving only the
    display name."""
    if not isinstance(node, dict):
        return None, None
    email = node.get("emailAddress")
    email = email.lower() if isinstance(email, str) and email else None
    name = node.get("displayName")
    name = name if isinstance(name, str) and name else None
    return email, name


def _is_external(email: str, owner_email: str | None) -> bool:
    """External if its domain differs from the owner's domain."""
    if not owner_email or "@" not in email or "@" not in owner_email:
        return False
    return email.rsplit("@", 1)[-1] != owner_email.rsplit("@", 1)[-1]


def _mime_label(mime: Any) -> str:
    if not isinstance(mime, str) or not mime:
        return "file"
    return _MIME_LABEL.get(mime, mime.split("/")[-1] or "file")


@register(_CHANNEL)
async def handle_google_drive_file(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """Dispatch on `_fyralis_record_type` (file | comment | revision). All three
    record kinds route through this one registered channel; each builds a draft
    with its own `source_channel` + versioned `external_id`."""
    if not isinstance(payload, dict):
        raise ValidationError(
            "google drive payload must be a JSON object", channel=_CHANNEL,
        )
    record_type = payload.get("_fyralis_record_type", "file")
    if record_type == "comment":
        return _build_comment_draft(payload)
    if record_type == "revision":
        return _build_revision_draft(payload)
    return _build_file_draft(payload)


def _build_file_draft(payload: dict[str, Any]) -> ObservationDraft:
    """Drive v3 file object -> ObservationDraft."""
    file_id = payload.get("id")
    if not isinstance(file_id, str) or not file_id:
        raise ValidationError(
            "google drive file missing id", channel=_CHANNEL,
        )

    drive_id = payload.get("_fyralis_drive_id") or payload.get("driveId") or "my-drive"
    drive_kind = payload.get("_fyralis_drive_kind") or "my_drive"
    owner_email = payload.get("_fyralis_owner_email")
    removed = bool(payload.get("_fyralis_removed")) or bool(payload.get("trashed"))
    change_time = payload.get("_fyralis_change_time")

    kind = "state_change" if removed else "signal"

    name = payload.get("name") or "(untitled)"
    mime = payload.get("mimeType")
    label = _mime_label(mime)
    version = payload.get("version")
    modified_time = payload.get("modifiedTime")
    created_time = payload.get("createdTime")
    last_modifier = _email(payload.get("lastModifyingUser"))
    owners = payload.get("owners")
    owner_emails: list[str] = []
    if isinstance(owners, list):
        for o in owners:
            em = _email(o)
            if em:
                owner_emails.append(em)
    primary_owner = owner_emails[0] if owner_emails else (
        owner_email if isinstance(owner_email, str) else None
    )

    # Version discriminator (see module docstring + idempotency.google_drive_file).
    external_id = idempotency.google_drive_file(
        file_id, version=version, removed=removed, change_time=change_time,
    )

    # Sharing recipients (explicit permissions, excluding owners).
    permissions = payload.get("permissions")
    shared_with: list[str] = []
    if isinstance(permissions, list):
        for p in permissions:
            em = _email(p)
            if em and em not in owner_emails:
                shared_with.append(em)

    # content_text — activity line + extracted body (both embedded).
    extracted = payload.get("_fyralis_extracted_text")
    if removed:
        content_text = f"{label.capitalize()} '{name}' was removed from Drive"
    else:
        actor = last_modifier or primary_owner or "someone"
        content_text = f"{actor} modified {label} '{name}'"
        if shared_with:
            shown = ", ".join(shared_with[:5])
            more = f" +{len(shared_with) - 5} more" if len(shared_with) > 5 else ""
            content_text += f" (shared with {shown}{more})"
    if isinstance(extracted, str) and extracted.strip():
        content_text += "\n\n" + extracted.strip()

    # entities_hint — owners / editor / sharing recipients + the document.
    entities: list[dict[str, Any]] = []
    owner_dom = owner_email if isinstance(owner_email, str) else primary_owner
    emitted: set[str] = set()
    for em in owner_emails:
        entities.append({
            "type": "email_address", "id": em, "role": "owner",
            "external": _is_external(em, owner_dom),
        })
        emitted.add(em)
    if last_modifier and last_modifier not in emitted:
        entities.append({
            "type": "email_address", "id": last_modifier, "role": "editor",
            "external": _is_external(last_modifier, owner_dom),
        })
        emitted.add(last_modifier)
    for em in shared_with:
        if em in emitted:
            continue
        entities.append({
            "type": "email_address", "id": em, "role": "shared_with",
            "external": _is_external(em, owner_dom),
        })
        emitted.add(em)
    if isinstance(name, str) and name and name != "(untitled)":
        entities.append({"type": "document", "id": file_id, "name": name})

    content: dict[str, Any] = {
        "object_type": "file",
        "file_id": file_id,
        "name": name,
        "mime_type": mime,
        "drive_id": drive_id,
        "drive_kind": drive_kind,
        "version": str(version) if version is not None else None,
        "trashed": bool(payload.get("trashed")),
        "removed": removed,
        "created_time": created_time,
        "modified_time": modified_time,
        "web_view_link": payload.get("webViewLink"),
        "size": payload.get("size"),
        "starred": bool(payload.get("starred")),
        "shared": bool(payload.get("shared")),
        "owner_emails": owner_emails,
        "primary_owner_email": primary_owner,
        "last_modifying_user": last_modifier,
        "shared_with": shared_with,
        "parents": payload.get("parents"),
        "has_extracted_text": bool(isinstance(extracted, str) and extracted.strip()),
        "extracted_chars": len(extracted) if isinstance(extracted, str) else 0,
    }

    # occurred_at — when the change happened: modifiedTime, then change time,
    # then createdTime, then now.
    occurred = (
        _parse_iso(modified_time)
        or _parse_iso(change_time)
        or _parse_iso(created_time)
        or _utcnow()
    )

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=occurred,
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=(
            f"email:{last_modifier or primary_owner}"
            if (last_modifier or primary_owner) else None
        ),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


def _build_comment_draft(payload: dict[str, Any]) -> ObservationDraft:
    """Drive comment (+ nested replies) -> ObservationDraft on
    `google_drive:comment`. A resolved comment is a `state_change` (discussion
    closed); else a `signal`. external_id is versioned on `modifiedTime` so a
    new reply / edit lands a fresh observation while re-fetches dedup."""
    comment_id = payload.get("id")
    file_id = payload.get("_fyralis_file_id")
    if not isinstance(comment_id, str) or not comment_id:
        raise ValidationError("google drive comment missing id", channel=_COMMENT_CHANNEL)
    if not isinstance(file_id, str) or not file_id:
        raise ValidationError("google drive comment missing file id", channel=_COMMENT_CHANNEL)

    file_name = payload.get("_fyralis_file_name") or file_id
    owner_email = payload.get("_fyralis_owner_email")
    resolved = bool(payload.get("resolved"))
    author_email, author_name = _user_ref(payload.get("author"))
    author = author_email or author_name or "someone"
    text = payload.get("content") or ""
    quoted = payload.get("quotedFileContent")
    quoted_text = quoted.get("value") if isinstance(quoted, dict) else None
    created = payload.get("createdTime")
    modified = payload.get("modifiedTime") or created

    replies_raw = payload.get("replies")
    replies: list[dict[str, Any]] = []
    if isinstance(replies_raw, list):
        for r in replies_raw:
            if not isinstance(r, dict):
                continue
            r_email, r_name = _user_ref(r.get("author"))
            replies.append({
                "author_email": r_email, "author_name": r_name,
                "content": r.get("content"), "created_time": r.get("createdTime"),
            })

    kind = "state_change" if resolved else "signal"
    mod_key = modified if isinstance(modified, str) and modified else "none"
    external_id = idempotency.google_drive_comment(file_id, comment_id, mod_key)

    verb = "resolved a comment on" if resolved else "commented on"
    content_text = f"{author} {verb} '{file_name}'"
    if text:
        content_text += f": {text}"
    if quoted_text:
        content_text += f"\n  (re: \"{quoted_text}\")"
    for rep in replies:
        ra = rep["author_email"] or rep["author_name"] or "someone"
        content_text += f"\n  ↳ {ra}: {rep['content'] or ''}"

    entities: list[dict[str, Any]] = []
    owner_dom = owner_email if isinstance(owner_email, str) else None
    seen: set[str] = set()
    if author_email:
        entities.append({"type": "email_address", "id": author_email,
                         "role": "commenter", "external": _is_external(author_email, owner_dom)})
        seen.add(author_email)
    for rep in replies:
        em = rep["author_email"]
        if em and em not in seen:
            entities.append({"type": "email_address", "id": em, "role": "commenter",
                             "external": _is_external(em, owner_dom)})
            seen.add(em)
    entities.append({"type": "document", "id": file_id, "name": file_name})

    content: dict[str, Any] = {
        "object_type": "comment",
        "comment_id": comment_id,
        "file_id": file_id,
        "file_name": file_name,
        "drive_id": payload.get("_fyralis_drive_id"),
        "text": text,
        "quoted_file_content": quoted_text,
        "resolved": resolved,
        "author_email": author_email,
        "author_name": author_name,
        "created_time": created,
        "modified_time": modified,
        "reply_count": len(replies),
        "replies": replies,
    }

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=_parse_iso(modified) or _parse_iso(created) or _utcnow(),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind=kind,  # type: ignore[arg-type]
        source_actor_ref=(f"email:{author_email}" if author_email else None),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


def _build_revision_draft(payload: dict[str, Any]) -> ObservationDraft:
    """Drive revision -> ObservationDraft on `google_drive:revision`. Revisions
    are immutable (the id is stable), so external_id is unversioned and the kind
    is always `signal` (an edit event in the file's timeline)."""
    revision_id = payload.get("id")
    file_id = payload.get("_fyralis_file_id")
    if not isinstance(revision_id, str) or not revision_id:
        raise ValidationError("google drive revision missing id", channel=_REVISION_CHANNEL)
    if not isinstance(file_id, str) or not file_id:
        raise ValidationError("google drive revision missing file id", channel=_REVISION_CHANNEL)

    file_name = payload.get("_fyralis_file_name") or file_id
    owner_email = payload.get("_fyralis_owner_email")
    modified = payload.get("modifiedTime")
    editor_email, editor_name = _user_ref(payload.get("lastModifyingUser"))
    editor = editor_email or editor_name or "someone"

    external_id = idempotency.google_drive_revision(file_id, revision_id)
    content_text = f"{editor} saved a revision of '{file_name}'"
    if isinstance(modified, str) and modified:
        content_text += f" at {modified}"

    entities: list[dict[str, Any]] = []
    owner_dom = owner_email if isinstance(owner_email, str) else None
    if editor_email:
        entities.append({"type": "email_address", "id": editor_email, "role": "editor",
                         "external": _is_external(editor_email, owner_dom)})
    entities.append({"type": "document", "id": file_id, "name": file_name})

    content: dict[str, Any] = {
        "object_type": "revision",
        "revision_id": revision_id,
        "file_id": file_id,
        "file_name": file_name,
        "drive_id": payload.get("_fyralis_drive_id"),
        "modified_time": modified,
        "size": payload.get("size"),
        "keep_forever": bool(payload.get("keepForever")),
        "published": bool(payload.get("published")),
        "last_modifying_user": editor_email,
        "last_modifying_name": editor_name,
    }

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content=content,
        occurred_at=_parse_iso(modified) or _utcnow(),
        trust_tier=_TRUST,  # type: ignore[arg-type]
        kind="signal",  # type: ignore[arg-type]
        source_actor_ref=(f"email:{editor_email}" if editor_email else None),
        external_id=external_id,
        entities_hint=entities,
        raw_payload=payload,
    )


# One routing channel for all three record types (file / comment / revision);
# they share source_channel=google_drive:file and are distinguished by
# content.object_type + external_id namespace.
CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)


__all__ = ["handle_google_drive_file"]
