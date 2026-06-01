"""services/ingestion/handlers/discord.py — Discord ingestion handlers.

Two channels are registered here:

  `discord:interaction` — slash commands / components / modals received
    over the Interactions HTTP webhook (IN-09). Signature verification
    happens upstream in `services/webhooks/signatures/discord.py`; this
    handler trusts the verified payload.

  `discord:message` — regular channel messages received via the
    Gateway WSS worker (IN-12). The worker filters `author.bot` and
    `webhook_id` at dispatch time before handing the payload here;
    this handler treats the payload as a Discord MESSAGE_CREATE event.

Both share `external_id = "discord:<snowflake>"` for dedup via the
existing `(source_channel, external_id, occurred_at)` unique index on
`observations`. The interaction handler's snowflake is the interaction
id; the message handler's snowflake is the message id. Cross-channel
collision is impossible because `source_channel` differs.

IN-09 contract (spec.md FR-001, Clarifications Q3) for interactions:
- `content_text` is the primary string option's value verbatim
- `content.metadata` carries the full payload MINUS the per-interaction
  `token` (credential-grade field; never persisted)

IN-12 contract (spec.md FR-007) for messages:
- `content_text` is `message.content` verbatim (no markdown strip)
- `content.metadata` carries channel_id, short_guild_hash, mention_user_ids,
  attachment_count — NEVER the raw guild_id (SC-006)
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


_CHANNEL = "discord:interaction"
_CHANNEL_MESSAGE = "discord:message"


def _strip_credentials(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a shallow copy of `payload` with credential-grade fields
    removed. The top-level `token` is the per-interaction follow-up
    credential Discord issues; `member.user.token` and `user.token`
    are defensive (Discord doesn't currently emit them, but if a
    future API change ever did, we strip them too).
    """
    cleaned: dict[str, Any] = {k: v for k, v in payload.items() if k != "token"}
    member = cleaned.get("member")
    if isinstance(member, dict):
        cleaned_member = dict(member)
        user = cleaned_member.get("user")
        if isinstance(user, dict) and "token" in user:
            cleaned_member["user"] = {k: v for k, v in user.items() if k != "token"}
        cleaned["member"] = cleaned_member
    user = cleaned.get("user")
    if isinstance(user, dict) and "token" in user:
        cleaned["user"] = {k: v for k, v in user.items() if k != "token"}
    return cleaned


def _primary_option_value(payload: dict[str, Any]) -> str:
    """For an ApplicationCommand (type=2), the user's input lives in
    `data.options[0].value` for a top-level required option, or
    `data.options[0].options[0].value` for a subcommand option.
    Return the first non-empty string we find, or an empty string if
    the interaction carries no option (e.g., a bare `/fyralis` with
    no args).
    """
    data = payload.get("data")
    if not isinstance(data, dict):
        return ""
    options = data.get("options")
    if not isinstance(options, list):
        return ""

    def _walk(opts: list[Any]) -> str:
        for opt in opts:
            if not isinstance(opt, dict):
                continue
            value = opt.get("value")
            if isinstance(value, str) and value:
                return value
            nested = opt.get("options")
            if isinstance(nested, list):
                inner = _walk(nested)
                if inner:
                    return inner
        return ""

    return _walk(options)


def _source_actor_ref(payload: dict[str, Any]) -> str | None:
    # Discord puts the user under `member.user` (guild context) or
    # `user` (DM context).
    member = payload.get("member")
    if isinstance(member, dict):
        user = member.get("user")
        if isinstance(user, dict) and user.get("id"):
            return f"discord:{user['id']}"
    user = payload.get("user")
    if isinstance(user, dict) and user.get("id"):
        return f"discord:{user['id']}"
    return None


@register(_CHANNEL)
async def handle_discord_webhook(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    if not isinstance(payload, dict):
        raise ValidationError(
            "discord payload must be a JSON object", channel=_CHANNEL
        )

    interaction_id = payload.get("id")
    app_id = payload.get("application_id")
    guild_id = payload.get("guild_id")
    channel_id = payload.get("channel_id")

    entities_hint: list[dict[str, Any]] = []
    if isinstance(app_id, str):
        entities_hint.append({"type": "discord_application", "id": app_id})
    if isinstance(guild_id, str):
        entities_hint.append({"type": "discord_guild", "id": guild_id})
    if isinstance(channel_id, str):
        entities_hint.append({"type": "discord_channel", "id": channel_id})

    content_text = _primary_option_value(payload)
    metadata = _strip_credentials(payload)

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=content_text,
        content={
            "text": content_text,
            "metadata": metadata,
        },
        occurred_at=datetime.now(tz=timezone.utc),
        trust_tier=CHANNEL_TRUST_MAP[_CHANNEL],  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=_source_actor_ref(payload),
        external_id=(
            f"discord:{interaction_id}"
            if isinstance(interaction_id, str)
            else None
        ),
        entities_hint=entities_hint,
        raw_payload=payload,
    )


def _short_guild_hash(guild_id: str) -> str:
    """8-byte BLAKE2b hex digest of guild_id. Stable, non-reversible.

    Duplicated here (not imported from services/integrations/discord/
    oauth.py) to keep the ingestion handlers package free of integration-
    layer imports — this module must not back-depend on
    services.integrations.*.
    """
    import hashlib
    return hashlib.blake2b(guild_id.encode("utf-8"), digest_size=8).hexdigest()


def _parse_discord_timestamp(raw: Any) -> datetime:
    """Discord sends ISO-8601 timestamps with offset. Fall back to
    now(UTC) if missing or unparseable so the observation still lands
    (timestamp is metadata, not load-bearing for dedup)."""
    if isinstance(raw, str) and raw:
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            pass
    return datetime.now(tz=timezone.utc)


def _message_actor_ref(payload: dict[str, Any]) -> str | None:
    """For MESSAGE_CREATE the author lives in top-level `author`."""
    author = payload.get("author")
    if isinstance(author, dict) and author.get("id"):
        return f"discord:{author['id']}"
    return None


def _message_entities_hint(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Mentioned users, the channel, and the guild (as a hash so the
    raw id doesn't land in entities — entity-alias resolution can use
    the hash if needed). SC-006: no raw guild_id in any surface."""
    hint: list[dict[str, Any]] = []
    mentions = payload.get("mentions")
    if isinstance(mentions, list):
        for m in mentions:
            if isinstance(m, dict) and m.get("id"):
                hint.append({"type": "discord_user", "id": m["id"]})
    channel_id = payload.get("channel_id")
    if isinstance(channel_id, str):
        hint.append({"type": "discord_channel", "id": channel_id})
    return hint


@register(_CHANNEL_MESSAGE)
async def handle_discord_message(
    payload: dict[str, Any], headers: dict[str, str]
) -> ObservationDraft:
    """MESSAGE_CREATE event from the Gateway worker (IN-12).

    The Gateway worker pre-filters `author.bot=true` and `webhook_id`
    before calling ingest(), so this handler trusts the payload as a
    human-authored guild message. It does NOT re-apply those filters
    (defense-in-depth would mask a worker bug rather than fix it).
    """
    if not isinstance(payload, dict):
        raise ValidationError(
            "discord MESSAGE_CREATE payload must be a JSON object",
            channel=_CHANNEL_MESSAGE,
        )

    message_id = payload.get("id")
    guild_id = payload.get("guild_id")
    channel_id = payload.get("channel_id")
    content = payload.get("content") or ""
    attachments = payload.get("attachments") or []
    mentions = payload.get("mentions") or []

    if not isinstance(message_id, str):
        raise ValidationError(
            "discord MESSAGE_CREATE missing string `id`",
            channel=_CHANNEL_MESSAGE,
        )
    if not isinstance(guild_id, str):
        raise ValidationError(
            "discord MESSAGE_CREATE missing string `guild_id` "
            "(DM messages should be filtered upstream)",
            channel=_CHANNEL_MESSAGE,
        )

    metadata: dict[str, Any] = {
        "channel_id": channel_id if isinstance(channel_id, str) else None,
        "short_guild_hash": _short_guild_hash(guild_id),
        "mention_user_ids": [
            m["id"] for m in mentions
            if isinstance(m, dict) and isinstance(m.get("id"), str)
        ],
        "attachment_count": len(attachments) if isinstance(attachments, list) else 0,
    }

    return ObservationDraft(
        source_channel=_CHANNEL_MESSAGE,
        content_text=content if isinstance(content, str) else "",
        content={
            "text": content if isinstance(content, str) else "",
            "metadata": metadata,
        },
        occurred_at=_parse_discord_timestamp(payload.get("timestamp")),
        trust_tier=CHANNEL_TRUST_MAP[_CHANNEL_MESSAGE],  # type: ignore[arg-type]
        kind="signal",
        source_actor_ref=_message_actor_ref(payload),
        external_id=f"discord:{message_id}",
        entities_hint=_message_entities_hint(payload),
        raw_payload=payload,
    )


__all__ = ["handle_discord_webhook", "handle_discord_message"]
