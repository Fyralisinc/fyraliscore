"""services/ingestion/handlers/discord.py — Discord interaction handler.

Discord delivers signed *interaction* payloads (slash commands,
buttons, etc). The PING handshake (`type=1`) is handled by the
webhook router itself before this handler ever sees the payload; this
handler only runs on real interactions (`type>=2`).

Signature verification happens in the webhook router
([services/webhooks/signatures/discord.py](../../webhooks/signatures/discord.py))
using ed25519. This handler trusts the verified payload.

`external_id` is the Discord interaction `id` (snowflake), which
Discord documents as globally unique — the canonical dedup key. The
existing UNIQUE index `observations_source_channel_external_id_occurred_at_key`
makes duplicate POSTs idempotent at the persistence layer (a Discord
retry within ~3s arrives twice with the same `interaction.id` and
produces exactly one Observation row).

IN-09 contract (spec.md FR-001, Clarifications Q3):
- `source_channel='discord:interaction'` (was 'discord:webhook' pre-IN-09)
- `content_text` is the primary string option's value verbatim
  (e.g. for `/fyralis ask "<query>"` it is `<query>`, NOT `"fyralis ask: <query>"`).
  The command verb is structural noise once source_channel encodes it.
- `content.metadata` carries the full interaction payload MINUS the
  per-interaction `token` field (and defensive minus `member.user.token` /
  `user.token` if those ever appear). The token is the credential for
  follow-up calls and must not land on a substrate row.
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


__all__ = ["handle_discord_webhook"]
