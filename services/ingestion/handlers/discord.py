"""services/ingestion/handlers/discord.py — Discord interaction handler.

Discord delivers signed *interaction* payloads (slash commands,
buttons, etc). The PING handshake (`type=1`) is handled by the
webhook router itself before this handler ever sees the payload; this
handler only runs on real interactions (`type>=2`).

Signature verification happens in the webhook router
([services/webhooks/signatures/discord.py](../../webhooks/signatures/discord.py))
using ed25519. This handler trusts the verified payload.

`external_id` is the Discord interaction `id` (snowflake), which
Discord documents as globally unique — the canonical dedup key.
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


_CHANNEL = "discord:webhook"


def _summary(payload: dict[str, Any]) -> str:
    bits: list[str] = ["discord:interaction"]
    interaction_type = payload.get("type")
    if interaction_type is not None:
        bits.append(f"type={interaction_type}")
    data = payload.get("data")
    if isinstance(data, dict):
        name = data.get("name") or data.get("custom_id")
        if name:
            bits.append(str(name))
    guild_id = payload.get("guild_id")
    if guild_id:
        bits.append(f"guild={guild_id}")
    return " ".join(bits)


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
    entities_hint: list[dict[str, Any]] = []
    app_id = payload.get("application_id")
    if isinstance(app_id, str):
        entities_hint.append({"type": "discord_application", "id": app_id})
    guild_id = payload.get("guild_id")
    if isinstance(guild_id, str):
        entities_hint.append({"type": "discord_guild", "id": guild_id})
    channel_id = payload.get("channel_id")
    if isinstance(channel_id, str):
        entities_hint.append({"type": "discord_channel", "id": channel_id})

    return ObservationDraft(
        source_channel=_CHANNEL,
        content_text=_summary(payload),
        content={
            "interaction_type": payload.get("type"),
            "interaction_id": interaction_id,
            "application_id": app_id,
            "guild_id": guild_id,
            "channel_id": channel_id,
            "data": payload.get("data"),
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
