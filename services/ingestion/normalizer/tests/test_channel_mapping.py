"""M2.3 — channel_mapping coverage.

The mapping table is small but load-bearing — the normalizer can
only dispatch to handlers that are reachable from the table.
A regression that drops a supported (source, ingress_kind) combo
would silently route those envelopes to the unsupported path.
"""
from __future__ import annotations

import pytest

from services.ingestion.normalizer.channel_mapping import resolve_channel


def test_slack_webhook_resolves_to_slack_message():
    assert resolve_channel("slack", "webhook") == "slack:message"


def test_github_webhook_resolves_to_github_webhook():
    assert resolve_channel("github", "webhook") == "github:webhook"


def test_discord_gateway_resolves_to_message_handler():
    """IN-12 MESSAGE_CREATE — gateway frames go through the message
    handler, NOT the interaction handler."""
    assert resolve_channel("discord", "gateway") == "discord:message"


def test_discord_webhook_resolves_to_interaction_handler():
    """IN-09 slash commands — webhook surface routes through
    interaction handler."""
    assert resolve_channel("discord", "webhook") == "discord:interaction"


def test_gmail_pubsub_intentionally_unmapped():
    """Pub/Sub notifications are NOT Gmail messages. M6 backfill maps
    the fetched-message ingress (`gmail`/`backfill`) — the Pub/Sub
    notification ingress stays unmapped; the normalizer skips it with
    reason="unsupported_combination"."""
    assert resolve_channel("gmail", "pubsub") is None


# ---------------------------------------------------------------------
# M6.7 (A27.2) — backfill ingress resolves to the same handler as the
# live surface for each source, so a re-fetched event derives the same
# external_id and dedups against its webhook/gateway twin.
# ---------------------------------------------------------------------
def test_resolve_channel_for_backfill_all_sources():
    assert resolve_channel("gmail", "backfill") == "gmail:"
    assert resolve_channel("github", "backfill") == "github:webhook"
    assert resolve_channel("slack", "backfill") == "slack:message"
    assert resolve_channel("discord", "backfill") == "discord:message"


def test_backfill_resolves_to_same_channel_as_live_surface():
    """The load-bearing property of A27.2: backfill and the live
    surface share a handler. Discord's live surface for messages is
    the gateway (not the interaction webhook); Gmail's live Pub/Sub
    notification has no handler, so its backfill points at the
    canonical "gmail:" message handler directly."""
    assert resolve_channel("gmail", "backfill") == "gmail:"
    assert resolve_channel("github", "backfill") == resolve_channel(
        "github", "webhook",
    )
    assert resolve_channel("slack", "backfill") == resolve_channel(
        "slack", "webhook",
    )
    assert resolve_channel("discord", "backfill") == resolve_channel(
        "discord", "gateway",
    )


def test_unknown_source_backfill_returns_none():
    """An unmapped source under backfill ingress preserves the
    skip-with-None behaviour (no accidental catch-all)."""
    assert resolve_channel("telegram", "backfill") is None  # type: ignore[arg-type]


def test_resolved_channels_all_have_callable_handlers():
    """Belt-and-braces: every mapping value must resolve to a
    callable handler via the registry. A typo in the mapping
    ('discord:msg' instead of 'discord:message') would lock 100% of
    Discord-gateway traffic out of normalization.

    Two assertions per entry:
      (i) `get_handler(channel)` returns without raising
         `HandlerNotFound` — equivalent to membership in
         `handler_channels()` but exercises the actual lookup
         path the normalizer uses.
      (ii) the returned value is `callable()` — guards against a
         hypothetical regression where the registry holds a
         non-function placeholder.
    """
    from services.ingestion.normalizer.channel_mapping import _CHANNEL_MAP
    from services.ingestion.handlers import get_handler

    for (source, ingress_kind), channel in _CHANNEL_MAP.items():
        handler = get_handler(channel)  # raises HandlerNotFound on miss
        assert callable(handler), (
            f"channel_mapping has {(source, ingress_kind)} -> {channel!r}, "
            f"and the registry returned a non-callable: {handler!r}"
        )
