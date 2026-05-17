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
    """Pub/Sub notifications are NOT Gmail messages. M6 will add the
    mapping for the fetched-message ingress; for M2 the normalizer
    skips with reason="unsupported_combination"."""
    assert resolve_channel("gmail", "pubsub") is None


def test_resolved_channels_all_have_registered_handlers():
    """Belt-and-braces: every mapping value must point at a channel
    that actually has a handler. A typo in the mapping ('discord:msg')
    would lock 100% of Discord gateway traffic out of normalization."""
    from services.ingestion.normalizer.channel_mapping import _CHANNEL_MAP
    from services.ingestion.handlers import handler_channels

    registered = set(handler_channels())
    for (source, ingress_kind), channel in _CHANNEL_MAP.items():
        assert channel in registered, (
            f"channel_mapping has {(source, ingress_kind)} -> {channel!r}, "
            f"but no handler is registered for {channel!r}. "
            f"Registered: {sorted(registered)}"
        )
