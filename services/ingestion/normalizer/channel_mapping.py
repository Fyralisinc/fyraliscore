"""Maps a raw envelope's (source, ingress_kind) → handler-registry channel.

Per M2 work-order §M2.3 and CHANNEL_TRUST_MAP in
`services/ingestion/handlers/__init__.py`.

The handler registry is keyed by channel name (e.g. "slack:message",
"discord:message", "github:webhook"); the raw envelope carries a
(source, ingress_kind) pair. The normalizer needs to translate so
it can dispatch each envelope through the right pure-transform
handler.

Combinations intentionally absent (returning None) are not bugs —
they're M2-scoped omissions documented in the table below. The
normalizer treats None as "skip this envelope" with a structured
log + `parse_failure` metric (per M2 work-order).
"""
from __future__ import annotations

from services.ingestion.raw_tier.envelope import (
    IngressKindLiteral,
    SourceLiteral,
)


# Mapping table. Keep alphabetic by source for grep-ability.
_CHANNEL_MAP: dict[tuple[str, str], str] = {
    # Discord — two live ingress surfaces + backfill.
    ("discord", "gateway"): "discord:message",      # IN-12 MESSAGE_CREATE
    ("discord", "webhook"): "discord:interaction",  # IN-09 slash commands
    ("discord", "backfill"): "discord:message",     # M6.7 (A27.2) — same
                                                    # handler as the gateway
                                                    # MESSAGE_CREATE path.
    # GitHub — webhook + backfill.
    ("github", "webhook"): "github:webhook",
    ("github", "backfill"): "github:webhook",       # M6.7 (A27.2)
    # Slack — webhook + backfill.
    ("slack", "webhook"): "slack:message",
    ("slack", "backfill"): "slack:message",         # M6.7 (A27.2)
    # Gmail — backfill resolves to the canonical "gmail:" message
    # handler (A27.2). The Pub/Sub notification ingress stays
    # INTENTIONALLY OMITTED: that payload is a notification
    # (emailAddress + historyId), NOT a Gmail message resource, so it
    # has no direct handler. M6's backfill path fetches the actual
    # message resources and publishes them under ingress_kind=backfill,
    # which the "gmail:" handler consumes.
    ("gmail", "backfill"): "gmail:",                # M6.7 (A27.2)
}


def resolve_channel(
    source: SourceLiteral, ingress_kind: IngressKindLiteral,
) -> str | None:
    """Return the handler-registry channel for (source, ingress_kind),
    or None if the combination has no handler in M2 scope.

    Callers MUST handle None — the normalizer skips with a structured
    log + a `parse_failure` metric increment (M2 work-order metric).
    """
    return _CHANNEL_MAP.get((source, ingress_kind))


__all__ = ["resolve_channel"]
