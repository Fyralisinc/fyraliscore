"""services/ingestion/handlers/__init__.py — handler registry.

BUILD-PLAN §3 Prompt 2.A:
    "services/ingestion/handlers/__init__.py:
       - Registry: channel name → handler callable.
       - Trust tier mapping table per §14 (channel → tier)."

ARCHITECTURE §14 `CHANNEL_TRUST_MAP` is the authoritative table for
source_channel → trust_tier. Only `slack:message` and the three
`internal:*` channels ship in Wave 2-A; Agent 2-B owns the rest.

Handler shape (the `ObservationDraft` model below):
- `content_text: str`             — human-legible representation
- `content: dict[str, Any]`       — JSONB blob stored as observations.content
- `source_channel: str`           — routing key; must match a registered channel
- `source_actor_ref: str | None`  — channel-native actor id ("slack:U01ALICE")
- `external_id: str | None`       — channel-native dedup key
- `occurred_at: datetime`         — event time from the source
- `entities_hint: list[dict]`     — pre-parsed entity candidates from the handler
- `trust_tier: str`               — copied from CHANNEL_TRUST_MAP; handler-specific
                                    overrides allowed (e.g. github comment vs merge)
- `raw_payload: dict | None`      — stashed for audit / replay; ingestion stores
                                    this in content["_raw"]

All handlers are pure functions:
    async def handle(payload: dict, request_headers: dict) -> ObservationDraft
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable

from lib.shared.errors import CompanyOSError
from lib.shared.types import ObservationKind, TrustTierValue


# ARCHITECTURE §14 CHANNEL_TRUST_MAP — authoritative mapping.
# Only the four Wave 2-A channels are listed here. Agent 2-B will
# extend via `register()` when those handlers land.
CHANNEL_TRUST_MAP: dict[str, str] = {
    "slack:message": "attested_agent",
    "email:inbound": "attested_agent",
    "linear:webhook": "authoritative",
    "github:webhook": "authoritative",
    "calendar:sync": "authoritative",
    "stripe:webhook": "authoritative",
    "discord:webhook": "attested_agent",
    "discord:interaction": "attested_agent",
    "journal:ui": "authoritative",
    "agent:attested": "attested_agent",
    "news:rss": "reputable",
    "news:web": "inferential_external",
    "social:twitter": "unvetted",
    "social:linkedin": "reputable",
    "market:api": "authoritative_external",
    "regulatory:api": "authoritative_external",
    "analyst:report": "reputable",
    "ui:contestation": "authoritative",
    # Internal channels used by system-originated observations; these
    # carry the highest trust and never enter through a signature-
    # verified webhook.
    "internal:state_change": "authoritative",
    "internal:anomaly": "authoritative",
    "internal:prediction_resolution": "authoritative",
}


class HandlerNotFound(CompanyOSError):
    default_code = "handler_not_found"


class HandlerError(CompanyOSError):
    default_code = "handler_error"


@dataclass
class ObservationDraft:
    """What a handler produces before the core path persists it.

    Fields here map 1:1 onto `ObservationCreate` plus a few hints the
    core path consumes (entities_hint, raw_payload).
    """

    source_channel: str
    content_text: str
    content: dict[str, Any]
    occurred_at: datetime
    trust_tier: TrustTierValue
    kind: ObservationKind = "signal"
    source_actor_ref: str | None = None
    external_id: str | None = None
    entities_hint: list[dict[str, Any]] = field(default_factory=list)
    unresolved_phrases: list[str] = field(default_factory=list)
    raw_payload: dict[str, Any] | None = None


HandlerFn = Callable[[dict[str, Any], dict[str, str]], Awaitable[ObservationDraft]]


_HANDLERS: dict[str, HandlerFn] = {}


def register(channel: str) -> Callable[[HandlerFn], HandlerFn]:
    """Decorator: register a handler for `channel`.

    Usage:
        @register("slack:message")
        async def handle_slack(payload, headers):
            ...

    Raises at import time if the channel is already registered
    (double-registration is a programmer error).
    """

    def _decorator(fn: HandlerFn) -> HandlerFn:
        if channel in _HANDLERS:
            raise RuntimeError(
                f"handler for {channel!r} already registered"
            )
        _HANDLERS[channel] = fn
        return fn

    return _decorator


def get_handler(channel: str) -> HandlerFn:
    """Look up the handler for `channel`. Raises `HandlerNotFound` when
    the channel has no handler registered."""
    fn = _HANDLERS.get(channel)
    if fn is None:
        raise HandlerNotFound(
            f"no handler registered for channel {channel!r}",
            channel=channel,
            registered=sorted(_HANDLERS.keys()),
        )
    return fn


def handler_channels() -> list[str]:
    """Return the list of channels that have a registered handler."""
    return sorted(_HANDLERS.keys())


def _clear_registry_for_tests() -> None:
    """Test helper: drop all registrations. NEVER call this from non-
    test code — the Gateway startup path re-registers by importing
    the handler modules, which is not idempotent."""
    _HANDLERS.clear()


# Import handlers so `register()` decorators run. Order matters only
# for error messages (first to import wins uniqueness check). These
# imports intentionally come after _HANDLERS is defined above.
from services.ingestion.handlers import system  # noqa: E402,F401
from services.ingestion.handlers import slack  # noqa: E402,F401
from services.ingestion.handlers import github  # noqa: E402,F401
from services.ingestion.handlers import linear  # noqa: E402,F401
from services.ingestion.handlers import stripe  # noqa: E402,F401
from services.ingestion.handlers import discord  # noqa: E402,F401


__all__ = [
    "CHANNEL_TRUST_MAP",
    "HandlerFn",
    "HandlerNotFound",
    "HandlerError",
    "ObservationDraft",
    "register",
    "get_handler",
    "handler_channels",
]
