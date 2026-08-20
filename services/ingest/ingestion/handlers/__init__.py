"""Handler registry for direct, non-source ingestion channels.

BUILD-PLAN §3 Prompt 2.A:
    "services/ingest/ingestion/handlers/__init__.py:
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
from services.ingest.source_contract.models import SourceObjectRef

# External data sources are normalized by Source Connector capabilities and
# deliberately do not register handlers here.
CHANNEL_TRUST_MAP: dict[str, str] = {
    "email:inbound": "attested_agent",
    "linear:webhook": "authoritative",
    "calendar:sync": "authoritative",
    "stripe:webhook": "authoritative",
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
    # Consolidation carry-forwards. These two rich semantic adapters predate
    # their SourceConnector-v1 ports and remain explicit rather than being
    # presented as members of the stable connector fleet.
    "facebook_pages:message": "attested_agent",
    "instagram:message": "attested_agent",
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
    # Private storage descriptors remain outside persisted observation content.
    # They are optional for contract connectors and retained for consolidated
    # artifact-producing ingestion extensions.
    artifact_descriptors: list[dict[str, Any]] = field(default_factory=list)
    source_object: SourceObjectRef | None = None


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
from services.ingest.ingestion.handlers import (
    calendar,  # noqa: E402,F401
    email,  # noqa: E402,F401
    facebook_pages,  # noqa: E402,F401
    instagram,  # noqa: E402,F401
    linear,  # noqa: E402,F401
    stripe,  # noqa: E402,F401
    system,  # noqa: E402,F401
)

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
