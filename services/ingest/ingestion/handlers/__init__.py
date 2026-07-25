"""Handler protocol and immutable contract-backed channel resolution.

BUILD-PLAN §3 Prompt 2.A:
    "services/ingest/ingestion/handlers/__init__.py:
       - Catalog: channel name → handler callable reference.
       - Trust tier mapping table per §14 (channel → tier)."

ARCHITECTURE §14 `CHANNEL_TRUST_MAP` is derived from the source-contract
catalog. Importing this package does not import every handler module or mutate
a process-local registry; a declared handler is imported only when requested.

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
from collections.abc import Mapping
from typing import Any, Awaitable, Callable, cast

from lib.shared.errors import CompanyOSError
from lib.shared.types import ObservationKind, TrustTierValue
from services.ingest.source_contract.catalog import (
    CHANNEL_TRUST_CATALOG,
    normalizer_channels,
)
from services.ingest.source_contract.runtime import (
    NormalizationChannelNotFoundError,
    resolve_handler,
)


CHANNEL_TRUST_MAP: Mapping[str, TrustTierValue] = CHANNEL_TRUST_CATALOG


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
    # Private durable-artifact catalog descriptors.  Unlike ``content`` these
    # may contain an internal S3 bucket/key and are carried only across the
    # normalizer/writer boundary; core persists them to blobs +
    # observation_artifacts in the same transaction as the observation.
    # Handlers must put only ``StoredArtifact.public_ref()`` in content.
    artifact_descriptors: list[dict[str, Any]] = field(default_factory=list)


HandlerFn = Callable[[dict[str, Any], dict[str, str]], Awaitable[ObservationDraft]]


def get_handler(channel: str) -> HandlerFn:
    """Resolve a declared handler, raising ``HandlerNotFound`` on a miss."""

    try:
        fn = resolve_handler(channel)
    except NormalizationChannelNotFoundError as exc:
        raise HandlerNotFound(
            f"no handler declared for channel {channel!r}",
            channel=channel,
            registered=handler_channels(),
        ) from exc
    return cast(HandlerFn, fn)


def handler_channels() -> list[str]:
    """Return every channel with a contract-declared normalizer binding."""

    return list(normalizer_channels())


__all__ = [
    "CHANNEL_TRUST_MAP",
    "HandlerFn",
    "HandlerNotFound",
    "HandlerError",
    "ObservationDraft",
    "get_handler",
    "handler_channels",
]
