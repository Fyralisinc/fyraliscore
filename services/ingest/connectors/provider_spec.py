"""Immutable wire descriptions used by connector-local capability classes.

This is an authoring utility, not a source registry. Provider modules construct
their own definitions explicitly and export concrete zero-argument factories.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

WebhookMode = Literal["hmac_sha256", "token", "ed25519"]


@dataclass(frozen=True)
class SourceProfile:
    source: str
    ingress_kinds: tuple[str, ...]
    api_origin: str
    collection_path: str
    channel: str
    native_type: str
    record_keys: tuple[str, ...]
    identity_fields: tuple[str, ...]
    occurred_fields: tuple[str, ...]
    text_fields: tuple[str, ...]
    auth_slot: str
    auth_scheme: str = "Bearer"
    webhook_mode: WebhookMode | None = None
    webhook_header: str | None = None
    webhook_secret_slot: str | None = None
    trust_tier: str = "attested_agent"
    cursor_parameter: str = "cursor"
    limit_parameter: str = "limit"
    next_cursor_fields: tuple[str, ...] = (
        "next_cursor",
        "nextCursor",
        "next_page_token",
        "nextPageToken",
        "continuation_token",
        "paging.next",
        "page.next",
    )

    @property
    def secret_slots(self) -> tuple[str, ...]:
        values = [self.auth_slot]
        if self.webhook_secret_slot is not None:
            values.append(self.webhook_secret_slot)
        return tuple(dict.fromkeys(values))


__all__ = ["SourceProfile", "WebhookMode"]
