"""Source-owned raw-envelope metadata builders for webhook ingress."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _header(headers: Mapping[str, str], name: str) -> str | None:
    value = headers.get(name) or headers.get(name.casefold())
    return value if isinstance(value, str) and value else None


def build_generic_metadata(
    headers: Mapping[str, str],
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Metadata for providers without a declared event discriminator."""

    del headers, payload
    return {"event_type": "unknown"}


def build_github_metadata(
    headers: Mapping[str, str],
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Preserve GitHub's header-carried event and delivery identities."""

    del payload
    metadata: dict[str, Any] = {
        "event_type": _header(headers, "X-GitHub-Event") or "unknown",
    }
    delivery_id = _header(headers, "X-GitHub-Delivery")
    if delivery_id is not None:
        metadata["delivery_id"] = delivery_id
    return metadata


def build_slack_metadata(
    headers: Mapping[str, str],
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Extract Slack's nested Events API event type."""

    del headers
    event_type = "unknown"
    if isinstance(payload, Mapping):
        event = payload.get("event")
        if isinstance(event, Mapping):
            declared = event.get("type")
            if isinstance(declared, str) and declared:
                event_type = declared
    return {"event_type": event_type}


def build_discord_metadata(
    headers: Mapping[str, str],
    payload: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Extract Discord's numeric interaction discriminator."""

    del headers
    interaction_type = payload.get("type") if isinstance(payload, Mapping) else None
    return {
        "event_type": (
            f"interaction:{interaction_type}"
            if isinstance(interaction_type, int)
            else "unknown"
        )
    }


__all__ = [
    "build_discord_metadata",
    "build_generic_metadata",
    "build_github_metadata",
    "build_slack_metadata",
]
