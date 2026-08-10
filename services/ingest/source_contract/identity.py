"""Validated identity primitives for source connectors and capabilities."""

from __future__ import annotations

import re
from typing import Annotated, TypeAlias

from pydantic import StringConstraints


_CONNECTOR_ID_RE = re.compile(
    r"^[a-z0-9](?:[a-z0-9._-]{0,62})/[a-z0-9](?:[a-z0-9._-]{0,62})$"
)
_SOURCE_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_CAPABILITY_ID_RE = re.compile(
    r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+$"
)
_SLOT_ID_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


ConnectorId: TypeAlias = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=127,
        pattern=_CONNECTOR_ID_RE.pattern,
    ),
]
SourceId: TypeAlias = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=63,
        pattern=_SOURCE_ID_RE.pattern,
    ),
]
CapabilityId: TypeAlias = Annotated[
    str,
    StringConstraints(
        min_length=3,
        max_length=127,
        pattern=_CAPABILITY_ID_RE.pattern,
    ),
]
SlotId: TypeAlias = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=63,
        pattern=_SLOT_ID_RE.pattern,
    ),
]


def connector_namespace(connector_id: str) -> str:
    """Return the publisher/namespace portion of a validated connector ID."""
    _require_match(_CONNECTOR_ID_RE, connector_id, "connector ID")
    return connector_id.split("/", 1)[0]


def connector_name(connector_id: str) -> str:
    """Return the name portion of a validated connector ID."""
    _require_match(_CONNECTOR_ID_RE, connector_id, "connector ID")
    return connector_id.split("/", 1)[1]


def validate_connector_id(value: str) -> str:
    return _require_match(_CONNECTOR_ID_RE, value, "connector ID")


def validate_source_id(value: str) -> str:
    return _require_match(_SOURCE_ID_RE, value, "source ID")


def validate_capability_id(value: str) -> str:
    return _require_match(_CAPABILITY_ID_RE, value, "capability ID")


def validate_slot_id(value: str) -> str:
    return _require_match(_SLOT_ID_RE, value, "slot ID")


def _require_match(pattern: re.Pattern[str], value: str, label: str) -> str:
    if not pattern.fullmatch(value):
        raise ValueError(f"invalid {label}: {value!r}")
    return value


__all__ = [
    "CapabilityId",
    "ConnectorId",
    "SlotId",
    "SourceId",
    "connector_name",
    "connector_namespace",
    "validate_capability_id",
    "validate_connector_id",
    "validate_slot_id",
    "validate_source_id",
]
