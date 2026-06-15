"""Helpers for carrying HTTP headers without leaking credentials."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any


REDACTED_HEADER_VALUE = "[redacted]"

_EXACT_SENSITIVE_HEADERS = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "cookie",
        "set-cookie",
        "x-bootstrap-secret",
        "x-api-key",
        "api-key",
    }
)
_SENSITIVE_HEADER_FRAGMENTS = ("token", "secret", "signature", "api-key")


def is_sensitive_header(name: str) -> bool:
    """Return True when a header value should not leave the request boundary."""
    normalized = name.strip().lower().replace("_", "-")
    if normalized in _EXACT_SENSITIVE_HEADERS:
        return True
    return any(fragment in normalized for fragment in _SENSITIVE_HEADER_FRAGMENTS)


def safe_headers(
    headers: Mapping[str, Any] | None,
    *,
    redacted_value: str = REDACTED_HEADER_VALUE,
) -> dict[str, str]:
    """Copy headers while masking bearer tokens, cookies, secrets, and signatures."""
    if not headers:
        return {}
    out: dict[str, str] = {}
    for name, value in headers.items():
        key = str(name)
        out[key] = redacted_value if is_sensitive_header(key) else str(value)
    return out


def redact_header_mapping(event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Redact header-shaped mappings in a log/event dictionary in place."""
    def redact(value: Any, *, parent_key: str | None = None) -> Any:
        if isinstance(value, Mapping):
            if parent_key and parent_key.lower() in {"headers", "request_headers"}:
                return safe_headers(value)
            return {
                str(k): redact(
                    REDACTED_HEADER_VALUE if is_sensitive_header(str(k)) else v,
                    parent_key=str(k),
                )
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [redact(item, parent_key=parent_key) for item in value]
        if parent_key and is_sensitive_header(parent_key):
            return REDACTED_HEADER_VALUE
        return value

    for key in list(event_dict.keys()):
        event_dict[key] = redact(event_dict[key], parent_key=str(key))
    return event_dict


def redact_header_values(
    logger: Any,
    method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """structlog processor that redacts header-shaped mappings in log events."""
    return redact_header_mapping(event_dict)


__all__ = [
    "REDACTED_HEADER_VALUE",
    "is_sensitive_header",
    "redact_header_mapping",
    "redact_header_values",
    "safe_headers",
]
