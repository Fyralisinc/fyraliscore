"""Helpers for carrying HTTP headers without leaking credentials."""
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
import re
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
_EXACT_SENSITIVE_LOG_KEYS = frozenset(
    {
        "authorization",
        "auth",
        "api_key",
        "apikey",
        "access_key",
        "secret_key",
        "client_secret",
        "password",
        "passcode",
        "token",
        "tokens",
        "access_token",
        "refresh_token",
        "id_token",
        "bot_token",
        "api_token",
        "session_token",
        "webhook_secret",
        "signature",
        "webhook_signature",
        "private_key",
        "oauth_code",
        "code_verifier",
        "cookie",
        "set_cookie",
        "payload",
        "raw_payload",
        "raw_body",
        "body",
        "channel_name",
        "content",
        "content_text",
        "prompt",
        "source_channel",
        "system_prompt",
        "user_prompt",
        "email",
        "account_number",
        "routing_number",
        "iban",
        "swift",
        "ssn",
        "tax_id",
        "card_number",
    }
)
_SENSITIVE_LOG_KEY_FRAGMENTS = (
    "api_key",
    "authorization",
    "secret",
    "signature",
    "private_key",
    "webhook",
    "password",
)
_EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
_BEARER_RE = re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]+", re.IGNORECASE)
_KEY_VALUE_SECRET_RE = re.compile(
    r"(?i)\b(password|secret|token|api[_-]?key|authorization|signature|"
    r"client_secret|access_token|refresh_token)\s*[:=]\s*[^,\s&]+"
)


def is_sensitive_header(name: str) -> bool:
    """Return True when a header value should not leave the request boundary."""
    normalized = name.strip().lower().replace("_", "-")
    if normalized in _EXACT_SENSITIVE_HEADERS:
        return True
    return any(fragment in normalized for fragment in _SENSITIVE_HEADER_FRAGMENTS)


def is_sensitive_log_key(name: str) -> bool:
    """Return True for structured-log keys that commonly carry private data."""

    normalized = name.strip().lower().replace("-", "_")
    if normalized in _EXACT_SENSITIVE_LOG_KEYS:
        return True
    if normalized.endswith("_token") or normalized.endswith("_email"):
        return True
    return any(fragment in normalized for fragment in _SENSITIVE_LOG_KEY_FRAGMENTS)


def _redact_string_value(value: str) -> str:
    if "PRIVATE KEY" in value.upper():
        return REDACTED_HEADER_VALUE
    value = _BEARER_RE.sub("Bearer [redacted]", value)
    value = _KEY_VALUE_SECRET_RE.sub(lambda m: f"{m.group(1)}=[redacted]", value)
    value = _EMAIL_RE.sub("[redacted-email]", value)
    return value


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


def redact_log_mapping(event_dict: MutableMapping[str, Any]) -> MutableMapping[str, Any]:
    """Redact sensitive structured log values in place."""

    def redact(value: Any, *, parent_key: str | None = None) -> Any:
        if parent_key and is_sensitive_log_key(parent_key):
            return REDACTED_HEADER_VALUE
        if isinstance(value, Mapping):
            if parent_key and parent_key.lower() in {"headers", "request_headers"}:
                return safe_headers(value)
            return {
                str(k): redact(
                    REDACTED_HEADER_VALUE
                    if is_sensitive_log_key(str(k))
                    else v,
                    parent_key=str(k),
                )
                for k, v in value.items()
            }
        if isinstance(value, list):
            return [redact(item, parent_key=parent_key) for item in value]
        if isinstance(value, str):
            return _redact_string_value(value)
        return value

    for key in list(event_dict.keys()):
        event_dict[key] = redact(event_dict[key], parent_key=str(key))
    return event_dict


def redact_header_mapping(
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Backward-compatible alias for the broader structured-log redactor."""
    return redact_log_mapping(event_dict)


def redact_log_values(
    logger: Any,
    method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """structlog processor that redacts sensitive log event values."""
    return redact_log_mapping(event_dict)


def redact_header_values(
    logger: Any,
    method_name: str,
    event_dict: MutableMapping[str, Any],
) -> MutableMapping[str, Any]:
    """Backward-compatible structlog processor alias."""
    return redact_log_values(logger, method_name, event_dict)


__all__ = [
    "REDACTED_HEADER_VALUE",
    "is_sensitive_header",
    "is_sensitive_log_key",
    "redact_header_mapping",
    "redact_header_values",
    "redact_log_mapping",
    "redact_log_values",
    "safe_headers",
]
