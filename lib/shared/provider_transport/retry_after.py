"""RFC-compatible ``Retry-After`` parsing for provider clients."""

from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Mapping, Sequence

from lib.shared.provider_transport.contracts import ProviderRateLimited


def _header_value(headers: Mapping[str, Any], name: str) -> Any:
    direct = headers.get(name)
    if direct is not None:
        return direct
    expected = name.casefold()
    for key, value in headers.items():
        if str(key).casefold() == expected:
            return value
    return None


def parse_retry_after(
    value: str | int | float | None,
    *,
    now: datetime | None = None,
) -> float | None:
    """Return delay seconds from delta-seconds or an HTTP-date.

    Invalid and non-finite values return ``None``. Dates in the past become
    ``0.0``. Decimal delta-seconds are accepted because providers such as
    Discord emit sub-second values even though the HTTP grammar uses integers.
    """
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    try:
        seconds = float(raw)
    except (TypeError, ValueError):
        seconds = None
    if seconds is not None:
        if seconds < 0 or seconds == float("inf") or seconds != seconds:
            return None
        return seconds

    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    return max(
        0.0,
        (
            parsed.astimezone(timezone.utc) - current.astimezone(timezone.utc)
        ).total_seconds(),
    )


def rate_limited_from_headers(
    headers: Mapping[str, Any],
    *,
    message: str = "provider rate limit",
    status_code: int = 429,
    now: datetime | None = None,
    affected_scopes: Sequence[str] | None = None,
    header_parser_id: str = "http.retry_after",
) -> ProviderRateLimited:
    """Build a typed throttle error without retaining raw response headers."""
    return ProviderRateLimited(
        message,
        retry_after_seconds=parse_retry_after(
            _header_value(headers, "Retry-After"),
            now=now,
        ),
        status_code=status_code,
        affected_scopes=affected_scopes,
        header_parser_id=header_parser_id,
    )


__all__ = ["parse_retry_after", "rate_limited_from_headers"]
