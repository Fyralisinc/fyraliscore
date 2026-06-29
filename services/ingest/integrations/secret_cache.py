"""Short-lived in-memory cache for source secrets resolved from secret_ref."""
from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any
from uuid import UUID


DEFAULT_SECRET_CACHE_TTL_SECONDS = 300.0
SECRET_CACHE_TTL_ENV = "SOURCE_CLIENT_SECRET_CACHE_TTL_SECONDS"


def secret_cache_ttl_seconds(env: dict[str, str] | None = None) -> float:
    source = os.environ if env is None else env
    raw = source.get(SECRET_CACHE_TTL_ENV)
    if raw is None or raw == "":
        return DEFAULT_SECRET_CACHE_TTL_SECONDS
    try:
        return max(0.0, float(raw))
    except ValueError as exc:
        raise ValueError(f"{SECRET_CACHE_TTL_ENV} must be a number") from exc


def coerce_secret_text(raw: Any) -> str:
    if isinstance(raw, (bytes, bytearray)):
        return bytes(raw).decode("utf-8")
    return str(raw)


class SecretValueCache:
    """Cache a secret-store value for a bounded TTL.

    Preset values are used for tests/spammer mode and do not expire. Values
    loaded from a secret provider expire, so rotations are picked up by
    long-lived clients without requiring a process restart.
    """

    def __init__(
        self,
        preset: str | None = None,
        *,
        ttl_seconds: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._value = preset
        self._expires_at = float("inf") if preset is not None else 0.0
        self._ttl_seconds = (
            secret_cache_ttl_seconds()
            if ttl_seconds is None
            else max(0.0, float(ttl_seconds))
        )
        self._clock = clock

    def set(self, value: str, *, ttl_seconds: float | None = None) -> str:
        ttl = self._ttl_seconds if ttl_seconds is None else max(0.0, ttl_seconds)
        self._value = value
        self._expires_at = self._clock() + ttl
        return value

    def clear(self) -> None:
        self._value = None
        self._expires_at = 0.0

    def get_if_fresh(self) -> str | None:
        if self._value is not None and self._clock() < self._expires_at:
            return self._value
        return None

    async def resolve(
        self,
        *,
        lock: Any,
        secret_store: Any | None,
        secret_ref: str | None,
        tenant_id: UUID | None,
        missing_error: Callable[[], Exception],
    ) -> str:
        value = self.get_if_fresh()
        if value is not None:
            return value
        async with lock:
            value = self.get_if_fresh()
            if value is not None:
                return value
            if secret_store is None or secret_ref is None or tenant_id is None:
                raise missing_error()
            raw = await secret_store.get(secret_ref, tenant_id=tenant_id)
            return self.set(coerce_secret_text(raw))


__all__ = [
    "DEFAULT_SECRET_CACHE_TTL_SECONDS",
    "SECRET_CACHE_TTL_ENV",
    "SecretValueCache",
    "coerce_secret_text",
    "secret_cache_ttl_seconds",
]
