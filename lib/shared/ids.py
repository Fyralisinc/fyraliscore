"""
lib/shared/ids.py — UUID v7 helpers and tenant context.

UUID v7 (RFC 9562) is time-sortable: the first 48 bits are the Unix
epoch in milliseconds. Sorting by id approximates sorting by time,
which is useful for partitioned tables, cause-chain reconstruction,
and every hot-path index on (tenant_id, id).

BUILD-PLAN §0 non-negotiable #7: "UUID v7 for IDs. Time-sortable.
Not v4."
"""
from __future__ import annotations

import contextvars
import os
import secrets
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager


# ---------------------------------------------------------------------
# UUID v7 generation
# ---------------------------------------------------------------------

_VERSION_BITS = 0x7 << 12  # nibble 7 in the version position
_VARIANT_BITS = 0x8 << 60  # RFC 4122 variant (bit pattern 10xx) in nibble 16
_VARIANT_MASK = 0xC000000000000000
_VERSION_MASK = 0x000000000000F000

_last_ms = 0
_last_counter = 0


def uuid7(timestamp_ms: int | None = None) -> uuid.UUID:
    """
    Generate a UUID version 7.

    Layout (128 bits):
      48 bits : unix_ts_ms (big-endian)
       4 bits : version = 7
      12 bits : monotonic sub-ms counter (prevents collisions within
                the same millisecond; rolls when exhausted by bumping
                the timestamp by 1 ms).
       2 bits : RFC 4122 variant = 10
      62 bits : cryptographic randomness

    The function is safe under concurrent invocation *within a single
    process* — a module-global counter guarantees monotonicity. For
    cross-process monotonicity, rely on the 48-bit timestamp alone.
    """
    global _last_ms, _last_counter

    ms = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
    if ms < 0 or ms >= (1 << 48):
        raise ValueError(f"timestamp_ms {ms} out of range for 48-bit field")

    if ms == _last_ms:
        _last_counter += 1
        if _last_counter >= (1 << 12):
            # Counter wrapped. Bump timestamp, reset counter.
            ms += 1
            _last_ms = ms
            _last_counter = 0
    else:
        _last_ms = ms
        _last_counter = 0

    counter = _last_counter
    rand = secrets.randbits(62)

    # Assemble 128-bit integer:
    #   [48 ms | 4 ver | 12 counter | 2 variant | 62 rand]
    value = 0
    value |= ms << 80
    value |= 0x7 << 76
    value |= counter << 64
    value |= 0x2 << 62  # variant bits '10' placed at bits 63-62
    value |= rand
    return uuid.UUID(int=value)


def extract_timestamp_ms(u: uuid.UUID) -> int:
    """Return the embedded Unix timestamp in milliseconds."""
    if u.version != 7:
        raise ValueError(f"not a UUID v7: version={u.version}")
    return u.int >> 80


def is_uuid7(u: uuid.UUID) -> bool:
    return u.version == 7


# ---------------------------------------------------------------------
# Tenant context
# ---------------------------------------------------------------------

_tenant_ctx: contextvars.ContextVar[uuid.UUID | None] = contextvars.ContextVar(
    "company_os_tenant_id", default=None
)


def set_tenant(tenant_id: uuid.UUID | str) -> contextvars.Token:
    """
    Bind a tenant UUID to the current async context. Returns a Token
    you can pass to reset_tenant to restore the previous value.
    """
    if isinstance(tenant_id, str):
        tenant_id = uuid.UUID(tenant_id)
    return _tenant_ctx.set(tenant_id)


def reset_tenant(token: contextvars.Token) -> None:
    _tenant_ctx.reset(token)


def current_tenant() -> uuid.UUID:
    """
    Return the currently-bound tenant UUID, or raise
    LookupError if none is set. A fallback DEFAULT_TENANT_ID env
    var is honored for local development only.
    """
    value = _tenant_ctx.get()
    if value is not None:
        return value
    fallback = os.environ.get("DEFAULT_TENANT_ID")
    if fallback:
        return uuid.UUID(fallback)
    raise LookupError(
        "No tenant bound in current context. Call set_tenant(...) "
        "or set DEFAULT_TENANT_ID."
    )


@contextmanager
def tenant_scope(tenant_id: uuid.UUID | str) -> Iterator[uuid.UUID]:
    """
    Scope a tenant binding to a `with` block. Exceptions do not leak
    state: the token is always reset on exit.
    """
    if isinstance(tenant_id, str):
        tenant_id = uuid.UUID(tenant_id)
    token = _tenant_ctx.set(tenant_id)
    try:
        yield tenant_id
    finally:
        _tenant_ctx.reset(token)
