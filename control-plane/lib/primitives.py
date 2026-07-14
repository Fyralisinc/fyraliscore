"""Shared low-level primitives for the Fyralis BYOC control plane.

These are the small, dependency-light building blocks the SPRINT_PLAN P1 line
item calls for: **fingerprinting, canonical JSON, RFC-3339 time**. They are used
across the CA, signing, registry reader, deployment record, and agent so that
"the SHA-256 of a cert", "the bytes we sign", and "an RFC-3339 timestamp" mean
exactly one thing everywhere.

Nothing here imports cryptography at module import time except inside the
fingerprint helper, so importing the rest of ``lib`` stays cheap and never fails
if the crypto stack is not yet installed in a given context.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
from typing import Any

__all__ = [
    "utcnow",
    "to_rfc3339",
    "parse_rfc3339",
    "canonical_json_bytes",
    "canonical_json_str",
    "sha256_hex",
    "fingerprint_der",
    "fingerprint_pem",
]


# --- RFC-3339 UTC time (C4 timestamps) -------------------------------------


def utcnow() -> _dt.datetime:
    """Timezone-aware 'now' in UTC (never a naive datetime)."""
    return _dt.datetime.now(_dt.timezone.utc)


def to_rfc3339(dt: _dt.datetime) -> str:
    """Serialize a datetime to an RFC-3339 UTC string ending in ``Z``.

    Naive datetimes are assumed to already be UTC. Any aware datetime is
    converted to UTC first so the wire form is canonical (``...T00:00:00Z``),
    matching the C4 ``last_heartbeat_ts`` / ``license_expiry`` examples.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    dt = dt.astimezone(_dt.timezone.utc)
    # isoformat() yields "+00:00"; normalize to the canonical trailing "Z".
    return dt.isoformat().replace("+00:00", "Z")


def parse_rfc3339(value: str) -> _dt.datetime:
    """Parse an RFC-3339 string (accepting a trailing ``Z``) to aware UTC.

    Always returns a timezone-aware datetime in UTC. Raises ``ValueError`` on
    an unparseable string (callers turn that into a typed error as needed).
    """
    if not isinstance(value, str):
        raise ValueError(f"expected an RFC-3339 string, got {type(value).__name__}")
    text = value.strip()
    if text.endswith(("z", "Z")):
        text = text[:-1] + "+00:00"
    dt = _dt.datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc)


# --- canonical JSON (C2 signed bytes) --------------------------------------


def canonical_json_bytes(obj: Any) -> bytes:
    """Deterministic, compact UTF-8 JSON bytes for signing / hashing.

    Per C2 the bytes that get ed25519-signed for a license/config are the
    *compact-JSON* UTF-8 bytes. "Canonical" here means: keys sorted, no
    insignificant whitespace, non-ASCII preserved (not \\u-escaped). Two
    semantically-equal objects therefore always produce identical bytes, which
    is exactly what a detached signature needs.
    """
    return json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def canonical_json_str(obj: Any) -> str:
    """Same as :func:`canonical_json_bytes` but returns a ``str``."""
    return canonical_json_bytes(obj).decode("utf-8")


# --- hashing / fingerprints (C1 cert fingerprint, C2 manifest sha256) ------


def sha256_hex(data: bytes) -> str:
    """Lowercase hex SHA-256 of arbitrary bytes."""
    return hashlib.sha256(data).hexdigest()


def fingerprint_der(der_bytes: bytes) -> str:
    """C1 cert fingerprint: lowercase-hex SHA-256 of the cert's DER bytes.

    This is the canonical fingerprint the registry is keyed by. A cert's
    fingerprint is, by universal convention (and what OpenSSL's
    ``-fingerprint -sha256`` reports), the SHA-256 over the *DER* encoding of
    the whole certificate — not over the PEM text. The auth proxy computes this
    from the verified leaf cert and looks it up in ``tenant_registry.json``.
    """
    return sha256_hex(der_bytes)


def fingerprint_pem(pem_text: str | bytes) -> str:
    """C1 cert fingerprint from a PEM certificate.

    Decodes the PEM to DER (via ``cryptography``) and fingerprints the DER, so
    this agrees byte-for-byte with :func:`fingerprint_der` and OpenSSL. Import
    of ``cryptography`` is deferred to here so the rest of ``lib`` does not
    require it.
    """
    from cryptography import x509  # deferred import — optional dependency
    from cryptography.hazmat.primitives.serialization import Encoding

    pem_bytes = pem_text.encode("utf-8") if isinstance(pem_text, str) else pem_text
    cert = x509.load_pem_x509_certificate(pem_bytes)
    der = cert.public_bytes(encoding=Encoding.DER)
    return fingerprint_der(der)
