"""Tenant revocation registry — fingerprint → tenant lookup (C1).

``tenant_registry.json`` maps a leaf cert's **SHA-256 fingerprint (lowercase
hex)** to its tenant identity and status:

    {
      "<fingerprint_sha256_hex>": {
        "tenant_id": "acme",
        "issued_at": "2026-06-24T00:00:00Z",
        "status": "active"
      }
    }

``status`` ∈ ``active | revoked``. The auth proxy (P2), on every request,
computes the presented leaf's fingerprint, looks it up here, and rejects (403)
when the entry is **missing** or **revoked**, or when the registry ``tenant_id``
disagrees with the SAN-derived one. This module is the read/write surface those
flows share so the JSON shape stays in lockstep.

Writes are atomic (temp file + ``os.replace``) so a crash mid-write can't leave a
half-written registry that would 403 the whole fleet.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import tempfile
from typing import Dict, Optional

# The registry lives at the contract path: control-plane/ca/tenant_registry.json
_HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_REGISTRY_PATH = os.path.join(_HERE, "tenant_registry.json")

STATUS_ACTIVE = "active"
STATUS_REVOKED = "revoked"
_VALID_STATUSES = {STATUS_ACTIVE, STATUS_REVOKED}


def _rfc3339_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_registry(path: str = DEFAULT_REGISTRY_PATH) -> Dict[str, dict]:
    """Load the registry dict; an absent file is an empty registry."""
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, dict):
        raise ValueError("registry root must be a JSON object: %s" % path)
    return data


def save_registry(registry: Dict[str, dict], path: str = DEFAULT_REGISTRY_PATH) -> None:
    """Atomically persist the registry (temp file + rename)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Stable key order keeps diffs reviewable in git.
    payload = json.dumps(registry, indent=2, sort_keys=True) + "\n"
    fd, tmp = tempfile.mkstemp(
        prefix=".tenant_registry.", suffix=".tmp", dir=os.path.dirname(path)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def add_entry(
    fingerprint: str,
    tenant_id: str,
    *,
    issued_at: Optional[str] = None,
    status: str = STATUS_ACTIVE,
    path: str = DEFAULT_REGISTRY_PATH,
) -> dict:
    """Insert (or overwrite) a registry row for ``fingerprint``.

    Returns the stored row. ``issued_at`` defaults to now (RFC-3339 UTC).
    Re-issuing for the same fingerprint is unusual (fingerprints are unique per
    cert) but we overwrite rather than dup-key, keeping the registry a true map.
    """
    fingerprint = _normalize_fp(fingerprint)
    if status not in _VALID_STATUSES:
        raise ValueError("status must be one of %s" % sorted(_VALID_STATUSES))
    registry = load_registry(path)
    row = {
        "tenant_id": tenant_id,
        "issued_at": issued_at or _rfc3339_now(),
        "status": status,
    }
    registry[fingerprint] = row
    save_registry(registry, path)
    return row


def set_status(
    fingerprint: str,
    status: str,
    *,
    path: str = DEFAULT_REGISTRY_PATH,
    extra: Optional[dict] = None,
) -> dict:
    """Flip a row's status (e.g. to ``revoked``). Raises if absent."""
    fingerprint = _normalize_fp(fingerprint)
    if status not in _VALID_STATUSES:
        raise ValueError("status must be one of %s" % sorted(_VALID_STATUSES))
    registry = load_registry(path)
    if fingerprint not in registry:
        raise KeyError("no registry entry for fingerprint %s" % fingerprint)
    registry[fingerprint]["status"] = status
    if extra:
        registry[fingerprint].update(extra)
    save_registry(registry, path)
    return registry[fingerprint]


def get_entry(
    fingerprint: str, *, path: str = DEFAULT_REGISTRY_PATH
) -> Optional[dict]:
    """Return the row for ``fingerprint`` or ``None`` if absent."""
    return load_registry(path).get(_normalize_fp(fingerprint))


def find_by_tenant(
    tenant_id: str, *, path: str = DEFAULT_REGISTRY_PATH
) -> Dict[str, dict]:
    """Return ``{fingerprint: row}`` for every cert ever issued to a tenant.

    A tenant may have several certs over time (rotation); revoke-by-tenant uses
    this to flip *all* of them.
    """
    return {
        fp: row
        for fp, row in load_registry(path).items()
        if row.get("tenant_id") == tenant_id
    }


def is_revoked(fingerprint: str, *, path: str = DEFAULT_REGISTRY_PATH) -> bool:
    """True if the fingerprint is **unknown** or **revoked**.

    This is the proxy's reject predicate: an unknown cert is treated as revoked
    (fail-closed) — only an explicit ``active`` row is accepted. Keeping the
    fail-closed default here means every caller inherits the safe behavior.
    """
    row = get_entry(fingerprint, path=path)
    if row is None:
        return True
    return row.get("status") != STATUS_ACTIVE


def is_active(fingerprint: str, *, path: str = DEFAULT_REGISTRY_PATH) -> bool:
    """Convenience inverse of :func:`is_revoked`."""
    return not is_revoked(fingerprint, path=path)


def _normalize_fp(fingerprint: str) -> str:
    """Lowercase + strip optional colon separators / 0x / whitespace."""
    if not isinstance(fingerprint, str):
        raise TypeError("fingerprint must be a hex str")
    fp = fingerprint.strip().lower().replace(":", "")
    if fp.startswith("0x"):
        fp = fp[2:]
    if not fp:
        raise ValueError("empty fingerprint")
    return fp
