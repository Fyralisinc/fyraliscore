"""Tenant identity + the read-only tenant registry reader (C1).

This module owns the **read** side of the C1 identity contract. WS-CA owns
*writing* ``control-plane/ca/tenant_registry.json``; everything else (notably the
auth proxy) only ever **reads** it through this reader to answer:

    * ``tenant_for_fingerprint(fp)`` → the ``tenant_id`` for a verified leaf-cert
      fingerprint, raising on unknown / revoked / inactive.
    * ``is_active(fp)`` / ``is_revoked(fp)`` → boolean status checks.

Registry format (normative, from SPRINT_PLAN C1)::

    {
      "<cert_fingerprint_sha256_hex>": {
        "tenant_id": "acme",
        "issued_at": "2026-06-24T00:00:00Z",
        "status": "active"          # active | revoked
      }
    }

Security stance (Invariant I4): the fingerprint is the *only* key. This reader
never accepts a tenant_id from anywhere but the registry row, and treats any
status other than the literal ``"active"`` as untrusted — an unknown or
malformed status fails closed (rejected), it does not "default to active".

The reader is intentionally *read-only* and *side-effect free*. It re-reads the
file on demand (with an mtime-guarded cache) so a revocation written by WS-CA is
picked up without a process restart — important because revocation must take
effect promptly.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import NewType

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .config import ControlPlaneConfig, get_config
from .errors import (
    RegistryFormatError,
    RegistryNotFoundError,
    TenantInactiveError,
    TenantNotFoundError,
    TenantRevokedError,
)
from .primitives import parse_rfc3339

__all__ = [
    "TenantId",
    "TenantStatus",
    "TenantRecord",
    "TenantRegistry",
]

# A nominal type so signatures read as "tenant id" not "some string".
TenantId = NewType("TenantId", str)

# Status literals from C1.
TenantStatus = str  # narrowed by validation to {"active", "revoked"} at read time

_FINGERPRINT_RE = re.compile(r"^[0-9a-f]{64}$")


class TenantRecord(BaseModel):
    """One registry row: the value side of a ``{fingerprint: row}`` mapping."""

    model_config = ConfigDict(frozen=True, extra="ignore")

    tenant_id: TenantId
    issued_at: str = Field(description="RFC-3339 UTC issuance timestamp")
    status: str = Field(description='one of "active" | "revoked"')

    @field_validator("tenant_id")
    @classmethod
    def _nonempty_tenant(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("tenant_id must be a non-empty string")
        return v

    @field_validator("issued_at")
    @classmethod
    def _valid_rfc3339(cls, v: str) -> str:
        # Validate parseability but keep the original wire string.
        parse_rfc3339(v)
        return v

    @field_validator("status")
    @classmethod
    def _known_status(cls, v: str) -> str:
        norm = v.strip().lower()
        if norm not in {"active", "revoked"}:
            # Fail closed: an unknown status is NOT silently treated as active.
            raise ValueError(
                f'status must be "active" or "revoked", got {v!r}'
            )
        return norm

    @property
    def is_active(self) -> bool:
        return self.status == "active"

    @property
    def is_revoked(self) -> bool:
        return self.status == "revoked"


def _normalize_fingerprint(fingerprint: str) -> str:
    """Normalize a presented fingerprint to lowercase hex, no colons/spaces.

    The auth proxy computes the SHA-256 of the leaf cert's DER (see
    ``primitives.fingerprint_der``) which is already lowercase hex; but tools
    like OpenSSL emit colon-separated uppercase. We normalize defensively so a
    lookup never misses on cosmetic formatting.
    """
    cleaned = fingerprint.strip().lower().replace(":", "").replace(" ", "")
    if cleaned.startswith("sha256:"):
        cleaned = cleaned[len("sha256:") :]
    return cleaned


class TenantRegistry:
    """Read-only reader over ``ca/tenant_registry.json`` (C1).

    Path is injected; it defaults to the ``ca/`` location from
    :class:`ControlPlaneConfig`. The file is loaded lazily and cached, keyed on
    the file's mtime+size so an out-of-band revocation by WS-CA is picked up on
    the next call without a restart. Pass ``cache=False`` to read fresh every
    time.
    """

    def __init__(
        self,
        registry_path: str | Path | None = None,
        *,
        config: ControlPlaneConfig | None = None,
        cache: bool = True,
    ) -> None:
        if registry_path is not None:
            self._path = Path(registry_path)
        else:
            cfg = config or get_config()
            self._path = Path(cfg.tenant_registry_path)
        self._cache_enabled = cache
        self._cached: dict[str, TenantRecord] | None = None
        self._cache_stamp: tuple[float, int] | None = None

    # --- path / loading ----------------------------------------------------

    @property
    def path(self) -> Path:
        return self._path

    def _stat_stamp(self) -> tuple[float, int]:
        st = self._path.stat()
        return (st.st_mtime, st.st_size)

    def _load(self) -> dict[str, TenantRecord]:
        if not self._path.is_file():
            raise RegistryNotFoundError(
                f"tenant registry not found at {self._path}"
            )

        if self._cache_enabled and self._cached is not None:
            try:
                if self._stat_stamp() == self._cache_stamp:
                    return self._cached
            except OSError:
                pass  # fall through to a fresh read

        try:
            raw = self._path.read_text(encoding="utf-8")
        except OSError as exc:
            raise RegistryFormatError(
                f"could not read tenant registry {self._path}: {exc}"
            ) from exc

        try:
            data = json.loads(raw) if raw.strip() else {}
        except json.JSONDecodeError as exc:
            raise RegistryFormatError(
                f"tenant registry {self._path} is not valid JSON: {exc}"
            ) from exc

        if not isinstance(data, dict):
            raise RegistryFormatError(
                f"tenant registry {self._path} must be a JSON object "
                f"({{fingerprint: row}}), got {type(data).__name__}"
            )

        records: dict[str, TenantRecord] = {}
        for fingerprint, row in data.items():
            fp = _normalize_fingerprint(str(fingerprint))
            if not _FINGERPRINT_RE.match(fp):
                raise RegistryFormatError(
                    f"registry key {fingerprint!r} is not a 64-char hex "
                    "SHA-256 fingerprint"
                )
            if not isinstance(row, dict):
                raise RegistryFormatError(
                    f"registry row for {fp} must be an object, got "
                    f"{type(row).__name__}"
                )
            try:
                records[fp] = TenantRecord(**row)
            except Exception as exc:  # pydantic ValidationError → format error
                raise RegistryFormatError(
                    f"registry row for {fp} is invalid: {exc}"
                ) from exc

        if self._cache_enabled:
            self._cached = records
            try:
                self._cache_stamp = self._stat_stamp()
            except OSError:
                self._cache_stamp = None
        return records

    def reload(self) -> "TenantRegistry":
        """Force a fresh read on the next access (drops the cache)."""
        self._cached = None
        self._cache_stamp = None
        return self

    # --- introspection -----------------------------------------------------

    def __len__(self) -> int:
        return len(self._load())

    def fingerprints(self) -> list[str]:
        """All registered fingerprints (lowercase hex)."""
        return list(self._load().keys())

    def record_for_fingerprint(self, fingerprint: str) -> TenantRecord:
        """Return the raw :class:`TenantRecord`, or raise ``TenantNotFoundError``.

        Does NOT enforce status — use this when you want the row regardless of
        active/revoked (e.g. an operator console listing). For the security
        decision use :meth:`tenant_for_fingerprint`.
        """
        fp = _normalize_fingerprint(fingerprint)
        records = self._load()
        rec = records.get(fp)
        if rec is None:
            raise TenantNotFoundError(
                f"fingerprint {fp} is not present in the tenant registry"
            )
        return rec

    # --- the C1 decision surface ------------------------------------------

    def tenant_for_fingerprint(self, fingerprint: str) -> TenantId:
        """Resolve a verified leaf-cert fingerprint to its ``tenant_id``.

        Enforces the full C1 gate and **fails closed**:
          * missing fingerprint  → :class:`TenantNotFoundError`
          * ``status == revoked`` → :class:`TenantRevokedError`
          * any non-active status → :class:`TenantInactiveError`

        The auth proxy turns any of these into a ``403``. Only an explicitly
        ``active`` row yields a tenant id.
        """
        rec = self.record_for_fingerprint(fingerprint)
        if rec.is_revoked:
            raise TenantRevokedError(
                f"cert fingerprint {_normalize_fingerprint(fingerprint)} is "
                f"revoked (tenant {rec.tenant_id})"
            )
        if not rec.is_active:
            raise TenantInactiveError(
                f"cert fingerprint {_normalize_fingerprint(fingerprint)} has "
                f"non-active status {rec.status!r}"
            )
        return TenantId(rec.tenant_id)

    def is_active(self, fingerprint: str) -> bool:
        """True iff the fingerprint is present AND status is ``active``.

        Never raises for an unknown fingerprint — an absent entry is simply not
        active. (Use :meth:`tenant_for_fingerprint` when you need the reason.)
        """
        try:
            rec = self.record_for_fingerprint(fingerprint)
        except TenantNotFoundError:
            return False
        return rec.is_active

    def is_revoked(self, fingerprint: str) -> bool:
        """True iff the fingerprint is present AND status is ``revoked``.

        An unknown fingerprint returns ``False`` (it is "not revoked" because it
        was never issued); callers that must reject the unknown case use
        :meth:`tenant_for_fingerprint`, which rejects unknown AND revoked.
        """
        try:
            rec = self.record_for_fingerprint(fingerprint)
        except TenantNotFoundError:
            return False
        return rec.is_revoked
