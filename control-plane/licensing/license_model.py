"""license_model — the LICENSE bundle contract (P4 shared contract) + (de)serialization.

The license is the signed grant that tells a data-plane agent it is *permitted to operate*
for a given tenant/deployment, on a given plan, with a given feature set, until it expires.

Wire shape (P4 / SPRINT_PLAN C2 ``artifact:"license"``)::

    {
      "tenant_id":     "acme",
      "deployment_id": "acme-use1-7f3a",
      "plan":          "enterprise",
      "issued_at":     "2026-06-24T00:00:00Z",
      "expires_at":    "2026-06-25T00:00:00Z",
      "features":      ["telemetry_t3", "byoc", "sso"],
      "license_id":    "lic-acme-3f9c1a2b",
      "version":       1
    }

The license is signed by ``control-plane/signing`` as a ``license`` artifact: the
**canonical compact JSON** of this document is what gets ed25519-signed (so signing is
independent of key ordering / whitespace), with a detached ``<file>.sig`` + ``<file>.manifest.json``
written alongside (C2). A *signed license bundle* on disk is therefore the trio:

    license.json
    license.json.sig
    license.json.manifest.json

This module owns ONLY the document shape and its canonical bytes. Signing lives in
``issue_license.py`` (which calls ``signing/sign_bundle``); verification + policy
(expiry / tenant-match / revocation, fail-closed) lives in ``validator.py``.

We deliberately do **not** import ``lib`` here for the time helpers so this module stays
importable even if the sibling ``lib`` package is mid-build; we use the same RFC-3339
grammar (``...Z``) the rest of the control plane uses. ``validator.py`` *does* reuse
``lib.DeploymentRecord`` semantics by matching field names, so a license and a
DeploymentRecord agree on ``tenant_id`` / ``deployment_id`` / expiry.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import secrets
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "LICENSE_VERSION",
    "KNOWN_PLANS",
    "License",
    "now_rfc3339",
    "to_rfc3339",
    "parse_rfc3339",
    "canonical_json_bytes",
]

LICENSE_VERSION = 1

# Plans are an *open* enum: we recognise these but accept any non-empty string so the
# control plane can introduce a plan without a code change. Unknown plans are allowed
# but ``known_plan`` reports whether it is one we ship today.
KNOWN_PLANS = ("trial", "standard", "pro", "enterprise")


# --------------------------------------------------------------------------- #
# RFC-3339 UTC time — same grammar as lib.primitives (kept local, no import).  #
# --------------------------------------------------------------------------- #


def now_rfc3339() -> str:
    """Current UTC time as ``YYYY-MM-DDTHH:MM:SSZ`` (whole-second, control-plane canonical)."""
    return to_rfc3339(_dt.datetime.now(_dt.timezone.utc))


def to_rfc3339(dt: _dt.datetime) -> str:
    """Serialize an aware/naive datetime to a canonical RFC-3339 UTC string ending in ``Z``."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    dt = dt.astimezone(_dt.timezone.utc).replace(microsecond=0)
    return dt.isoformat().replace("+00:00", "Z")


def parse_rfc3339(value: str) -> _dt.datetime:
    """Parse an RFC-3339 string (trailing ``Z`` accepted) to an aware UTC datetime."""
    if not isinstance(value, str):
        raise ValueError(f"expected an RFC-3339 string, got {type(value).__name__}")
    text = value.strip()
    if text.endswith(("z", "Z")):
        text = text[:-1] + "+00:00"
    dt = _dt.datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc)


def canonical_json_bytes(obj: Any) -> bytes:
    """Deterministic compact UTF-8 JSON (sorted keys, no whitespace) — the C2 signed bytes.

    This MUST match ``signing_lib.canonical_json_bytes`` so the bytes we hash here are the
    same bytes ``sign_bundle`` signs and ``verify_bundle`` re-derives.
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
        "utf-8"
    )


def _new_license_id(tenant_id: str) -> str:
    """A stable, collision-resistant license id: ``lic-<tenant>-<8 hex>``."""
    return f"lic-{tenant_id}-{secrets.token_hex(4)}"


@dataclass
class License:
    """The license document (P4 contract).

    Construct a fresh license with :meth:`mint` (stamps ``issued_at`` + computes
    ``expires_at`` from a duration). Round-trip an on-disk license with
    :meth:`from_dict` / :meth:`to_dict`. The dataclass carries no signing state — a
    license is *unsigned data* until ``issue_license`` writes the detached signature.
    """

    tenant_id: str
    deployment_id: str
    plan: str
    issued_at: str  # RFC-3339 UTC
    expires_at: str  # RFC-3339 UTC
    features: list[str] = field(default_factory=list)
    license_id: str = ""
    version: int = LICENSE_VERSION

    # -- construction ------------------------------------------------------- #

    @classmethod
    def mint(
        cls,
        *,
        tenant_id: str,
        deployment_id: str,
        plan: str = "standard",
        duration_days: float | None = None,
        duration_seconds: float | None = None,
        features: list[str] | None = None,
        issued_at: "str | _dt.datetime | None" = None,
        expires_at: "str | _dt.datetime | None" = None,
        license_id: str | None = None,
    ) -> "License":
        """Mint a fresh, *unsigned* license.

        Expiry is set by exactly one of: an explicit ``expires_at``, or a duration
        (``duration_days`` / ``duration_seconds``) added to ``issued_at`` (default now).
        A *negative* duration is allowed on purpose so tests can mint an already-expired
        license. Raises ``ValueError`` on empty ids or a missing/ambiguous expiry.
        """
        tenant_id = (tenant_id or "").strip()
        deployment_id = (deployment_id or "").strip()
        plan = (plan or "").strip()
        if not tenant_id:
            raise ValueError("tenant_id is required")
        if not deployment_id:
            raise ValueError("deployment_id is required")
        if not plan:
            raise ValueError("plan is required")

        issued_dt = (
            _dt.datetime.now(_dt.timezone.utc)
            if issued_at is None
            else (issued_at if isinstance(issued_at, _dt.datetime) else parse_rfc3339(issued_at))
        )

        n_expiry_inputs = sum(
            x is not None for x in (expires_at, duration_days, duration_seconds)
        )
        if n_expiry_inputs == 0:
            raise ValueError(
                "must supply an expiry: expires_at, or duration_days, or duration_seconds"
            )
        if n_expiry_inputs > 1:
            raise ValueError(
                "supply exactly one of expires_at / duration_days / duration_seconds"
            )

        if expires_at is not None:
            expires_dt = (
                expires_at
                if isinstance(expires_at, _dt.datetime)
                else parse_rfc3339(expires_at)
            )
        else:
            delta = _dt.timedelta(
                days=duration_days or 0.0, seconds=duration_seconds or 0.0
            )
            expires_dt = issued_dt + delta

        return cls(
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            plan=plan,
            issued_at=to_rfc3339(issued_dt),
            expires_at=to_rfc3339(expires_dt),
            features=list(features or []),
            license_id=license_id or _new_license_id(tenant_id),
            version=LICENSE_VERSION,
        )

    # -- (de)serialization -------------------------------------------------- #

    def to_dict(self) -> dict:
        """The exact wire dict (the thing that gets signed)."""
        return {
            "tenant_id": self.tenant_id,
            "deployment_id": self.deployment_id,
            "plan": self.plan,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "features": list(self.features),
            "license_id": self.license_id,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, obj: dict) -> "License":
        """Parse a license dict; raises ``ValueError`` on a malformed document (fail-closed).

        A document that is missing a required field, has the wrong type, or carries an
        empty id is *rejected here* rather than silently coerced — the validator treats a
        parse failure as deny.
        """
        if not isinstance(obj, dict):
            raise ValueError("license must be a JSON object")
        required = ("tenant_id", "deployment_id", "plan", "issued_at", "expires_at")
        for k in required:
            if k not in obj:
                raise ValueError(f"license missing required field: {k!r}")
            if not isinstance(obj[k], str) or not obj[k].strip():
                raise ValueError(f"license field {k!r} must be a non-empty string")

        features = obj.get("features", [])
        if not isinstance(features, list) or not all(isinstance(f, str) for f in features):
            raise ValueError("license 'features' must be a list of strings")

        # Validate the timestamps parse (fail closed on garbage time).
        parse_rfc3339(obj["issued_at"])
        parse_rfc3339(obj["expires_at"])

        version = obj.get("version", LICENSE_VERSION)
        if not isinstance(version, int):
            raise ValueError("license 'version' must be an integer")

        return cls(
            tenant_id=obj["tenant_id"],
            deployment_id=obj["deployment_id"],
            plan=obj["plan"],
            issued_at=obj["issued_at"],
            expires_at=obj["expires_at"],
            features=list(features),
            license_id=obj.get("license_id", ""),
            version=version,
        )

    @classmethod
    def from_file(cls, path: str) -> "License":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    def to_json(self, *, indent: int | None = 2) -> str:
        """Human-readable JSON for writing ``license.json`` (signing re-canonicalizes)."""
        return json.dumps(self.to_dict(), indent=indent, sort_keys=True) + (
            "\n" if indent is not None else ""
        )

    def canonical_bytes(self) -> bytes:
        """The exact bytes the signing layer signs for this license (C2)."""
        return canonical_json_bytes(self.to_dict())

    # -- derived / predicates ----------------------------------------------- #

    @property
    def issued_dt(self) -> _dt.datetime:
        return parse_rfc3339(self.issued_at)

    @property
    def expires_dt(self) -> _dt.datetime:
        return parse_rfc3339(self.expires_at)

    def fingerprint(self) -> str:
        """A stable content fingerprint (sha256 of canonical bytes) — handy for audit/revoke-by-content."""
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def known_plan(self) -> bool:
        return self.plan in KNOWN_PLANS

    def is_expired(self, *, now: _dt.datetime | None = None, skew_seconds: int = 0) -> bool:
        """True iff ``now`` is at/after ``expires_at`` (minus an allowed clock-skew grace).

        Fail-closed semantics: the boundary instant (``now == expires_at``) is treated as
        EXPIRED — a license is valid strictly before its expiry. ``skew_seconds`` lets a
        deployment tolerate small clock drift; with the default 0 it is exact.
        """
        now = now or _dt.datetime.now(_dt.timezone.utc)
        # An expired license is one whose expiry is <= now (after granting skew grace).
        return self.expires_dt <= (now - _dt.timedelta(seconds=skew_seconds))
