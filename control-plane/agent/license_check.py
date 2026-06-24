"""license_check — load + verify the local signed license; gate privileged actions.

The license is a **signed JSON bundle** (contract: signed by ``control-plane/signing``,
ed25519, verify-before-use per I6)::

    {
      "tenant_id":  "acme",
      "deployment_id": "acme-use1-0001",
      "plan": "enterprise",
      "issued_at":  "2026-06-24T00:00:00Z",
      "expires_at": "2027-06-24T00:00:00Z",
      "features": ["fleet", "anomaly", "deadline"]
    }

shipped alongside its detached signature + manifest (``license.json``,
``license.json.sig``, ``license.json.manifest.json``) exactly as
``signing/sign_bundle.py`` emits them.

``is_licensed()`` returns True only when ALL hold:

* the detached ed25519 signature verifies against the trust root (I6: an
  unverified / tampered / wrong-key license is *never* trusted), AND
* the ``manifest.artifact`` is ``license`` (a release/config bundle is not a
  license), AND
* the license has not **expired** (``expires_at`` is in the future) and is
  already in effect (``issued_at`` <= now).

The agent calls ``is_licensed()`` before performing any privileged action and
**refuses** (returns / no-ops) when it is False — an unlicensed or expired
deployment does not get to operate. This module performs **no network I/O**; it
reads local files only.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import dataclass
from pathlib import Path

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)
import verify_bundle as vb
from lib.primitives import parse_rfc3339, utcnow

__all__ = ["LicenseStatus", "LicenseChecker", "load_license_status"]


@dataclass(frozen=True)
class LicenseStatus:
    """The evaluated state of the local license at a point in time."""

    ok: bool
    reason: str
    tenant_id: str | None = None
    deployment_id: str | None = None
    plan: str | None = None
    expires_at: _dt.datetime | None = None
    features: tuple[str, ...] = ()

    @property
    def expired(self) -> bool:
        if self.expires_at is None:
            return True
        return self.expires_at <= utcnow()


def _as_aware(value) -> _dt.datetime:
    if isinstance(value, _dt.datetime):
        return value if value.tzinfo else value.replace(tzinfo=_dt.timezone.utc)
    return parse_rfc3339(str(value))


class LicenseChecker:
    """Verifies the local signed license and answers ``is_licensed()``.

    Verification is re-run on every :meth:`evaluate` (cheap, local) so a license
    that expires *while the agent is running* flips to unlicensed without a
    restart, and a license file swapped on disk is re-verified — never cached as
    "once valid, always valid".
    """

    def __init__(
        self,
        license_path: "str | Path",
        *,
        trust_root_path: "str | Path | None" = None,
    ) -> None:
        self.license_path = Path(license_path)
        self.trust_root_path = (
            str(trust_root_path) if trust_root_path is not None else None
        )

    # -- core evaluation ----------------------------------------------------

    def evaluate(self, *, now: _dt.datetime | None = None) -> LicenseStatus:
        """Verify the license signature + expiry and return a :class:`LicenseStatus`."""
        now = now or utcnow()
        path = str(self.license_path)

        if not self.license_path.is_file():
            return LicenseStatus(False, f"license file not found: {path}")

        # 1. Cryptographic verify-before-use (I6): signature, key_id policy,
        #    sha256 cross-check — delegated to the shared signing primitive.
        res = vb.verify_file(path, trust_root_path=self.trust_root_path)
        if not res.ok:
            return LicenseStatus(False, f"license signature rejected: {res.reason}")
        if (res.artifact or "").lower() != "license":
            return LicenseStatus(
                False,
                f"signed artifact is {res.artifact!r}, not a license "
                "(refusing to treat a non-license bundle as a license)",
            )

        # 2. Parse the (now-authenticated) license body.
        try:
            with open(path, "r", encoding="utf-8") as fh:
                body = json.load(fh)
        except (ValueError, OSError) as exc:
            return LicenseStatus(False, f"license JSON unreadable: {exc}")

        tenant_id = body.get("tenant_id")
        deployment_id = body.get("deployment_id")
        plan = body.get("plan")
        features = tuple(body.get("features", []) or [])

        exp_raw = body.get("expires_at")
        iss_raw = body.get("issued_at")
        if exp_raw is None:
            return LicenseStatus(
                False, "license has no expires_at", tenant_id, deployment_id, plan
            )
        try:
            expires_at = _as_aware(exp_raw)
            issued_at = _as_aware(iss_raw) if iss_raw is not None else None
        except ValueError as exc:
            return LicenseStatus(
                False,
                f"license timestamp unparseable: {exc}",
                tenant_id,
                deployment_id,
                plan,
            )

        # 3. Temporal validity.
        if issued_at is not None and issued_at > now:
            return LicenseStatus(
                False,
                f"license not yet in effect (issued_at {issued_at.isoformat()} > now)",
                tenant_id,
                deployment_id,
                plan,
                expires_at,
                features,
            )
        if expires_at <= now:
            return LicenseStatus(
                False,
                f"license EXPIRED at {expires_at.isoformat()}",
                tenant_id,
                deployment_id,
                plan,
                expires_at,
                features,
            )

        return LicenseStatus(
            True,
            f"license valid ({plan}) until {expires_at.isoformat()}",
            tenant_id,
            deployment_id,
            plan,
            expires_at,
            features,
        )

    # -- convenience --------------------------------------------------------

    def is_licensed(self, *, now: _dt.datetime | None = None) -> bool:
        """True iff the license verifies AND is currently in its validity window."""
        return self.evaluate(now=now).ok

    def license_expiry(self, *, now: _dt.datetime | None = None) -> _dt.datetime | None:
        """The license ``expires_at`` (even if expired) for stamping the heartbeat.

        Returns ``None`` only if the license is missing/unverifiable/malformed —
        the caller treats a missing expiry as "do not operate".
        """
        return self.evaluate(now=now).expires_at


def load_license_status(
    license_path: "str | Path",
    *,
    trust_root_path: "str | Path | None" = None,
    now: _dt.datetime | None = None,
) -> LicenseStatus:
    """One-shot convenience: evaluate the license at ``license_path``."""
    return LicenseChecker(license_path, trust_root_path=trust_root_path).evaluate(now=now)
