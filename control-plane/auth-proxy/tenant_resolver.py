"""tenant_resolver.py — the server-side identity decision (C1 / Invariant I4).

This is the security core of the auth proxy. Given the **verified** client leaf
certificate presented during the mTLS handshake, it answers one question:

    "Which tenant is this, and is this cert allowed right now?"

…and it answers it **fail-closed**: any ambiguity, any missing material, any
verification failure, any unknown/revoked fingerprint, any SAN↔registry
mismatch ⇒ the request is rejected. There is no code path that returns a tenant
id from caller-supplied input — identity comes *only* from the verified cert's
SPIFFE URI SAN (I4).

Pipeline (in order; every step can only reject, never relax)
------------------------------------------------------------
1. **Chain verification** — the leaf must chain to the Fyralis CA root with the
   ``clientAuth`` purpose and be within its validity window. (Reuses
   ``ca/verify_chain.verify_chain``.) NOTE: TLS termination already requires +
   verifies the client cert; this is a belt-and-suspenders re-verification so
   the security decision never depends solely on the TLS stack's configuration.
2. **SAN extraction** — read ``tenant_id`` out of the verified leaf's
   ``spiffe://fyralis/tenant/<id>`` URI SAN. (Reuses
   ``ca/ca_lib.extract_tenant_from_cert``.)
3. **Revocation / registry check** — compute the leaf's SHA-256 fingerprint and
   look it up in ``tenant_registry.json``. The chain can be cryptographically
   valid yet the cert revoked, so this post-verification check is **mandatory**.
   (Reuses ``ca/registry.is_revoked`` which is already fail-closed: unknown OR
   revoked ⇒ rejected.)
4. **SAN↔registry agreement** — the ``tenant_id`` parsed from the SAN MUST equal
   the ``tenant_id`` recorded in the registry row; a mismatch is rejected (C1).

On success the resolver returns the registry-and-SAN-agreed ``tenant_id``, which
the proxy injects as ``X-Scope-OrgID``. On any failure it raises
:class:`TenantResolutionError`; the proxy turns that into a flat ``403`` with no
detail leak.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from cryptography import x509

# --- wire up the ca/ package -------------------------------------------------
# The ca/ modules use script-style imports (``import ca_lib``/``import registry``)
# exactly as ca/bootstrap_ca.py and ca/issue_cert.py do, so we put ca/ on the
# path and import the SAME modules WS-CA owns — no re-implementation of crypto.
_HERE = Path(__file__).resolve().parent
_CA_DIR = _HERE.parent / "ca"
if str(_CA_DIR) not in sys.path:
    sys.path.insert(0, str(_CA_DIR))

import ca_lib  # noqa: E402  (ca/ca_lib.py — fingerprint + SAN extraction)
import registry  # noqa: E402  (ca/registry.py — fail-closed revocation)
import verify_chain as _verify_chain_mod  # noqa: E402  (ca/verify_chain.py)


CertLike = Union[bytes, str, x509.Certificate]


class TenantResolutionError(Exception):
    """Raised for ANY failure resolving a verified cert to an active tenant.

    Carries a machine ``reason`` (for proxy-side audit logging) but the proxy
    NEVER puts this on the wire — clients see a flat 403. The reasons are a
    closed set so logs are greppable without leaking cert internals.
    """

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail


# Closed set of rejection reasons (for audit logs / metrics labels).
REASON_NO_CERT = "no_client_cert"
REASON_BAD_PARSE = "cert_parse_failed"
REASON_CHAIN_INVALID = "chain_verification_failed"
REASON_SAN_INVALID = "san_missing_or_invalid"
REASON_REVOKED_OR_UNKNOWN = "revoked_or_unknown_fingerprint"
REASON_SAN_REGISTRY_MISMATCH = "san_registry_tenant_mismatch"
REASON_REGISTRY_ERROR = "registry_read_error"


@dataclass(frozen=True)
class ResolvedTenant:
    """A successfully resolved, currently-active tenant identity."""

    tenant_id: str
    fingerprint: str


class TenantResolver:
    """Fail-closed verify→extract→revocation resolver bound to a CA + registry.

    Construct once at proxy start with the CA chain (the trust root the leaf must
    chain to) and the registry path; call :meth:`resolve` per request with the
    DER bytes of the verified peer cert.
    """

    def __init__(
        self,
        ca_chain_pem: bytes,
        *,
        registry_path: Union[str, Path],
        require_client_auth: bool = True,
    ) -> None:
        if not ca_chain_pem:
            # No trust anchor ⇒ we could never verify anything. Fail at
            # construction, not silently per request.
            raise ValueError("ca_chain_pem is empty — refusing to start fail-open")
        self._ca_chain_pem = ca_chain_pem
        self._registry_path = str(registry_path)
        self._require_client_auth = require_client_auth

    # --- constructors ------------------------------------------------------

    @classmethod
    def from_paths(
        cls,
        ca_chain_path: Union[str, Path],
        registry_path: Union[str, Path],
        *,
        require_client_auth: bool = True,
    ) -> "TenantResolver":
        """Build a resolver by reading the CA chain PEM off disk."""
        chain_path = Path(ca_chain_path)
        ca_chain_pem = chain_path.read_bytes()
        return cls(
            ca_chain_pem,
            registry_path=registry_path,
            require_client_auth=require_client_auth,
        )

    # --- the per-request decision -----------------------------------------

    def resolve(self, peer_cert_der: Optional[bytes]) -> ResolvedTenant:
        """Resolve a verified peer cert (DER bytes) to an active tenant id.

        Raises :class:`TenantResolutionError` (fail-closed) on every rejection
        path. Returns a :class:`ResolvedTenant` only when ALL of chain-verify,
        SAN-extract, revocation-active, and SAN↔registry-agreement pass.
        """
        # 0. No cert at all → reject. (The TLS layer should already enforce this,
        #    but we never forward an unauthenticated request.)
        if not peer_cert_der:
            raise TenantResolutionError(
                REASON_NO_CERT, "no client certificate presented"
            )

        # 0b. Parse DER → x509 once.
        try:
            leaf = x509.load_der_x509_certificate(peer_cert_der)
        except Exception as exc:  # malformed / truncated DER
            raise TenantResolutionError(
                REASON_BAD_PARSE, f"could not parse peer cert DER: {exc}"
            ) from exc

        # 1. Re-verify the chain ourselves (belt and suspenders). verify_chain
        #    NEVER raises for an untrusted cert — it returns ok=False with a
        #    reason — so a verification failure is a clean reject, not a 5xx.
        result = _verify_chain_mod.verify_chain(
            leaf,
            self._ca_chain_pem,
            require_client_auth=self._require_client_auth,
        )
        if not result.ok:
            raise TenantResolutionError(
                REASON_CHAIN_INVALID,
                f"chain verification failed: {result.reason}",
            )

        # 2. Extract tenant_id from the VERIFIED leaf's SPIFFE URI SAN (I4).
        try:
            san_tenant = ca_lib.extract_tenant_from_cert(leaf)
        except Exception as exc:  # missing / multiple / malformed SAN
            raise TenantResolutionError(
                REASON_SAN_INVALID, f"SAN extraction failed: {exc}"
            ) from exc

        # 3. Fingerprint + revocation. A revoked cert's chain stays valid, so this
        #    post-verify check is mandatory. ca/registry.is_revoked is fail-closed:
        #    unknown OR revoked ⇒ True ⇒ reject.
        fingerprint = ca_lib.fingerprint_sha256(leaf)
        try:
            revoked = registry.is_revoked(fingerprint, path=self._registry_path)
        except Exception as exc:
            # A registry we cannot read is treated as "deny everything" — we
            # never fail open when the source of truth is unreadable.
            raise TenantResolutionError(
                REASON_REGISTRY_ERROR,
                f"could not consult revocation registry: {exc}",
            ) from exc
        if revoked:
            raise TenantResolutionError(
                REASON_REVOKED_OR_UNKNOWN,
                f"fingerprint {fingerprint} is revoked or unknown",
            )

        # 4. SAN↔registry agreement (C1): the registry row's tenant_id MUST equal
        #    the SAN-derived one. (We know the row exists & is active because
        #    is_revoked returned False.)
        row = registry.get_entry(fingerprint, path=self._registry_path)
        registry_tenant = (row or {}).get("tenant_id")
        if registry_tenant != san_tenant:
            raise TenantResolutionError(
                REASON_SAN_REGISTRY_MISMATCH,
                f"SAN tenant {san_tenant!r} != registry tenant {registry_tenant!r}",
            )

        return ResolvedTenant(tenant_id=san_tenant, fingerprint=fingerprint)


__all__ = [
    "TenantResolver",
    "ResolvedTenant",
    "TenantResolutionError",
    "REASON_NO_CERT",
    "REASON_BAD_PARSE",
    "REASON_CHAIN_INVALID",
    "REASON_SAN_INVALID",
    "REASON_REVOKED_OR_UNKNOWN",
    "REASON_SAN_REGISTRY_MISMATCH",
    "REASON_REGISTRY_ERROR",
]
