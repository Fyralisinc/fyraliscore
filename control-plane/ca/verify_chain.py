"""Chain verification for tenant leaf certs (used by the auth proxy + tests).

The auth proxy (P2) verifies a presented client cert *before* it trusts the
SPIFFE SAN. This module is the verification primitive: given a leaf PEM and the
CA chain (intermediate + root), confirm the leaf chains to the root with the
``clientAuth`` purpose and is within its validity window.

Two backends are provided:

* :func:`verify_chain` — uses ``cryptography``'s native
  :class:`~cryptography.x509.verification.ClientVerifier` when available
  (cryptography >= 42). This does real path building, signature checks, EKU and
  validity enforcement.
* a manual fallback (signature + issuer/AKI + validity + basic-constraints +
  EKU walk) for environments where the verification API is unavailable, so the
  tests and proxy still get a real (not stubbed) check.

The public surface is intentionally tiny and boolean-or-raise so callers can do
``if not verify_chain(...): reject()`` or rely on the structured
:class:`ChainVerificationResult`.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import List, Optional, Union

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.ec import ECDSA
from cryptography.x509.oid import ExtendedKeyUsageOID

try:  # cryptography >= 42 ships a real verifier; prefer it.
    from cryptography.x509.verification import (
        PolicyBuilder,
        Store,
    )

    _HAVE_VERIFIER = True
except Exception:  # pragma: no cover - exercised only on old cryptography
    _HAVE_VERIFIER = False


PemLike = Union[bytes, str, x509.Certificate]


@dataclass(frozen=True)
class ChainVerificationResult:
    """Structured outcome so callers can log *why* a verify failed."""

    ok: bool
    reason: str = ""
    backend: str = ""

    def __bool__(self) -> bool:  # truthy == verified
        return self.ok


def _coerce(value: PemLike) -> x509.Certificate:
    if isinstance(value, x509.Certificate):
        return value
    data = value.encode() if isinstance(value, str) else value
    return x509.load_pem_x509_certificate(data)


def _load_chain(chain: Union[PemLike, List[PemLike]]) -> List[x509.Certificate]:
    """Accept a single PEM blob (possibly concatenated) or a list of certs."""
    if isinstance(chain, list):
        return [_coerce(c) for c in chain]
    if isinstance(chain, x509.Certificate):
        return [chain]
    data = chain.encode() if isinstance(chain, str) else chain
    # A concatenated PEM bundle (intermediate + root) parses to N certs.
    return list(x509.load_pem_x509_certificates(data))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def verify_chain(
    leaf: PemLike,
    chain: Union[PemLike, List[PemLike]],
    *,
    at_time: Optional[_dt.datetime] = None,
    require_client_auth: bool = True,
) -> ChainVerificationResult:
    """Verify ``leaf`` chains to the trust root in ``chain``.

    ``chain`` is the intermediate+root bundle (order-independent; we sort out the
    root). Returns a :class:`ChainVerificationResult` that is truthy on success.
    Never raises for an *untrusted* cert — only returns ``ok=False`` with a
    reason — so the proxy can turn any failure into a clean ``403``.
    """
    try:
        leaf_cert = _coerce(leaf)
        chain_certs = _load_chain(chain)
    except Exception as exc:  # malformed PEM
        return ChainVerificationResult(False, "could not parse certs: %s" % exc)

    if not chain_certs:
        return ChainVerificationResult(False, "empty CA chain")

    roots, intermediates = _split_roots(chain_certs)
    if not roots:
        return ChainVerificationResult(False, "no self-signed root in CA chain")

    if _HAVE_VERIFIER:
        return _verify_native(
            leaf_cert, roots, intermediates, at_time, require_client_auth
        )
    return _verify_manual(
        leaf_cert, roots, intermediates, at_time, require_client_auth
    )


def is_trusted(leaf: PemLike, chain: Union[PemLike, List[PemLike]]) -> bool:
    """Boolean convenience wrapper around :func:`verify_chain`."""
    return bool(verify_chain(leaf, chain))


# ---------------------------------------------------------------------------
# Native backend (cryptography.x509.verification)
# ---------------------------------------------------------------------------

def _verify_native(
    leaf: x509.Certificate,
    roots: List[x509.Certificate],
    intermediates: List[x509.Certificate],
    at_time: Optional[_dt.datetime],
    require_client_auth: bool,
) -> ChainVerificationResult:
    store = Store(roots)
    builder = PolicyBuilder().store(store)
    if at_time is not None:
        builder = builder.time(_aware(at_time))

    # SPIFFE leaves carry a URI SAN, not a DNS/IP subject, so we cannot use the
    # server verifier (it requires a Subject/DNS to match). The client verifier
    # validates path + signatures + validity + the clientAuth EKU without
    # demanding a hostname.
    try:
        verifier = builder.build_client_verifier()
        verifier.verify(leaf, intermediates)
    except Exception as exc:
        return ChainVerificationResult(
            False, "chain verification failed: %s" % exc, backend="native"
        )

    if require_client_auth and not _has_client_auth_eku(leaf):
        return ChainVerificationResult(
            False, "leaf missing clientAuth EKU", backend="native"
        )
    return ChainVerificationResult(True, backend="native")


# ---------------------------------------------------------------------------
# Manual fallback backend
# ---------------------------------------------------------------------------

def _verify_manual(
    leaf: x509.Certificate,
    roots: List[x509.Certificate],
    intermediates: List[x509.Certificate],
    at_time: Optional[_dt.datetime],
    require_client_auth: bool,
) -> ChainVerificationResult:
    now = _aware(at_time) if at_time else _dt.datetime.now(_dt.timezone.utc)

    # Build leaf -> ... -> root by matching issuer to subject, verifying each
    # signature with the parent's public key as we climb.
    pool = list(intermediates) + list(roots)
    current = leaf
    visited_serials = set()
    for _ in range(8):  # bounded depth; our hierarchy is depth 3
        if not _within_validity(current, now):
            return ChainVerificationResult(
                False, "cert not temporally valid: %s" % current.subject.rfc4514_string(),
                backend="manual",
            )
        # Reached a self-signed root that we trust?
        if _is_self_issued(current):
            if any(current == r for r in roots):
                if require_client_auth and not _has_client_auth_eku(leaf):
                    return ChainVerificationResult(
                        False, "leaf missing clientAuth EKU", backend="manual"
                    )
                return ChainVerificationResult(True, backend="manual")
            return ChainVerificationResult(
                False, "self-signed cert is not the trusted root", backend="manual"
            )

        issuer = _find_issuer(current, pool)
        if issuer is None:
            return ChainVerificationResult(
                False,
                "no issuer found for %s" % current.subject.rfc4514_string(),
                backend="manual",
            )
        if not _signature_ok(current, issuer):
            return ChainVerificationResult(
                False,
                "bad signature on %s" % current.subject.rfc4514_string(),
                backend="manual",
            )
        # An issuer in the path must be a CA.
        if not _is_ca(issuer):
            return ChainVerificationResult(
                False, "issuer is not a CA", backend="manual"
            )
        if issuer.serial_number in visited_serials:
            return ChainVerificationResult(False, "loop in chain", backend="manual")
        visited_serials.add(issuer.serial_number)
        current = issuer

    return ChainVerificationResult(False, "chain too long / no anchor", backend="manual")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _split_roots(certs: List[x509.Certificate]):
    roots, inters = [], []
    for c in certs:
        (roots if _is_self_issued(c) else inters).append(c)
    return roots, inters


def _is_self_issued(cert: x509.Certificate) -> bool:
    return cert.subject == cert.issuer


def _is_ca(cert: x509.Certificate) -> bool:
    try:
        bc = cert.extensions.get_extension_for_class(x509.BasicConstraints).value
        return bool(bc.ca)
    except x509.ExtensionNotFound:
        return False


def _find_issuer(
    cert: x509.Certificate, pool: List[x509.Certificate]
) -> Optional[x509.Certificate]:
    for cand in pool:
        if cand.subject == cert.issuer:
            return cand
    return None


def _signature_ok(cert: x509.Certificate, issuer: x509.Certificate) -> bool:
    pub = issuer.public_key()
    try:
        if isinstance(pub, ec.EllipticCurvePublicKey):
            pub.verify(
                cert.signature,
                cert.tbs_certificate_bytes,
                ECDSA(cert.signature_hash_algorithm),
            )
        elif isinstance(pub, rsa.RSAPublicKey):
            pub.verify(
                cert.signature,
                cert.tbs_certificate_bytes,
                padding.PKCS1v15(),
                cert.signature_hash_algorithm,
            )
        else:  # ed25519/ed448
            pub.verify(cert.signature, cert.tbs_certificate_bytes)
        return True
    except InvalidSignature:
        return False
    except Exception:
        return False


def _within_validity(cert: x509.Certificate, now: _dt.datetime) -> bool:
    nb = _aware(cert.not_valid_before_utc)
    na = _aware(cert.not_valid_after_utc)
    return nb <= now <= na


def _has_client_auth_eku(cert: x509.Certificate) -> bool:
    try:
        eku = cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
        return ExtendedKeyUsageOID.CLIENT_AUTH in eku
    except x509.ExtensionNotFound:
        # No EKU == any purpose. Treat as acceptable for clientAuth.
        return True


def _aware(ts: _dt.datetime) -> _dt.datetime:
    return ts if ts.tzinfo else ts.replace(tzinfo=_dt.timezone.utc)


if __name__ == "__main__":  # tiny smoke when run directly
    import ca_lib

    root, inter = ca_lib.bootstrap_hierarchy()
    leaf = ca_lib.issue_tenant_cert("smoke", inter)
    res = verify_chain(leaf.cert_pem(), ca_lib.chain_pem(inter, root))
    print("verify_chain:", res)
    raise SystemExit(0 if res.ok else 1)
