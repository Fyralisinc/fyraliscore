"""Fyralis BYOC private CA — core library (local/testable path).

This module implements the *Python* trust-root path called out in
``SPRINT_PLAN.md`` §P1. It mints a Fyralis **root CA**, a **signing intermediate**,
and **per-tenant mTLS client certificates** whose identity lives in a URI
Subject Alternative Name following the C1 contract:

    spiffe://fyralis/tenant/<tenant_id>

The auth proxy (P2) terminates mTLS, verifies the leaf chains to this CA, and
extracts ``tenant_id`` **server-side from the verified SAN** — never from a
caller-supplied header (Invariant I4). This module provides the building blocks
the proxy and the tests rely on:

* :func:`generate_root_ca`       — self-signed root (long-lived, offline).
* :func:`generate_intermediate`  — CA-signing intermediate signed by the root.
* :func:`issue_tenant_cert`      — leaf client cert for a tenant, SPIFFE URI SAN.
* :func:`extract_tenant_from_cert` — read ``tenant_id`` back out of a leaf's SAN.
* :func:`fingerprint_sha256`     — lowercase-hex SHA-256 of the cert (DER), the
                                   key the revocation registry is keyed on (C1).

Design notes / why these choices
--------------------------------
* **Production path is step-ca** (see ``config/ca.json``). This Python path is
  the deterministic, dependency-light path used for local dev, CI, and the proxy
  test-suite — no external ``step`` binary required.
* The intermediate (not the root) signs leaves, so the root key can stay
  offline. Verification walks leaf → intermediate → root.
* Leaves are **clientAuth-only** EKU and carry a single URI SAN. The proxy keys
  revocation on the leaf's SHA-256 fingerprint, so we expose that helper here and
  reuse it everywhere (issuance, registry, revocation) to avoid digest drift.
* No private key ever leaves this process except where a CLI explicitly writes it
  to a gitignored ``keys/`` directory.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Optional, Tuple

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ec import EllipticCurvePrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

# ---------------------------------------------------------------------------
# Contract constants — these are part of the C1 contract; do not rename.
# ---------------------------------------------------------------------------

#: SPIFFE trust domain for Fyralis tenant identities.
SPIFFE_TRUST_DOMAIN = "fyralis"

#: URI SAN template. ``tenant_id`` is substituted verbatim.
SPIFFE_URI_TEMPLATE = "spiffe://" + SPIFFE_TRUST_DOMAIN + "/tenant/{tenant_id}"

#: Required SPIFFE URI prefix used when parsing a tenant id back out.
SPIFFE_URI_PREFIX = "spiffe://" + SPIFFE_TRUST_DOMAIN + "/tenant/"

# Subject Common Names for the CA hierarchy (informational; identity is the SAN).
ROOT_CN = "Fyralis Root CA"
INTERMEDIATE_CN = "Fyralis Intermediate CA"

# Default validity windows. Roots are long-lived and offline; leaves are short
# so a missed revocation has a bounded blast radius (CRL/OCSP are not wired —
# revocation is registry-lookup; see README caveats).
_ROOT_DAYS = 3650          # 10 years
_INTERMEDIATE_DAYS = 1825  # 5 years
_LEAF_DAYS = 90            # 90 days


# ---------------------------------------------------------------------------
# Small carrier types so callers get (cert, key) as a named pair, not a tuple
# they have to remember the order of.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CertKeyPair:
    """A certificate and the private key that owns its public key."""

    cert: x509.Certificate
    key: EllipticCurvePrivateKey

    # --- convenience PEM accessors (used by the CLIs) ----------------------
    def cert_pem(self) -> bytes:
        return cert_to_pem(self.cert)

    def key_pem(self, password: Optional[bytes] = None) -> bytes:
        return key_to_pem(self.key, password=password)

    def fingerprint_sha256(self) -> str:
        return fingerprint_sha256(self.cert)


# ---------------------------------------------------------------------------
# Time helpers — RFC-3339 UTC, no naive datetimes leaking into x509.
# ---------------------------------------------------------------------------

def _utcnow() -> _dt.datetime:
    """Timezone-aware UTC now (cryptography accepts aware datetimes)."""
    return _dt.datetime.now(_dt.timezone.utc)


def rfc3339(ts: Optional[_dt.datetime] = None) -> str:
    """Return ``ts`` (default: now) as an RFC-3339 UTC string with ``Z``.

    Matches the timestamp shape used in ``tenant_registry.json`` and the C4
    deployment record so producers/consumers agree on format.
    """
    ts = ts or _utcnow()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=_dt.timezone.utc)
    return ts.astimezone(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_ec_key() -> EllipticCurvePrivateKey:
    """Fresh P-256 key. EC keeps certs/keys small for the mTLS handshake."""
    return ec.generate_private_key(ec.SECP256R1())


# ---------------------------------------------------------------------------
# Root CA
# ---------------------------------------------------------------------------

def generate_root_ca(
    *,
    common_name: str = ROOT_CN,
    valid_days: int = _ROOT_DAYS,
    key: Optional[EllipticCurvePrivateKey] = None,
) -> CertKeyPair:
    """Create a self-signed Fyralis **root** CA.

    The root is the trust anchor: long-lived, kept offline, and used only to
    sign the intermediate. It is a CA with an unrestricted (within the chain)
    path but ``path_length=1`` so it can sign exactly one tier of CA below it.
    """
    key = key or _new_ec_key()
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])

    now = _utcnow()
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=5))  # small skew cushion
        .not_valid_after(now + _dt.timedelta(days=valid_days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=1), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
    )
    cert = builder.sign(private_key=key, algorithm=hashes.SHA256())
    return CertKeyPair(cert=cert, key=key)


# ---------------------------------------------------------------------------
# Intermediate CA
# ---------------------------------------------------------------------------

def generate_intermediate(
    root: CertKeyPair,
    *,
    common_name: str = INTERMEDIATE_CN,
    valid_days: int = _INTERMEDIATE_DAYS,
    key: Optional[EllipticCurvePrivateKey] = None,
) -> CertKeyPair:
    """Create an intermediate CA signed by ``root``.

    The intermediate is the *online* signer that issues tenant leaves, so the
    root key can stay offline. ``path_length=0`` means it can sign leaves but no
    further CAs. Authority Key Identifier ties it to the root's subject key id.
    """
    key = key or _new_ec_key()
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])

    now = _utcnow()
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(root.cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=5))
        .not_valid_after(now + _dt.timedelta(days=valid_days))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(root.key.public_key()),
            critical=False,
        )
    )
    # Signed by the ROOT key — this is what makes the chain verify.
    cert = builder.sign(private_key=root.key, algorithm=hashes.SHA256())
    return CertKeyPair(cert=cert, key=key)


# ---------------------------------------------------------------------------
# Tenant leaf (mTLS client cert)
# ---------------------------------------------------------------------------

def _validate_tenant_id(tenant_id: str) -> str:
    """Reject tenant ids that would corrupt the SPIFFE URI.

    A tenant id is embedded directly into the URI SAN that the proxy parses as
    the *sole* source of identity (I4). Anything that could change how the URI
    is parsed (whitespace, ``/``, control chars, empties) must be rejected at
    issuance time so the SAN round-trips exactly.
    """
    if not isinstance(tenant_id, str):
        raise TypeError("tenant_id must be a str")
    tid = tenant_id.strip()
    if not tid:
        raise ValueError("tenant_id must be non-empty")
    if tid != tenant_id:
        raise ValueError("tenant_id must not have leading/trailing whitespace")
    # Conservative allowlist: lowercase/upper alnum plus - and _ . No slashes,
    # no spaces, no scheme separators — keeps the URI unambiguous to parse back.
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    bad = sorted(set(tid) - allowed)
    if bad:
        raise ValueError(
            "tenant_id contains disallowed characters %r; allowed: [A-Za-z0-9._-]" % bad
        )
    return tid


def spiffe_uri_for(tenant_id: str) -> str:
    """Return the canonical SPIFFE URI for ``tenant_id`` (validated)."""
    return SPIFFE_URI_TEMPLATE.format(tenant_id=_validate_tenant_id(tenant_id))


def issue_tenant_cert(
    tenant_id: str,
    intermediate: CertKeyPair,
    *,
    valid_days: int = _LEAF_DAYS,
    key: Optional[EllipticCurvePrivateKey] = None,
) -> CertKeyPair:
    """Issue a per-tenant mTLS **client** certificate signed by ``intermediate``.

    The leaf:

    * carries the C1 identity as a URI SAN ``spiffe://fyralis/tenant/<tenant_id>``
      (the proxy reads ``tenant_id`` from this, server-side);
    * is **clientAuth-only** (it authenticates the data-plane agent *to* the
      control plane — it is never a server cert);
    * is *not* a CA (BasicConstraints ca=False);
    * is signed by the intermediate's key so it chains leaf→intermediate→root.
    """
    tid = _validate_tenant_id(tenant_id)
    key = key or _new_ec_key()
    uri = x509.UniformResourceIdentifier(SPIFFE_URI_TEMPLATE.format(tenant_id=tid))

    # Subject CN is informational only (browsers/tools display it); the SAN is
    # authoritative. We still set it so logs/inspection are human-friendly.
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "fyralis-tenant-" + tid)])

    now = _utcnow()
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(intermediate.cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - _dt.timedelta(minutes=5))
        .not_valid_after(now + _dt.timedelta(days=valid_days))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,  # EC client auth: no key encipherment
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .add_extension(x509.SubjectAlternativeName([uri]), critical=False)
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(
                intermediate.key.public_key()
            ),
            critical=False,
        )
    )
    cert = builder.sign(private_key=intermediate.key, algorithm=hashes.SHA256())
    return CertKeyPair(cert=cert, key=key)


# ---------------------------------------------------------------------------
# Tenant extraction — the server-side identity read (I4)
# ---------------------------------------------------------------------------

def extract_tenant_from_cert(cert_pem) -> str:
    """Read the ``tenant_id`` out of a leaf cert's SPIFFE URI SAN.

    Accepts a PEM ``bytes``/``str`` or an already-parsed
    :class:`x509.Certificate`. This is the function the auth proxy calls *after*
    chain verification to derive the tenant id server-side. It is intentionally
    strict: exactly one ``spiffe://fyralis/tenant/<id>`` URI must be present.

    Raises :class:`ValueError` if the SAN is missing, has no SPIFFE URI, has more
    than one, or the tenant component is empty.
    """
    cert = _coerce_cert(cert_pem)
    try:
        san = cert.extensions.get_extension_for_class(
            x509.SubjectAlternativeName
        ).value
    except x509.ExtensionNotFound:
        raise ValueError("certificate has no SubjectAlternativeName extension")

    uris = [
        u
        for u in san.get_values_for_type(x509.UniformResourceIdentifier)
        if u.startswith(SPIFFE_URI_PREFIX)
    ]
    if not uris:
        raise ValueError(
            "no SPIFFE tenant URI (%s...) found in SAN" % SPIFFE_URI_PREFIX
        )
    if len(uris) > 1:
        # Ambiguous identity — refuse rather than pick one.
        raise ValueError("multiple SPIFFE tenant URIs found in SAN: %r" % uris)

    tenant_id = uris[0][len(SPIFFE_URI_PREFIX):]
    if not tenant_id or "/" in tenant_id:
        raise ValueError("malformed SPIFFE tenant URI: %r" % uris[0])
    return tenant_id


# ---------------------------------------------------------------------------
# Fingerprinting — the revocation-registry key (C1)
# ---------------------------------------------------------------------------

def fingerprint_sha256(cert) -> str:
    """Lowercase-hex SHA-256 fingerprint of the cert's DER encoding.

    This is the exact value ``tenant_registry.json`` is keyed on and the value
    the proxy computes from the presented leaf to do its revocation lookup, so
    issuance, registry, and the proxy all agree. Accepts a PEM bytes/str or an
    :class:`x509.Certificate`.
    """
    cert = _coerce_cert(cert)
    return cert.fingerprint(hashes.SHA256()).hex()


# ---------------------------------------------------------------------------
# (De)serialization helpers
# ---------------------------------------------------------------------------

def cert_to_pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def key_to_pem(
    key: EllipticCurvePrivateKey, *, password: Optional[bytes] = None
) -> bytes:
    """PEM-encode a private key, optionally encrypted with ``password``."""
    enc = (
        serialization.BestAvailableEncryption(password)
        if password
        else serialization.NoEncryption()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=enc,
    )


def load_cert(pem) -> x509.Certificate:
    """Parse a single PEM certificate from bytes/str."""
    return _coerce_cert(pem)


def load_key(
    pem, *, password: Optional[bytes] = None
) -> EllipticCurvePrivateKey:
    """Parse a PEM private key from bytes/str."""
    data = pem.encode() if isinstance(pem, str) else pem
    key = serialization.load_pem_private_key(data, password=password)
    if not isinstance(key, EllipticCurvePrivateKey):
        raise ValueError("expected an EC private key")
    return key


def chain_pem(intermediate: CertKeyPair, root: CertKeyPair) -> bytes:
    """Return the CA chain PEM (intermediate first, then root).

    This is the bundle a TLS server presents / a verifier loads as the trust
    chain for leaf verification.
    """
    return cert_to_pem(intermediate.cert) + cert_to_pem(root.cert)


def _coerce_cert(value) -> x509.Certificate:
    if isinstance(value, x509.Certificate):
        return value
    data = value.encode() if isinstance(value, str) else value
    return x509.load_pem_x509_certificate(data)


# ---------------------------------------------------------------------------
# In-memory convenience: full hierarchy in one call (used by tests/self-test)
# ---------------------------------------------------------------------------

def bootstrap_hierarchy() -> Tuple[CertKeyPair, CertKeyPair]:
    """Generate ``(root, intermediate)`` in-memory. Convenience for tests."""
    root = generate_root_ca()
    intermediate = generate_intermediate(root)
    return root, intermediate
