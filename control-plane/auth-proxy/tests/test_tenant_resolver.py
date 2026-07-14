"""Unit tests for tenant_resolver — the fail-closed verify→extract→revoke core.

These exercise the security decision *without* any sockets, so they are fast and
deterministic. They build an in-memory CA hierarchy with ``ca/ca_lib`` and a tiny
JSON registry on /tmp, then assert that every rejection path (no cert, untrusted
chain, missing SAN, revoked, unknown, SAN↔registry mismatch) is denied and only a
fully-valid, registered, active cert resolves.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Put auth-proxy/ and ca/ on the path (mirrors how the proxy resolves imports).
_AUTH_DIR = Path(__file__).resolve().parent.parent
_CA_DIR = _AUTH_DIR.parent / "ca"
for p in (str(_AUTH_DIR), str(_CA_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

import ca_lib  # noqa: E402
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import serialization  # noqa: E402

from tenant_resolver import (  # noqa: E402
    REASON_CHAIN_INVALID,
    REASON_NO_CERT,
    REASON_REVOKED_OR_UNKNOWN,
    REASON_SAN_INVALID,
    REASON_SAN_REGISTRY_MISMATCH,
    ResolvedTenant,
    TenantResolutionError,
    TenantResolver,
)


@pytest.fixture(scope="module")
def hierarchy():
    root, intermediate = ca_lib.bootstrap_hierarchy()
    chain_pem = ca_lib.chain_pem(intermediate, root)
    return root, intermediate, chain_pem


def _registry_file(tmp_path: Path, rows: dict) -> Path:
    p = tmp_path / "tenant_registry.json"
    p.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    return p


def _der(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.DER)


def _resolver(chain_pem: bytes, reg_path: Path) -> TenantResolver:
    return TenantResolver(chain_pem, registry_path=reg_path)


def test_valid_active_cert_resolves(hierarchy, tmp_path):
    root, inter, chain = hierarchy
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    fp = leaf.fingerprint_sha256()
    reg = _registry_file(
        tmp_path,
        {fp: {"tenant_id": "acme", "issued_at": "2026-06-24T00:00:00Z", "status": "active"}},
    )
    out = _resolver(chain, reg).resolve(_der(leaf.cert))
    assert isinstance(out, ResolvedTenant)
    assert out.tenant_id == "acme"
    assert out.fingerprint == fp


def test_no_cert_is_rejected(hierarchy, tmp_path):
    _root, _inter, chain = hierarchy
    reg = _registry_file(tmp_path, {})
    with pytest.raises(TenantResolutionError) as ei:
        _resolver(chain, reg).resolve(None)
    assert ei.value.reason == REASON_NO_CERT


def test_revoked_cert_is_rejected(hierarchy, tmp_path):
    _root, inter, chain = hierarchy
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    fp = leaf.fingerprint_sha256()
    reg = _registry_file(
        tmp_path,
        {fp: {"tenant_id": "acme", "issued_at": "2026-06-24T00:00:00Z", "status": "revoked"}},
    )
    with pytest.raises(TenantResolutionError) as ei:
        _resolver(chain, reg).resolve(_der(leaf.cert))
    assert ei.value.reason == REASON_REVOKED_OR_UNKNOWN


def test_unknown_fingerprint_is_rejected(hierarchy, tmp_path):
    """A valid chain but no registry row ⇒ fail-closed reject (not 'allowed')."""
    _root, inter, chain = hierarchy
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    reg = _registry_file(tmp_path, {})  # empty registry
    with pytest.raises(TenantResolutionError) as ei:
        _resolver(chain, reg).resolve(_der(leaf.cert))
    assert ei.value.reason == REASON_REVOKED_OR_UNKNOWN


def test_untrusted_chain_is_rejected(hierarchy, tmp_path):
    """A leaf from a DIFFERENT CA must not verify against our chain."""
    _root, _inter, chain = hierarchy
    other_root, other_inter = ca_lib.bootstrap_hierarchy()
    rogue = ca_lib.issue_tenant_cert("acme", other_inter)
    fp = rogue.fingerprint_sha256()
    # Even if the attacker somehow got a registry row, the chain check fails.
    reg = _registry_file(
        tmp_path,
        {fp: {"tenant_id": "acme", "issued_at": "2026-06-24T00:00:00Z", "status": "active"}},
    )
    with pytest.raises(TenantResolutionError) as ei:
        _resolver(chain, reg).resolve(_der(rogue.cert))
    assert ei.value.reason == REASON_CHAIN_INVALID


def test_san_registry_mismatch_is_rejected(hierarchy, tmp_path):
    """Registry says 'globex' for a cert whose SAN says 'acme' ⇒ reject (C1)."""
    _root, inter, chain = hierarchy
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    fp = leaf.fingerprint_sha256()
    reg = _registry_file(
        tmp_path,
        {fp: {"tenant_id": "globex", "issued_at": "2026-06-24T00:00:00Z", "status": "active"}},
    )
    with pytest.raises(TenantResolutionError) as ei:
        _resolver(chain, reg).resolve(_der(leaf.cert))
    assert ei.value.reason == REASON_SAN_REGISTRY_MISMATCH


def test_cert_without_spiffe_san_is_rejected(hierarchy, tmp_path):
    """A leaf lacking the SPIFFE SAN is rejected (fail-closed).

    We forge a leaf with a DNS SAN instead of the SPIFFE URI, signed by our
    intermediate. Depending on the cryptography backend this is denied either at
    chain verification (the native ClientVerifier is strict about leaf SANs) or
    at our explicit SAN-extraction step — BOTH are valid fail-closed denials of a
    cert with no SPIFFE tenant identity. The security property is "denied", and
    we additionally assert the SAN gate itself rejects it directly below.
    """
    import datetime as dt

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    _root, inter, chain = hierarchy
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "no-san")]))
        .issuer_name(inter.cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("example.com")]), critical=False
        )
        .sign(private_key=inter.key, algorithm=hashes.SHA256())
    )
    fp = ca_lib.fingerprint_sha256(cert)
    reg = _registry_file(
        tmp_path,
        {fp: {"tenant_id": "acme", "issued_at": "2026-06-24T00:00:00Z", "status": "active"}},
    )
    with pytest.raises(TenantResolutionError) as ei:
        _resolver(chain, reg).resolve(_der(cert))
    # Either gate is a valid fail-closed denial of a no-SPIFFE-SAN cert.
    assert ei.value.reason in (REASON_SAN_INVALID, REASON_CHAIN_INVALID)


def test_san_extraction_gate_rejects_non_spiffe_directly(hierarchy):
    """Prove the SAN gate (step 2) itself rejects a non-SPIFFE leaf.

    We call ``ca_lib.extract_tenant_from_cert`` — the exact function the resolver
    uses at step 2 — on a DNS-SAN leaf to show the SAN gate is real and would
    deny even if chain verification let it through.
    """
    import datetime as dt

    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    _root, inter, _chain = hierarchy
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "no-san")]))
        .issuer_name(inter.cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("example.com")]), critical=False
        )
        .sign(private_key=inter.key, algorithm=hashes.SHA256())
    )
    with pytest.raises(ValueError):
        ca_lib.extract_tenant_from_cert(cert)


def test_unreadable_registry_fails_closed(hierarchy, tmp_path):
    """If the registry path is a directory (unreadable as JSON) ⇒ deny."""
    _root, inter, chain = hierarchy
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    bad_path = tmp_path / "is_a_dir"
    bad_path.mkdir()
    # ca/registry.is_revoked on a directory raises; resolver maps to deny, not 5xx.
    with pytest.raises(TenantResolutionError):
        _resolver(chain, bad_path).resolve(_der(leaf.cert))


def test_empty_ca_chain_refuses_to_start(tmp_path):
    """A resolver with no trust anchor must refuse construction (never fail-open)."""
    reg = _registry_file(tmp_path, {})
    with pytest.raises(ValueError):
        TenantResolver(b"", registry_path=reg)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
