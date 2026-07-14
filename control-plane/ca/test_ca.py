"""pytest suite for the Fyralis CA trust-root path (P1 exit gate).

Run from this directory:  pytest -q
These tests are import-only against a temp PKI/registry and leave no artifacts.
They also document the exact behaviors the auth proxy (P2) depends on, so the
proxy can import ``ca_lib``/``verify_chain``/``registry`` with confidence.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import bootstrap_ca  # noqa: E402
import ca_lib  # noqa: E402
import issue_cert  # noqa: E402
import registry  # noqa: E402
import revoke  # noqa: E402
import verify_chain  # noqa: E402


@pytest.fixture()
def ca(tmp_path):
    """A throwaway on-disk CA + registry path."""
    pki = str(tmp_path / "pki")
    reg = str(tmp_path / "tenant_registry.json")
    bootstrap_ca.bootstrap(pki, force=True, key_password=None)
    return {"pki": pki, "reg": reg, "chain": os.path.join(pki, "ca-chain.crt")}


# --- SAN / identity contract (C1) -----------------------------------------

def test_san_roundtrips_tenant_id():
    root, inter = ca_lib.bootstrap_hierarchy()
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    assert ca_lib.extract_tenant_from_cert(leaf.cert_pem()) == "acme"


def test_san_uri_is_spiffe_contract():
    root, inter = ca_lib.bootstrap_hierarchy()
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    from cryptography import x509

    san = leaf.cert.extensions.get_extension_for_class(
        x509.SubjectAlternativeName
    ).value
    uris = san.get_values_for_type(x509.UniformResourceIdentifier)
    assert uris == ["spiffe://fyralis/tenant/acme"]


def test_extract_rejects_cert_without_san():
    root, inter = ca_lib.bootstrap_hierarchy()
    # The intermediate has no SPIFFE SAN.
    with pytest.raises(ValueError):
        ca_lib.extract_tenant_from_cert(inter.cert_pem())


@pytest.mark.parametrize("bad", ["", " ", "a/b", "a b", "acme/../root", "tab\tid"])
def test_issue_rejects_bad_tenant_ids(bad):
    root, inter = ca_lib.bootstrap_hierarchy()
    with pytest.raises((ValueError, TypeError)):
        ca_lib.issue_tenant_cert(bad, inter)


# --- chain verification ----------------------------------------------------

def test_chain_verifies(ca):
    res = issue_cert.issue("acme", pki_dir=ca["pki"], registry_path=ca["reg"])
    leaf_pem = open(res["cert_path"], "rb").read()
    chain_pem = open(ca["chain"], "rb").read()
    assert verify_chain.verify_chain(leaf_pem, chain_pem).ok


def test_foreign_cert_does_not_verify(ca):
    chain_pem = open(ca["chain"], "rb").read()
    _, other_inter = ca_lib.bootstrap_hierarchy()
    foreign = ca_lib.issue_tenant_cert("evil", other_inter)
    assert not verify_chain.verify_chain(foreign.cert_pem(), chain_pem).ok


def test_manual_backend_matches_native(ca):
    """The fallback verifier agrees with the native one on accept/reject."""
    res = issue_cert.issue("acme", pki_dir=ca["pki"], registry_path=ca["reg"])
    leaf_pem = open(res["cert_path"], "rb").read()
    chain_pem = open(ca["chain"], "rb").read()
    _, other_inter = ca_lib.bootstrap_hierarchy()
    foreign = ca_lib.issue_tenant_cert("evil", other_inter)

    saved = verify_chain._HAVE_VERIFIER
    try:
        verify_chain._HAVE_VERIFIER = False
        assert verify_chain.verify_chain(leaf_pem, chain_pem).ok
        assert not verify_chain.verify_chain(foreign.cert_pem(), chain_pem).ok
    finally:
        verify_chain._HAVE_VERIFIER = saved


# --- fingerprint -----------------------------------------------------------

def test_fingerprint_is_lowercase_hex_sha256():
    root, inter = ca_lib.bootstrap_hierarchy()
    leaf = ca_lib.issue_tenant_cert("acme", inter)
    fp = ca_lib.fingerprint_sha256(leaf.cert)
    assert len(fp) == 64 and fp == fp.lower()
    # PEM and cert object agree.
    assert fp == ca_lib.fingerprint_sha256(leaf.cert_pem())


# --- registry + revocation (C1) -------------------------------------------

def test_registry_roundtrip_and_revoke(ca):
    res = issue_cert.issue("acme", pki_dir=ca["pki"], registry_path=ca["reg"])
    fp = res["fingerprint_sha256"]

    row = registry.get_entry(fp, path=ca["reg"])
    assert row["tenant_id"] == "acme"
    assert row["status"] == "active"
    assert not registry.is_revoked(fp, path=ca["reg"])

    revoke.revoke("acme", registry_path=ca["reg"])
    assert registry.is_revoked(fp, path=ca["reg"])
    assert revoke.is_revoked(fp, path=ca["reg"])


def test_revoke_by_fingerprint(ca):
    res = issue_cert.issue("beta", pki_dir=ca["pki"], registry_path=ca["reg"])
    fp = res["fingerprint_sha256"]
    revoke.revoke(fp, registry_path=ca["reg"])
    assert registry.is_revoked(fp, path=ca["reg"])


def test_unknown_fingerprint_is_fail_closed(ca):
    assert registry.is_revoked("0" * 64, path=ca["reg"]) is True


def test_revoke_all_certs_for_tenant(ca):
    r1 = issue_cert.issue("gamma", pki_dir=ca["pki"], registry_path=ca["reg"])
    r2 = issue_cert.issue("gamma", pki_dir=ca["pki"], registry_path=ca["reg"])
    # Two distinct certs (rotation) for the same tenant.
    assert r1["fingerprint_sha256"] != r2["fingerprint_sha256"]
    out = revoke.revoke("gamma", registry_path=ca["reg"])
    assert set(out["revoked"]) >= {r1["fingerprint_sha256"], r2["fingerprint_sha256"]}
    assert registry.is_revoked(r1["fingerprint_sha256"], path=ca["reg"])
    assert registry.is_revoked(r2["fingerprint_sha256"], path=ca["reg"])


# --- the proxy's authorization predicate (documented behavior) ------------

def test_proxy_authz_predicate(ca):
    """Models what the auth proxy does per request: verify chain, extract SAN,
    check the fingerprint+SAN agree and are active."""
    res = issue_cert.issue("acme", pki_dir=ca["pki"], registry_path=ca["reg"])
    leaf_pem = open(res["cert_path"], "rb").read()
    chain_pem = open(ca["chain"], "rb").read()

    def authorize(leaf_pem_bytes) -> str:
        if not verify_chain.verify_chain(leaf_pem_bytes, chain_pem).ok:
            raise PermissionError("chain")
        tenant = ca_lib.extract_tenant_from_cert(leaf_pem_bytes)
        fp = ca_lib.fingerprint_sha256(leaf_pem_bytes)
        if registry.is_revoked(fp, path=ca["reg"]):
            raise PermissionError("revoked/unknown")
        row = registry.get_entry(fp, path=ca["reg"])
        if row["tenant_id"] != tenant:
            raise PermissionError("san/registry mismatch")
        return tenant

    assert authorize(leaf_pem) == "acme"
    revoke.revoke("acme", registry_path=ca["reg"])
    with pytest.raises(PermissionError):
        authorize(leaf_pem)
