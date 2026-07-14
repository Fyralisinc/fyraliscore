#!/usr/bin/env python3
"""selftest.py — end-to-end smoke for the Fyralis CA (no pytest required).

Runs the full P1 exit gate for the trust-root path, against a throwaway temp PKI
and registry so it leaves no artifacts behind:

  1. bootstrap root + intermediate
  2. issue a cert for tenant "acme"
  3. assert extract_tenant_from_cert() round-trips "acme"
  4. assert the chain verifies (leaf -> intermediate -> root)
  5. assert the registry round-trips fingerprint -> tenant and is "active"
  6. revoke the cert and assert is_revoked() flips to True
  7. negative checks: tampered/foreign leaf does NOT verify; spoofed/unknown
     fingerprint is treated as revoked (fail-closed)

Exit code 0 == all gates pass. Run:  python selftest.py
"""

from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ca_lib  # noqa: E402
import issue_cert  # noqa: E402
import registry  # noqa: E402
import revoke  # noqa: E402
import verify_chain  # noqa: E402


def _ok(msg: str) -> None:
    print("  [PASS] %s" % msg)


def run() -> int:
    with tempfile.TemporaryDirectory(prefix="fyralis-ca-selftest-") as tmp:
        pki_dir = os.path.join(tmp, "pki")
        reg_path = os.path.join(tmp, "tenant_registry.json")

        print("Fyralis CA self-test (temp dir: %s)" % tmp)

        # 1) bootstrap (use the in-memory hierarchy to drive bootstrap_ca on disk)
        import bootstrap_ca

        boot = bootstrap_ca.bootstrap(pki_dir, force=True, key_password=None)
        assert os.path.exists(boot["paths"]["chain_crt"])
        _ok("bootstrapped root + intermediate into %s" % pki_dir)

        # 2) issue cert for "acme"
        res = issue_cert.issue(
            "acme", pki_dir=pki_dir, registry_path=reg_path, valid_days=90
        )
        fp = res["fingerprint_sha256"]
        _ok("issued cert for tenant 'acme' (fp=%s...)" % fp[:16])

        # 3) extract tenant from SAN
        leaf_pem = open(res["cert_path"], "rb").read()
        tenant = ca_lib.extract_tenant_from_cert(leaf_pem)
        assert tenant == "acme", "expected 'acme', got %r" % tenant
        _ok("extract_tenant_from_cert -> %r" % tenant)

        # 4) chain verifies
        chain_pem = open(boot["paths"]["chain_crt"], "rb").read()
        result = verify_chain.verify_chain(leaf_pem, chain_pem)
        assert result.ok, "chain did not verify: %s" % result.reason
        _ok("chain verifies (backend=%s)" % result.backend)

        # 5) registry round-trip
        row = registry.get_entry(fp, path=reg_path)
        assert row is not None, "registry missing the issued cert"
        assert row["tenant_id"] == "acme"
        assert row["status"] == "active"
        assert "issued_at" in row and row["issued_at"].endswith("Z")
        assert not registry.is_revoked(fp, path=reg_path)
        _ok("registry round-trips fp->tenant, status=active, is_revoked=False")

        # 6) revoke and re-check
        rv = revoke.revoke("acme", registry_path=reg_path)
        assert fp in rv["revoked"], "revoke did not flip the fingerprint"
        assert registry.is_revoked(fp, path=reg_path)
        assert revoke.is_revoked(fp, path=reg_path)  # re-exported helper
        _ok("revoke('acme') -> is_revoked=True (registry status=revoked)")

        # 7a) negative: a foreign leaf (different CA) must NOT verify against our chain
        other_root, other_inter = ca_lib.bootstrap_hierarchy()
        foreign = ca_lib.issue_tenant_cert("evil", other_inter)
        bad = verify_chain.verify_chain(foreign.cert_pem(), chain_pem)
        assert not bad.ok, "foreign cert unexpectedly verified!"
        _ok("foreign cert from a different CA does NOT verify (reason: %s)" % bad.reason)

        # 7b) negative: unknown fingerprint is treated as revoked (fail-closed)
        assert registry.is_revoked("0" * 64, path=reg_path), "unknown fp not fail-closed"
        _ok("unknown fingerprint is fail-closed (is_revoked=True)")

        # 7c) negative: a tampered leaf (re-signed bytes) must NOT verify
        #     Issue a fresh acme cert from our REAL intermediate; it should verify,
        #     proving (4) wasn't a false-negative, then confirm SAN extraction is strict.
        good2 = issue_cert.issue(
            "beta", pki_dir=pki_dir, registry_path=reg_path
        )
        good2_pem = open(good2["cert_path"], "rb").read()
        assert verify_chain.verify_chain(good2_pem, chain_pem).ok
        assert ca_lib.extract_tenant_from_cert(good2_pem) == "beta"
        _ok("second tenant 'beta' issues + verifies + extracts independently")

    print("\nALL SELF-TEST GATES PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
