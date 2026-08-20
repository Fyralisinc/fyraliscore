"""Fail-closed regression test for ``lib.tenant.TenantRegistry.is_revoked``.

This pins the reconciliation called out in WS-AUTHPROXY: the two
fingerprint-status reader surfaces in the repo (``lib/tenant.py`` and
``ca/registry.py``) MUST agree, and both MUST be **fail-closed** — an *unknown*
fingerprint is treated as not-authorized (``is_revoked == True``), so a caller
doing ``if reg.is_revoked(fp): reject()`` can never let an unregistered cert
through (Invariant I4).

Run from the ``control-plane/`` root so ``lib`` imports as a package::

    cd control-plane && python -m pytest lib/test_tenant_failclosed.py -q
    # or
    cd control-plane && python -m lib.test_tenant_failclosed   # standalone runner
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Ensure THIS control-plane/ is the first place ``lib`` resolves from. The git
# worktree root has a different (empty) ``lib`` package that can otherwise shadow
# ours when pytest inserts the rootdir ahead of control-plane/. control-plane/ is
# the parent of this file's package dir.
_CONTROL_PLANE_ROOT = Path(__file__).resolve().parent.parent
if str(_CONTROL_PLANE_ROOT) not in sys.path:
    sys.path.insert(0, str(_CONTROL_PLANE_ROOT))
else:
    sys.path.remove(str(_CONTROL_PLANE_ROOT))
    sys.path.insert(0, str(_CONTROL_PLANE_ROOT))
# Drop any already-imported shadowed ``lib`` so the import below binds to ours.
if "lib" in sys.modules and getattr(
    sys.modules["lib"], "__file__", ""
) and not str(Path(sys.modules["lib"].__file__).resolve()).startswith(
    str(_CONTROL_PLANE_ROOT)
):
    for _m in [k for k in sys.modules if k == "lib" or k.startswith("lib.")]:
        del sys.modules[_m]

from lib import TenantRegistry  # noqa: E402
from lib.errors import (  # noqa: E402
    TenantNotFoundError,
    TenantRevokedError,
)

# 64-char lowercase-hex SHA-256 fingerprints (shape required by the registry).
_FP_ACTIVE = "a" * 64
_FP_REVOKED = "b" * 64
_FP_UNKNOWN = "c" * 64


def _registry(tmp_path: Path) -> TenantRegistry:
    sample = {
        _FP_ACTIVE: {
            "tenant_id": "acme",
            "issued_at": "2026-06-24T00:00:00Z",
            "status": "active",
        },
        _FP_REVOKED: {
            "tenant_id": "globex",
            "issued_at": "2026-06-24T00:00:00Z",
            "status": "revoked",
        },
    }
    reg_path = tmp_path / "tenant_registry.json"
    reg_path.write_text(json.dumps(sample, indent=2), encoding="utf-8")
    # cache=False so each call re-reads — matches the security-component stance.
    return TenantRegistry(registry_path=reg_path, cache=False)


def test_active_fingerprint_is_not_revoked(tmp_path: Path) -> None:
    reg = _registry(tmp_path)
    assert reg.is_revoked(_FP_ACTIVE) is False
    assert reg.is_active(_FP_ACTIVE) is True
    assert reg.tenant_for_fingerprint(_FP_ACTIVE) == "acme"


def test_revoked_fingerprint_is_revoked(tmp_path: Path) -> None:
    reg = _registry(tmp_path)
    assert reg.is_revoked(_FP_REVOKED) is True
    assert reg.is_active(_FP_REVOKED) is False


def test_unknown_fingerprint_is_revoked_fail_closed(tmp_path: Path) -> None:
    """THE reconciliation: an unknown fingerprint denies (fail-closed)."""
    reg = _registry(tmp_path)
    # The old behavior returned False here (fail-OPEN). It must now deny.
    assert reg.is_revoked(_FP_UNKNOWN) is True
    assert reg.is_active(_FP_UNKNOWN) is False


def test_reject_predicate_blocks_unknown(tmp_path: Path) -> None:
    """A caller using is_revoked as a gate rejects unknown certs."""
    reg = _registry(tmp_path)

    def admitted(fp: str) -> bool:
        # The canonical proxy-style gate.
        return not reg.is_revoked(fp)

    assert admitted(_FP_ACTIVE) is True
    assert admitted(_FP_REVOKED) is False
    assert admitted(_FP_UNKNOWN) is False  # the fix: unknown is NOT admitted


def test_matches_ca_registry_semantics(tmp_path: Path) -> None:
    """lib.tenant and ca.registry must give the SAME deny answer per fp.

    Skips gracefully if ca/registry isn't importable in this environment (it
    uses script-style imports), but asserts agreement when it is.
    """
    import sys

    ca_dir = Path(__file__).resolve().parent.parent / "ca"
    if str(ca_dir) not in sys.path:
        sys.path.insert(0, str(ca_dir))
    try:
        import registry as ca_registry  # noqa: WPS433
    except Exception:  # pragma: no cover
        import pytest

        pytest.skip("ca/registry not importable in this environment")

    sample = {
        _FP_ACTIVE: {
            "tenant_id": "acme",
            "issued_at": "2026-06-24T00:00:00Z",
            "status": "active",
        },
        _FP_REVOKED: {
            "tenant_id": "globex",
            "issued_at": "2026-06-24T00:00:00Z",
            "status": "revoked",
        },
    }
    reg_path = tmp_path / "tenant_registry.json"
    reg_path.write_text(json.dumps(sample), encoding="utf-8")
    lib_reg = TenantRegistry(registry_path=reg_path, cache=False)

    for fp in (_FP_ACTIVE, _FP_REVOKED, _FP_UNKNOWN):
        assert lib_reg.is_revoked(fp) == ca_registry.is_revoked(
            fp, path=str(reg_path)
        ), f"deny answer disagreed for {fp}"


def test_tenant_for_fingerprint_still_raises_unknown(tmp_path: Path) -> None:
    """The precise-reason surface is unchanged: unknown raises NotFound."""
    reg = _registry(tmp_path)
    try:
        reg.tenant_for_fingerprint(_FP_UNKNOWN)
    except TenantNotFoundError:
        pass
    else:  # pragma: no cover
        raise AssertionError("unknown fp should raise TenantNotFoundError")
    try:
        reg.tenant_for_fingerprint(_FP_REVOKED)
    except TenantRevokedError:
        pass
    else:  # pragma: no cover
        raise AssertionError("revoked fp should raise TenantRevokedError")


def _run_standalone() -> int:
    import tempfile as _tf

    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        with _tf.TemporaryDirectory() as d:
            try:
                fn(Path(d))
                print(f"  PASS {name}")
            except Exception as exc:  # noqa: BLE001
                failures += 1
                print(f"  FAIL {name}: {exc}")
    print("OK" if failures == 0 else f"{failures} FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(_run_standalone())
