"""auth-proxy configuration (C1 / C5).

Every knob the tenant auth proxy needs, resolved from the environment with safe
defaults. Nothing here is identity-bearing — identity comes *only* from the
verified client-cert SAN at request time (Invariant I4); this module just tells
the server which port to listen on, where the CA trust material lives, where the
revocation registry lives, and which upstream to reverse-proxy to.

Contract anchors
----------------
* C1 — CA chain path + revocation-registry path (the proxy verifies leaves
  against the chain and keys revocation on the registry).
* C5 — Mimir multi-tenancy is keyed by ``X-Scope-OrgID``; the upstream defaults
  to a Mimir address and the injected header name is fixed by contract.

Resolution order for every value: explicit constructor arg > environment
variable > built-in default. Paths default relative to the ``control-plane/``
root so the proxy works out of the box after ``ca/bootstrap_ca.py`` has run.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Repo-anchored default paths.
# auth-proxy/config.py  ->  control-plane/  is one level up.
# ---------------------------------------------------------------------------

_HERE = Path(__file__).resolve().parent
CONTROL_PLANE_ROOT = _HERE.parent

#: CA chain (intermediate + root) the proxy trusts for client-cert verification.
#: Produced by ``ca/bootstrap_ca.py`` (writes ``ca/pki/ca-chain.crt``).
DEFAULT_CA_CHAIN_PATH = CONTROL_PLANE_ROOT / "ca" / "pki" / "ca-chain.crt"

#: Revocation/identity registry (fingerprint -> {tenant_id, status}) — C1.
DEFAULT_TENANT_REGISTRY_PATH = CONTROL_PLANE_ROOT / "ca" / "tenant_registry.json"

#: The contract header that carries tenant scope to Mimir/Loki (C5). The proxy
#: STRIPS any client-supplied value and INJECTS the server-derived tenant id.
SCOPE_ORG_HEADER = "X-Scope-OrgID"

#: Default upstream is local Mimir's HTTP listener.
DEFAULT_UPSTREAM_URL = "http://mimir:9009"

#: Default listen address/port for the mTLS-terminating proxy.
DEFAULT_LISTEN_HOST = "0.0.0.0"
DEFAULT_LISTEN_PORT = 8443


def _env_str(name: str, default: str) -> str:
    val = os.environ.get(name)
    return val if val not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    val = os.environ.get(name)
    if val in (None, ""):
        return default
    try:
        return int(val)
    except ValueError as exc:
        raise ValueError(f"env {name}={val!r} is not an integer") from exc


def _env_path(name: str, default: Path) -> Path:
    val = os.environ.get(name)
    return Path(val) if val not in (None, "") else default


@dataclass(frozen=True)
class ProxyConfig:
    """Resolved, immutable proxy configuration.

    The proxy's TLS server cert/key are *separate* from the CA trust material:
    the proxy presents ``tls_cert_path``/``tls_key_path`` to clients and verifies
    clients against ``ca_chain_path``. In dev these may be derived from the same
    CA, but they are independent knobs.
    """

    listen_host: str = DEFAULT_LISTEN_HOST
    listen_port: int = DEFAULT_LISTEN_PORT

    # Trust material for verifying client (data-plane agent) certs — C1.
    ca_chain_path: Path = DEFAULT_CA_CHAIN_PATH
    tenant_registry_path: Path = DEFAULT_TENANT_REGISTRY_PATH

    # The proxy's own server identity (what it presents to clients).
    tls_cert_path: Optional[Path] = None
    tls_key_path: Optional[Path] = None

    # Reverse-proxy target — C5 (default: Mimir).
    upstream_url: str = DEFAULT_UPSTREAM_URL

    # The injected scope header name (contract-fixed; exposed for tests).
    scope_header: str = SCOPE_ORG_HEADER

    # Upstream request timeout (seconds).
    upstream_timeout_s: float = 30.0

    # When True the registry is re-read fresh on every request (no mtime cache),
    # so a revocation takes effect immediately. Defaults True for a security
    # component — correctness over the tiny stat() cost.
    registry_fresh_every_request: bool = True

    # Extra request-header name *prefixes* that are stripped before forwarding,
    # so no client can smuggle scope under a casing/variant trick. The canonical
    # scope header is always stripped; these add defense in depth.
    strip_header_prefixes: tuple[str, ...] = field(
        default_factory=lambda: ("x-scope-org",)
    )

    def __post_init__(self) -> None:
        # Coerce to Path (callers may pass str) without mutating frozen fields
        # in a surprising way — use object.__setattr__ on the frozen dataclass.
        for attr in ("ca_chain_path", "tenant_registry_path"):
            v = getattr(self, attr)
            if v is not None and not isinstance(v, Path):
                object.__setattr__(self, attr, Path(v))
        for attr in ("tls_cert_path", "tls_key_path"):
            v = getattr(self, attr)
            if v is not None and not isinstance(v, Path):
                object.__setattr__(self, attr, Path(v))

    # --- validation --------------------------------------------------------

    def require_files(self) -> None:
        """Raise if any required on-disk material is missing.

        Called at server start so a misconfigured proxy fails *loudly at boot*
        rather than 5xx-ing (or worse, fail-opening) per request.
        """
        missing = []
        if not self.ca_chain_path.is_file():
            missing.append(f"CA chain: {self.ca_chain_path}")
        # The registry file may legitimately be empty ({}), but it must exist —
        # an absent registry would otherwise be ambiguous with "no tenants".
        if not self.tenant_registry_path.exists():
            missing.append(f"tenant registry: {self.tenant_registry_path}")
        if self.tls_cert_path is None or not self.tls_cert_path.is_file():
            missing.append(f"TLS server cert: {self.tls_cert_path}")
        if self.tls_key_path is None or not self.tls_key_path.is_file():
            missing.append(f"TLS server key: {self.tls_key_path}")
        if missing:
            raise FileNotFoundError(
                "auth-proxy is missing required material:\n  - "
                + "\n  - ".join(missing)
            )


def load_config(**overrides) -> ProxyConfig:
    """Build a :class:`ProxyConfig` from the environment, then apply overrides.

    Recognized env vars:

    * ``AUTH_PROXY_LISTEN_HOST`` / ``AUTH_PROXY_LISTEN_PORT``
    * ``AUTH_PROXY_CA_CHAIN``       — path to the intermediate+root chain PEM
    * ``AUTH_PROXY_TENANT_REGISTRY``— path to ``tenant_registry.json``
    * ``AUTH_PROXY_TLS_CERT`` / ``AUTH_PROXY_TLS_KEY`` — the proxy's server cert
    * ``AUTH_PROXY_UPSTREAM_URL``   — reverse-proxy target (default Mimir)
    * ``AUTH_PROXY_UPSTREAM_TIMEOUT`` — upstream timeout seconds
    """
    base = dict(
        listen_host=_env_str("AUTH_PROXY_LISTEN_HOST", DEFAULT_LISTEN_HOST),
        listen_port=_env_int("AUTH_PROXY_LISTEN_PORT", DEFAULT_LISTEN_PORT),
        ca_chain_path=_env_path("AUTH_PROXY_CA_CHAIN", DEFAULT_CA_CHAIN_PATH),
        tenant_registry_path=_env_path(
            "AUTH_PROXY_TENANT_REGISTRY", DEFAULT_TENANT_REGISTRY_PATH
        ),
        upstream_url=_env_str("AUTH_PROXY_UPSTREAM_URL", DEFAULT_UPSTREAM_URL),
    )
    tls_cert = os.environ.get("AUTH_PROXY_TLS_CERT")
    tls_key = os.environ.get("AUTH_PROXY_TLS_KEY")
    if tls_cert:
        base["tls_cert_path"] = Path(tls_cert)
    if tls_key:
        base["tls_key_path"] = Path(tls_key)
    timeout = os.environ.get("AUTH_PROXY_UPSTREAM_TIMEOUT")
    if timeout:
        base["upstream_timeout_s"] = float(timeout)

    base.update(overrides)
    return ProxyConfig(**base)
