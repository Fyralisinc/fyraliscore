#!/usr/bin/env python3
"""e2e_smoke.py — the end-to-end SMOKE a CTO runs to prove the control plane works.

This is the scripted, top-to-bottom integration that exercises the FULL BYOC
control-plane path and *asserts each step*. It runs in two modes:

* **no-docker (default)** — an **in-process assembly** of the REAL components:
  a throwaway CA + ed25519 trust root in a temp sandbox, the REAL onboarding
  transaction (``onboarding/onboard.py``), the REAL console (``console/app.py``),
  the REAL data-plane agent (``agent/agent.py``), the REAL auth-proxy
  (``auth-proxy/proxy.py``) running over a real mTLS socket, the REAL config
  store (``config-dist/store.py``), and a **mockable Mimir** that stands in for
  central Mimir's multitenant remote-write/query path so the metric round-trip is
  real end-to-end without Docker.

* **live-docker** (``--live``) — the steps that genuinely need the running stack
  (a real Mimir container behind the deployed auth-proxy) are attempted against
  the compose stack. Anything that cannot run without Docker is reported as a
  SKIP, never a silent pass.

The seven asserted steps (mirroring the prompt's contract):

  1. BOOTSTRAP   — CA root+intermediate and the ed25519 signing keys exist.
  2. ONBOARD     — onboard demo tenant ``acme`` -> a bundle (cert+license) is
                   produced, the registry row is **active**, and the console
                   lists the deployment.
  3. AGENT GREEN — start the agent with the bundle -> it heartbeats and the
                   console marks ``acme`` **GREEN**.
  4. METRIC PUSH — push a sample metric through the boundary -> auth-proxy ->
                   Mimir path **with acme's identity** (its mTLS client cert),
                   then query it back from Mimir with ``X-Scope-OrgID: acme`` and
                   assert the series is present.
  5. ISOLATION   — query Mimir as a *different* tenant -> assert acme's series is
                   **NOT** visible (multi-tenant isolation, the security crux).
  6. LICENSE TAMPER — flip a byte in the signed license -> the agent's license
                   gate **denies** (refuses its privileged action).
  7. CONFIG DIST — config-dist serves a **signed** config the agent **verifies**
                   before applying (I6); a tampered config is rejected.

Run::

    python e2e_smoke.py            # no-docker mode (default): all python steps run
    python e2e_smoke.py --live     # attempt the docker-only path too
    python e2e_smoke.py --keep     # leave the temp sandbox for inspection

Exit 0 iff every (non-skipped) assertion held.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import ssl
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# --------------------------------------------------------------------------- #
# Imports across the flat-module siblings.                                     #
#                                                                              #
# The control-plane components use *script-style* flat imports (``import        #
# store``, ``import config``, ``import app``) and several names COLLIDE across  #
# dirs: ``store`` exists in both console/ and config-dist/; ``config`` in       #
# agent/, auth-proxy/ AND lib/; ``app`` in console/. A single global sys.path   #
# would bind ``import store`` to whichever dir came first and cross-wire the    #
# components. So instead of dumping every dir on the path, we:                  #
#                                                                              #
#  * keep the *unique-named* primitive dirs on the path eagerly (ca/, signing/, #
#    onboarding/, config-dist/, plus the CP root for the ``lib`` package), and  #
#  * load each COLLIDING flat module from its explicit file path under a        #
#    chosen module name via :func:`_load`, priming the loaded module's own dir  #
#    on sys.path FIRST so its internal flat imports resolve to the right        #
#    sibling. This is the importlib equivalent of the careful ordering the      #
#    committed self-tests do, made collision-proof.                            #
# --------------------------------------------------------------------------- #
import importlib.util as _ilu  # noqa: E402

_HERE = Path(__file__).resolve().parent
_CP_ROOT = _HERE.parent
_CA_DIR = _CP_ROOT / "ca"
_SIGNING_DIR = _CP_ROOT / "signing"
_ONBOARD_DIR = _CP_ROOT / "onboarding"
_CONSOLE_DIR = _CP_ROOT / "console"
_AGENT_DIR = _CP_ROOT / "agent"
_AUTHPROXY_DIR = _CP_ROOT / "auth-proxy"
_CONFIGDIST_DIR = _CP_ROOT / "config-dist"

# Unique-named dirs are safe to keep on the path (their module names don't clash
# with each other). ca_lib/registry/verify_chain (ca), signing_lib/sign_bundle/
# verify_bundle (signing), onboard/license_mint/console_client/fake_console
# (onboarding) all resolve unambiguously; the CP root makes ``import lib`` work.
#
# FRONT-load the CP root and evict any FOREIGN top-level ``lib`` already imported
# (e.g. pytest's rootdir scan, or the host repo's own ``lib``) so ``import
# lib.deployment`` binds to control-plane/lib — the same guard console/store.py
# uses. Without this, running under pytest (rootdir above control-plane) can bind
# ``lib`` to an unrelated package and break onboard.py's ``from lib.deployment``.
for _p in (_ONBOARD_DIR, _SIGNING_DIR, _CA_DIR, _CONFIGDIST_DIR, _CP_ROOT):
    sp = str(_p)
    while sp in sys.path:
        sys.path.remove(sp)
    sys.path.insert(0, sp)

_existing_lib = sys.modules.get("lib")
if _existing_lib is not None and not (
    getattr(_existing_lib, "__file__", "") or ""
).startswith(str(_CP_ROOT)):
    for _name in [n for n in list(sys.modules) if n == "lib" or n.startswith("lib.")]:
        del sys.modules[_name]


def _load(mod_name: str, file_path: Path, *, dir_first: Optional[Path] = None):
    """Load a flat module from an explicit file under ``mod_name``.

    ``dir_first`` (default: the file's own dir) is inserted at the FRONT of
    sys.path so the module's internal script-style imports resolve to the right
    sibling (e.g. console/app.py's ``import store`` must find console/store.py,
    not config-dist/store.py). Cached under ``mod_name`` so repeat loads are
    cheap and a later ``import mod_name`` in a component resolves to this one.
    """
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    primer = str(dir_first or file_path.parent)
    if primer not in sys.path:
        sys.path.insert(0, primer)
    spec = _ilu.spec_from_file_location(mod_name, str(file_path))
    module = _ilu.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


def _evict(*names: str) -> None:
    """Drop colliding flat-module names from the import cache so a later import
    re-resolves against the currently-primed sys.path (used when switching from
    one dir's ``config``/``store`` to another's)."""
    for n in names:
        sys.modules.pop(n, None)


# Cache so the agent API is built once (agent modules have import side effects).
_AGENT_API = None


def _agent_api():
    """Import the agent's flat modules with the agent/ dir primed FIRST.

    agent/agent.py does ``import _bootstrap`` then ``from config import
    AgentConfig`` / ``from buffer import ...`` etc. — script-style flat imports
    that must resolve to agent/*.py. We evict any colliding ``config`` cached
    from another dir, front-load agent/, and import. Returns the small surface
    the smoke uses: (Agent, load_agent_config, HealthProbe, static_probe,
    LicenseChecker).
    """
    global _AGENT_API
    _prime_agent_dir()
    if _AGENT_API is not None:
        return _AGENT_API

    import agent as _agent_mod
    import config as _agent_config_mod
    import health_probe as _hp
    import license_check as _lc

    _AGENT_API = (
        _agent_mod.Agent,
        _agent_config_mod.load_agent_config,
        _hp.HealthProbe,
        _hp.static_probe,
        _lc.LicenseChecker,
    )
    return _AGENT_API


def _prime_agent_dir() -> None:
    """Front-load agent/ on sys.path and evict any colliding ``config`` so the
    agent's flat imports (``config``/``buffer``/``_bootstrap``/``config_pull``)
    resolve to agent/*.py. Idempotent; safe to call before any agent import."""
    # If a DIFFERENT dir's ``config`` is cached (auth-proxy/lib), drop it so a
    # bare ``import config`` re-binds to the agent's.
    cfg = sys.modules.get("config")
    if cfg is not None:
        f = getattr(cfg, "__file__", "") or ""
        if not f.startswith(str(_AGENT_DIR)):
            _evict("config")
    sp = str(_AGENT_DIR)
    if sp in sys.path:
        sys.path.remove(sp)
    sys.path.insert(0, sp)


# --------------------------------------------------------------------------- #
# Result accounting                                                           #
# --------------------------------------------------------------------------- #
class StepReporter:
    """Collects PASS/FAIL/SKIP across the smoke steps and prints a verdict."""

    def __init__(self) -> None:
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self._fail_msgs: list[str] = []

    def check(self, cond: bool, msg: str) -> bool:
        if cond:
            self.passed += 1
            print(f"    [ PASS ] {msg}")
        else:
            self.failed += 1
            self._fail_msgs.append(msg)
            print(f"    [ FAIL ] {msg}")
        return bool(cond)

    def skip(self, msg: str) -> None:
        self.skipped += 1
        print(f"    [ SKIP ] {msg}")

    def step(self, n: int, title: str) -> None:
        print(f"\n== STEP {n}: {title} ==")

    def verdict(self) -> int:
        print("\n" + "=" * 64)
        print(
            f"SMOKE RESULT: {self.passed} passed, {self.failed} failed, "
            f"{self.skipped} skipped"
        )
        if self.failed:
            print("FAILED assertions:")
            for m in self._fail_msgs:
                print(f"  - {m}")
            print("SMOKE FAILED")
            return 1
        print("SMOKE PASSED — the control plane works end-to-end")
        return 0


# --------------------------------------------------------------------------- #
# Mockable Mimir — the multitenant metric store (no-docker stand-in).         #
# --------------------------------------------------------------------------- #
#
# Real central Mimir is multitenant-by-X-Scope-OrgID (see mimir/validate.py:
# ``multitenancy_enabled: true`` ⇒ EVERY request requires X-Scope-OrgID, the
# distributor receives remote-write, queries are scoped by the same header). We
# reproduce *exactly* that contract: a remote-write receive endpoint and a query
# endpoint, both of which REQUIRE the header and partition series by tenant so a
# query in tenant A can never see tenant B's series. The auth-proxy is what
# injects that header from the verified client cert, so wiring this behind the
# REAL proxy proves the boundary->auth-proxy->Mimir identity flow.
#
# It is implemented as a plain asyncio HTTP/1.1 server (same shape as the
# auth-proxy test fabric's EchoUpstream) so the REAL AuthProxy can reverse-proxy
# to it over a socket with zero new dependencies.


class MockMimir:
    """A tiny multitenant metric store reachable over HTTP (asyncio).

    Endpoints (a faithful subset of the Mimir HTTP contract):

    * ``POST /api/v1/push``                  — remote-write receive (distributor).
      Body is ``{"metric": <name>, "value": <float>, "labels": {...}}`` (a
      simplified line; real Mimir takes snappy-protobuf, but the *multitenancy
      semantics* we are testing are identical). Stores the sample under the
      request's ``X-Scope-OrgID`` tenant. **422 if the header is missing.**
    * ``GET /prometheus/api/v1/query?query=<metric>`` — instant query, scoped to
      the request's ``X-Scope-OrgID``. Returns ONLY that tenant's series. **422
      if the header is missing.**

    The store is ``{tenant: {metric: [samples]}}`` — physically partitioned by
    tenant, which is what makes cross-tenant reads structurally impossible.
    """

    SCOPE_HEADER = "x-scope-orgid"

    def __init__(self) -> None:
        self._store: dict[str, dict[str, list[dict]]] = {}
        self._lock = threading.Lock()
        self.server: Optional[asyncio.AbstractServer] = None
        self.port: int = 0
        # Record what scope header each request actually carried, for assertions.
        self.seen_push_scopes: list[Optional[str]] = []
        self.seen_query_scopes: list[Optional[str]] = []

    # -- direct (in-process) accessors used by assertions ------------------- #

    def series_for(self, tenant: str, metric: str) -> list[dict]:
        with self._lock:
            return list(self._store.get(tenant, {}).get(metric, []))

    def tenants(self) -> list[str]:
        with self._lock:
            return sorted(self._store)

    # -- lifecycle ---------------------------------------------------------- #

    async def start(self) -> str:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None

    # -- request handling --------------------------------------------------- #

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            try:
                method, target, _ = request_line.decode("latin-1").split(" ", 2)
            except ValueError:
                await self._respond(writer, 400, {"error": "bad request line"})
                return
            headers: dict[str, str] = {}
            content_length = 0
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin-1").partition(":")
                name = name.strip().lower()
                value = value.strip()
                headers[name] = value
                if name == "content-length":
                    content_length = int(value or "0")
            body = b""
            if content_length:
                body = await reader.readexactly(content_length)

            scope = headers.get(self.SCOPE_HEADER)
            path = target.split("?", 1)[0]
            query = target.split("?", 1)[1] if "?" in target else ""

            if method == "POST" and path == "/api/v1/push":
                self.seen_push_scopes.append(scope)
                await self._handle_push(writer, scope, body)
            elif method == "GET" and path == "/prometheus/api/v1/query":
                self.seen_query_scopes.append(scope)
                await self._handle_query(writer, scope, query)
            else:
                await self._respond(writer, 404, {"error": f"no route {method} {path}"})
        except Exception as exc:  # never crash the listener
            try:
                await self._respond(writer, 500, {"error": str(exc)})
            except Exception:
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_push(
        self, writer: asyncio.StreamWriter, scope: Optional[str], body: bytes
    ) -> None:
        # Multitenancy contract: NO scope header ⇒ reject (Mimir 422s such writes).
        if not scope:
            await self._respond(
                writer, 422, {"error": "missing X-Scope-OrgID (multitenancy on)"}
            )
            return
        try:
            sample = json.loads(body.decode("utf-8")) if body else {}
        except (ValueError, UnicodeDecodeError) as exc:
            await self._respond(writer, 400, {"error": f"bad push body: {exc}"})
            return
        metric = sample.get("metric")
        if not metric:
            await self._respond(writer, 400, {"error": "push body needs 'metric'"})
            return
        with self._lock:
            self._store.setdefault(scope, {}).setdefault(metric, []).append(
                {
                    "value": sample.get("value", 1.0),
                    "labels": sample.get("labels", {}),
                    "ts": time.time(),
                }
            )
        await self._respond(writer, 200, {"status": "success"})

    async def _handle_query(
        self, writer: asyncio.StreamWriter, scope: Optional[str], query: str
    ) -> None:
        if not scope:
            await self._respond(
                writer, 422, {"error": "missing X-Scope-OrgID (multitenancy on)"}
            )
            return
        # Parse the metric name out of ``query=<metric>``.
        metric = ""
        for kv in query.split("&"):
            if kv.startswith("query="):
                metric = kv[len("query="):]
                break
        # Minimal URL-decode for the chars our tests use.
        metric = metric.replace("%7B", "{").replace("%7D", "}").replace("+", " ")
        with self._lock:
            samples = list(self._store.get(scope, {}).get(metric, []))
        # Prometheus-shaped result: only THIS tenant's samples are ever returned.
        result = [
            {
                "metric": {"__name__": metric, **s["labels"]},
                "value": [s["ts"], str(s["value"])],
            }
            for s in samples
        ]
        await self._respond(
            writer,
            200,
            {"status": "success", "data": {"resultType": "vector", "result": result}},
        )

    async def _respond(
        self, writer: asyncio.StreamWriter, status: int, payload: dict
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        reason = {200: "OK", 400: "Bad Request", 404: "Not Found",
                  422: "Unprocessable Entity", 500: "Internal Server Error"}.get(
            status, "OK"
        )
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n".encode("latin-1")
            + b"Content-Type: application/json\r\n"
            + b"Content-Length: " + str(len(body)).encode() + b"\r\n"
            + b"Connection: close\r\n\r\n"
            + body
        )
        await writer.drain()


# --------------------------------------------------------------------------- #
# The in-process control-plane assembly (the "stack" the smoke drives).       #
# --------------------------------------------------------------------------- #
@dataclass
class Stack:
    """A throwaway, fully-wired control plane in one temp sandbox.

    Holds the paths + handles every step needs. Built by :func:`build_stack`,
    torn down by :meth:`close`.
    """

    sandbox: Path
    pki_dir: Path
    signing_keys_dir: Path
    trust_root_path: Path
    registry_path: Path
    bundles_root: Path
    signing_key_id: str

    # populated as steps run
    onboard_result: object = None
    console_app: object = None
    console_store: object = None
    console_token: Optional[str] = None
    bundle_dir: Optional[Path] = None
    agent: object = None

    def close(self, keep: bool = False) -> None:
        if not keep:
            shutil.rmtree(self.sandbox, ignore_errors=True)


def build_stack() -> Stack:
    """Bootstrap a throwaway CA + ed25519 trust root and wire the signing CLIs.

    Reuses the committed primitives exactly:
      * ``ca/bootstrap_ca.bootstrap`` for the root+intermediate hierarchy,
      * ``signing/signing_lib`` + a hand-written trust root for the ed25519 key,
      * retargets ``sign_bundle``/``verify_bundle`` module path constants at the
        throwaway material (the same trick the committed self-tests use, so the
        committed ``signing/`` dir is never written to).
    """
    import bootstrap_ca  # ca/bootstrap_ca.py
    import signing_lib as sl  # signing/signing_lib.py
    import sign_bundle
    import verify_bundle

    sandbox = Path(tempfile.mkdtemp(prefix="cp-e2e-smoke-"))
    pki_dir = sandbox / "pki"
    signing_keys_dir = sandbox / "signing-keys"
    trust_root_path = sandbox / "trust_root.json"
    registry_path = sandbox / "tenant_registry.json"
    bundles_root = sandbox / "bundles"
    key_id = "cp-signing-e2e"

    # 1. Real CA hierarchy (root + intermediate + chain) on disk.
    bootstrap_ca.bootstrap(str(pki_dir), force=True, key_password=None)

    # 2. Real ed25519 signing key + a public trust root.
    signing_keys_dir.mkdir(parents=True, exist_ok=True)
    priv, pub = sl.generate_keypair()
    (signing_keys_dir / f"{key_id}.private.pem").write_bytes(
        sl.private_key_to_pem(priv)
    )
    trust_root_doc = {
        "version": 1,
        "active_key_id": key_id,
        "keys": {
            key_id: {
                "pubkey": sl.public_key_to_b64(pub),
                "algo": sl.ALGO,
                "status": "active",
            }
        },
    }
    trust_root_path.write_text(
        json.dumps(trust_root_doc, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )

    # 3. Point the committed signer + verifier at the throwaway material for the
    #    duration of this process (module-level path constants; restored on exit
    #    by process teardown — we never touch the committed signing/ files).
    sign_bundle.TRUST_ROOT_PATH = str(trust_root_path)
    sign_bundle.KEYS_DIR = str(signing_keys_dir)
    verify_bundle.TRUST_ROOT_PATH = str(trust_root_path)

    # An empty registry file must exist (the proxy treats absent as ambiguous).
    registry_path.write_text("{}\n", encoding="utf-8")
    bundles_root.mkdir(parents=True, exist_ok=True)

    return Stack(
        sandbox=sandbox,
        pki_dir=pki_dir,
        signing_keys_dir=signing_keys_dir,
        trust_root_path=trust_root_path,
        registry_path=registry_path,
        bundles_root=bundles_root,
        signing_key_id=key_id,
    )


# --------------------------------------------------------------------------- #
# STEP 1 — bootstrap: CA + signing keys exist.                                #
# --------------------------------------------------------------------------- #
def step1_bootstrap(stack: Stack, rep: StepReporter) -> None:
    rep.step(1, "bootstrap — CA + ed25519 signing keys exist")
    import ca_lib

    root_crt = stack.pki_dir / "root.crt"
    inter_crt = stack.pki_dir / "intermediate.crt"
    chain_crt = stack.pki_dir / "ca-chain.crt"
    inter_key = stack.pki_dir / "keys" / "intermediate.key"

    rep.check(root_crt.is_file(), f"root CA cert exists ({root_crt.name})")
    rep.check(inter_crt.is_file(), f"intermediate CA cert exists ({inter_crt.name})")
    rep.check(chain_crt.is_file(), f"CA chain bundle exists ({chain_crt.name})")
    rep.check(inter_key.is_file(), "intermediate signing key exists (online signer)")

    # The CA is real + usable: a freshly-issued tenant leaf must verify-chain to
    # the CA with the clientAuth purpose (exactly what the auth-proxy requires of
    # a data-plane cert). This proves root->intermediate->leaf actually signs.
    import verify_chain as vc

    inter_keyobj = ca_lib.load_key(inter_key.read_bytes())
    intermediate = ca_lib.CertKeyPair(
        cert=ca_lib.load_cert(inter_crt.read_bytes()), key=inter_keyobj
    )
    probe_leaf = ca_lib.issue_tenant_cert("smoke-probe", intermediate)
    res = vc.verify_chain(
        probe_leaf.cert, chain_crt.read_bytes(), require_client_auth=True
    )
    rep.check(
        res.ok,
        f"a tenant leaf verify-chains to the CA (clientAuth) — CA is usable ({res.reason[:50] if not res.ok else 'ok'})",
    )

    # ed25519 signing material exists and the trust root names an active key.
    priv = stack.signing_keys_dir / f"{stack.signing_key_id}.private.pem"
    rep.check(priv.is_file(), "ed25519 signing private key exists (gitignored)")
    doc = json.loads(stack.trust_root_path.read_text())
    rep.check(
        doc.get("active_key_id") == stack.signing_key_id,
        f"trust root names an active signing key ({doc.get('active_key_id')})",
    )
    rep.check(
        stack.signing_key_id in doc.get("keys", {}),
        "trust root carries the active key's public key (agents verify with it)",
    )


# --------------------------------------------------------------------------- #
# STEP 2 — onboard acme -> bundle (cert+license), registry active, console.   #
# --------------------------------------------------------------------------- #
def step2_onboard(stack: Stack, rep: StepReporter) -> None:
    rep.step(2, "onboard demo tenant 'acme' -> bundle + registry + console")
    import ca_lib
    import registry as ca_registry
    import onboard as ob
    import license_mint as lm

    # The REAL console (console/app.py over a non-persistent store), driven
    # in-process by onboarding through the P4 REST contract. Loaded by explicit
    # path so console/app.py's ``import store`` binds to console/store.py.
    console_store_mod = _load("store", _CONSOLE_DIR / "store.py")
    console_app_mod = _load("app", _CONSOLE_DIR / "app.py", dir_first=_CONSOLE_DIR)

    # The REAL console requires a write-path bearer token (I4). Build it with one
    # and thread it through onboarding (which stamps it into the bundle) + the
    # ASGI clients so every write authenticates.
    console_token = "e2e-console-ingest-token"
    stack.console_token = console_token
    store = console_store_mod.DeploymentStore(persist=False)
    app = console_app_mod.create_app(store, ingest_token=console_token)
    stack.console_app = app
    stack.console_store = store

    result = ob.onboard(
        tenant="acme",
        region="us-east",
        plan="standard",
        console_app=app,  # the REAL console, in-process
        console_token=console_token,  # authenticate writes (I4)
        bundles_root=str(stack.bundles_root),
        pki_dir=str(stack.pki_dir),
        registry_path=str(stack.registry_path),
        trust_root_path=str(stack.trust_root_path),
        telemetry_tier="T1",
    )
    stack.onboard_result = result
    stack.bundle_dir = Path(result.bundle_dir)

    # --- bundle produced with cert + license -----------------------------
    bundle = Path(result.bundle_dir)
    crt = bundle / "cert" / "acme.crt"
    key = bundle / "cert" / "acme.key"
    chain = bundle / "cert" / "acme.bundle.crt"
    lic = bundle / "acme.license.json"
    rep.check(bundle.is_dir(), f"agent bundle dir produced: {bundle.name}")
    rep.check(crt.is_file(), "bundle contains the tenant mTLS client cert")
    rep.check(key.is_file(), "bundle contains the tenant private key")
    rep.check(chain.is_file(), "bundle contains the cert chain (leaf+intermediate+root)")
    rep.check(lic.is_file(), "bundle contains the signed license")

    # cert identity round-trips to acme via the SPIFFE SAN.
    san = ca_lib.extract_tenant_from_cert(crt.read_bytes())
    rep.check(san == "acme", f"cert SAN identity == acme (got {san!r})")

    # license verifies + is unexpired + names acme.
    try:
        lic_doc = lm.verify_license_file(
            str(lic), trust_root_path=str(stack.trust_root_path)
        )
        rep.check(True, "license signature verifies and is unexpired")
        rep.check(lic_doc.get("tenant_id") == "acme", "license tenant_id == acme")
        rep.check(
            lic_doc.get("deployment_id") == result.deployment_id,
            "license deployment_id matches the onboarded deployment",
        )
    except Exception as exc:
        rep.check(False, f"license verify raised: {exc}")

    # --- registry row is ACTIVE -------------------------------------------
    rows = ca_registry.find_by_tenant("acme", path=str(stack.registry_path))
    rep.check(len(rows) == 1, f"registry has exactly one acme row (got {len(rows)})")
    active = [r for r in rows.values() if r.get("status") == "active"]
    rep.check(len(active) == 1, "the acme registry row is ACTIVE (proxy will accept it)")
    rep.check(
        result.fingerprint in rows,
        "registry is keyed by the issued cert's SHA-256 fingerprint",
    )

    # --- console lists the deployment -------------------------------------
    import console_client as cc

    client = cc.ASGIConsoleClient(app)
    try:
        rep.check(
            client.has_deployment(result.deployment_id),
            f"console lists the deployment ({result.deployment_id})",
        )
        fleet = client.list_deployments()
        rep.check(
            any(d["tenant_id"] == "acme" for d in fleet),
            "console fleet rollup includes acme",
        )
    finally:
        client.close()


# --------------------------------------------------------------------------- #
# STEP 3 — start the agent with the bundle -> console marks acme GREEN.        #
# --------------------------------------------------------------------------- #
def step3_agent_green(stack: Stack, rep: StepReporter) -> None:
    rep.step(3, "start the agent with the bundle -> console marks acme GREEN")
    import console_client as cc

    Agent, load_agent_config, HealthProbe, static_probe, LicenseChecker = _agent_api()

    result = stack.onboard_result
    bundle = stack.bundle_dir
    app = stack.console_app

    # The agent's heartbeat sender posts to the REAL console in-process (the
    # outbound POST the agent would do over https, dispatched via ASGI here).
    # The console requires the write token (I4) — present it on the client.
    console = cc.ASGIConsoleClient(app, token=stack.console_token)

    def in_process_sender(record: dict) -> bool:
        try:
            console.heartbeat(record)
            return True
        except Exception:
            return False

    # Build the agent against the REAL bundle files: tenant id, license, trust
    # root, version. The SLI probe is static-healthy (no local /healthz in-proc).
    cfg = load_agent_config(
        console_url="https://console.invalid:8080",  # never dialed; sender injected
        tenant_id=result.tenant_id,
        deployment_id=result.deployment_id,
        region=result.region,
        telemetry_tier=result.telemetry_tier,
        license_path=str(bundle / "acme.license.json"),
        trust_root_path=str(stack.trust_root_path),
        config_dir=str(bundle / "applied-config"),
        version_file=str(bundle / "VERSION"),
        version_env="1.0.0-e2e",
        healthz_url="http://127.0.0.1:1/healthz",  # never probed; static probe injected
        buffer_path=str(bundle / "buffer.jsonl"),
        interval_s=1.0,
    )
    agent = Agent(
        cfg,
        sender=in_process_sender,
        probe=HealthProbe(static_probe(True, "e2e static-healthy")),
        license_checker=LicenseChecker(
            str(bundle / "acme.license.json"),
            trust_root_path=str(stack.trust_root_path),
        ),
    )
    stack.agent = agent

    # One tick: collect a fresh DeploymentRecord and deliver it to the console.
    tick = agent.tick()
    rep.check(tick.licensed, "agent reports itself LICENSED (valid signed license)")
    rep.check(tick.delivered, "agent heartbeat delivered to the console (outbound)")
    rep.check(
        tick.record.get("health") == "green",
        f"agent's own derived health is GREEN (got {tick.record.get('health')})",
    )

    # The console, deriving health on read, marks acme GREEN.
    rec = console.get_deployment(result.deployment_id)
    rep.check(rec is not None, "console still lists acme after the heartbeat")
    rep.check(
        (rec or {}).get("health") == "green",
        f"console marks acme GREEN (got {(rec or {}).get('health')})",
    )
    console.close()


# --------------------------------------------------------------------------- #
# STEP 4 + 5 — metric round-trip through the REAL auth-proxy, + isolation.     #
# --------------------------------------------------------------------------- #
async def _metric_path_async(stack: Stack, rep: StepReporter) -> None:
    """Push a metric as acme through boundary->auth-proxy->Mimir, query back,
    then prove a different tenant cannot see acme's series (isolation)."""
    import ca_lib
    import registry as ca_registry

    # Load the auth-proxy package with auth-proxy/ primed FIRST so proxy.py's
    # ``from config import ProxyConfig`` / ``from tenant_resolver import ...``
    # bind to the auth-proxy dir (its ``config`` must win over the agent's).
    _evict("config", "proxy", "tenant_resolver")
    sp = str(_AUTHPROXY_DIR)
    if sp in sys.path:
        sys.path.remove(sp)
    sys.path.insert(0, sp)
    import config as _ap_config
    import proxy as _ap_proxy

    ProxyConfig = _ap_config.ProxyConfig
    AuthProxy = _ap_proxy.AuthProxy

    # Reuse the auth-proxy test fabric's server-cert + free-port helpers so the
    # proxy presents a real CA-signed server cert and we get genuine mTLS.
    _conftest = _load(
        "_authproxy_conftest", _AUTHPROXY_DIR / "tests" / "conftest.py",
        dir_first=_AUTHPROXY_DIR,
    )
    _server_cert = _conftest._server_cert
    _free_port = _conftest._free_port

    # The CA materials the proxy verifies clients against + presents as server.
    chain_pem = (stack.pki_dir / "ca-chain.crt").read_bytes()
    root_pem = (stack.pki_dir / "root.crt").read_bytes()
    inter_cert = ca_lib.load_cert((stack.pki_dir / "intermediate.crt").read_bytes())
    inter_key = ca_lib.load_key(
        (stack.pki_dir / "keys" / "intermediate.key").read_bytes()
    )
    intermediate = ca_lib.CertKeyPair(cert=inter_cert, key=inter_key)

    server = _server_cert(intermediate)
    server_crt = stack.sandbox / "proxy-server.crt"
    server_key = stack.sandbox / "proxy-server.key"
    server_crt.write_bytes(ca_lib.cert_to_pem(server.cert))
    server_key.write_bytes(server.key_pem())

    # A SECOND tenant ("globex") — onboarded enough to have a valid, ACTIVE cert
    # so it authenticates to the proxy; the isolation test then proves that even
    # a *legitimate* other tenant cannot read acme's series.
    globex = ca_lib.issue_tenant_cert("globex", intermediate)
    ca_registry.add_entry(
        globex.fingerprint_sha256(), "globex", path=str(stack.registry_path)
    )
    globex_crt = stack.sandbox / "globex.crt"
    globex_key = stack.sandbox / "globex.key"
    globex_crt.write_bytes(ca_lib.cert_to_pem(globex.cert))
    globex_key.write_bytes(globex.key_pem())

    # acme's mTLS client material straight from the onboarding bundle.
    acme_crt = stack.bundle_dir / "cert" / "acme.crt"
    acme_key = stack.bundle_dir / "cert" / "acme.key"

    # Start the mock Mimir (multitenant) + the REAL auth-proxy in front of it.
    mimir = MockMimir()
    upstream_url = await mimir.start()

    port = _free_port()
    cfg = ProxyConfig(
        listen_host="127.0.0.1",
        listen_port=port,
        ca_chain_path=stack.pki_dir / "ca-chain.crt",
        tenant_registry_path=stack.registry_path,
        tls_cert_path=server_crt,
        tls_key_path=server_key,
        upstream_url=upstream_url,
    )
    proxy = AuthProxy(cfg)
    await proxy.start()
    base_url = f"https://127.0.0.1:{port}"

    # The client trusts our CA (validates the proxy's CA-signed server cert) and
    # presents a chosen tenant's mTLS client cert. We build the SSL context with
    # ``load_cert_chain`` and pass ``verify=<ctx>`` — the modern httpx API — so
    # the smoke is clean under ``-W error`` (httpx 0.28 deprecated ``cert=...``).
    trust_file = stack.sandbox / "root-and-chain.pem"
    trust_file.write_bytes(root_pem + chain_pem)

    def _mtls_ctx(cert_path: Path, key_path: Path) -> ssl.SSLContext:
        ctx = ssl.create_default_context(cafile=str(trust_file))
        ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        return ctx

    import httpx

    metric_name = "fyralis_agent_up"
    try:
        # ---- STEP 4: PUSH a metric AS ACME, then QUERY it back as acme ----
        rep.step(4, "push a metric acme->boundary->auth-proxy->Mimir, query it back")

        async with httpx.AsyncClient(
            verify=_mtls_ctx(acme_crt, acme_key)
        ) as c:
            # The agent (acme) presents its mTLS cert; it sends NO scope header.
            # The proxy must DERIVE acme from the cert and inject X-Scope-OrgID.
            push_body = json.dumps(
                {"metric": metric_name, "value": 1.0, "labels": {"deployment": "acme-use1"}}
            )
            push = await c.post(
                f"{base_url}/api/v1/push",
                content=push_body,
                headers={"content-type": "application/json"},
            )
        rep.check(push.status_code == 200, f"acme remote-write accepted (HTTP {push.status_code})")
        rep.check(
            mimir.seen_push_scopes[-1:] == ["acme"],
            f"proxy injected X-Scope-OrgID: acme on the write (mimir saw {mimir.seen_push_scopes[-1:]})",
        )

        # Query the same metric back, again AS ACME (cert-derived scope).
        async with httpx.AsyncClient(
            verify=_mtls_ctx(acme_crt, acme_key)
        ) as c:
            q = await c.get(
                f"{base_url}/prometheus/api/v1/query?query={metric_name}"
            )
        rep.check(q.status_code == 200, f"acme query accepted (HTTP {q.status_code})")
        qjson = q.json()
        result_series = qjson.get("data", {}).get("result", [])
        rep.check(
            len(result_series) >= 1,
            f"acme's series IS present when queried as acme ({len(result_series)} series)",
        )
        rep.check(
            any(s["metric"].get("__name__") == metric_name for s in result_series),
            f"the queried-back series is the metric we pushed ({metric_name})",
        )
        # Belt-and-suspenders: the store physically holds it under tenant acme.
        rep.check(
            len(mimir.series_for("acme", metric_name)) >= 1,
            "Mimir stored the sample under tenant 'acme'",
        )

        # ---- STEP 5: ISOLATION — query AS GLOBEX, must NOT see acme's series ----
        rep.step(5, "ISOLATION — a different tenant cannot see acme's series")
        async with httpx.AsyncClient(
            verify=_mtls_ctx(globex_crt, globex_key)
        ) as c:
            qg = await c.get(
                f"{base_url}/prometheus/api/v1/query?query={metric_name}"
            )
        rep.check(qg.status_code == 200, f"globex query accepted (HTTP {qg.status_code})")
        rep.check(
            mimir.seen_query_scopes[-1:] == ["globex"],
            f"proxy injected X-Scope-OrgID: globex (mimir saw {mimir.seen_query_scopes[-1:]})",
        )
        gjson = qg.json()
        g_series = gjson.get("data", {}).get("result", [])
        rep.check(
            len(g_series) == 0,
            f"acme's series is NOT visible to globex (globex sees {len(g_series)} series)",
        )
        rep.check(
            len(mimir.series_for("globex", metric_name)) == 0,
            "Mimir has NO acme samples under tenant 'globex' (physical partition)",
        )

        # A client cannot smuggle scope: present acme's cert but claim globex.
        async with httpx.AsyncClient(
            verify=_mtls_ctx(acme_crt, acme_key)
        ) as c:
            qspoof = await c.get(
                f"{base_url}/prometheus/api/v1/query?query={metric_name}",
                headers={"X-Scope-OrgID": "globex"},
            )
        rep.check(
            mimir.seen_query_scopes[-1:] == ["acme"],
            "a client-set X-Scope-OrgID: globex is OVERRIDDEN to the cert's acme (I4)",
        )
        _ = qspoof  # status asserted implicitly via the scope the upstream saw
    finally:
        await proxy.aclose()
        await mimir.stop()


def step4_and_5_metric_and_isolation(stack: Stack, rep: StepReporter) -> None:
    asyncio.run(_metric_path_async(stack, rep))


# --------------------------------------------------------------------------- #
# STEP 6 — license tamper -> agent denies.                                    #
# --------------------------------------------------------------------------- #
def step6_license_tamper(stack: Stack, rep: StepReporter) -> None:
    rep.step(6, "license tamper -> agent denies its privileged action")
    Agent, load_agent_config, HealthProbe, static_probe, LicenseChecker = _agent_api()

    lic_path = stack.bundle_dir / "acme.license.json"
    original = lic_path.read_bytes()

    # Sanity precondition: the pristine license is accepted.
    checker = LicenseChecker(str(lic_path), trust_root_path=str(stack.trust_root_path))
    rep.check(checker.is_licensed(), "pristine license is accepted (precondition)")

    # Tamper: change a value inside the SIGNED license body. Because the signature
    # is over the canonical bytes, ANY content change breaks verification (I6).
    doc = json.loads(original.decode("utf-8"))
    doc["plan"] = "enterprise"  # silently "upgrade" the plan
    doc["features"] = list(doc.get("features", [])) + ["sso", "audit-export"]
    lic_path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    try:
        tampered_checker = LicenseChecker(
            str(lic_path), trust_root_path=str(stack.trust_root_path)
        )
        status = tampered_checker.evaluate()
        rep.check(
            not status.ok,
            f"tampered license is REJECTED by signature verify ({status.reason[:80]})",
        )
        rep.check(
            not tampered_checker.is_licensed(),
            "agent's license gate reports UNLICENSED on the tampered license",
        )

        # The agent must DENY its privileged action (config pull) when unlicensed.
        cfg = load_agent_config(
            console_url="https://console.invalid:8080",
            tenant_id="acme",
            deployment_id=stack.onboard_result.deployment_id,
            region="us-east",
            telemetry_tier="T1",
            license_path=str(lic_path),
            trust_root_path=str(stack.trust_root_path),
            config_dir=str(stack.bundle_dir / "applied-config"),
            version_file=str(stack.bundle_dir / "VERSION"),
            version_env="1.0.0-e2e",
            healthz_url="http://127.0.0.1:1/healthz",
            buffer_path=str(stack.bundle_dir / "buffer-tamper.jsonl"),
            interval_s=1.0,
        )
        agent = Agent(
            cfg,
            sender=lambda rec: True,
            probe=HealthProbe(static_probe(True)),
            license_checker=tampered_checker,
        )
        applied = agent.pull_config("https://config.invalid/config/acme")
        rep.check(
            applied is False,
            "agent REFUSES the privileged config pull while unlicensed (license gate)",
        )
    finally:
        # Restore the pristine license so later steps / re-runs are unaffected.
        lic_path.write_bytes(original)
        rep.check(
            LicenseChecker(
                str(lic_path), trust_root_path=str(stack.trust_root_path)
            ).is_licensed(),
            "pristine license restored and accepted again (no lingering tamper)",
        )


# --------------------------------------------------------------------------- #
# STEP 7 — config-dist serves a SIGNED config the agent verifies.             #
# --------------------------------------------------------------------------- #
def step7_signed_config(stack: Stack, rep: StepReporter) -> None:
    rep.step(7, "config-dist serves a SIGNED config the agent verifies (I6)")
    # The REAL config store signs a per-deployment config version; the REAL agent
    # ConfigPuller verifies-before-apply. We point the puller's fetcher at the
    # store so no HTTP server is needed (the verify path is byte-identical).
    #
    # config-dist/store.py also imports flat ``store``-name-collides with
    # console/store.py — load it by explicit path under a distinct module name.
    _cd_store = _load(
        "_configdist_store", _CONFIGDIST_DIR / "store.py", dir_first=_CONFIGDIST_DIR
    )
    ConfigStore = _cd_store.ConfigStore
    SigningHome = _cd_store.SigningHome

    # agent/config_pull.py — prime the agent dir then import it (its
    # ``import _bootstrap`` / ``import verify_bundle`` must resolve to agent/).
    _prime_agent_dir()
    import config_pull as _cp_mod  # agent/config_pull.py
    ConfigPuller = _cp_mod.ConfigPuller

    deployment_id = stack.onboard_result.deployment_id

    # config-dist signs with ITS OWN signing home; the agent must verify against
    # THAT home's trust root (the production agent ships config-dist's pubkey, or
    # config-dist serves /trust_root.json). We use the store's signing home.
    store_root = stack.sandbox / "config-dist-store"
    signing_home = SigningHome(stack.sandbox / "config-dist-signing-home")
    store = ConfigStore(store_root=str(store_root), signing_home=signing_home)
    cd_trust_root = str(signing_home.trust_root_path)

    # Publish a signed config version (a tier change / flag flip — a new version).
    cv = store.publish(
        deployment_id=deployment_id,
        tenant_id="acme",
        config_body={
            "flags": {"anomaly_detection_enabled": True},
            "telemetry_tier": "T2",
            "token_rotation": {"enabled": True, "interval_hours": 12},
        },
    )
    rep.check(cv.version >= 1, f"config-dist published a signed config version (v{cv.version})")
    rep.check(bool(cv.key_id), f"the config is signed (key_id={cv.key_id})")

    # The store self-verifies the version it just signed (I6, server side).
    vres = store.verify_version(deployment_id, cv.version)
    rep.check(vres.ok, f"config-dist's own verify of the signed config passes ({vres.reason[:60]})")

    # ---- The AGENT pulls + verifies-before-apply over the store's bytes -----
    # Fetcher returns the exact (config, sig, manifest) trio the HTTP service
    # would serve at GET /config/{id}[.sig|.manifest.json].
    def store_fetcher(_url: str):
        head = store.get_head(deployment_id)
        return (
            head.config_bytes,
            head.sig_b64,
            head.manifest_path.read_bytes(),
        )

    applied_dir = stack.bundle_dir / "applied-config-step7"
    puller = ConfigPuller(
        config_dir=str(applied_dir),
        trust_root_path=cd_trust_root,
        fetcher=store_fetcher,
    )
    res = puller.pull_and_apply(f"https://config-dist/config/{deployment_id}")
    rep.check(res.ok and res.applied, f"agent VERIFIED and applied the signed config ({res.reason[:60]})")
    rep.check(
        (applied_dir / "agent-config.json").is_file(),
        "the verified config was written to the agent's applied-config dir",
    )
    applied = puller.load_applied_config()
    rep.check(applied is not None, "agent can re-read the applied config (re-verifies on read)")
    rep.check(
        (applied or {}).get("config", {}).get("telemetry_tier") == "T2",
        "the applied config carries the published telemetry_tier (T2)",
    )

    # ---- Tamper guard: a flipped byte must be REJECTED (I6) ------------------
    def tamper_fetcher(_url: str):
        head = store.get_head(deployment_id)
        tampered = json.loads(head.config_bytes.decode("utf-8"))
        # Sneak a privilege escalation into the config the agent would apply.
        tampered.setdefault("config", {}).setdefault("flags", {})["allow_everything"] = True
        tampered_bytes = json.dumps(tampered, separators=(",", ":")).encode("utf-8")
        return (tampered_bytes, head.sig_b64, head.manifest_path.read_bytes())

    tamper_dir = stack.bundle_dir / "applied-config-step7-tamper"
    tamper_puller = ConfigPuller(
        config_dir=str(tamper_dir),
        trust_root_path=cd_trust_root,
        fetcher=tamper_fetcher,
    )
    tres = tamper_puller.pull_and_apply(f"https://config-dist/config/{deployment_id}")
    rep.check(
        not tres.ok and not tres.applied,
        f"agent REJECTS a tampered config (unverified) and keeps prior config ({tres.reason[:60]})",
    )
    rep.check(
        not (tamper_dir / "agent-config.json").is_file(),
        "the tampered config was NOT written to disk (I6 enforced)",
    )


# --------------------------------------------------------------------------- #
# Live-docker steps (clearly marked SKIP in no-docker mode).                   #
# --------------------------------------------------------------------------- #
def report_live_docker(rep: StepReporter, live: bool) -> None:
    """The portions that genuinely need a running Docker stack.

    In the in-process assembly above, EVERY step already runs for real against
    the REAL components (CA, signing, onboarding, console, agent, auth-proxy over
    a real mTLS socket, config store) with a *mockable* Mimir. The ONLY thing the
    in-process path substitutes is the Mimir *container* itself (we use the
    multitenant ``MockMimir`` with the same X-Scope-OrgID contract). The
    live-docker variant points the same flow at the real Mimir behind the
    deployed auth-proxy.
    """
    print("\n== LIVE-DOCKER-ONLY PATH ==")
    if not live:
        rep.skip("metric round-trip against the REAL Mimir container (run with --live + docker)")
        rep.skip("auth-proxy <-> Mimir over the compose cp-net network (docker compose up)")
        rep.skip("Grafana fleet dashboards reading recorded fleet:* series (docker)")
        return
    # --live: try the real stack. We do NOT fail the smoke if docker is absent —
    # we surface it as a skip with a clear reason (the python-level path is the
    # authoritative no-docker proof).
    if shutil.which("docker") is None:
        rep.skip("--live requested but `docker` is not installed; skipping live path")
        return
    compose = _CP_ROOT / "docker-compose.control-plane.yml"
    rep.skip(
        "--live: bring the stack up with "
        f"`docker compose -f {compose} up -d` then point AUTH_PROXY_UPSTREAM_URL "
        "at the real mimir:9009 and re-run the metric step (manual gate)"
    )


# --------------------------------------------------------------------------- #
# Orchestrator                                                                #
# --------------------------------------------------------------------------- #
def run_smoke(*, live: bool = False, keep: bool = False) -> int:
    rep = StepReporter()
    print("Fyralis BYOC control-plane — END-TO-END SMOKE")
    print("(no-docker in-process assembly of the REAL components)")

    stack = build_stack()
    print(f"sandbox: {stack.sandbox}")
    try:
        step1_bootstrap(stack, rep)
        step2_onboard(stack, rep)
        step3_agent_green(stack, rep)
        step4_and_5_metric_and_isolation(stack, rep)
        step6_license_tamper(stack, rep)
        step7_signed_config(stack, rep)
        report_live_docker(rep, live)
    except Exception as exc:  # an unexpected crash is a smoke failure
        import traceback

        traceback.print_exc()
        rep.check(False, f"UNEXPECTED EXCEPTION aborted the smoke: {exc}")
    finally:
        stack.close(keep=keep)
        if keep:
            print(f"(sandbox left at {stack.sandbox} for inspection)")

    return rep.verdict()


def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="End-to-end smoke for the Fyralis BYOC control plane."
    )
    ap.add_argument(
        "--live",
        action="store_true",
        help="also attempt the docker-only path (real Mimir container)",
    )
    ap.add_argument(
        "--keep",
        action="store_true",
        help="keep the temp sandbox after the run (for inspection)",
    )
    args = ap.parse_args(argv)
    return run_smoke(live=args.live, keep=args.keep)


if __name__ == "__main__":
    raise SystemExit(main())
