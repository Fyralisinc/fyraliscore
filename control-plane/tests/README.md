# Fyralis BYOC control plane — end-to-end SMOKE + integration suite

This is the **CTO smoke**: one scripted run that exercises the *full* BYOC
control-plane path and asserts every step. If `make smoke` exits 0, the control
plane works end-to-end — bootstrap → onboard → agent-green → metric round-trip →
tenant isolation → license-tamper denial → signed-config verify.

It runs against an **in-process assembly of the REAL components** (no Docker
required for the Python-level proof), with a **mockable Mimir** standing in for
the one piece that genuinely needs a container. The parts that can only run
against a live Docker stack are reported as explicit **SKIP**s — never silent
passes.

```
control-plane/tests/
  e2e_smoke.py     # scripted end-to-end smoke (the centerpiece)
  test_e2e.py      # pytest wrapper of the same steps, granular assertions + skips
  conftest.py      # registers the --live-docker opt-in flag + live_docker marker
  Makefile         # make smoke / make test / make live
  README.md        # you are here
```

## Run it

```bash
# the scripted smoke (exit 0 == every assertion held)
make smoke

# the pytest wrapper (per-step PASS/FAIL/SKIP, good for CI)
make test

# both
make all
```

`make` auto-selects the project venv
(`/home/.../fyraliscore/.venv/bin/python`, which already has
`cryptography` / `fastapi` / `httpx` / `h11`). Override with `PYTHON=...` or run
directly:

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python e2e_smoke.py
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python -m pytest test_e2e.py -v
```

Flags:

| command | what it does |
| --- | --- |
| `python e2e_smoke.py` | no-docker mode (default): every Python-level step runs for real |
| `python e2e_smoke.py --live` | also attempt the docker-only path (real Mimir container) |
| `python e2e_smoke.py --keep` | leave the temp sandbox on disk for inspection |
| `make smoke-keep` | same as `--keep` |
| `pytest test_e2e.py -m live_docker --live-docker` | run the docker-gated pytest step |

## What it proves (the seven asserted steps)

| # | Step | Asserted |
| - | ---- | -------- |
| 1 | **Bootstrap** | CA root + intermediate + chain exist; a tenant leaf verify-chains to the CA with `clientAuth`; the ed25519 signing key + trust root with an active key exist. |
| 2 | **Onboard `acme`** | the REAL `onboard()` transaction produces a bundle (mTLS cert+key+chain, a **signature-verified, unexpired license**); the tenant registry row is **active** and keyed by the cert fingerprint; the REAL console lists the deployment. |
| 3 | **Agent → GREEN** | the REAL `Agent`, built from the bundle, reports itself **licensed**, delivers a heartbeat to the console, and both its own derived health and the **console's derived-on-read health are GREEN**. |
| 4 | **Metric push** | a metric is pushed **as acme** through the boundary → **REAL `AuthProxy` over a real mTLS socket** → Mimir; the proxy derives `acme` from the verified client cert and injects `X-Scope-OrgID: acme`; querying back **as acme** returns the series. |
| 5 | **Isolation** | querying the same metric **as a different tenant (`globex`)** returns **zero** series — acme's data is physically partitioned by tenant and structurally invisible to globex. A client-set `X-Scope-OrgID: globex` while presenting acme's cert is **overridden to acme** (I4: identity comes only from the cert). |
| 6 | **License tamper** | flipping a byte in the signed license breaks ed25519 verification; the agent's license gate reports **unlicensed** and **refuses its privileged action** (config pull). The pristine license is restored afterward. |
| 7 | **Config distribution** | the REAL config store signs a per-deployment config version; the REAL agent `ConfigPuller` **verifies-before-apply (I6)** and applies it; a **tampered** config is **rejected and never written to disk**. |

Steps 4 and 5 use the **REAL** `auth-proxy/proxy.py` (mTLS-terminating reverse
proxy) over a genuine TLS socket with CA-signed server + client certs. The only
substitution in no-docker mode is the Mimir **container**: a multitenant
`MockMimir` reproduces Mimir's exact `X-Scope-OrgID` contract (a remote-write
receive endpoint + a scoped query endpoint, both rejecting a missing scope with
422, series partitioned by tenant). That is precisely what makes the
boundary → auth-proxy → Mimir **identity + isolation** contract testable without
Docker.

## How the in-process assembly is wired

`build_stack()` creates one throwaway sandbox under `/tmp` and:

- bootstraps a **real** CA hierarchy via the committed `ca/bootstrap_ca.py`,
- mints a **real** ed25519 signing key + public trust root via `signing/signing_lib`,
- retargets the committed `sign_bundle` / `verify_bundle` module path constants at
  the throwaway material (the same trick the committed self-tests use, so the
  committed `signing/` directory is never written to).

Every component imported afterward is the **committed** code: `onboard()`,
`console/app.py`, `agent/agent.py`, `auth-proxy/proxy.py`,
`auth-proxy/tenant_resolver.py`, `config-dist/store.py`, `agent/config_pull.py`.
The smoke never re-implements crypto, signing, onboarding, or the proxy.

### A note on imports

The control-plane components use script-style flat imports and several module
names **collide** across directories (`store` in both `console/` and
`config-dist/`; `config` in `agent/`, `auth-proxy/` and `lib/`; `app` in
`console/`). The smoke loads each colliding module from its **explicit file
path** under a chosen name (see `_load` / `_agent_api` / `_prime_agent_dir` in
`e2e_smoke.py`) and front-loads `control-plane/` so `import lib.deployment`
always binds to the control-plane `lib` — even under pytest whose rootdir sits
above `control-plane/`.

## The live-docker path

In no-docker mode the metric round-trip already runs end-to-end against the
multitenant `MockMimir`. To run it against the **real Mimir image** behind the
**deployed** auth-proxy:

```bash
docker compose -f ../docker-compose.control-plane.yml up -d
# point AUTH_PROXY_UPSTREAM_URL at the real mimir:9009 behind the deployed proxy
python e2e_smoke.py --live
pytest test_e2e.py -m live_docker --live-docker
```

The `--live` / `--live-docker` steps print an actionable manual-gate message
(and skip cleanly if `docker` is absent) — automating container lifecycle is out
of scope for the unit run; the no-docker path is the authoritative proof.

## Self-test result

`make smoke` (no-docker) → **52 assertions pass, 0 fail, 3 docker steps skipped**.
`make test` (pytest) → **6 step-tests pass, 1 live-docker step skipped**.

## Caveats

- **Mimir is mocked in no-docker mode.** `MockMimir` faithfully reproduces the
  multitenancy contract under test (`X-Scope-OrgID` required, series partitioned
  by tenant) but takes a simplified JSON push body rather than Mimir's
  snappy-protobuf remote-write wire format. The *identity + isolation* semantics
  — what this smoke asserts — are identical; the byte-level wire format is only
  exercised on the `--live` path against the real image.
- **The console heartbeat is delivered in-process** (ASGI), standing in for the
  agent's outbound HTTPS POST. The agent's REAL collect/deliver/health-derivation
  path runs; only the socket hop is short-circuited.
- **config-dist is driven through its store + a fetcher shim**, not its HTTP
  service — the agent's verify-before-apply path is byte-identical either way
  (the fetcher returns the exact `(config, sig, manifest)` trio the HTTP endpoint
  would serve).
- Each run uses a **fresh throwaway CA + signing key + registry** under `/tmp`;
  the committed `ca/pki`, `signing/trust_root.json` and `ca/tenant_registry.json`
  are never touched. Use `--keep` to inspect the sandbox.
```
