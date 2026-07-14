# Fyralis BYOC — data-plane Agent (WS-AGENT)

The **outbound-only** agent that runs **in the customer VPC**. It makes the
control plane's vendor-side console aware of each deployment, pulls signed config,
and enforces the local license — without ever exposing an inbound surface.

```
customer VPC                                   vendor control plane
┌─────────────────────────────┐                 ┌──────────────────────┐
│  fyralis-agent (this)        │  outbound https │                      │
│  • collect DeploymentRecord  │ ───────────────▶│  console             │
│  • POST /api/v1/heartbeat    │   (I2: out only)│  /api/v1/heartbeat    │
│  • GET signed config bundle  │ ◀─────────────  │  signed config/release│
│  • verify-before-apply (I6)  │                 │                      │
│  • local license gate        │                 └──────────────────────┘
│  • buffer+retry on outage(I3)│
└─────────────────────────────┘
        (no listening socket — nothing reaches in)
```

## What it does each tick

1. **Collect** a `DeploymentRecord` (the C4 contract, from
   `control-plane/lib/deployment.py`):
   - `version` from the local `VERSION` file (env `AGENT_VERSION` fallback),
   - `region`, `telemetry_tier`, `tenant_id`, `deployment_id` from config,
   - `license_expiry` from the **verified** local license,
   - `health` (`green|yellow|red`) **derived** from heartbeat freshness folded
     with a **local SLI probe** of the data-plane `/healthz` (a breach degrades
     `green→yellow`) and the license expiry (expired ⇒ `red`).
2. **POST** it to `<console>/api/v1/heartbeat` over an **outbound** https call.
3. If the console is **unreachable**, append the record to a **durable local
   buffer** and retry with exponential backoff. On reconnect the backlog is
   flushed **oldest-first** before the live heartbeat. The daemon **never crashes
   and never blocks local ops** (I3).

Separately (and license-gated), the agent can **pull a signed config bundle** and
**verify it against the trust root before applying** (I6) — an unverified /
tampered / unknown-key / wrong-kind bundle is **rejected** and the previous
config is kept.

## Invariants this component owns

| Inv | Guarantee | Where |
|-----|-----------|-------|
| **I2** | No inbound listener — outbound only. No `EXPOSE`, no `ports:`, no server framework, no `listen()`. | whole component; asserted by `tests/test_no_listener.py` + `selftest.py` |
| **I3** | Console outage never crashes the agent or blocks local ops; heartbeats buffer + retry with backoff. | `buffer.py`, `agent.py:deliver/tick/run_forever` |
| **I6** | Signed config/release is **verified before apply**; unverified ⇒ rejected. | `config_pull.py` (delegates to `signing/verify_bundle`) |
| license gate | The agent refuses privileged actions when the local signed license is missing/expired/tampered. | `license_check.py`, `agent.py:pull_config` |

## Files

| File | Purpose |
|------|---------|
| `agent.py` | The daemon loop: collect → deliver (send/buffer/flush) → backoff; `main()` entrypoint, SIGINT/SIGTERM-aware. |
| `config.py` | `AgentConfig` — console URL, interval, identity, file paths, buffer/backoff knobs (env `AGENT_*`). **No listen host/port — by design.** |
| `config_pull.py` | Pull a signed config bundle (outbound GET) and **verify before apply** (I6); reject unverified; atomic apply. |
| `license_check.py` | Load + verify the local signed license (signature + key policy + expiry); `is_licensed()`. |
| `buffer.py` | Durable, bounded, append-only JSONL queue for un-sent heartbeats (I3). |
| `health_probe.py` | Local SLI probe of the data-plane `/healthz` → the heartbeat's derived health. |
| `selftest.py` | End-to-end scenario against a fake in-process console (the spec's self-test). |
| `run.sh` / `Dockerfile` / `service.compose.yml` | Run locally / containerized / in compose. |
| `tests/` | Unit + loopback + I2-no-listener tests. |

It reuses the committed siblings (does **not** redefine them):
`control-plane/lib` (`DeploymentRecord`, tiers), `control-plane/signing`
(`verify_bundle`, `signing_lib`).

## How to run

### Locally (daemon)

```sh
# 1. Mint a signing key + trust root (control-plane side), if not already present:
python ../signing/keygen.py --activate

# 2. Issue + sign a license for this deployment (any signed JSON matching the
#    license contract works; the simplest path is the license CLI / sign_bundle):
#    echo '{...license json...}' > license.json
python ../signing/sign_bundle.py sign license.json --kind license --version 2027-06-24

# 3. Run the agent (env-configured; defaults point at files in this dir):
AGENT_CONSOLE_URL=https://console.vendor.example \
AGENT_TENANT_ID=acme \
AGENT_DEPLOYMENT_ID=acme-use1-0001 \
AGENT_REGION=us-east-1 \
AGENT_HEALTHZ_URL=http://127.0.0.1:8088/healthz \
./run.sh
```

Key env vars (all optional, sane defaults): `AGENT_CONSOLE_URL`,
`AGENT_TENANT_ID`, `AGENT_DEPLOYMENT_ID`, `AGENT_REGION`, `AGENT_TELEMETRY_TIER`
(`T1|T2|T3`), `AGENT_VERSION_FILE`, `AGENT_LICENSE_PATH`, `AGENT_TRUST_ROOT`,
`AGENT_HEALTHZ_URL`, `AGENT_INTERVAL_S`, `AGENT_BUFFER_PATH`,
`AGENT_BACKOFF_BASE_S`, `AGENT_BACKOFF_MAX_S`.

### Self-test (the spec scenario, no external services)

```sh
./run.sh selftest      # or: python selftest.py
```

Drives the agent against a fake in-process console over loopback and asserts:
heartbeats a valid `DeploymentRecord` → kill console → **buffers + retries**
(does not crash) → reconnect flushes backlog → **expired license ⇒
`is_licensed()` False** → **tampered config ⇒ rejected** → **no listening socket
opened (I2)**.

### Tests

```sh
python -m pytest        # run from agent/
```

37 tests: license verify/expiry, config verify-before-apply, buffer
durability/ordering/bounding, agent tick/heartbeat/buffer/backoff/license-gate, a
real loopback console round-trip, and the I2 no-listener guard (behavioral trap +
`/proc` listen-port snapshot + a source-level forbidden-primitive scan).

### Container

```sh
# Build from the control-plane ROOT (so signing/ + lib/ are in context):
docker build -f agent/Dockerfile -t fyralis/agent .
docker run --rm \
  -e AGENT_CONSOLE_URL=https://console.vendor.example \
  -e AGENT_DEPLOYMENT_ID=acme-use1-0001 \
  -v "$PWD/signing/trust_root.json:/app/signing/trust_root.json:ro" \
  -v "$PWD/agent/deploy/license.json:/run/secrets/license.json:ro" \
  -v "$PWD/agent/deploy/license.json.sig:/run/secrets/license.json.sig:ro" \
  -v "$PWD/agent/deploy/license.json.manifest.json:/run/secrets/license.json.manifest.json:ro" \
  fyralis/agent
```

The image has **no `EXPOSE`** — the agent never listens.

## Design notes / how the invariants are mechanized

- **Outbound only (I2).** The agent has exactly one network egress path
  (`requests.post`/`requests.get`). No server framework is imported; no socket is
  bound for listening. The self-test and `tests/test_no_listener.py` prove it
  three ways: a `socket.socket.listen` trap during a full run, a `/proc/net/tcp`
  listen-port snapshot diff, and a static scan that fails if any module references
  `uvicorn`/`HTTPServer`/`start_server`/`.listen(`/`FastAPI(`/etc.
- **Never crash / never block (I3).** Every network touch is wrapped: a sender
  exception or non-2xx is treated as *undelivered* (not raised), the record is
  parked in a durable JSONL buffer, and `run_forever` has an outer `try/except`
  backstop so one bad tick can't kill the loop. Backoff grows (capped) while a
  backlog exists and resets on the first success. The buffer is **bounded**
  (oldest dropped past the cap — a stale heartbeat is worthless) and survives a
  restart.
- **Verify before apply (I6).** `config_pull` stages the pulled trio into a temp
  dir and calls `signing/verify_bundle.verify_file` (ed25519 against the trust
  root, key-id policy, sha256 cross-check) **before** atomically copying it into
  the applied-config dir. `load_applied_config` re-verifies on read, so a config
  edited on disk after apply is not trusted.
- **License gate.** `license_check` re-verifies on every evaluation (no
  "once-valid-always-valid" cache), so a license that expires while the agent runs
  flips to unlicensed without a restart. Privileged actions (config pull/apply)
  refuse when unlicensed; the agent still heartbeats so the console can *see* the
  deployment is unlicensed (its derived health goes `red`).

## Caveats / not-built / assumptions

- **License/config issuance is upstream.** The agent only *consumes* signed
  bundles. Minting them is the control-plane's job (`signing/sign_bundle.py`, the
  WS-LICENSE service). For local runs you sign with `sign_bundle.py`/`keygen.py`;
  the agent ships only the **public** trust root.
- **`register` is not driven by the daemon loop.** Per the P4 contract the
  console mints `deployment_id`/`tenant_id` via `POST /api/v1/register`. This
  agent is built for the steady-state heartbeat path and assumes its
  `deployment_id`/`tenant_id` are already provisioned (env/config) — matching the
  BYOC install flow where the installer registers the deployment and seeds the
  agent's config + license. A one-shot `register` bootstrap is a small,
  outbound-only addition if needed, but is intentionally out of scope here.
- **The SLI probe is single-source.** Health folds in **one** local SLI: the
  data-plane `/healthz` (reachable + 2xx ⇒ healthy; unreachable / degraded body /
  non-2xx ⇒ breach → `yellow`). Richer golden-SLI burn signals (from the fleet-SLI
  rules) are not wired into the agent here; the C4 record already carries a
  `sli_breached` hook and the probe is injectable, so adding more SLI inputs is a
  drop-in. The console *re-derives* freshness-based health on read regardless.
- **`config_dir` apply is file-drop, not hot-reload.** A verified config is
  written to disk so a restart re-reads it; the running daemon does not currently
  hot-swap its in-memory `AgentConfig` mid-loop. (The applied config is available
  via `ConfigPuller.load_applied_config()` for a future reload hook.)
- **Buffer cap drops oldest.** Past `AGENT_BUFFER_MAX_RECORDS` (default 10k) the
  oldest buffered heartbeat is dropped to bound disk use. This is deliberate
  (freshest fleet state matters most) but means a multi-day outage past the cap
  loses the *oldest* history, not the newest. Tune the cap for the retention you
  need.
- **TLS verification is `requests`' default** (system trust store) for the
  outbound console call. Pinning the console cert to the Fyralis CA is a
  hardening step left to deployment config (set `REQUESTS_CA_BUNDLE` or extend the
  sender) — the agent does not disable verification anywhere.
- **Single-process buffer ownership.** The JSONL buffer assumes one agent process
  owns its file (the normal deployment). Running two agents against the same
  `AGENT_BUFFER_PATH` is unsupported.
