# Fyralis BYOC Control Plane — CTO Test Guide

This is the **hands-on runbook** for verifying the control plane yourself. Every
command below is real and runnable **as written**, with the **expected output**
shown so you know what PASS looks like. There are two paths:

- **Path A — fast, no Docker (~10s).** Runs the REAL control-plane code paths
  (CA, ed25519 signing, onboarding, the agent license/config gates, and the
  auth-proxy mTLS tenant-isolation contract over a genuine mTLS socket) with
  Mimir mocked. This is the authoritative correctness proof and the one to run
  first.
- **Path B — full live stack (Docker).** Brings up all 17 services, you open the
  Console + Grafana, watch a real metric flow data-plane → boundary → auth-proxy
  → Mimir, query it tenant-scoped, **prove isolation** against a second tenant,
  then exercise the break-glass + tamper-evident audit chain.

A **Troubleshooting** section and a **"what each check proves"** invariant map
are at the end.

> **Run everything from the control-plane root** unless a command says
> otherwise: `cd <repo>/fyralis-control-plane/control-plane`. The flat-module
> imports and the compose file's relative bind-mounts assume this CWD.

---

## 0. Prerequisites

### 0.1 Python (both paths)

The tooling (CA, signing, agent, console, onboarding, smoke) is Python 3.12. Use
the repo dev venv if present, else create one.

```bash
# Repo dev venv (preferred — bootstrap.sh / Makefile auto-detect it):
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python --version
# → Python 3.12.x
```

If that venv does not exist, create one **from the control-plane root**:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

> The `bootstrap.sh` script and `Makefile` both prefer
> `/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python` and fall back
> to `python3`. To force a specific interpreter, export `PYTHON=/path/to/python`
> before running them.

### 0.2 Docker (Path B only)

```bash
docker --version            # → Docker version 24+ (Compose v2)
docker compose version      # → Docker Compose version v2.x
```

Path B builds local images (`fyralis/auth-proxy`, `fyralis/console`,
`fyralis/agent`, …) and pulls upstream images (Mimir 2.13, Loki 3.4, Grafana
11.1, otel-collector-contrib 0.105). First `up` takes a few minutes.

### 0.3 Known dev gotchas (read once — these are already handled, listed so a
failure makes sense)

| Gotcha | Why it happens | What handles it |
|---|---|---|
| **Run from the control-plane root.** | Components use script-style flat imports (`import store`, `import config`) that collide across dirs, and the compose bind-mounts are relative (`./ca`, `./_runtime`). | `bootstrap.sh`/`Makefile` `cd` to their own dir; you should `cd control-plane` too. |
| **auth-proxy registry-read 403** (`registry_read_error` on every push). | The proxy container runs as a **non-root uid (10001)** and bind-mounts `ca/tenant_registry.json` **read-only**; the CA tooling writes it `0600` owned by the operator, which uid 10001 cannot read → fail-closed 403. | `bootstrap.sh` relaxes the gitignored demo registry to `0644` (it is the public revocation list, not a secret). In prod it ships with matching container ownership. |
| **mTLS key unreadable inside the container.** | Same non-root container vs `0600` host key (auth-proxy server key; boundary client key). | `bootstrap.sh` `chmod 0644`s the **gitignored dev/demo** key material so the container can load it. Prod delivers keys via a secrets manager with matching ownership. |
| **Trust-root activation.** | The agent verifies signed license/config against the **active** ed25519 key named in `signing/trust_root.json`. A non-activated keygen leaves no `active_key_id` and every verify fails. | `bootstrap.sh` runs `signing/keygen.py --activate`; the idempotency check asserts `active_key_id` is set before skipping. |
| **`dataplane-net` is declared external.** | The boundary collector + demo-dataplane share an external docker network so the CP can scrape the local "data plane". | `bootstrap.sh`/`make up` create `dataplane-net` idempotently before `up`. |

---

## Path A — Fast verification (no Docker)

This is the **proof you run first**. ~10 seconds, no containers. It assembles the
REAL components in-process and asserts each of the seven BYOC steps.

### A.1 One command

```bash
make smoke
```

(equivalently: `/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python tests/e2e_smoke.py`)

### A.2 What PASS looks like

The run prints seven numbered steps, each with `[ PASS ]` lines, ending in a
verdict. The **last three lines are what you check**:

```
== STEP 1: bootstrap — CA + ed25519 signing keys exist ==
    [ PASS ] root CA cert exists (root.crt)
    [ PASS ] intermediate CA cert exists (intermediate.crt)
    [ PASS ] CA chain bundle exists (ca-chain.crt)
    [ PASS ] a tenant leaf verify-chains to the CA (clientAuth) — CA is usable (ok)
    [ PASS ] trust root names an active signing key (cp-signing-e2e)
    ...
== STEP 2: onboard demo tenant 'acme' -> bundle + registry + console ==
    [ PASS ] cert SAN identity == acme (got 'acme')
    [ PASS ] license signature verifies and is unexpired
    [ PASS ] the acme registry row is ACTIVE (proxy will accept it)
    [ PASS ] console fleet rollup includes acme
== STEP 3: start the agent with the bundle -> console marks acme GREEN ==
    [ PASS ] agent reports itself LICENSED (valid signed license)
    [ PASS ] agent's own derived health is GREEN (got green)
    [ PASS ] console marks acme GREEN (got green)
== STEP 4: push a metric acme->boundary->auth-proxy->Mimir, query it back ==
    [ PASS ] acme remote-write accepted (HTTP 200)
    [ PASS ] proxy injected X-Scope-OrgID: acme on the write (mimir saw ['acme'])
    [ PASS ] acme's series IS present when queried as acme (1 series)
== STEP 5: ISOLATION — a different tenant cannot see acme's series ==
    [ PASS ] acme's series is NOT visible to globex (globex sees 0 series)
    [ PASS ] a client-set X-Scope-OrgID: globex is OVERRIDDEN to the cert's acme (I4)
== STEP 6: license tamper -> agent denies its privileged action ==
    [ PASS ] tampered license is REJECTED by signature verify (ed25519 signature INVALID — artifact tampered)
    [ PASS ] agent REFUSES the privileged config pull while unlicensed (license gate)
== STEP 7: config-dist serves a SIGNED config the agent verifies (I6) ==
    [ PASS ] agent VERIFIED and applied the signed config (config v1 verified ... and applied)
    [ PASS ] agent REJECTS a tampered config (unverified) and keeps prior config

== LIVE-DOCKER-ONLY PATH ==
    [ SKIP ] metric round-trip against the REAL Mimir container (run with --live + docker)
    [ SKIP ] auth-proxy <-> Mimir over the compose cp-net network (docker compose up)
    [ SKIP ] Grafana fleet dashboards reading recorded fleet:* series (docker)

================================================================
SMOKE RESULT: 52 passed, 0 failed, 3 skipped
SMOKE PASSED — the control plane works end-to-end
```

**PASS criteria:**

- The verdict reads **`SMOKE PASSED`** and **`52 passed, 0 failed`**. (The 3
  SKIPs are the docker-only steps; that is expected in no-docker mode.)
- The process **exit code is 0**:

  ```bash
  make smoke ; echo "exit=$?"
  # → ... SMOKE PASSED ...
  # → exit=0
  ```

> Note `[5/6] seeded heartbeat` and a line `refusing config pull: deployment is
> unlicensed/expired` may appear in STEP 2/6 — those are the components' own
> stdout (the license-gate **working**), not errors.

### A.3 Bootstrap the persistent trust material (so Path B can start)

`make smoke` runs in a throwaway `/tmp` sandbox and leaves no on-disk trust
material. To stand up the **persistent** CA + signing key + onboarded demo tenant
(without Docker), run:

```bash
./bootstrap.sh --no-docker
```

Expected (first run, from a clean tree):

```
[bootstrap] python: /home/.../.venv/bin/python
[bootstrap] generating the Fyralis CA (root + intermediate) …
[ ok ] CA chain at .../ca/pki/ca-chain.crt
[bootstrap] generating + ACTIVATING the CP signing key …
[ ok ] trust root at .../signing/trust_root.json (private key stays gitignored ...)
[bootstrap] minting the auth-proxy server cert (SANs: localhost 127.0.0.1 auth-proxy) …
[ ok ] auth-proxy server cert at .../auth-proxy/tls/server.crt
[bootstrap] onboarding demo tenant 'acme' (region=us-east-1 plan=standard, embedded console) …
[1/6] registered with console: tenant=acme deployment=acme-use1-XXXX
...
[6/6] confirmed: console lists deployment acme-use1-XXXX
[ ok ] onboarded acme -> acme-use1-XXXX; runtime staged; wrote .env
[bootstrap] --no-docker: skipping docker. Running the python e2e smoke instead …
... SMOKE PASSED — the control plane works end-to-end
[ ok ] no-docker bootstrap complete (CA + signing + demo onboard + smoke).
```

**This produces (all gitignored):**

- `ca/pki/*` — the root + intermediate CA + chain
- `signing/trust_root.json` (+ private key under `signing/keys/`)
- `auth-proxy/tls/server.{crt,key}` — the proxy's own server identity
- `_runtime/agent/{license.json[.sig|.manifest.json], client.crt, client.key}`
  and `_runtime/ca/ca.crt` — the material compose bind-mounts
- `ca/tenant_registry.json` — the active `acme` revocation row
- `.env` — `AGENT_DEPLOYMENT_ID=acme-use1-XXXX` etc.

`bootstrap.sh` is **idempotent**: a second run reports `already present` for each
step and re-runs the smoke.

**Verify the onboarding side effects directly:**

```bash
cat .env
# → AGENT_TENANT_ID=acme / AGENT_DEPLOYMENT_ID=acme-use1-XXXX / AGENT_REGION=us-east-1 / AGENT_TELEMETRY_TIER=T1

python -c "import json;d=json.load(open('ca/tenant_registry.json'));print(*[(v['tenant_id'],v['status']) for v in d.values()])"
# → ('acme', 'active')          ← exactly one active acme row, keyed by cert fingerprint
```

### A.4 (Optional) License + isolation asserts in isolation

The smoke already asserts these, but you can re-run a single concern. Onboard a
second tenant and confirm the registry keeps them **disjoint and active**:

```bash
make onboard TENANT=globex REGION=eu-west PLAN=standard
python -c "import json;d=json.load(open('ca/tenant_registry.json'));[print(v['tenant_id'],v['status']) for v in d.values()]"
# → acme   active
# → globex active
```

The auth-proxy resolver self-test proves the isolation gate end-to-end over a
real socket (valid cert → scoped; spoofed header → overridden; revoked/unknown →
403; no cert → never proxied):

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python auth-proxy/selftest.py ; echo "exit=$?"
# →   PASS valid acme cert → 200
# →   PASS client-set X-Scope-OrgID: globex overridden to acme
# →   PASS revoked cert → 403   /   PASS unknown cert → 403 (fail-closed)
# →   PASS no client cert → 403 (resolver fail-closed; never forwarded)
# → ALL AUTH-PROXY SELF-TESTS PASSED
# → exit=0
```

---

## Path B — Full live stack (Docker)

This brings up the real Mimir/Loki/Grafana/auth-proxy/console + the demo data
plane, then you watch a metric travel the full BYOC path and prove tenant
isolation in Grafana / Mimir directly.

### B.1 Bring it up

```bash
make bootstrap        # = ./bootstrap.sh : CA + signing + onboard acme + `docker compose up -d --build` + wait-for-health
```

(If you already ran `./bootstrap.sh --no-docker`, the trust material is staged;
`make up` alone will start the stack. `make bootstrap` is safe to re-run.)

Expected tail once images build and services pass health:

```
[bootstrap] waiting for core services to become healthy (console + grafana + mimir) …
[ ok ] Mimir ready (http://localhost:9009/ready)
[ ok ] Console ready (http://localhost:8080/healthz)
[ ok ] Grafana ready (http://localhost:3000/api/health)
[ ok ] CP self-obs Prometheus ready (http://localhost:9091/-/healthy)

╔══════════════════════════════════════════════════════════════════════════╗
║  Fyralis BYOC control plane is UP.                                        ║
╠══════════════════════════════════════════════════════════════════════════╣
║  Operator Console : http://localhost:8080      (fleet registry + health) ║
║  Grafana          : http://localhost:3000      ... admin / fyralis-operator
║  CP self-obs Prom : http://localhost:9091      (silence != health)       ║
║  Demo tenant      : acme  (golden-12 metrics flowing ...)                ║
╚══════════════════════════════════════════════════════════════════════════╝
```

### B.2 Wait for health / confirm services

```bash
docker compose -f docker-compose.control-plane.yml ps
```

Expected — these should be `running` (and `healthy` where a healthcheck exists):

| Service (container) | Port (host) | Role |
|---|---|---|
| `cp-auth-proxy` | `8443` | mTLS termination + X-Scope-OrgID injection (the gate) |
| `mimir` | `9009` | multi-tenant metrics store |
| `cp-loki` | `3100` | multi-tenant logs store (T2+) |
| `cp-grafana` | `3000` | operator dashboards |
| `console` | `8080` | fleet registry + health |
| `cp-demo-dataplane` | `9300` | golden-12 SLI stub (stands in for the real DP) |
| `cp-boundary-collector` | — | scrape → redact → identity-stamp → mTLS remote-write |
| `fyralis-agent` | — (no ports, I2) | outbound-only heartbeat |
| `cp-self-obs-exporter` | `9110` | independent watchdog prober |
| `cp-prometheus` | `9091` | "silence != health" watchdog |

> `restart: "no"` services (`audit`, `metering`, `mimir-ruler-loader`,
> `loki-init`, `cp-upgrade-tools`) run once and **exit 0** — that is expected,
> not a failure. The `audit` service's default command runs `audit verify` on the
> (empty on first boot) log and exits 0.

Direct readiness probes (no auth needed — these are the operator surfaces):

```bash
curl -fsS http://localhost:9009/ready        ; echo            # → ready
curl -fsS http://localhost:8080/healthz       | head -c120; echo  # → {"status":"ok","fleet_size":1}
curl -fsS http://localhost:3000/api/health    | head -c120; echo  # → {"database":"ok",...,"version":"11.1.0"}
curl -fsS http://localhost:9091/-/healthy     ; echo            # → Prometheus Server is Healthy.
```

### B.3 Open the Console (http://localhost:8080)

Open it in a browser. **What to look for:** an HTML fleet rollup table with **one
row** for the demo tenant `acme`, showing tenant / deployment id / version /
region / tier `T1` / a **green** health badge / a recent last-heartbeat age. The
green badge means the `fyralis-agent` container is heartbeating and the console
derived freshness ≤ 90s.

Same data via the API:

```bash
curl -fsS http://localhost:8080/api/v1/deployments | python -m json.tool
```

Expected (one record, health `green`, tier `T1`):

```json
[
  {
    "tenant_id": "acme",
    "deployment_id": "acme-use1-XXXX",
    "version": "1.0.0-...",
    "region": "us-east-1",
    "last_heartbeat_ts": "2026-06-24T...Z",
    "health": "green",
    "license_expiry": "2027-...Z",
    "telemetry_tier": "T1"
  }
]
```

### B.4 Open Grafana (http://localhost:3000)

Login: **`admin`** / **`fyralis-operator`** (override with `GF_ADMIN_USER` /
`GF_ADMIN_PASSWORD`). Three folders are provisioned:

- **Fyralis Fleet — Overview** (`fyralis-fleet-overview`): cross-fleet rollup —
  green/yellow/red deployment counts, a deployments table, and the golden-12
  fleet panels. Reads the **`Mimir (fleet)`** datasource (org id `__fleet__`, a
  tenant-federation cross-reader).
- **Fyralis Per-Customer — Drill-down** (`fyralis-tenant-drilldown`): pick a
  customer in the **`tenant_scope`** dropdown; that value is **also** the
  `X-Scope-OrgID` header, so every panel hard-scopes to that one tenant.
- **Control-Plane** folder: the **CP self-obs** watchdog dashboard (reads
  `cp-prometheus`, the independent "silence != health" stack).

**What to look for:** within ~30–60s of bring-up, the Fleet Overview shows
**1 green deployment** and the golden-12 panels (worker liveness, DLQ depth, LLM
breaker, schema version, …) start populating from the demo tenant. Open the
Per-Customer dashboard, select `acme` in the dropdown, and confirm the same
golden-12 series render scoped to acme.

> If panels are momentarily empty on first boot, give it ~60s: the boundary
> collector scrapes on an interval and Grafana datasources flap until Mimir is
> query-ready. Use the **`Mimir (fleet)`** datasource for any ad-hoc *Explore* —
> the per-customer `Mimir` datasource needs the `tenant_scope` variable, which is
> only set on its dashboard.

### B.5 PROVE the metric flow: data-plane → boundary → proxy → Mimir

The chain: `cp-demo-dataplane:9300` emits golden-12 `fyralis_*` SLIs →
`cp-boundary-collector` scrapes, applies the **I1 allowlist + PII-label drop**,
stamps `tenant_id=acme/deployment_id/region` (C4), and **remote-writes through
`https://auth-proxy:8443`** authenticating with **acme's mTLS client cert** → the
proxy verifies the cert, reads `tenant_id=acme` from its SPIFFE SAN, and
**injects `X-Scope-OrgID: acme`** → Mimir stores it under tenant `acme`.

**(a) Confirm the source is emitting** the golden-12 families:

```bash
curl -fsS http://localhost:9300/metrics | grep -E '^fyralis_' | head
# → fyralis_worker_expected_running 8
# → fyralis_dlq_unresolved 0
# → fyralis_schema_version 145
# → fyralis_llm_breaker_state{provider="deepseek",state="closed"} 1
# → ...
```

**(b) Confirm the boundary collector is up and pushing** (it has no ports; check
logs for the remote-write exporter, which should NOT be erroring):

```bash
docker compose -f docker-compose.control-plane.yml logs --tail=30 boundary-collector \
  | grep -iE 'remote_?write|error|Everything is ready' | tail
# → "Everything is ready. Begin running and processing data."  (and NO repeated 4xx/5xx remote-write errors)
```

**(c) Query the metric back from Mimir, TENANT-SCOPED as acme.** Mimir's HTTP API
is published on `:9009` for operator convenience; it requires `X-Scope-OrgID`
(multitenancy is on). Query as `acme`:

```bash
curl -fsS -H 'X-Scope-OrgID: acme' \
  'http://localhost:9009/prometheus/api/v1/query?query=fyralis_worker_expected_running' \
  | python -m json.tool
```

Expected — **acme's series IS present**, stamped with the C4 identity labels:

```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "fyralis_worker_expected_running",
          "tenant_id": "acme",
          "deployment_id": "acme-use1-XXXX",
          "region": "us-east-1",
          "telemetry_tier": "T1"
        },
        "value": [ 1750000000.0, "8" ]
      }
    ]
  }
}
```

> If `data.result` is `[]`, the boundary hasn't pushed a scrape yet — wait ~30s
> and re-run. The label set is the I1 proof: only bounded enums + the four C4
> identity labels survive; no ids/emails/urls/free-text.

**(d) (Optional) Prove the proxy is the ONLY identity source** — push as acme
through the proxy with the demo client cert and watch the proxy inject the scope.
This is the same path the boundary takes:

```bash
curl -sS -o /dev/null -w '%{http_code}\n' \
  --cacert _runtime/ca/ca.crt \
  --cert   _runtime/agent/client.crt \
  --key    _runtime/agent/client.key \
  --resolve auth-proxy:8443:127.0.0.1 \
  -H 'Content-Type: application/json' \
  --data '{"metric":"cto_probe","value":1}' \
  'https://auth-proxy:8443/api/v1/push'
# → 200    (the proxy verified acme's cert and forwarded with X-Scope-OrgID: acme)
```

`--resolve auth-proxy:8443:127.0.0.1` is needed because the proxy's server cert
SAN is `auth-proxy` (not `localhost`); pointing the hostname `auth-proxy` at
`127.0.0.1` lets the TLS handshake validate.

### B.6 PROVE isolation: query as a DIFFERENT tenant → empty

This is the security crux (I4). acme's series must be **invisible** to any other
tenant. Query the very same metric as `globex`:

```bash
curl -fsS -H 'X-Scope-OrgID: globex' \
  'http://localhost:9009/prometheus/api/v1/query?query=fyralis_worker_expected_running' \
  | python -m json.tool
```

Expected — **zero series** (globex has its own physically-partitioned tenant
space; acme's data simply does not exist there):

```json
{ "status": "success", "data": { "resultType": "vector", "result": [] } }
```

And the same proof through the **auth-proxy** with cert identity — a client that
presents acme's cert but *claims* `X-Scope-OrgID: globex` is **overridden to
acme** (the cert wins, never the header):

```bash
# acme's cert + a SPOOFED globex header → proxy strips the header, injects acme,
# so the query returns ACME's data, not globex's (I4).
curl -fsS \
  --cacert _runtime/ca/ca.crt \
  --cert   _runtime/agent/client.crt \
  --key    _runtime/agent/client.key \
  --resolve auth-proxy:8443:127.0.0.1 \
  -H 'X-Scope-OrgID: globex' \
  'https://auth-proxy:8443/prometheus/api/v1/query?query=fyralis_worker_expected_running' \
  | python -m json.tool
# → result is ACME's series (the spoofed globex header was ignored — cert SAN wins)
```

You can also prove it visually in Grafana: open **Per-Customer — Drill-down**,
select `globex` in `tenant_scope` (it exists if you onboarded it in A.4, else add
it) — every golden-12 panel is **empty**, because globex has pushed nothing.
Switch back to `acme` and the panels fill. One dashboard, hard-scoped per tenant.

### B.7 Break-glass + tamper-evident audit chain (I5)

The `audit` service is an on-demand CLI image over the **hash-chained,
ed25519-checkpointed** audit log and the **customer-granted, scoped, time-boxed,
audit-logged** break-glass workflow. Run it via `docker compose run --rm audit`
(state persists on the `audit-data` volume; the private signing key is mounted
read-only so each new chain head is signed).

**(a) The chain starts intact:**

```bash
docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json audit verify
echo "exit=$?"
# → CHAIN OK: <reason> | head=<hash>… | checkpoint-sig=valid
# → exit=0
```

(`checkpoint-sig=valid` because the private signing key is mounted; it would be
`n/a` if no key were present, and the hash chain would still hold.)

**(b) Break-glass: request → customer-approve → use (allowed) → over-broad scope
(denied).** Each transition is appended to the hash-chained log.

```bash
# 1. an operator REQUESTS a scoped, time-boxed grant (INERT until approved).
#    Grant ids are bg-<random hex> — capture the printed id into a shell var so the
#    next step is copy-paste runnable (do NOT hard-code an id; it changes every run).
GID=$(docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json \
  breakglass request --actor sre@fyralis --scope tenant:acme/logs:read --ttl 900 --reason inc-4127 \
  | grep -oE 'bg-[0-9a-f]+' | head -1)
echo "grant=$GID"
# → requested grant bg-XXXXXXXXXXXX for sre@fyralis scope='tenant:acme/logs:read' ttl=900.0s
# →   -> AWAITING CUSTOMER APPROVAL (inert until approved)
# → grant=bg-XXXXXXXXXXXX

# 2. the CUSTOMER approves it (starts the 900s time-box) — "customer-granted".
#    NOTE: the MVP does NOT authenticate the approver — `approved_by` is recorded as-is,
#    not verified or bound to the tenant; that is a next-sprint item (LIMITATIONS.md L-11).
docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json \
  breakglass approve --grant-id "$GID" --approved-by acme-admin@acme.com
# → approved bg-XXXXXXXXXXXX by acme-admin@acme.com; expires in 900.0s

# 3. access in-scope + in-window is ALLOWED (and the use is audited)
docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json \
  breakglass check --actor sre@fyralis --scope tenant:acme/logs:read ; echo "exit=$?"
# → ALLOW: <reason>
# → exit=0

# 4. a DIFFERENT / broader scope is DENIED (never authorizes a sibling scope)
docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json \
  breakglass check --actor sre@fyralis --scope tenant:bossco/logs:read ; echo "exit=$?"
# → DENY: <reason: no live grant for this scope>      (printed to stderr)
# → exit=1
```

**(c) The audit trail recorded every transition, and the chain still verifies:**

```bash
docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json audit list --limit 10
# → [   0] 2026-...Z  sre@fyralis          breakglass.request       -> tenant:acme/logs:read    {...}
# → [   1] 2026-...Z  acme-admin@acme.com  breakglass.approve       -> tenant:acme/logs:read    {...}
# → [   2] 2026-...Z  sre@fyralis          breakglass.use           -> tenant:acme/logs:read    {...}
# → [   3] 2026-...Z  sre@fyralis          breakglass.check_denied  -> tenant:bossco/logs:read  {...}
#   (seqs start at 0; the over-broad check in step (b)4 is audited as breakglass.check_denied —
#    each underlying JSONL line carries prev_hash / entry_hash, so the chain links)

docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json audit verify ; echo "exit=$?"
# → CHAIN OK: <reason> | head=<hash>… | checkpoint-sig=valid
# → exit=0
```

**(d) Tamper-detection demo (optional, proves it's tamper-EVIDENT).** Edit any
past entry in the log on the volume and re-verify — `verify` fails and pinpoints
the broken sequence:

```bash
# flip a byte in a past entry inside the volume, then verify:
docker compose -f docker-compose.control-plane.yml run --rm --entrypoint sh audit -c \
  "sed -i '1s/sre@fyralis/mallory@evil/' /data/audit.log.jsonl"
docker compose -f docker-compose.control-plane.yml run --rm audit \
  --log /data/audit.log.jsonl --store /data/breakglass_grants.json audit verify ; echo "exit=$?"
# → CHAIN BROKEN at seq 0: <reason: entry_hash / prev_hash mismatch>   (stderr)
#   (sed line 1 is seq 0 — seqs are 0-based)
# → exit=1
```

> A whole-file rewrite that re-links every hash would still fail the **signed
> checkpoint** (`<log>.checkpoint.json`) because an attacker without the CP
> private signing key cannot re-sign the new head — that is the second layer of
> tamper-evidence. To reset the demo log after tampering:
> `docker volume rm fyralis-control-plane_audit-data` (then it re-creates empty
> on the next `audit` run).

For the same flow without Docker, the committed self-test proves it end-to-end:

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python audit/selftest.py ; echo "exit=$?"
# → # 23/23 checks passed — ALL GREEN
# → exit=0
```

### B.8 Optional: the live-docker smoke

With the stack up you can run the smoke's docker-aware variant; the in-process
assertions still run for real and the docker-only steps are surfaced as
informative SKIPs (never silent passes):

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python tests/e2e_smoke.py --live
# → SMOKE PASSED — 52 passed, 0 failed, 3 skipped   (skips name the manual live gates)
```

### B.9 Tear down

```bash
make down            # stop the stack, KEEP volumes (Mimir/Grafana/audit data persists)
make clean           # full reset: down -v + wipe generated CA/keys/_runtime/.env, registry → {}
```

Expected `make clean` tail:

```
cleaned generated CA/keys/runtime material (ca/tenant_registry.json reset to {})
```

After `make clean` you are back to a pristine tree; re-run `make bootstrap` to
start over.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| **`docker not found` / `make bootstrap` bails to no-docker.** | Docker isn't installed / not on PATH. | Install Docker, or use **Path A** (`./bootstrap.sh --no-docker`, `make smoke`) which needs no containers. |
| **Every push/query through the proxy → `403`**, and the proxy log says `registry_read_error`. | The auth-proxy container (uid 10001) can't read `ca/tenant_registry.json` (host `0600`). | Re-run `./bootstrap.sh` (it `chmod 0644`s the gitignored demo registry). Manually: `chmod 0644 ca/tenant_registry.json`. |
| **Proxy starts then exits; log mentions the server key.** | The container can't read the `0600` host TLS key bind-mounted read-only. | Re-run `./bootstrap.sh` (it relaxes the gitignored dev keys). Manually: `chmod 0644 auth-proxy/tls/server.key _runtime/agent/client.key`. |
| **Mimir query → `HTTP 401` / "no org id".** | You omitted `X-Scope-OrgID`. Multitenancy is on — every Mimir request needs it. | Add `-H 'X-Scope-OrgID: acme'` (or `__fleet__` for cross-tenant). The auth-proxy injects it for the ingest path; on `:9009` you set it yourself. |
| **`data.result` is `[]` when querying `acme`.** | The boundary collector hasn't pushed a scrape yet (interval), or it's erroring. | Wait ~30–60s; check `docker compose logs boundary-collector` for repeated remote-write 4xx/5xx (→ usually the registry/key 403 above). |
| **Agent verify fails / smoke STEP 6/7 unexpectedly.** | The signing key was generated but not **activated** (no `active_key_id`). | Re-run `python signing/keygen.py --activate`, or `./bootstrap.sh`. Confirm: `python -c "import json;print(json.load(open('signing/trust_root.json'))['active_key_id'])"`. |
| **`curl https://localhost:8443/...` → TLS hostname / cert error.** | The proxy server cert SAN is `auth-proxy`, not `localhost`. | Use `--resolve auth-proxy:8443:127.0.0.1` and the URL `https://auth-proxy:8443/...` (as in B.5d/B.6). |
| **`ImportError` / wrong `lib`/`store`/`config` when running a python tool.** | Ran it from the wrong CWD; flat-module imports collide across dirs. | Run from the **control-plane root**. The smoke/selftests handle path priming; ad-hoc tools assume root CWD. |
| **`make up` → "network dataplane-net not found".** | The external data-plane network wasn't created. | `docker network create dataplane-net` (or use `make up`/`./bootstrap.sh`, which create it idempotently). |
| **`audit`/`metering`/`ruler-loader` show `Exited (0)` in `ps`.** | They are **one-shot** (`restart: "no"`), not daemons. | This is expected. Re-run on demand via `docker compose run --rm <svc> ...`. |
| **Grafana *Explore* on the per-customer `Mimir` datasource → Mimir rejects `${tenant_scope}`.** | The template variable is only bound on its dashboard, not in raw Explore. | Use the **`Mimir (fleet)`** datasource for Explore, or open the per-customer dashboard (which sets `tenant_scope`). |
| **Port already in use (3000/8080/8443/9009/9091/9300/...).** | Another process holds the host port. | Stop it, or remap in the compose `ports:`. Each published host port is unique by design. |
| **Loki panels empty for `acme`.** | T1 is **metrics-only** (C3). Logs exist only at T2+. | Expected. Opt a tenant up to T2 (config-dist) to see logs. |

---

## What each check proves → invariant map

| Check (where) | Proves | Invariant / contract |
|---|---|---|
| Onboard `acme` → bundle (cert+license), **one ACTIVE registry row keyed by cert fingerprint** (A.3, smoke STEP 2) | Atomic enrollment mints a real mTLS identity + signed license + the proxy binding; cert SAN = `spiffe://fyralis/tenant/acme`. | C1 (cert→tenant), C4 (registry row), FR-E (atomic onboard) |
| License **signature verifies + unexpired**; cert **verify-chains to the CA with clientAuth** (smoke STEP 1–2) | The trust roots are real and usable; everything shipped is ed25519-signed. | C2 / **I6** |
| Agent **heartbeats → console GREEN**; agent has **no listening socket** (B.3, smoke STEP 3) | The agent dials home **outbound-only**; health derived from heartbeat freshness. | **I2** (outbound-only), C4, NFR-5 |
| Metric `acme→boundary→auth-proxy→Mimir`, **proxy injects `X-Scope-OrgID: acme` from the cert SAN**, queried back present (B.5, smoke STEP 4) | Tenant identity is established **server-side from the verified client cert**, never a header; the full telemetry path works. | C1 / C5 / **I4** |
| Boundary-stamped series carry **only C4 identity + bounded enums** (no PII labels) (B.5c) | The default tier egresses **aggregated metrics only, zero PII**; the allowlist + label-drop hold. | **I1** (no PII at T1), C3 |
| Query as `globex` → **0 series**; a spoofed `X-Scope-OrgID` header is **overridden to the cert's tenant** (B.6, smoke STEP 5) | Cross-tenant reads are structurally impossible; the cert wins over any caller-supplied scope. | **I4** (tenant isolation server-side) |
| **Tampered license → agent denies** the privileged config pull (smoke STEP 6) | The agent verifies the signed license before privileged actions; a flipped byte breaks verification. | **I6** + license gate |
| **Signed config applied; tampered config rejected** and not written (smoke STEP 7) | Config is ed25519-signed and **verified before apply**; verify-on-read too. | **I6** |
| Data plane keeps running / agent **buffers + retries** across a console outage (agent design, `agent/selftest.py`) | The data plane survives a control-plane outage; heartbeats buffer with backoff, the loop never crashes. | **I3** |
| Break-glass **request → customer-approve → in-window allow → over-scope deny**, every step audited (B.7) | Emergency access is customer-granted, scoped, time-boxed, and audit-logged. | **I5** (break-glass) |
| Audit **chain verifies**, a flipped past entry is **detected** (and a full rewrite fails the **signed checkpoint**) (B.7d) | The "who did what" record is append-only + tamper-evident (hash chain + ed25519 checkpoint). | **I5** / C2 |
| `cp-prometheus` + `cp-self-obs-exporter` independently probe every CP service (B.2) | "Silence != health": an independent watchdog survives a fleet-pipeline outage and can alarm on silence. | NFR-10 |

---

## Quick reference — every command in one place

```bash
# --- Path A (no docker) ---
make smoke                         # 52 passed / 0 failed / 3 skipped → SMOKE PASSED, exit 0
./bootstrap.sh --no-docker         # persistent CA + signing + onboard acme + smoke

# --- Path B (live) ---
make bootstrap                     # CA + signing + onboard acme + docker up + wait-for-health
docker compose -f docker-compose.control-plane.yml ps     # services running/healthy
# Console:  http://localhost:8080      (acme, green, T1)
# Grafana:  http://localhost:3000      (admin / fyralis-operator)
# CP-Prom:  http://localhost:9091
curl -fsS http://localhost:9300/metrics | grep ^fyralis_ | head            # source SLIs
curl -fsS -H 'X-Scope-OrgID: acme'   'http://localhost:9009/prometheus/api/v1/query?query=fyralis_worker_expected_running' | python -m json.tool   # present
curl -fsS -H 'X-Scope-OrgID: globex' 'http://localhost:9009/prometheus/api/v1/query?query=fyralis_worker_expected_running' | python -m json.tool   # [] (isolation)
docker compose -f docker-compose.control-plane.yml run --rm audit --log /data/audit.log.jsonl --store /data/breakglass_grants.json audit verify    # CHAIN OK ... checkpoint-sig=valid
make down                          # stop, keep volumes
make clean                         # full reset
```
