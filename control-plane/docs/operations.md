# Fyralis BYOC Control Plane — Operations Runbook

> Operator runbooks for the lifecycle tasks: onboard / offboard a tenant, change a
> telemetry tier, run a release + canary rollout, push config, export metering, grant
> break-glass, and perform a zero-disruption upgrade. Grounded in the committed tree under
> `control-plane/`. See [`reference.md`](./reference.md) for per-component config/caveats and
> [`architecture.md`](./architecture.md) for the trust model. All commands run from
> `control-plane/`.

**Prerequisites for everything below.** The trust roots must exist. `bootstrap.sh` does
this idempotently, or run the two steps directly:

```bash
python ca/bootstrap_ca.py              # CA root + intermediate  → ca/pki/
python signing/keygen.py --activate    # ed25519 active key + public trust_root.json
```

The auth proxy needs its own server cert (`python auth-proxy/gen_server_cert.py`).
Operator/one-shot tools (`onboarding`, `licensing`, `metering`, `audit`,
`cp-upgrade-tools`) are profile-gated or `restart: no` — invoke them with `docker compose
run --rm <svc> ...` or as Python CLIs.

---

## 1. Bring the control plane up / down

```bash
./bootstrap.sh                 # first run: trust roots + onboard demo tenant acme + up
make up                        # build + up -d
make logs                      # tail the stack
make down                      # stop
make clean                     # full reset: down -v + wipe generated CA/keys/_runtime
./bootstrap.sh --no-docker     # offline: trust roots + demo onboard + python e2e smoke
make smoke                     # the in-process e2e smoke (no docker)
```

Surfaces after a successful `up`: Console http://localhost:8080, Grafana
http://localhost:3000 (`admin`/`fyralis-operator`), CP self-obs Prometheus
http://localhost:9091.

---

## 2. Onboard a tenant

Onboarding is an **all-or-nothing transaction** (`onboarding/onboard.py`): register → issue
cert → mint+sign license → assemble bundle → seed heartbeat → confirm listed. Any failure
rolls every side effect back (revoke+delete the registry row, rmtree the partial bundle).

```bash
# against a running console
python onboarding/onboard.py \
  --tenant globex --region eu-west --plan standard \
  --console-url http://console:8080 --json

# or the ops one-shot container
docker compose -f docker-compose.control-plane.yml \
  run --rm onboarding onboard --tenant globex --region eu-west --plan standard

# Makefile shortcut
make onboard TENANT=globex REGION=eu-west
```

**What you get.** A bundle at `onboarding/bundles/<deployment_id>/` containing the tenant
cert+key+chain, the **signed** license trio, a **signed** agent-config pointing the agent at
the console, the **public** `trust_root.json`, and `BUNDLE.json`. The tenant's `active` row
lands in `ca/tenant_registry.json` keyed by the cert fingerprint — **that row is the
auth-proxy binding** (the proxy now accepts this cert and 403s everything else).

**Deliver the bundle.** It contains a tenant private key (gitignored) — deliver it over a
secure channel, then run the customer-VPC installer with it (`installer/install.sh
<bundle-dir>`, or Helm/Terraform in prod).

**Verify.** The deployment appears in the console
(`GET /api/v1/deployments/{deployment_id}`); once the agent dials home it heartbeats green.

> Plan → features: `trial`=metrics; `standard`=metrics,logs,fleet-dashboards;
> `enterprise`=+traces,sso,audit-export. Plan features are product entitlements; the
> **telemetry tier** (default T1) independently gates egress at the boundary.

---

## 3. Offboard a tenant

```bash
python onboarding/offboard.py --tenant globex --deployment globex-euw1-1a2b \
  --console-url http://console:8080 --purge-bundle
```

`offboard.py` **revokes every active cert** for the tenant (the proxy 403s it immediately —
the registry is re-read per request), best-effort deregisters from the console, and with
`--purge-bundle`/`--purge-registry` removes the local bundle / deletes the rows.

**Caveat.** The P4 console contract has **no DELETE verb**, so against a *real* console the
deployment record can't be deleted on offboard — it ages to `red`/expired and is reaped by
the console operator. Revocation at the proxy is immediate regardless.

To revoke a single cert without a full offboard: `python ca/revoke.py revoke <tenant|fingerprint>`.

---

## 4. Change a tenant's telemetry tier

A tier change has **two halves**: the **enforcement** side (the boundary collector inside
the VPC) and the **record** side (the signed config the agent applies). Both are
config-only — **no redeploy**.

1. **Publish a new signed config version** (advances HEAD; the agent pulls + verifies +
   applies it, I6):
   ```bash
   python config-dist/publish_config.py <deployment_id> --tenant-id <tenant> --tier T2
   ```
2. **Update the boundary collector** to the same tier so the new signal class can physically
   egress. Set `FYRALIS_TELEMETRY_TIER=T2` and merge the `t2_increment.*` blocks from
   `boundary/tier_policy.yaml` (logs pipeline → redact → strip body → Loki via the proxy).
   For T3 also merge `t3_increment.*` (sampled, redacted traces → OTLP). **Down-tier** by
   removing the higher pipeline block(s).

Because a higher signal class has no receiver/exporter unless its block is present, the
boundary **physically cannot** egress above the configured tier (C3 by absence). Confirm
the new tier in the C4 record (the agent advertises `telemetry_tier` in its next heartbeat)
and in the console.

> T1 = metrics only, zero PII (the default and the only PII-free tier, I1). T2 adds logs
> that were **already redacted inside the VPC** before egress (Loki is the sink, not the
> redactor). T3 adds sampled, redacted traces.

---

## 5. Release + canary rollout

```bash
# 1. build + sign a deterministic release tarball (keys are excluded from the tarball)
python release/build_release.py build --src ./dataplane --version 1.4.3 --out ./_dist
python release/build_release.py verify ./_dist/fyralis-release-1.4.3.tar.gz

# 2. publish into the registry (re-verifies before publish; moves `latest`)
python release/publish.py publish ./_dist/fyralis-release-1.4.3.tar.gz --registry ./_registry
python release/publish.py serve   --registry ./_registry --port 8090   # agents pull + verify

# 3. canary → fleet rollout with halt-on-drift
python release/rollout.py promote --console http://localhost:8080 --version 1.4.3 \
  --canary-count 1 --watch-seconds 30 --poll-seconds 3
#   healthy canary → fleet promoted (exit 0)
#   drifted canary → HALT, fleet untouched, canary rolled back (exit 1)
python release/rollout.py rollback --console http://localhost:8080 --to 1.4.2
```

**How the controller decides.** It reads the fleet from the console (health derived on
read), deterministically picks a canary (lowest `deployment_id` first, always leaving a
gating remainder), promotes it, **watches** its health, and **halts immediately on any
canary non-green / missing-from-registry / window-expiry**, rolling the canary back; it
promotes the fleet only after a clean watch. By default **only `green`** counts as healthy
(`--tolerate-yellow` to accept a stale-but-not-dead canary).

**Caveats.** Promotion delivers a **version** + moves `latest`; the per-deployment signed
**bytes** are delivered by config-dist via the injected `Promoter` seam — a promotion can
never bypass the agent's verify-before-apply (I6). Heartbeat-freshness alone won't catch a
release that stays green but misbehaves under traffic — pair the rollout with fleet-SLI
burn alerts (`fleet-sli/`).

---

## 6. Push config to a deployment

```bash
python config-dist/publish_config.py <deployment_id> --tenant-id <tenant> --flag anomaly_detection_enabled=true
python config-dist/publish_config.py <deployment_id> --tenant-id <tenant> --rotation interval_hours=12
python config-dist/publish_config.py <deployment_id> --tenant-id <tenant> --config-file body.json
python config-dist/publish_config.py <deployment_id> --list
```

Each publish **appends a new immutable signed version** and advances HEAD (no redeploy) and
**self-verifies** before reporting success. The agent normally pulls HEAD
(`GET /config/<deployment_id>`) and applies only if the ed25519 signature verifies, the
`key_id` is known + not retired, and `artifact == "config"` — otherwise it keeps the
previous config. Old versions stay immutable at `/config/<id>/v<N>` for pinning/rollback.

> Point the agent's `AGENT_CONFIG_URL` at `http://config-dist:8090/config/<deployment_id>`
> (behind the proxy in prod). By default config-dist mints its **own** signing key — the
> agent must pin that service's `GET /trust_root.json`; to chain to one CP trust root, mount
> a shared keystore and set `CONFIG_DIST_SIGNING_HOME`/`CONFIG_DIST_KEY_ID`.

---

## 7. Metering / billing export

```bash
# compute + sign a per-tenant Tier-1 usage rollup for a period
PYTHONPATH=metering:signing python metering/rollup.py acme --month 2026-06 \
  --mimir-url http://localhost:9009 --out-dir /tmp/billing/acme-2026-06 --verify

# export signed rollups for billing (verifies each bundle first; refuses any that don't)
PYTHONPATH=metering:signing python metering/export.py \
  /tmp/billing/acme-2026-06 /tmp/billing/globex-2026-06 \
  --format csv --out /tmp/billing/2026-06.csv     # or --format json (system-of-record)
```

Metering reads **only aggregate Tier-1 counters** (obs-per-source, think runs, USD spend) —
no PII (I1). The rollup is ed25519-signed, so any later edit to a usage number breaks
verification (FR-F2); export is **fail-closed** (an unverifiable rollup is never billed) and
carries a per-row signature receipt.

**Before trusting a zero rollup**, cross-check the C4 heartbeat (`console`): a deployment
down for part of the period under-counts, and a never-reporting one yields a valid all-zero
bill — distinguish "no usage" from "no telemetry". `think_cost_usd` is the data plane's
self-reported spend; reconcile against the provider bill if the contract requires.

---

## 8. Break-glass grant (I5)

Break-glass is **customer-granted, scoped, time-boxed, and audit-logged**. A request is
**inert** until a **customer** principal approves it (which starts the time-box).

```bash
# 1. an SRE requests a scoped, time-boxed grant (INERT)
python audit/cli.py breakglass request --actor sre@fyralis \
  --scope tenant:acme/logs:read --ttl 900 --reason inc-4127
#    → prints grant id bg-xxxxxxxxxxxx

# 2. the CUSTOMER approves it — this starts the 900s clock
python audit/cli.py breakglass approve --grant-id bg-xxxxxxxxxxxx --approved-by acme-admin@acme.com

# 3. each access is checked + AUDITED; allowed only while approved + unexpired + in-scope
python audit/cli.py breakglass check --actor sre@fyralis --scope tenant:acme/logs:read   # exit 0 ALLOW / 1 DENY

# kill it early, or expire elapsed grants now
python audit/cli.py breakglass revoke --grant-id bg-xxxxxxxxxxxx --revoked-by ops@fyralis
python audit/cli.py breakglass sweep
```

Every transition (request/approve/deny/**use**/expire/revoke/denied-check) is appended to
the **hash-chained, signed-checkpoint** audit log. Verify the trail any time:

```bash
python audit/cli.py audit verify     # exit 0 = chain OK; 1 = tampered (prints the bad seq)
python audit/cli.py audit list --limit 20
```

> Mount the active CP private key read-only on the audit host for whole-file
> tamper-evidence (without it the log still hash-chains but a full rewrite would go
> undetected). Expiry is wall-clock with no skew grace (fails toward expiring sooner — by
> design for short-lived, high-stakes access).
>
> **MVP caveat — approver identity is NOT yet authenticated.** `approve_grant` records the
> `approved_by` string as-supplied; it does not verify the approver is the customer principal
> owning the grant's tenant. The grant is genuinely scoped + time-boxed + audit-logged, but
> authenticating the approver and binding identity-to-tenant is delegated to the
> console/auth-proxy and tracked as a next-sprint item (`LIMITATIONS.md` L-11).

---

## 9. Zero-disruption upgrade (NFR-6)

The data plane keeps running through a CP gap because the agent buffers/retries (I3), so a
rolling/blue-green window is invisible to the customer.

**Stateless services (auth-proxy → config-dist → console), one at a time, health-gated:**
```bash
DRY_RUN=1 ./upgrade/rolling_upgrade.sh     # preview
./upgrade/rolling_upgrade.sh               # real: pre-gate → recreate → post-gate → auto-rollback on failure
```
Not-yet-wired services are **skipped with a warning**; stateful mimir/loki/grafana are
**refused with exit 2** (they take the blue-green path).

**CA rotation with trust overlap (FR-A5) — add the new CA BEFORE rotating agents:**
```bash
./upgrade/trust_overlap.sh add    --new-ca ca/pki-new/ca-chain.crt   # proxy now trusts {old,new}
./upgrade/trust_overlap.sh verify --leaf  /path/to/existing-agent-leaf.crt
# ... rotate each agent's cert at its own pace; both CAs verify ...
./upgrade/trust_overlap.sh remove --root-cn "Fyralis Root CA"        # ONLY after the console shows all rotated
```
The helper enforces the one safe backstop (never empty the bundle) but cannot know whether
your **fleet** has fully rotated — confirm via the console before `remove`.

**Stateful Mimir/Loki:** follow `upgrade/UPGRADE_RUNBOOK.md` §3 — blue-green on **shared
object storage** (S3/GCS) with the remote-write cut-over ordering that never drops a sample.
On the dev single-host stack (local volumes) prefer the rolling stateful variant.

---

## 10. Watch the watchers (self-obs)

The CP monitors itself independently ("silence != health"):

- Exporter metrics: `http://localhost:9110/metrics` (`cp_service_up`, probe latency,
  `cp_ingest_path_alive`, the scrape heartbeat).
- CP Prometheus: `http://localhost:9091` — alerts in `self-obs/cp_rules.yml` (auth-proxy
  down, Mimir/Loki unreachable, console/config-dist/release down, ingest-path down, and the
  critical `ControlPlaneSelfObsSilent`/`...Stale` **silence** pages).
- Grafana: the **Control-Plane** dashboard folder (`cp_self.json`), reading the dedicated
  CP-Prometheus datasource (`uid fyralis-cp-prometheus`).

Route `severity=page, scope=control-plane` (especially `silence="true"`) to the on-call
pager. auth-proxy liveness comes solely from the exporter's TLS-handshake probe — never add
a direct `auth-proxy:8443` Prometheus scrape job (it is mTLS-only and would read as
permanently down).
