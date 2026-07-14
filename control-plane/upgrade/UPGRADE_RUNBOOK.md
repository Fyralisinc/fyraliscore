# Control-Plane Upgrade Runbook (NFR-6: zero-disruption upgrade/migration)

> **Goal.** Upgrade or migrate the Fyralis BYOC **control plane** with **no fleet
> disruption**: every customer data plane keeps processing its own data, no
> in-flight agent mTLS handshake breaks, and no remote-write / heartbeat is
> dropped. This is **NFR-6** (zero-disruption CP upgrade) and it leans on
> **FR-A5** (non-disruptive CA rotation/revocation) and **I3** (the data plane
> survives a CP outage).
>
> **Audience.** A control-plane operator running the upgrade.
> **Scope.** This directory (`control-plane/upgrade/`) ships the procedure +
> tooling. It imports the committed siblings (`ca/`, `signing/`, the master
> compose) and **edits none of them** — the upgrade tools live entirely here.

---

## 0. The single idea that makes this safe

The control plane splits cleanly into two kinds of service, and we upgrade each
kind differently:

| Kind | Services | Upgrade method | Why |
|------|----------|----------------|-----|
| **Stateless** | `auth-proxy`, `config-dist`, `console` | **Rolling**, one at a time, health-gated (`rolling_upgrade.sh`) | No durable in-process state; a restart only drops in-flight connections that the agent retries. |
| **Stateful** | `mimir`, `loki` (`grafana` is config-stateful) | **Blue-green** (or careful rolling) on **shared object storage**, with **remote-write ordering** that never drops a sample | They own the metrics/logs store; a naive recreate can drop in-flight remote-write or lose un-flushed blocks. |

And one cross-cutting safety net underneath both:

> **I3 — the data plane survives a CP outage.** The agent (`agent/`) is
> outbound-only and **buffers** telemetry/heartbeats to a durable on-disk queue
> (`agent/buffer.py`, JSONL) and **retries** with backoff. It also keeps the
> last **verified** config/license and keeps running on them. So a control-plane
> service that is briefly down — exactly what a rolling restart is — is **invisible
> to the customer's workload and re-converges automatically** when the CP comes
> back. The upgrade does not need a maintenance window for the fleet.

Because of I3, "zero disruption" does **not** require zero CP downtime per
instance; it requires that each gap is **short, bounded, and absorbed by the
agent's buffer**. The rolling + blue-green procedures below keep every gap inside
the agent's retry/buffer envelope.

---

## 1. Pre-flight (run before any change)

1. **Sign the release/config you are rolling to (I6).** Everything shipped is
   ed25519-signed via `signing/`. Build + sign the new release/config bundle and
   confirm `signing/verify_bundle.py verify <artifact>` passes against the
   keyring. The CP never deploys an artifact it has not itself signed; the agent
   *verifies before apply* on the other side.

2. **Snapshot the fleet baseline.** `GET console:8080/api/v1/deployments` and
   record every deployment's `health`, `version`, `last_heartbeat_ts`. This is
   your "all green before, all green after" gate. (Health is derived on read from
   heartbeat freshness — NFR-5 — so a momentarily stale row during the roll is
   expected and self-heals.)

3. **Confirm the agent buffer headroom.** The agent's durable buffer
   (`AGENT_BUFFER_PATH`, default 10 000 records) must comfortably hold the
   heartbeats that will queue during the longest single-service gap. At the
   default 30 s heartbeat interval, even a multi-minute roll queues only a handful
   of records — far inside the cap. This is the concrete I3 margin.

4. **Back up the trust bundle + registry.** `ca/pki/ca-chain.crt` and
   `ca/tenant_registry.json`. (`trust_bundle.py` also makes a timestamped `.bak`
   on every write, but take an explicit one too.)

5. **Verify config-as-code is the source of truth.** Every CP service is defined
   declaratively: service topology in `docker-compose.control-plane.yml`,
   Mimir/Loki in their `*.yaml`, Grafana via `grafana/provisioning/`, fleet-SLI
   rules in `fleet-sli/*.yml`, the trust bundle in `ca/pki/`, the keyring in
   `signing/trust_root.json`. **The upgrade changes these files, commits them,
   then applies them** — never a hand-edit on a live container. Roll-back ==
   re-apply the previous commit.

---

## 2. Stateless rolling upgrade (`auth-proxy`, `config-dist`, `console`)

**Tool:** `./rolling_upgrade.sh` (this directory).

It rolls the stateless services **one at a time**, **health-gating between every
step**, and **stops + rolls back** the moment a service fails its gate. The order
is fixed and intentional:

```
auth-proxy  ->  config-dist  ->  console
```

Per service it does: **pre-gate** (don't roll onto a sick baseline) →
**pull/build** the new image → **`docker compose up -d --no-deps <svc>`** (recreate
*only* that container; peers keep serving) → **post-gate** (poll the service's
health until `healthy`, else auto-rollback).

```bash
cd control-plane

# Dry-run the plan first (changes nothing):
DRY_RUN=1 ./upgrade/rolling_upgrade.sh

# Roll all stateless services, health-gated, one at a time:
./upgrade/rolling_upgrade.sh

# Or a subset / explicit order:
./upgrade/rolling_upgrade.sh auth-proxy console
```

### Why each stateless roll is non-disruptive

- **`auth-proxy`** — a stateless mTLS terminator. Recreating it drops only the
  connections *on that instance*; the agent dials **outbound** and simply retries
  the next heartbeat/poll. **Critically, before you ever roll a proxy that trusts
  a new CA, you must have run the trust-overlap `add` step (§4) so the new proxy
  still trusts every cert already in the field.** A proxy that came up trusting
  only a *new* CA would 403 every in-flight agent — that is the disruption the
  overlap exists to prevent.
- **`config-dist`** — a stateless publisher of **signed** config bundles. Agents
  *pull* on their own loop and *verify before apply* (I6). A short gap only delays
  one poll; nothing is applied unsigned, nothing breaks.
- **`console`** — a stateless API over the fleet registry. The registry is a
  **mounted volume** (`console-data`), not in-process state, so a restart loses
  nothing. Heartbeats that miss the restart window are **buffered by the agent and
  replayed in order** (I3) — the fleet view re-converges within one heartbeat
  interval.

### Health-gating contract

`rolling_upgrade.sh` reads each service's state via `docker inspect` off the
compose-assigned container id:

- A service **with** a compose `healthcheck` (e.g. `console`'s `/healthz`,
  `loki`'s `/ready`) must report `healthy` before the next service is rolled.
- A service **without** a healthcheck must reach and *stay* `running` through a
  short settle window (so a crash-on-boot is caught, not reported healthy).
- `HEALTH_TIMEOUT` (default 120 s) bounds the wait; on timeout/`unhealthy` the
  step **fails, auto-rolls-back** (`--force-recreate` to the last good compose
  definition), re-gates, and the run **stops** — one-at-a-time means we never
  proceed past a sick service.

> The script **refuses to roll stateful services** (`mimir`, `loki`, `grafana`).
> Asking it to do so exits `2` and points here. Those use §3.

---

## 3. Stateful migration (`mimir`, `loki`) — shared object storage + ordering

Mimir and Loki own the metrics/logs store. A naive `up -d` recreate risks: (a)
losing the un-flushed in-memory head / WAL, and (b) dropping the remote-write
samples that arrive during the gap. We avoid both with **shared object storage**
plus **strict ordering**.

### 3.1 Shared object storage is the enabler

In production, Mimir `blocks_storage.backend` and Loki `object_store` are **S3/GCS
(or any S3-compatible bucket)**, *not* the local filesystem the dev compose uses
(`mimir/mimir.yaml` → `backend: filesystem`; `loki/loki.yaml` → `object_store:
filesystem`). Because the durable blocks/chunks/index live in a **shared bucket**,
a **new (green) Mimir/Loki instance reads exactly the same data the old (blue) one
wrote** — there is nothing to copy and no data to migrate between instances. The
store *is* the bucket; the compute is disposable. This is what makes blue-green
(or zero-copy rolling) possible for a stateful store.

> **Migration-readiness gate:** before the first stateful upgrade, confirm both
> stores point at a shared bucket (not a per-container volume). On the dev
> single-host stack they use `mimir-data` / `loki-data` named volumes — fine for
> dev, but a **blue-green swap there shares the volume**, so blue and green must
> not run two writers against the same local TSDB head simultaneously. Prefer the
> rolling variant (§3.3) on the filesystem backend; reserve true blue-green for
> the object-storage backend.

### 3.2 Blue-green (object-storage backend) — recommended for major upgrades

Standing up a *new* version alongside the old, then cutting traffic over:

1. **Flush the blue head.** Trigger/await a TSDB block flush so the bucket holds
   the latest blocks (Mimir flushes head→blocks on a timer; for a clean cut you
   may shorten the flush or wait one block period). Loki's ingester flushes
   chunks to the bucket on its own interval; await a flush.
2. **Bring up green** (new image) **pointed at the same bucket**, with ingestion
   (remote-write) still routed to **blue**. Green replays the bucket + its own WAL
   and reaches `/ready`.
3. **Health-gate green** on `GET /ready` (Mimir) / `/ready` (Loki). Do **not**
   proceed until green is ready and serving queries off the shared bucket.
4. **Cut remote-write to green, then queries** (ordering in §3.4). Blue keeps
   running, draining its in-flight requests.
5. **Drain + retire blue** after a soak (one block/chunk-flush period) so any
   sample blue still held is flushed to the shared bucket and green has picked it
   up. Then stop blue.

Because blue and green share the bucket, **no historical data moves** and queries
are continuous across the cut.

### 3.3 Rolling (single-instance dev / filesystem backend)

When you cannot run two writers (single-host, filesystem volume):

1. **Pause/queue ingestion at the boundary, not at the store.** The boundary OTel
   Collector (customer VPC) and the agent buffer hold telemetry while the store is
   briefly down — that is the I3 envelope again. Remote-write retries; nothing is
   dropped as long as the gap stays inside the agent/collector retry window.
2. **Stop → recreate the store on the new image → await `/ready`.** The named
   volume (`mimir-data` / `loki-data`) carries the WAL + blocks across the
   restart, so on restart Mimir/Loki replay the WAL and lose nothing committed.
3. **Re-gate on `/ready`** before declaring the store back. The agent then flushes
   its buffered backlog in order.

> **Never** delete the data volume / bucket prefix as part of an upgrade. The
> store is the durable asset; the binary is replaceable.

### 3.4 Remote-write ordering that avoids dropping samples

The cut-over order is what prevents a dropped remote-write:

```
1. green /ready  (it can ACCEPT writes)         <-- gate, do not skip
2. switch the WRITE path (remote-write target) from blue -> green
3. switch the READ path (queries/Grafana datasource) from blue -> green
4. keep blue alive through one flush period (soak), THEN retire it
```

Rationale, step by step:

- **Writes flip only after green is `/ready`.** If you flipped writes to a
  not-ready green, those samples would be refused → the *sender* (Prometheus
  remote-write / the agent) **retries with backoff and replays from its WAL/buffer**,
  so they are not lost — but you would burn retry budget for nothing. Gating first
  keeps the flip clean.
- **Writes flip before reads.** Once green accepts writes, new samples land in
  green's head (and the shared bucket). Flipping reads *after* means queries never
  point at an instance that is missing the freshest data.
- **Blue soaks before retirement.** Blue may still hold an un-flushed head;
  keeping it for one flush period lets it flush to the shared bucket so green sees
  the full series. Retiring blue before the flush is the classic way to lose the
  last block.
- **The remote-write sender is the durability backstop.** Both Prometheus
  remote-write and the Fyralis agent keep an outbound WAL/buffer and **replay on
  reconnect**. So even a small ordering hiccup degrades to *delayed* delivery, not
  *lost* delivery — provided the gap stays inside the sender's retention. This is
  the same I3 property, applied to the metrics path.

`X-Scope-OrgID` is unaffected by any of this: it is injected by the auth-proxy
from the verified cert SAN (C1/C5), independent of which Mimir/Loki instance is
behind it, so per-tenant isolation holds across the swap.

### 3.5 Grafana / fleet-SLI rules (config-stateful)

Grafana is upgraded like a stateless service **but** its datasources + dashboards
are **config-as-code** under `grafana/provisioning/` and `grafana/dashboards/`;
its only durable state (`grafana-data`: annotations/prefs) rides a volume. The
**fleet-SLI rules** (`fleet-sli/*.yml`) are not read off disk — they are **pushed**
into Mimir's `__fleet__` ruler tenant via `mimirtool rules load` (the
`mimir-ruler-loader` one-shot). After a Mimir upgrade, **re-run the ruler-loader**
so the golden-12 recording/alerting rules are present on the new instance.

---

## 4. Trust-chain overlap during cutover (FR-A5) — in-flight mTLS never breaks

Full detail + the helper API are in **[`trust_overlap.md`](./trust_overlap.md)**.
The runbook-level summary:

When the upgrade rotates the **CA** (new issuing CA, or revoking + replacing the
old one), the auth-proxy's trust bundle (`ca/pki/ca-chain.crt`, loaded via
`ssl.load_verify_locations`) must trust **both** the old and the new CA *during*
the cutover, so an agent whose current client cert was signed by the **old** CA
keeps verifying while agents migrate to **new**-CA certs at their own pace.

**Tooling:** `trust_bundle.py` (helper) + `trust_overlap.sh` (operator front-end),
both in this directory. They reuse the committed `ca/verify_chain.py` (the *same*
verifier the proxy resolver uses) and `signing/` (sign the bundle, I6).

**The ordering — do it in exactly this order:**

```bash
cd control-plane

# 1. ADD the new CA to the bundle  => proxy trusts {old, new}.  Sign it (I6).
./upgrade/trust_overlap.sh add --new-ca ca/pki-new/ca-chain.crt
#    (this re-signs the bundle and rolls JUST the auth-proxy so it loads it)

# 2. PROVE the overlap: an OLD-CA leaf still verifies against the new bundle.
./upgrade/trust_overlap.sh verify --leaf /path/to/an-existing-agent-leaf.crt

# 3. ... now start issuing NEW-CA leaves (ca/issue_cert.py) and rotate agents.
#        Each agent re-enrolls on its own schedule; both CAs are trusted, so no
#        agent is ever locked out mid-rotation.

# 4. Once EVERY active agent presents a NEW-CA leaf, REMOVE the old CA:
./upgrade/trust_overlap.sh remove --root-cn "Fyralis Root CA"   # the OLD root
#    (re-signs + rolls the auth-proxy again => proxy now trusts {new} only)
```

**The two rules you must not break:**

1. **`add` (and its proxy reload) happen BEFORE you issue/rotate to the new CA.**
   Issuing first means an agent could present a new-CA leaf to a proxy that does
   not yet trust it → 403.
2. **`remove` happens only AFTER every active agent has rotated.** Removing first
   means an agent still on an old-CA leaf gets locked out → disruption. The helper
   also **refuses to leave the bundle empty** as a backstop.

Revocation of an *individual* tenant (not a CA rotation) does **not** touch the
bundle at all: it flips that fingerprint to `status:"revoked"` in
`ca/tenant_registry.json`, which the proxy re-reads **fresh every request** — so
it is instantaneous and inherently non-disruptive to *other* tenants.

---

## 5. Post-upgrade verification

1. **Stateless health.** `rolling_upgrade.sh` already gated each service; confirm
   `docker compose -f docker-compose.control-plane.yml ps` shows them `healthy`.
2. **Stateful readiness + continuity.** `GET mimir:9009/ready` and
   `GET loki:3100/ready` return ready; run a query that spans the cut-over instant
   (e.g. last 15 min) and confirm **no gap** in a known series — that proves no
   remote-write was dropped.
3. **mTLS continuity.** `./upgrade/trust_overlap.sh verify --leaf <a-live-leaf>`
   passes for an agent that did **not** rotate during the window (proves overlap
   held).
4. **Fleet re-convergence (I3).** `GET console:8080/api/v1/deployments` — every
   deployment is back to `green` within ~one heartbeat interval, and any agent
   that buffered during the roll shows a **fresh** `last_heartbeat_ts` (its backlog
   flushed). No deployment went `red`.
5. **Signing intact (I6).** A freshly published config/release `verify`s against
   the keyring; the agent applied it (audit log shows an `applied`, not a
   `rejected`).
6. **Ruler rules present.** `mimirtool rules list --id=__fleet__` shows the
   golden-12 groups after a Mimir upgrade.

---

## 6. Rollback

Because everything is **config-as-code** and signed:

- **Stateless:** re-run `rolling_upgrade.sh` against the **previous** committed
  compose / image tags — same health-gated, one-at-a-time path. (A *single* failed
  step already auto-rolls-back in place.)
- **Stateful:** with shared object storage, point the **previous** image's green
  back at the same bucket and cut over with the §3.4 ordering — no data to restore.
  With the filesystem backend, recreate from the prior image against the same
  volume (the WAL/blocks are unchanged).
- **Trust bundle:** restore the timestamped `.bak` the helper wrote (or the
  previous committed `ca/pki/ca-chain.crt`) and roll the proxy. The bundle is
  signed, so a restored bundle is verifiable before load.

---

## 7. Invariants this procedure preserves

| Inv | How the upgrade preserves it |
|-----|------------------------------|
| **I1** No PII at T1 | Untouched — the boundary collector + tier enforcement are unchanged by a CP roll. |
| **I2** No inbound to customer VPC | The upgrade only restarts vendor-side services; nothing dials into a data plane. The `cp-upgrade-tools` container publishes no ports. |
| **I3** Data plane survives CP outage | The whole reason rolling/blue-green is safe: the agent buffers + retries across every gap; the data plane keeps processing throughout. |
| **I4** Tenant isolation at the proxy | `X-Scope-OrgID` is injected from the cert SAN regardless of which proxy/Mimir instance serves the request; trust-overlap keeps the *cert* path valid across CA rotation. |
| **I6** Sign + verify everything | New release/config/trust-bundle are ed25519-signed before deploy; the agent verifies before apply; the trust bundle itself is signed + verifiable. |

---

## 8. Files in this directory

| File | Purpose |
|------|---------|
| `UPGRADE_RUNBOOK.md` | **This** procedure. |
| `trust_overlap.md` | Deep dive on the CA trust-overlap + the `add`/`remove` ordering. |
| `trust_bundle.py` | Helper: add/remove/list/verify/sign a CA trust bundle (reuses `ca/verify_chain.py` + `signing/`). |
| `trust_overlap.sh` | Operator front-end for the overlap dance (+ auto proxy reload). |
| `rolling_upgrade.sh` | Health-gated, one-at-a-time rolling restart of the **stateless** CP services. |
| `service.compose.yml` | On-demand `cp-upgrade-tools` operator container (overlay; does not edit the master compose). |
| `selftest.py` | Proves trust-overlap end-to-end + that the docs/scripts/YAML hold. |
| `README.md` | Quick start + caveats. |
