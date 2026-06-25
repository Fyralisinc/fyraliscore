# Fyralis BYOC Control Plane — Console Roadmap

> **What can be added to the operator Console to turn it from a read-only
> dashboard into a true control surface.** This is a forward-looking design
> catalog, not a record of shipped work — every feature below is a *proposal*
> grounded in an engine that already exists under `control-plane/`. Read
> [`architecture.md`](./architecture.md) for the two-plane model + the six
> invariants, [`reference.md`](./reference.md) for the components these features
> build on, and [`LIMITATIONS.md`](./LIMITATIONS.md) for what is deliberately
> deferred today.
>
> Status: **PROPOSAL / BACKLOG.** Nothing here is implemented yet. Priorities and
> effort are the author's estimate, to be ratified before scheduling.

All paths are relative to `control-plane/`.

---

## 1. Where the Console is today

The Console (`console/app.py`, served on `:8080`) is an **observability surface**:
it renders an operator rollup over the C4 deployment registry and derives health
on read (`stale > 90s → yellow`, `missing > 300s → red`, NFR-5).

Current API surface:

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/api/v1/register` | write-token (I4) | agent/onboarding creates a deployment row |
| `POST` | `/api/v1/heartbeat` | write-token (I4) | agent reports actual state (version, health, SLIs) |
| `GET` | `/api/v1/deployments[/{id}]` | open (operator LAN) | read the fleet |
| `DELETE`| `/api/v1/deployments/{id}` | write-token (I4) | deregister (offboard) |
| `GET` | `/` , `/healthz` | open | HTML rollup + liveness |

Every write is **agent → console** (the data plane reporting *up*). The operator
can **look** but cannot **act**: there is no path for an operator to change
anything about a running deployment. That is the gap this roadmap closes.

---

## 2. The governing principle: desired-state reconciliation over an outbound-only channel

One constraint shapes every feature below:

> **The agent is outbound-only (I2). The control plane can never open a
> connection *into* a customer VPC.**

So the Console cannot be a remote-exec terminal. Instead it becomes a
**desired-state reconciliation surface**, the GitOps / Kubernetes-controller
pattern adapted to BYOC:

1. an operator writes **desired state** for a deployment (config vN, release vX,
   tier T2, license = suspended, action = backfill);
2. the agent **pulls** that desired state on its next heartbeat (outbound);
3. the agent **verifies** it (ed25519 signature, I6) and applies it;
4. the agent reports **actual state** back via heartbeat;
5. the Console renders **drift** (desired ≠ actual) until they converge.

This single pattern is invariant-preserving by construction:

| Invariant | How the pattern honors it |
|---|---|
| **I1** no PII at T1 | desired state is config/control metadata, never customer data |
| **I2** agent outbound-only | the agent *pulls*; the console never dials in |
| **I3** data plane survives CP outage | desired state is advisory; a stale console just means no new changes, the data plane keeps running last-applied state |
| **I4** server-side tenant isolation | writes are bearer-authenticated; desired state is scoped per `deployment_id` (and, server-side, per cert/tenant) |
| **I5** break-glass + audit | every desired-state mutation is appended to the hash-chained audit log |
| **I6** sign + verify | desired config/release/license is signed by the CP signing key; the agent refuses unsigned/relabeled/wrong-key payloads |

**Implication for the data model (§4): the registry must carry a *desired* facet
next to the *actual* facet it stores today.** That one extension powers most of
the features below.

---

## 3. Feature catalog

Grouped by theme. Each entry: **What** it does · **Why** it matters in BYOC ·
**Builds on** (the engine that already exists) · **New pieces** to add ·
**Invariant notes** · **Effort / priority**.

Effort is T-shirt (S/M/L); priority is P0 (flagship) → P3 (nice-to-have).

### A. Operate the fleet (desired-state writes)

#### A1 · Remote config push  — **P0, M** ⭐ flagship
- **What:** edit a deployment's config (telemetry tier, heartbeat/scrape
  interval, sampling, redaction toggles, feature flags) in the Console; it is
  signed and staged; the agent pulls + verifies + applies; the Console shows
  applied-vs-desired drift.
- **Why:** the single most-requested BYOC capability — reconfigure a customer's
  deployment **without entering their VPC**. Turns "I can see my fleet" into "I
  can operate my fleet."
- **Builds on:** `config-dist/` (already serves signed, versioned config bundles;
  the agent already has `config_pull` that verifies before apply — see
  [`reference.md`](./reference.md)).
- **New pieces:** console write-endpoint `PUT /api/v1/deployments/{id}/desired-config`;
  a small "edit → sign → stage" UI; a `desired_config_version` column; a drift
  view (desired vs `applied_config_version` from heartbeat).
- **Invariants:** I6 (signed), I2 (pull), I5 (audited). Config schema must stay
  PII-free (I1).
- **Smallest viable demo:** flip acme's telemetry tier T1→T2 from the Console and
  watch the agent apply it.

#### A2 · Fleet release / upgrade orchestration — **P1, L**
- **What:** see version skew across the fleet; promote a release to a **canary**
  subset; auto-roll-forward on green SLIs, auto-rollback on breach; ring-by-ring
  promotion.
- **Why:** coordinating upgrades across N customer VPCs is the hardest BYOC ops
  problem. Today it is a CLI; the Console makes it observable + governed.
- **Builds on:** `release-registry/` (signed release artifacts + `latest`
  pointer) and `release/rollout.py` (the canary controller already exists as a
  CLI with health-gated promoters).
- **New pieces:** a `desired_release` column; a console UI over `rollout.py`
  (pick a canary %, watch health, promote/halt); wire the controller's
  health gate to the Golden-12 SLIs.
- **Invariants:** I6 (release signed + verified by the agent before swap), I3
  (rollback is desired-state, never a forced push).

#### A3 · Action queue (bounded pull-based remote ops) — **P2, M**
- **What:** enqueue a small, typed, signed command for a deployment that the
  agent pulls and executes: `trigger-backfill`, `force-reconcile`,
  `rotate-secret`, `flush-dlq`, `re-pull-config`. Bounded allowlist, never
  arbitrary exec.
- **Why:** "do one specific thing to that deployment" without inbound access —
  the safe, audited substitute for SSH.
- **Builds on:** the config-pull channel (rides the same outbound poll).
- **New pieces:** a typed `pending_actions[]` per deployment; agent-side handlers
  for each action; ack/result reported on heartbeat.
- **Invariants:** I6 (each action signed), I5 (every action audited), I2 (pull).
  Keep the action set a **closed allowlist** — no generic shell.

#### A4 · Telemetry-tier self-service — **P2, S**
- **What:** operator (or the customer, in a customer-facing console) dials a
  deployment between **T1 / T2 / T3** (more or less telemetry).
- **Why:** the design promises customer-configurable telemetry tiers; this is the
  control for it. A customer worried about T2 logs can stay T1; one that wants
  deep support can opt up.
- **Builds on:** A1 (it is one field of the config) + the boundary collector's
  tiered redaction.
- **Invariants:** I1 (tier gates *what* leaves the VPC), I6.

### B. Lifecycle

#### B1 · Onboard from the UI — **P1, M**
- **What:** "**+ Onboard tenant**" wizard: name, region, plan → mints the signed
  bundle and shows the customer their one-line install command / bundle download.
- **Why:** today onboarding is an operator shell script (`onboard.py`). A UI makes
  it a self-serve, repeatable, audited flow — and is the seam for a future
  customer self-signup.
- **Builds on:** `onboarding/onboard.py` (atomic onboard with rollback already
  exists) + `installer/` (the customer-side bundle).
- **New pieces:** console endpoint wrapping `onboard.py`; a bundle-handoff view.
- **Invariants:** I4 (cert SAN → tenant), I5 (audited), I6 (bundle signed).

#### B2 · Offboard from the UI — **P1, S**
- **What:** a guarded "decommission" action that runs the **revoke-first** flow
  (revoke cert → deregister console row → purge bundle).
- **Builds on:** `onboarding/offboard.py` (revoke-first, already wired).
- **Invariants:** I4 (cert revoke is the security-critical step, runs first), I5.

#### B3 · License & entitlement management — **P1, M**
- **What:** issue/renew/revoke licenses; change plan/tier; set expiry; **suspend**
  a non-paying tenant (agent's `is_licensed()` flips false → degrades cleanly).
- **Why:** entitlement + monetization control per tenant.
- **Builds on:** `licensing/` (signed licenses; the agent already gates on
  `is_licensed()`).
- **New pieces:** console endpoints over the licensing minter; a `license_state`
  desired field the agent reconciles.
- **Invariants:** I6 (license signed), I3 (suspension degrades, never crashes —
  the data plane keeps last-valid state).

### C. See deeper

#### C1 · Per-deployment drill-down — **P0, M** ⭐
- **What:** click a fleet row → a deployment page: its Golden-12 SLIs (embedded
  Grafana per-customer), desired-vs-applied config, version, license, firing
  alerts, recent audit events.
- **Why:** one pane per customer instead of bouncing between Console, Grafana, and
  logs. The natural home for every action above.
- **Builds on:** the registry + the Grafana per-customer dashboard (already
  per-tenant scoped) + (once wired) the alert state.
- **New pieces:** a `/deployments/{id}` console page; embed/deeplink the
  per-customer dashboard scoped to that tenant.
- **Priority rationale:** low-cost, high-clarity, and it is the shell every other
  write-action lives in. Pairs with A1 as the flagship pair.

#### C2 · Alert / incident center — **P1, M**
- **What:** firing/pending alerts per deployment, with acknowledge + silence;
  severity (`page` vs `ticket`) and a route to on-call.
- **Why:** closes the loop on the 21 ruler alerts that today fire into the void
  (`alertmanager_url: ""`). Ack an incident right next to the deployment it is on.
- **Builds on:** `fleet-sli/alert_rules.yml` + `slo_burnrate_rules.yml` (21 rules
  in the Mimir ruler) and `self-obs/cp_rules.yml` (5 self-health rules). Needs the
  **notification path wired first** (see [`alerting`](#) below / the alerting
  discussion).
- **Invariants:** routing must keep self-health alerts (`MimirUnreachable`,
  `AuthProxyDown`) on a path independent of what they watch.

#### C3 · Audit / break-glass viewer — **P2, S**
- **What:** browse the tamper-evident, hash-chained audit log; request + approve
  **break-glass** elevated access with a reason + expiry.
- **Why:** provable accountability — *who changed what, when* — and emergency
  access that is itself auditable (I5).
- **Builds on:** `audit/` (hash-chained log + `verify`) and `breakglass.py`.
- **Invariants:** I5 (break-glass), I6 (checkpoint signature).

### D. Monetize & report

#### D1 · Metering / billing view — **P2, M**
- **What:** per-tenant usage rollup (writes, think-runs, LLM spend) over a period;
  export a **signed** invoice/usage receipt (tamper-evident, verify-before-export).
- **Why:** turn the telemetry you already collect into revenue + customer-facing
  usage transparency.
- **Builds on:** `metering/` (signed Tier-1 usage rollup already built —
  `mimir_client` + `rollup` + `export`, aggregate-only/no-PII per I1).
- **Invariants:** I1 (aggregate counts only), I6 (signed export).

---

## 4. The enabling data model: a *desired* facet on the registry

Most of §3.A–B reduces to one schema change. The deployment record today is the
**actual** facet (what the agent reports). Add a **desired** facet the operator
writes and the agent reconciles:

```
DeploymentRecord (today, actual)        DesiredState (new, operator-written)
  deployment_id                           deployment_id
  tenant_id, region, tier                 desired_config_version   (A1, A4)
  version            (actual)             desired_release          (A2)
  health, last_heartbeat_ts               desired_tier             (A4)
  applied_config_version                  license_state            (B3)
  ...SLI snapshot...                       pending_actions[]        (A3)
                                          updated_by, updated_at, reason  (audit)
```

- the agent's **config-pull** response is computed from DesiredState;
- the heartbeat carries the **applied** versions so the Console can compute drift;
- every DesiredState write is **signed** (I6) and **appended to the audit log**
  (I5);
- DesiredState is advisory — if the Console is down the agent keeps last-applied
  state (I3).

This is the smallest core change that unlocks the largest surface of features.

---

## 5. Suggested sequencing

1. **Foundation:** §4 desired-state model + the drift view in `GET /deployments`.
2. **Flagship pair (P0):** **C1** per-deployment drill-down (the shell) +
   **A1** remote config push (the first real action) — demo: flip acme's tier
   from the Console, watch it apply.
3. **Lifecycle (P1):** B1 onboard, B2 offboard, B3 license — operators stop using
   shell scripts.
4. **Ops depth (P1):** A2 release orchestration + C2 alert center (after the
   alerting notification path is wired).
5. **Governance & money (P2):** A3 action queue, C3 audit viewer, D1 metering,
   A4 tier self-service.

---

## 6. Cross-cutting concerns to decide first

- **Operator authentication (READ + WRITE).** Today writes are a shared bearer
  token and reads are open on the operator LAN (see
  [`LIMITATIONS.md`](./LIMITATIONS.md): operator console READ auth is a
  next-sprint item). The moment the Console can *act*, it needs real operator
  identity + RBAC (who may push config vs revoke a license vs break-glass). This
  is a **prerequisite** for §3.A–B, not an afterthought.
- **Customer-facing vs operator-facing console.** Some features (A4 tier,
  B-onboard self-serve, D1 usage) are things a *customer* should see for *their
  own* tenant. That implies a second, tenant-scoped console view with strict I4
  row-level scoping — a deliberate product decision before building.
- **API-first.** Every action should be an authenticated REST endpoint with the
  UI as a thin client, so the same control surface is scriptable (Terraform
  provider, CI-driven rollouts).
- **Multi-operator concurrency.** Desired-state writes need optimistic
  concurrency (version/etag) so two operators don't clobber each other.

---

## 7. Open decisions (for ratification)

1. **Desired-state transport** — extend the existing config-pull channel, or a
   dedicated `GET /api/v1/deployments/{id}/desired` the agent polls?
2. **Action-queue scope** — which actions make the initial allowlist (A3)? Keep it
   minimal and audited.
3. **Operator-auth mechanism** — SSO/OIDC vs mTLS-for-operators vs VPN + token.
   Gates everything in §3.A–B.
4. **Customer console** — ship a tenant-scoped view now, or operator-only first?
5. **Notification path** — Grafana Alertmanager vs Mimir Alertmanager for C2
   (see the alerting design discussion).

---

*This document is a proposal catalog. Implementing any item starts by ratifying
the §4 data model and the §6 operator-auth decision, then the §5 sequencing.*
