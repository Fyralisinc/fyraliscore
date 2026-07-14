# Fyralis BYOC Control Plane — Design Document

> **Status:** Draft / Proposal — pre-build
> **Date:** 2026-06-24
> **Owner:** Platform
> **Branch context:** authored on `feat/generative-ui-revamp` (doc only; no code)
> **Supersedes:** none

---

## 0. How to read this document

This is the consolidated design for operating Fyralis under a **Bring-Your-Own-Cloud (BYOC)** model: the
product runs inside each customer's cloud account, and Fyralis-the-vendor operates a centralized **control
plane** to keep the fleet healthy without ever touching customer data.

Sections in order: **Proposal → Context → Goals → Invariants → Locked Decisions → Architecture →
Connectivity → Telemetry Tiers → What We Collect → Security/Trust → Build-vs-Buy → Cost → Gaps →
Requirements (FR/NFR) → Phasing → Risks → Appendix.**

The research backing the external claims (vendor patterns, Mimir multi-tenancy, etc.) was a 21-source,
25-claim adversarially-verified deep-research pass (25 confirmed / 0 refuted). Citations in §16.

---

## 1. Proposal (TL;DR)

Operate Fyralis as a **vendor-owned control plane + customer-hosted data plane**. The full data-plane stack
(26 ingestion connectors, Kafka, Postgres/pgvector, Python workers, in-stack Prometheus+Grafana) deploys
into the **customer's** VPC; their signal/payload data never leaves it. Fyralis runs a central control plane
that ingests **filtered telemetry** from each deployment, manages lifecycle (releases/config/licensing), and
proves "everything is working" across the fleet.

**The four locked decisions that shape everything:**

| # | Decision | Choice |
|---|---|---|
| D1 | Scope | **Full control plane** — observability + lifecycle + health/SLA + metering/licensing + provisioning |
| D2 | Connectivity | **Outbound-only agent** (primary) · PrivateLink (enterprise) · air-gapped pull (regulated) |
| D3 | Telemetry egress | **Customer-configurable tiers** (T1 metrics → T2 +scrubbed logs → T3 +traces) |
| D4 | Build vs buy | **Build / self-host everything feasible**; pay only for the genuinely unavoidable |

This is the pattern ClickHouse, StarTree, and Cribl converged on independently: *the control plane monitors
but never stores or accesses customer data.*

---

## 2. Context & problem statement

### 2.1 The system under study

Fyralis is a multi-tenant signal-ingestion + reasoning/memory B2B platform. Data-plane stack:

- ~26 ingestion source connectors (Slack, Jira, GitHub, Discord, Telegram, Notion, Gmail, GDrive, finance
  sources, WhatsApp, …)
- Apache Kafka data plane (4 topic stages × ~26 sources ≈ 104 topics)
- PostgreSQL with pgvector (partitioned `observations`, multi-tenant via app-level `tenant_id`)
- Python worker services (normalizer, observation_writer, dlq_writer, think_worker, reconciler, …)
- An **already-built** in-stack observability layer: a hand-rolled Prometheus exposition registry
  (`lib/observability/metrics.py`), 29 scrape targets, 13 alert rules, 7 recording rules, a postgres-exporter
  with custom queries, and 6 Grafana dashboards.

### 2.2 The tension

Customers choose BYOC for **data residency / compliance** — their signal data (employee Slack messages, Jira
tickets, GitHub activity) must stay in their environment. But Fyralis still has to **operate** every
deployment: detect stalled ingestion, lagging Kafka consumers, failed migrations, zeroed-out source
backfills, dead workers — across a fleet we cannot SSH into.

**The core question:** How do we get fleet-wide health visibility (enough to guarantee "it's working")
*without* exfiltrating customer data or requiring intrusive access that defeats the reason they chose BYOC?

### 2.3 Mental model

> The control plane is a **flight-tracking tower**: we see every plane's altitude, heading, and
> fuel-warning lights in real time (metrics/SLIs), and can radio instructions (config/release pulls) — but
> we are never *on* the plane, and we cannot see the passengers (their data).

---

## 3. Goals & non-goals

### Goals
- Cross-fleet observability/health: prove each deployment is working, detect failure fast.
- Full lifecycle: signed releases, canary→fleet rollout, config distribution, version/drift tracking.
- Health/SLA model on infra we don't own.
- Metering/licensing/billing from a remote (possibly air-gapped) data plane.
- Provisioning/onboarding of new customer deployments.
- Preserve the data-isolation guarantee by construction; build SOC 2-ready.

### Non-goals (v1)
- Multi-cloud data plane (AWS-first; GCP/Azure later).
- Full self-service onboarding (assisted onboarding is acceptable early).
- Tier-3 traces by default (opt-in, per-incident only).
- A formal SOC 2 report + external pen-test on day one (build *ready*; defer the *audit*).

---

## 4. Key invariants (non-negotiable — violate any and it's not BYOC)

| # | Invariant | Rationale |
|---|---|---|
| **I1** | Customer payload/PII **never** leaves the customer VPC at Tier 1 | The entire reason customers chose BYOC |
| **I2** | Control plane has **zero standing inbound path** into the customer VPC | Outbound-only; shrinks customer attack surface + trust ask |
| **I3** | The **data plane keeps running** if the control plane is unreachable | A control-plane outage must never stall ingestion |
| **I4** | **No cross-tenant exposure** — isolation enforced *server-side*, never trusted from the client | Mimir trusts the `X-Scope-OrgID` header it's given; the auth proxy is the linchpin |
| **I5** | Every human/privileged access into a customer env is **customer-granted, scoped, time-boxed, audit-logged** | Least privilege; nothing standing |
| **I6** | Everything shipped to the data plane is **cryptographically signed and verified before apply** | Supply-chain integrity |

---

## 5. Locked decisions (decision register)

| ID | Decision | Choice & rationale | Status |
|---|---|---|---|
| D1 | Control-plane scope | **Full** (observability + lifecycle + SLA + metering/licensing + provisioning) | LOCKED |
| D2 | Connectivity model | **Outbound-only agent** primary; PrivateLink secondary (enterprise); air-gapped pull fallback (regulated). Lowest trust-ask, free NAT traversal, zero inbound attack surface | LOCKED |
| D3 | Telemetry egress | **Customer-configurable tiers** enforced at a boundary OTel Collector inside the customer VPC | LOCKED |
| D4 | Build vs buy | **Build/self-host everything feasible.** Self-host OSS (Mimir/Loki/Grafana/OTel); build the differentiators; pay only for the unavoidable | LOCKED |
| D5 | Central metrics store | **Grafana Mimir** (multi-tenant via `X-Scope-OrgID`), self-hosted | LOCKED |
| D6 | Central logs store | **Grafana Loki** (Tier-2), self-hosted | LOCKED |
| D7 | Boundary enforcement | **OpenTelemetry Collector** with filter/redaction processors, customer-auditable config | LOCKED |
| D8 | Crypto / trust roots | **Private CA** (step-ca / Vault PKI) for agent mTLS; **cosign/minisign** for release signing; **Let's Encrypt** for public HTTPS — all $0 | LOCKED |
| D9 | Licensing | **Build** signed/expiring license tokens (or self-host Keygen OSS); do not pay for licensing SaaS | LOCKED |
| D10 | Compliance | Build SOC 2 controls from day one; **defer** the formal audit + external pen-test until a regulated deal requires them | LOCKED |
| D11 | Control-plane hosting | May run **off-AWS** (commodity infra) since it never touches customer data — cost lever | LOCKED |

**The one residual risk inside D4:** the *orchestrator* (agent + config-dist + installer + release pipeline)
is the single most expensive thing to build and maintain (see §13). A pragmatic fallback — *rent an
orchestrator (Nuon/Replicated) for an interim phase, then build it once the BYOC motion is proven* — is
captured as a sequencing option in §14, **not** a reversal of D4.

---

## 6. System architecture

### 6.1 Reference topology

```
   ┌────────────────────── FYRALIS CONTROL PLANE (our infra; off-AWS OK) ─────────────────┐
   │  Fleet console   │  Central Mimir (metrics, multi-tenant)   │  Central Loki (logs)    │
   │  Release/CD      │  Central Grafana (per-customer scoped via X-Scope-OrgID)           │
   │  Licensing svc   │  🔑 Tenant auth proxy ◄── mTLS ──┐   Provisioning / onboarding     │
   │  Metering rollup │  Private CA (step-ca)            │   Audit log (break-glass)       │
   └─────────────────────────────────────────────────── │ ─────────────────────────────────┘
                                                         │  outbound-only, 443, agent-initiated
   ┌─────────────────────── CUSTOMER VPC (data plane) ───│────────────────────────────────┐
   │  Fyralis Agent ─────────────────────────────────────┘                                 │
   │     ├─ heartbeat / health / version-drift report                                      │
   │     ├─ pulls signed config + releases (verify-before-apply)                            │
   │     └─ local license validator (signed, expiring)                                     │
   │                                                                                       │
   │  Boundary OTel Collector ──(tier filter + redaction at the boundary)──► remote-write  │
   │     ▲ scrapes the existing /metrics + /healthz                                        │
   │  26 connectors │ Kafka │ PG+pgvector │ Python workers │ LOCAL Prom+Grafana            │
   │  (customer signal/payload data stays HERE, always)                                    │
   └───────────────────────────────────────────────────────────────────────────────────────┘
```

**Structural rule (verified across vendors):** the *full* observability stack runs **locally** in the
customer VPC; only essential, filtered telemetry leaves. Fyralis's existing Prom/Grafana stack stays where it
is — we add an egress path, we don't relocate it.

### 6.2 Component inventory (build / buy / have)

Legend: ✅ already exists · 🆕 new · 🔨 build · 🛠 self-host OSS · 🔑 = highest-leverage build.

**Customer VPC (data plane)**

| # | Component | Status | Disposition |
|---|---|---|---|
| 1 | Connectors, Kafka, PG+pgvector, workers | ✅ | ships as-is |
| 2 | Local Prometheus + Grafana + postgres-exporter | ✅ | stays local, unchanged |
| 3 | **Fyralis Agent** (heartbeat, health, drift, config-pull, release-pull) | 🆕 | 🔨 build (or interim rent) |
| 4 | **Boundary OTel Collector** (tier filter/redact → remote-write) | 🆕 | 🛠 assemble OSS |
| 5 | **Redaction / tier policy** (Fyralis-specific PII rules) | 🆕 | 🔨 build |
| 6 | Local license validator (signed, expiring) | 🆕 | 🔨/🛠 build or Keygen OSS |

**Connectivity layer**

| # | Component | Status | Disposition |
|---|---|---|---|
| 7 | mTLS cert issuance + identity (private CA, cert→tenant map) | 🆕 | 🔨 build on step-ca/Vault |
| 8 | Outbound 443 channel (+ PrivateLink / air-gap variants) | 🆕 | 🛠 |

**Control plane (our infra)**

| # | Component | Status | Disposition |
|---|---|---|---|
| 9 | Central multi-tenant **Mimir** | 🆕 | 🛠 self-host |
| 10 | Central **Loki** | 🆕 | 🛠 self-host |
| 11 | 🔑 **Tenant auth proxy** (mTLS → inject `X-Scope-OrgID`) | 🆕 | 🔨 **build — most important** |
| 12 | Central **Grafana** (fleet + per-customer scoped) | ✅→extend | 🛠 |
| 13 | **Fyralis SLI/alert rules** (fleet-level) | 🆕 (port existing) | 🔨 build |
| 14 | **Fleet console** (registry: version/health/license/heartbeat) | 🆕 | 🔨 build |
| 15 | **Release/CD** (signed bundles, canary→rollout, halt-on-drift) | 🆕 | 🔨 build |
| 16 | **Config distribution** (agent-pull: flags, token-rotate) | 🆕 | 🔨 build |
| 17 | **Provisioning installer** (Helm/Terraform bundle) | 🆕 | 🔨 build |
| 18 | **Licensing service** (issue signed expiring bundles) | 🆕 | 🔨/🛠 |
| 19 | **Metering / billing rollup** (signed Tier-1 usage) | 🆕 | 🔨 build (thin) |
| 20 | **Audit log** (break-glass grants + admin actions) | 🆕 | 🔨 build |

**The three things that gate the MVP (P0 critical path):** the **tenant auth proxy (#11)**, the **boundary
redaction enforcing Tier 1 (#5)**, and the **Fyralis SLI engine (#13)**.

---

## 7. Connectivity design (D2)

| Model | Inbound ports in customer VPC | Trust ask | Real-time | NAT/firewall | Best for |
|---|---|---|---|---|---|
| **Outbound agent (PRIMARY)** | none | **low** | yes | free (egress 443) | default / most customers |
| PrivateLink / VPC-peering | none | medium (AWS wiring) | yes | moderate setup | enterprise wanting private networking |
| Air-gapped pull | none | lowest, ops-heavy | **no** (batch) | n/a | regulated / offline |

**Recommendation: outbound-only agent.** The data plane initiates every connection out over 443 with an
mTLS client cert; no inbound listener exists in the customer VPC. This mirrors Databricks Secure Cluster
Connectivity (cluster initiates relay on 443, no public IPs), Cribl Workers (heartbeat to Leader), and
ClickHouse (outbound agent). PrivateLink and air-gap are *config variants of the same agent*, not rebuilds.

---

## 8. Telemetry tier design (D3)

Tiers are enforced **at the boundary**, inside the customer VPC, by an OTel Collector the customer can
inspect and configure. A tier = which pipelines/processors are enabled.

| Tier | What egresses | Fyralis mapping | Mechanism |
|---|---|---|---|
| **T1 — SLIs only** *(default)* | Aggregated metrics, **no payloads/PII** | consumer lag, ingestion rate, backfill counts, migration status, worker up/down, DLQ depth | Prometheus **remote-write → Mimir**; filter processor drops high-cardinality/PII labels |
| **T2 — + scrubbed logs** | T1 + redacted logs/events | error logs, "backfill zeroed for source X", failed-migration events | OTel **transform/redaction processor** strips PII before export → **Loki** |
| **T3 — + traces & samples** | T1+T2 + traces & sample payloads | worker→Kafka→PG traces; sampled envelopes | OTLP traces; opt-in, per-incident, time-boxed |

**Central multi-tenancy:** Mimir isolates each customer by `X-Scope-OrgID`; a single central Grafana scopes
per-customer views by the same header.

**🔑 Critical gotcha (verified):** **Mimir ships with no authentication** — it trusts whatever
`X-Scope-OrgID` it receives. The **tenant auth proxy (#11) must authenticate each agent's mTLS cert and
inject the correct tenant ID server-side.** Without it, any customer could read any tenant's metrics. This is
invariant **I4** and the single highest-priority build.

---

## 9. What we collect (signal catalog)

**Great news from the codebase scan:** Fyralis is already heavily instrumented, so **T1 is largely
"remote-write what already exists."** Below, each signal is tagged **Tier** and **Status**: ✅ already a
metric · 🟡 in DB / needs a query or exporter · 🔴 log-only or not instrumented (a gap — see §12).

### 9.1 The "golden signals" → per-deployment 🟢/🟡/🔴 (FR-B4)

If we capture nothing else, these ~12 define "is it working":

1. **Workers up** (`up{job=~"fyralis-.*"}` + `worker_heartbeat_age_seconds`)
2. **Kafka consumer lag** (`fyralis:kafka_worst_group_lag`)
3. **DLQ depth** (`ingestion_dlq_writer_unresolved_depth` / `fyralis_dlq_unresolved`)
4. **Ingestion rate ≠ 0 per active source** (zeroed-backfill detector)
5. **Backfill shard progress** (`fyralis_onboarding_shards`, behind_schedule events)
6. **Shadow-drop = 0 on backfill** (silent-loss invariant)
7. **Think queue depth** (`fyralis_think_queue_pending`, backpressure >500)
8. **Think failure rate + stuck runs**
9. **Embedding backlog + failure ratio** (`fyralis_embedding_backlog_pending`, `fyralis:embed_failure_ratio:10m`)
10. **LLM circuit-breaker closed + spend burn rate** (`fyralis:llm_spend_usd_per_hour`)
11. **DB pool saturation + partition coverage + schema version**
12. **OAuth source-token health + webhook verification rate**

### 9.2 Full catalog by subsystem

**Worker liveness**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| `up{job=~"fyralis-.*"}` | worker dead/unreachable | T1 | ✅ |
| `worker_heartbeat_age_seconds` (>120s) | hung worker (loop wedged) | T1 | ✅ |
| `worker_uptime_seconds` | crash-loop (uptime resets) | T1 | ✅ |
| expected-vs-present worker count (29 targets) | a worker class missing | T1 | 🟡 |
| `anomaly_processor` / `deadline_resolver` deployed? | coded-but-not-in-compose → T2/T3 reasoning silently never fires | T1 | 🔴 |

**Ingestion & backfill**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| `writer.full_mode_writes` / obs-per-source rate | stalled ingestion | T1 | ✅ |
| `ShardFetched.observation_count == 0` after positive fetch time | fetcher returned nothing (pagination/API dead-zone) | T2 | 🟡 |
| `workflow_states.cursor` advance + `last_advanced_at` | backfill cursor stalled / shard crashed | T1 | 🟡 |
| `fyralis_onboarding_shards{status}` | shards stuck pending/failed | T1 | ✅ |
| `onboarding.progress`: feels_onboarded / behind_schedule / complete + coverage_confidence | backfill behind / gappy | T2 | 🟡 |
| `reconciliation_pass_count` | reconciler oscillating / cascading gaps | T1 | 🟡 |
| `register_pool_provider` coverage | a source's reconciler silently disabled | T2 | 🔴 |
| `writer.full_mode_dedup_hits` (high, 0 new) | retry storm / re-emit without progress | T1 | ✅ |

**Kafka data plane**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| `kafka_consumergroup_lag_sum` / `fyralis:kafka_worst_group_lag` | consumer wedged (>1000) | T1 | ✅ |
| `normalizer.consumer_lag_seconds_last` | normalizer behind ingress | T1 | ✅ |
| breaker trips / breach increments | auto-cutover tripped a tenant to inline | T1 | ✅ |
| producer `flush undelivered` on shutdown | silent loss on restart | T2 | 🔴 |
| topic/partition/replication config | under-provisioned/missing topics | T1 | 🟡 |

**DLQ / poison / silent-loss**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| `ingestion_dlq_writer_unresolved_depth` / `fyralis_dlq_unresolved` | failures piling up (>25) | T1 | ✅ |
| `writer.shadow_drop` (backfill path) | **invariant violation = silent data loss** | T1 | 🟡 |
| `writer.poison_dlq` / `writer_poison_attempts` (mig 0137) | deterministic poison burning cap | T1 | 🟡 |
| `writer.parse_failure` / `full_mode_failures` | schema drift / garbage producer | T1 | ✅ |
| `fyralis_dead_letter_rows{table}` | reasoning/post-commit poison backlog | T1 | ✅ |
| `kafka_path_enabled` flag state | kill-switch left off = ongoing live-loss | T1 | 🟡 |

**Database & schema integrity**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| **schema version / applied migrations** | failed/pending migration, drift | T1 | 🔴 **no `schema_migrations` table** |
| observation **partition coverage** (current + 3mo) | missing partition → insert failure → silent loss | T1 | 🟡 |
| `check_schema_drift.py` result | column/index/extension drift | T2 | 🔴 manual script |
| postgres-exporter: connections/locks/sizes/`pg_stat_activity` | pool exhaustion, long queries, bloat | T1 | ✅ |
| `db_pool_*` + `fyralis:db_pool_saturation` (>0.9) | pool saturated → fetchers block | T1 | ✅ |
| required extensions (vector, pg_trgm, btree_gin) | missing ext = failures | T1 | 🟡 |
| RLS note | **no RLS** — isolation is app-level `WHERE tenant_id` only | — | 🔴 awareness |

**Reasoning / think pipeline**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| `fyralis_think_queue_pending` | reasoning backlog (>500) | T1 | ✅ |
| `think_runs.status=failed` rate | reasoning failures | T1 | 🟡 |
| stuck runs (`running` >5min) + orphaned leases | crashed worker mid-apply | T1 | 🟡 |
| `runs_total/failed/latency` by trigger_kind | per-trigger throughput | T1 | ✅ (⚠️ in-memory) |
| `validation_dropped_ops{reason,op_type}` | ops silently dropped during apply | T2 | 🔴 in-memory only |

**LLM & embedding dependency**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| LLM circuit-breaker state (per provider) | provider down → **all** think runs fast-fail | T1 | 🔴 log/runtime |
| `fyralis:llm_spend_usd_per_hour` + tokens | cost blowout (>$5/hr) | T1 | ✅ |
| LLM rate-limit/timeout/permanent rates | quota / auth / outage | T2 | 🔴 log-only |
| `DEEPSEEK_API_KEY` missing | app won't start | T1 | 🔴 startup-log |
| `fyralis_embedding_backlog_pending` + `:embed_failure_ratio:10m` | embeddings stalled (>5%) | T1 | ✅ |
| Ollama unreachable / dim-mismatch (≠768) | embeddings dead / breaks HNSW | T2 | 🔴 log-only |

**Auth, secrets & ingress**
| Signal | Catches | Tier | Status |
|---|---|---|---|
| **OAuth source-token expiry / refresh failure** | a source silently stops ingesting | T1 | 🔴 **gap** |
| `actor_sessions` expiry/revocation counts | human-auth token issues | T1 | 🟡 |
| `webhook_verification_failures_total` / `fyralis:webhook_failure_rate:5m` | bad secret / spoof / rotation miss (>1/s) | T1 | ✅ |
| `webhook_resolver_outcomes_total{unknown_installation}` | webhook for unmapped tenant | T1 | ✅ |
| `http_requests_total` 5xx / `fyralis:gateway_5xx_rate:5m` | gateway errors per route | T1 | ✅ |
| secret_ref resolution failures | missing/invalid credential | T2 | 🔴 gap |

**Cost & metering (feeds §11 licensing/billing)**
| Signal | Use | Tier | Status |
|---|---|---|---|
| obs-per-source counters | usage metering → billing | T1 | ✅ |
| `think_run_costs` (tokens, cost_usd) | LLM cost attribution per tenant | T1 | 🟡 |
| `think_cost_recent_usd_1h` | live spend rate | T1 | ✅ |

---

## 10. The foundation that already exists

| What | Detail |
|---|---|
| Metrics layer | Hand-rolled Prometheus exposition (`lib/observability/metrics.py`); `/metrics` on gateway:8000 + workers:9300 |
| Scrape targets | 29 (23 workers + 6 infra: postgres-exporter, kafka-exporter, redis, minio, grafana, prometheus) |
| Health protocol | `/healthz` 503 if heartbeat >120s; workers touch every 5s |
| Alerts (13) | heartbeat-stale, scrape-down, DLQ-depth, consumer-lag, signature-failure, embed-failure-ratio, think-backpressure, db-pool-saturated, llm-spend-burn, dead-letter-rows, … |
| Recording rules (7) | `:kafka_worst_group_lag`, `:db_pool_saturation`, `:embed_failure_ratio:10m`, `:gateway_5xx_rate:5m`, `:llm_spend_usd_per_hour`, … |
| postgres-exporter | `fyralis_dlq_unresolved`, `fyralis_think_queue_pending`, `fyralis_embedding_backlog_pending`, `fyralis_dead_letter_rows`, `fyralis_onboarding_shards` |

---

## 11. Security & trust boundary

- **What we cannot see by construction:** customer signal/payload data never leaves the data plane; only
  filtered telemetry does. There is no inbound path (I2) and no standing credentials into the customer VPC.
- **Least-privilege break-glass:** any human access is **customer-granted, scoped, time-boxed, audit-logged**
  (the ClickHouse "system-tables-only, cert-based" / StarTree "deploy-then-manage role" pattern). Nothing
  standing.
- **Crypto, all free (D8):**
  - *Agent ↔ control-plane mTLS* → **private CA** (step-ca / Vault PKI). No public CA needed — we issue the
    agent, so we are its trust root. Cert → `X-Scope-OrgID` mapping lives in the auth proxy.
  - *Release signing* → **cosign** (container images) or **minisign** (tarballs); agent verifies before
    apply (I6). No commercial code-signing cert (those are only for public OS trust).
  - *Public HTTPS* (human-hit dashboards/API) → **Let's Encrypt** via cert-manager/certbot.
- **Compliance (D10):** build SOC 2 controls (audit log, least-privilege, access grants) from day one; defer
  the formal report. Note: SOC 2 Type II needs a 3–12 month observation window, so *start the clock* before a
  deal demands it. Do free SAST/DAST + an internal boundary review before the first real customer's data
  rides on the path; defer the formal external pen-test.

---

## 12. Gaps to fill before building

These fell out of the codebase scan. They are the high-value instrumentation tasks; the redaction/SLI build
(#5, #13) should prioritize them.

| # | Gap | Why it matters | Action |
|---|---|---|---|
| G1 | **No `schema_migrations` table** | can't remotely answer "right schema / did a migration fail?" | add a version ledger + expose as a metric |
| G2 | **OAuth source-token refresh failures aren't a metric** | the most common "source silently dies" failure is invisible to fleet monitoring | instrument refresh success/failure + token-expiry-soon gauge |
| G3 | **LLM circuit-breaker state & provider errors are log-only** | "deepseek down → all reasoning fast-fails" | export breaker state + error rates as gauges |
| G4 | **In-memory think metrics reset on restart** | `validation_dropped_ops`, cost-by-kind lost on restart | export to `/metrics` or rely on `think_run_costs` DB |
| G5 | **`anomaly_processor` / `deadline_resolver` not in compose** | deployment looks healthy while T2/T3 reasoning never runs | capture "expected vs running" worker set |
| G6 | **Producer flush-undelivered & shadow-drop are log-only** | these are data-loss signals | promote to counters |
| G7 | **postgres-exporter custom queries** may be unpopulated in some checkouts | DLQ/think/embedding gauges have no backend | verify they ship in the BYOC bundle |

**Readiness gaps beyond instrumentation:** RLS is not implemented (tenant isolation is app-level only) —
acceptable for single-tenant-per-VPC BYOC, but worth noting; and the agent/release/installer machinery (#3,
15, 16, 17) does not exist yet.

---

## 13. Build-vs-buy & cost

### 13.1 Per-component disposition

We **buy/self-host** the heavy OSS infra (Mimir, Loki, Grafana, OTel, step-ca, cosign) and **build** the
thin differentiators. The only genuine SaaS temptation is the orchestrator; under D4 we build it (with the
interim-rent fallback in §14).

### 13.2 Cost (full-build posture)

Assumptions: loaded engineer ≈ $200k/yr ($3.85k/eng-week); 2-engineer build team; control plane self-hosted
(possibly off-AWS, D11); most customers on T1.

| Bucket | Estimate |
|---|---|
| One-time build (incl. self-built orchestrator) | **~45–70 eng-weeks ≈ $173k–$270k**; 5.5–8.5 calendar months |
| Recurring SaaS/tooling | **$0** (everything self-hosted/built) |
| Control-plane infra (off-AWS / capped) | **~$10k–$40k/yr** |
| Ongoing maintenance | **~0.75–1.25 FTE (~$150k–$250k/yr)** |
| Per-customer marginal (T1) | **<$10/customer/month** |

**Truly unavoidable spend:** control-plane infra (~$10–40k/yr) is the only standing cash cost until the first
regulated deal triggers a SOC 2 auditor fee (~$10–20k, auditor-only if we collect evidence ourselves).
Code-signing + TLS are **$0** (private CA + cosign + Let's Encrypt). Pen-test deferred (free SAST/DAST +
internal review in the interim).

**Honest trade:** full-build *saves* ~$25k–$85k/yr in orchestrator SaaS but *adds* one-time build + more
maintenance labor; over 3 years at ~25 customers it is ~$130k–$190k more expensive than renting — **until**
scale (per-install fees climbing past ~100 customers) or the strategic value of owning the boundary (a
sales asset for regulated buyers) flips the math. D4 accepts this trade deliberately.

---

## 14. Implementation phasing

**P0 — Minimal fleet-visibility MVP (the critical path):**
1. 🔑 **Tenant auth proxy (#11)** — mTLS termination + `X-Scope-OrgID` injection (I4). Nothing is safe
   before this.
2. **Private CA + cert issuance (#7)** — identity root.
3. **Boundary OTel Collector + T1 redaction (#4, #5)** — enforce I1.
4. **Central Mimir + Grafana (#9, #12)** — receive + scope.
5. **Fyralis SLI/alert rules (#13)** — port the existing 13 alerts to fleet level + the golden signals.
6. **Minimal agent (#3)** — heartbeat + health + version report.
7. **Fleet console (#14, thin)** — list deployments + health rollup.

**P1 — Operate the fleet:** signed release/CD + canary (#15), config distribution (#16), Loki + T2 redaction
(#10), installer (#17), licensing (#18, #6), metering rollup (#19), audit log (#20).

**P2 — Scale & edges:** PrivateLink + air-gap variants (#8), multi-cloud, customer self-service, Tier-3
traces.

**Pre-build instrumentation (parallel):** close G1–G7 (§12) — these block trustworthy fleet health.

**Sequencing option (does not reverse D4):** during P0/P1, an orchestrator (Nuon/Replicated) *may* be rented
to ship the BYOC motion faster, then replaced by the self-built agent/release stack once proven — this is the
single costliest build and benefits most from real fleet data informing its design.

---

## 15. Functional & non-functional requirements

### 15.1 Functional (by module)

**FR-A Identity & Connectivity** — A1 per-tenant mTLS cert at onboarding (P0) · A2 authenticate every agent,
map cert→tenant (P0) · A3 outbound-only, no inbound listener (P0) · A4 heartbeat w/ health+version+license+drift
(P0) · A5 revoke/rotate/quarantine (P1) · A6 PrivateLink + air-gap transports (P2).

**FR-B Fleet Observability & Health** — B1 ingest per-tenant filtered telemetry (metrics→Mimir, logs→Loki,
traces→OTLP) (P0) · B2 compute Fyralis SLIs (ingestion rate, consumer lag, backfill counts, migration status,
worker up/down, DLQ depth) (P0) · B3 fleet-level alerting (P0) · B4 per-deployment 🟢/🟡/🔴 + cross-fleet view
(P0) · B5 per-customer Grafana drill-down via `X-Scope-OrgID` (P0) · B6 configurable alert routing (P1).

**FR-C Telemetry Tier Control** — C1 customer selects T1/T2/T3 per deployment (P0) · C2 enforce
redaction/aggregation at the boundary; no payload/PII at T1 (P0) · C3 tier change via config push, no redeploy
(P1) · C4 customer-inspectable redaction config (P1).

**FR-D Lifecycle / Release** — D1 publish signed bundles; agent verifies before apply (P0) · D2 version/drift
tracking (P0) · D3 canary→staged rollout, halt-on-failure (P1) · D4 config push via agent pull (P1) · D5
rollback (P1) · D6 air-gapped bundle export/import (P2).

**FR-E Provisioning** — E1 per-customer installer that stands up data plane + registers agent (P0) · E2
register→issue cert+tenant ID→appear in console (P0) · E3 onboarding progress tracking (P1).

**FR-F Metering / Licensing** — F1 signed expiring license bundles; local validation blocks on expiry (P0) ·
F2 signed usage counters from T1 metrics (P1) · F3 per-customer usage export for billing (P2).

**FR-G Security / Access / Audit** — G1 control plane holds no customer secrets (P0) · G2 break-glass:
customer-granted, scoped, time-boxed (P1) · G3 immutable audit log of grants + admin actions (P0).

**FR-H Operator / Fleet Console** — H1 list deployments: version/health/region/license/heartbeat (P0) · H2
per-tenant drill-down (P1).

### 15.2 Non-functional (with targets)

| ID | Quality | Target |
|---|---|---|
| NFR-1 | Privacy/Isolation | T1 egress = **0 bytes** payload/PII (verifiable by inspecting boundary config) |
| NFR-2 | Tenant isolation | cross-tenant leakage = **0 by construction**; tenant ID injected server-side from mTLS cert, never from a client header |
| NFR-3 | Availability & decoupling | control plane **99.9%**; data plane **survives indefinite control-plane outage** (agent buffers + backoff; ingestion unaffected) |
| NFR-4 | Scalability / cardinality | **≥100 customers** w/o re-architecture; per-tenant active-series budget (≤10k); horizontal Mimir/Loki |
| NFR-5 | Latency / freshness | dead deployment detected within **3 missed heartbeats (~90s)**; SLI breach → page **≤2 min**; ingest lag **<60s** |
| NFR-6 | Operability | 100% self-hostable OSS; control-plane upgrades cause **no fleet disruption**; config as code |
| NFR-7 | Compliance-ready | SOC 2 controls from day one; configurable retention; control-plane location flexible (off-AWS allowed) |
| NFR-8 | Portability | tolerate heterogeneous customer envs; outbound traversal works behind NAT/firewall w/ no customer network changes; AWS-first |
| NFR-9 | Cost efficiency | T1 control-plane cost **<$10/customer/month**; infra ~flat to 100 customers |
| NFR-10 | Self-observability | the control plane monitors itself (auth proxy, Mimir, ingest path) — silence ≠ health |

---

## 16. Open questions, risks, sources

### Open questions
- **Per-deployment vs per-tenant:** BYOC implies one data plane per customer; do we ever co-tenant multiple
  customer orgs in one VPC? (Affects whether app-level `WHERE tenant_id` isolation is sufficient — see RLS
  note, §12.)
- **Orchestrator: build now or rent-then-build?** (D4 says build; §14 allows interim rent — decide at P0.)
- **Retention windows** per tier (cost vs debuggability).
- **Air-gap demand:** do we have a near-term regulated customer that forces the pull model into P1 rather
  than P2?

### Key risks
- **R1 — auth proxy is a single point of trust (I4).** A bug here breaches tenant isolation. Mitigation:
  it's the first thing built, smallest possible surface, adversarially reviewed + SAST before any real data.
- **R2 — self-built orchestrator maintenance burden** (§13). Mitigation: interim rent option (§14).
- **R3 — instrumentation gaps (§12) mean early fleet health is partially blind.** Mitigation: close G1–G7
  before onboarding a paying BYOC customer.
- **R4 — data-isolation guarantees are self-asserted, not yet audited** (research caveat). Mitigation:
  build-ready for SOC 2 + boundary pen-test before regulated GA.

### Sources (verified, deep-research pass)
ClickHouse BYOC architecture & "Building ClickHouse BYOC on AWS"; StarTree deployment models; Cribl
distributed deploy; Databricks PrivateLink / Secure Cluster Connectivity; Grafana Mimir auth & multi-tenancy
+ OTel Collector config; OpenTelemetry data scrubbing (dash0); Replicated KOTS (intro + air-gapped); Nuon
runner architecture; Omnistrate BYOC; Keygen offline licenses; PrivateLink-vs-peering (ngrok).
**Caveat:** evidence is mostly vendor documentation; data-isolation guarantees are self-asserted, not
independently audited — our own SOC 2 / pen-test must validate the boundary we build.

---

*End of document.*
