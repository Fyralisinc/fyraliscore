# Fleet SLOs — derived from NFR-5

> These SLOs are the contract the fleet-SLI rules enforce. The recording +
> burn-rate rules in [`slo_burnrate_rules.yml`](./slo_burnrate_rules.yml) and the
> alerts in [`alert_rules.yml`](./alert_rules.yml) implement them. They load into
> the **Mimir ruler** under the synthetic tenant `__fleet__` and evaluate
> **centrally, per-deployment**, over the metrics every tenant's data plane
> remote-writes (see [`README.md`](./README.md)).

## Source of truth

From `docs/plans/byoc-control-plane.md` **NFR-5** (Latency / freshness):

> dead deployment detected within **3 missed heartbeats (~90s)**; SLI breach →
> page **≤ 2 min**; ingest lag **< 60s**.

These three clauses become two SLOs (liveness, availability) plus one fast SLI
guardrail (ingest freshness), each with an explicit objective, window, and the
rule that enforces it.

---

## SLO-1 — Liveness (freshness)

| Field | Value |
|---|---|
| **Statement** | A dead/hung deployment is detected within **3 missed heartbeats (~90s)**. |
| **SLI** | `fyralis:worker_heartbeat_age_max_seconds` (worst worker heartbeat age per deployment) and `up{job=~"fyralis-.*"}`. |
| **Objective** | Detection latency ≤ ~90s (3 × the ~30s heartbeat/scrape cadence; workers touch every ~5s, scrape is 30s). |
| **Why this is a freshness SLO, not an error-budget one** | "Detect within 90s" is a *time-to-detect* target, so it is a direct fast-firing page, not a 30-day budget burn. The burn-rate method (SLO-2) is the wrong tool for a freshness deadline. |
| **Enforced by** | `FleetSLOLivenessHeartbeatMissed` (`heartbeat_age > 90` `for: 30s` → **page**) and `FleetSLOLivenessDeploymentSilent` (`up == 0` for all targets `for: 30s` → **page**) in `slo_burnrate_rules.yml`. The `for: 30s` hold keeps total detect time inside the ~90s budget while filtering a single late scrape. |
| **Cross-check** | The fleet registry's `last_heartbeat_ts` / `derive_health` (lib/deployment.py: fresh→green, stale→yellow, missing→red) is the second, independent liveness path (the agent heartbeat, C4). The metric SLI and the registry heartbeat are deliberately redundant — either can detect a dead deployment. |

---

## SLO-2 — Availability (error budget + multi-window burn rate)

| Field | Value |
|---|---|
| **Statement** | **99.5%** of gateway requests are non-5xx, measured over a rolling **30 days**. |
| **SLI** | `fyralis:gateway_error_ratio:<window>` = `rate(http_requests_total{status=~"5.."}) / rate(http_requests_total)` per deployment. |
| **Objective / error budget** | 99.5% success → **0.5% error budget** (`fyralis:slo_availability_error_budget = 0.005`). |
| **Freshness requirement** | NFR-5: an SLI breach must **page within 2 minutes**. |
| **Enforced by** | The Google-SRE **multi-window, multi-burn-rate** alerts in `slo_burnrate_rules.yml`. |

### Burn-rate alerting (the "page within 2 min" requirement)

Burn rate = `error_ratio / 0.005`. A burn rate of `1` exhausts the 30-day budget
in exactly 30 days; higher burns exhaust it faster.

| Alert | Burn | Short window | Long window | `for` | Action | Budget exhausts in |
|---|---|---|---|---|---|---|
| `FleetSLOAvailabilityFastBurn` | **14.4×** | 5m | 1h | 2m | **page** | ~2 days |
| `FleetSLOAvailabilitySlowBurn` | **6×** | 30m | 6h | 15m | ticket | ~5 days |

**Why two windows per alert.** Each alert requires **both** its short and long
window to exceed the burn threshold (joined with `and on(...)`). The short
window gives fast detection; the long window prevents a single-request blip from
paging. The fast alert's `5m` short window with `for: 2m` satisfies the NFR-5
"page ≤ 2 min" target without flapping. The slow alert catches a low, steady
burn that never trips the fast window but still drains the budget.

**Budget gauge.** `fyralis:slo_availability_budget_remaining` records the
fraction of the 30-day error budget left per deployment (1 = full, 0 = spent)
for the operator console / Grafana, independent of the page logic.

---

## SLI guardrail — Ingest freshness (`< 60s`)

NFR-5's third clause ("ingest lag < 60s") is covered operationally by the kafka
lag SLIs rather than a separate burn-rate SLO: `fyralis:normalizer_lag_seconds_max`
(seconds-behind-ingress) and `fyralis:kafka_worst_group_lag` (message lag), with
`FleetConsumerLagHigh` / `FleetConsumerLagCritical` as the alerts. A
seconds-behind value over ~60s sustained is the ingest-lag breach; the message-lag
thresholds (>1000 / >50000) are the coarser, always-available proxy when the
seconds gauge is absent.

---

## How the SLOs map to the green/yellow/red fleet view (FR-B4)

`recording_rules.yml` group 10 records `fyralis:health_code` (0 green / 1 yellow /
2 red) per deployment from the same SLIs, so the fleet console renders the
per-deployment dot from one query while the SLO alerts above drive paging. The
two are consistent by construction (red conditions ⊇ page conditions).

## Caveats / assumptions

- **30-day windows** (`[30d]`, `[6h]`) require the Mimir ruler to have that much
  history; on a fresh deployment the budget-remaining gauge and slow-burn alert
  are meaningful only after the window fills. The fast page (5m/1h) is useful
  immediately.
- The availability SLO is **gateway-request** availability. A deployment with no
  gateway traffic has a guarded denominator (`clamp_min(..., 1)`) so its error
  ratio is 0 (not NaN) — it cannot false-page, but it also cannot true-page on
  availability while idle; liveness (SLO-1) covers an idle-but-dead deployment.
- Thresholds (99.5%, 14.4×, 6×) are the standard SRE defaults; tune per the
  per-tenant SLA once real traffic baselines exist (per-tenant overrides can live
  alongside the Mimir runtime overrides).
