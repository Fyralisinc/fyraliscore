# Ingestion Platform Hardening — Engagement Report

**Date:** 2026-06-09 · **Branch:** `main` · **Scope:** `services/ingest/` ingestion
platform + the webhook ingress edge (`services/app/webhooks/`).

**Method:** Multi-agent orchestration. A 30-agent specialist swarm (5 platform
dimensions + 25 per-source specialists) produced 122 candidate findings; every
HIGH/CRITICAL was adversarially re-verified against the real code by independent
skeptic agents (CRITICALs got 2). The orchestrator (review board) then
independently re-read each implemented finding against source before approving,
implemented only safe/self-contained fixes, and validated each with tests.

---

## 1. Executive Summary

The ingestion platform's **core invariants are sound**: the two-paths-one-writer
design (`ingest_from_draft`), the `(source_channel, external_id, occurred_at)`
dedup, the centralized `external_id` composition, and the N1 publish→flush→advance
cursor crash-safety are all correctly implemented and well-guarded. The all-25
synthetic overlap gate is `READY` (`run6_report.md`).

However, the analysis surfaced a structural blind spot: **the synthetic gate's
mock clients emit field shapes that match the code, so real-provider payload
drift is invisible to it.** A large share of confirmed defects are live-path
integration bugs (camelCase vs snake_case webhook fields, REST-vs-GraphQL APIs,
PascalCase CloudTrail, missing OAuth refresh) that would only fail against real
provider APIs — not in any test that exists today.

- **Swarm output:** 122 findings → 66 HIGH/CRITICAL verified → **44 confirmed**,
  22 rejected as false positives, 56 MEDIUM/LOW registered.
- **Implemented this engagement:** **6 fixes** — all platform/control-plane or
  security, source-agnostic, fully self-contained and test-validated.
- **Validation:** 277 tests pass across all touched subsystems (10 new
  regression tests added; 0 regressions).
- **Verdict:** **CONDITIONALLY_READY (72/100)**. The control plane is solid and
  now hardened; full production readiness is gated on a real-provider contract
  layer and resolving the per-source live-path drift findings (§7).

---

## 2. System Architecture Assessment

| Area | Assessment |
|------|-----------|
| Shared write path (`core.ingest`/`ingest_from_draft`) | **Solid.** Single writer, payload guards (>1MB, NUL), partition self-heal, dedup pre-check. |
| Idempotency (`idempotency/__init__.py`) | **Solid.** Central composition; immutable vs versioned families; parity test is load-bearing. |
| Backfill N1 cursor (`shard_fetch.py`) | **Solid.** publish→flush→advance; cursor homed in `workflow_states`; crash re-publishes rather than skips. |
| Kafka data plane | **Solid.** Per-source lane isolation; idempotent producer; DLQ-on-poison + offset commit. |
| Circuit breaker | **Was fragile → now hardened.** Blocking probes, sampler truncation, and a trip-evasion gap fixed (§6). |
| Steady-state reconciliation | **Was broken for 18/25 sources → now fixed.** (§6, #19) |
| Webhook ingress security | **Mixed.** Core verifier framework is sound; secret rotation and grafana replay fixed (§6); several per-source verifier/tenant-resolution drift bugs remain (§7). |

---

## 3. Documentation vs Implementation Discrepancies

- The `data-ingestion.md` reference matches the code on every load-bearing
  invariant spot-checked (two paths, dedup key, N1, kill-switch). No contract
  drift found in the shared path.
- The reference's own §18 caveats (source-count drift in older pages; routing
  not wired in ingest) remain accurate.
- **New discrepancy found & fixed:** the doc describes a single coherent
  reconciliation safety net, but `periodic_reconciler` actually wired only 7 of
  25 sources — steady-state gap detection was silently dead for 18 sources
  (§6, #19). Now reconciled with a drift-proof shared helper.

---

## 4. Review-Board Discipline (false-positive pruning)

22 of 66 HIGH/CRITICAL claims were **rejected** after adversarial re-reading —
e.g. *"core.py dedup pre-check misses `occurred_at` → duplicate T1 triggers"*:
the pre-check keys on `(source_channel, external_id)`, which is *broader* than
the UNIQUE, so it returns `deduped` and skips the T1 enqueue; the claimed
duplicate path does not exist. Rejecting these kept the implemented set
high-confidence.

---

## 5. Findings Disposition

| Bucket | Count |
|--------|------:|
| Confirmed — **implemented & validated** | 6 |
| Confirmed — **report-only** (need real-provider schema / feature work / product decision) | 38 |
| Rejected (false positives) | 22 |
| Register (MEDIUM/LOW) | 56 |

---

## 6. Implemented Fixes

All six are source-agnostic, low-blast-radius, and test-validated. None alter the
two-paths-one-writer or dedup invariants.

### F1 — Circuit breaker blocks its own event loop *(CRITICAL→HIGH; reliability)*
`feature_flags/circuit_breaker.py`. Both production Kafka probes
(`_measure_kafka_lag_default`, `_sample_active_tenants_default`) were `async def`
but contained only blocking confluent_kafka C-calls — stalling the event loop
(and the `/healthz` heartbeat) for up to `25 lanes × timeout` seconds during the
exact degraded-broker incident the breaker exists to handle (it would fail its
own healthcheck and get restarted mid-tick, never completing the 5-tick trip).
**Fix:** extracted sync bodies, offloaded via `asyncio.to_thread`.
**Validation:** 2 new tests assert each probe runs off the event-loop thread.

### F2 — Active-tenant sampler truncates the breach window *(HIGH; observability)*
`circuit_breaker.py`. The sampler `break`-ed on the first `poll()==None`, but
`None` means "no message this poll window," not "end of partition" — silently
dropping tenants that emitted earlier in the 90s lookback and freezing their
breach counter (delaying/preventing a trip). **Fix:** `break`→`continue`; the 5s
deadline already bounds the loop. **Validation:** new unit test feeds
`[msg, None, msg, None, None]` and asserts both tenants are captured.

### F3 — Flag-flip failure lets a tenant evade the breaker indefinitely *(HIGH; reliability)*
`circuit_breaker.py`. On a trip, `tripped=True` was persisted *before* the flag
flip; if the flip then failed (DB outage), the next tick saw `flag=TRUE +
tripped=TRUE` and misread it as an *operator re-enable*, resetting the breach
counter — so a lagging tenant evaded the breaker for the whole outage (the code
comment literally admitted "will be retried by… no, it won't. This is a gap").
**Fix:** flip first, record `tripped` only on success, leave the counter pinned
on failure so the next tick retries (no migration; verifier-endorsed). Added a
`breaker.flag_flip_failures` metric. **Validation:** new DB-backed test proves
the retry path and would fail on the old evasion behavior.

### F4 — Steady-state gap detection dead for 18/25 sources *(HIGH; reliability)*
`workflows/periodic_reconciler.py`, `workflows/reconciler.py`,
`reconcilers/__init__.py`. `periodic_reconciler` hand-listed `set_pool_provider`
for only 7 sources; the other 18 raised `RuntimeError` on every periodic
gap-check (swallowed as a dispatch exception) — so post-backfill gaps for
jira/mercury/brex/… were never reshared. **Fix:** a drift-proof
`register_pool_provider(pool)` helper derived from `RECONCILER_DISPATCH`, called
by *both* the at-completion and periodic services so they cannot diverge again.
**Validation:** new unit test asserts all 25 sources register and each resolves
its pool; both reconciler suites (67 tests) stay green.

### F5 — DB-backed webhook secret rotation broken *(HIGH; security)*
`app/webhooks/secrets.py`. `_load_from_db` used `LIMIT 1`, returning only the
newest secret. During a zero-downtime rotation overlap (and for any tenant with
multiple installations of one provider), old-secret-signed deliveries were
dropped with 401 — gapping the observation stream. **Fix:** fetch *all* active
`secret_ref`s (newest first); the verifier already tries each and accepts the
first match. **Validation:** new DB-backed test seeds two active secrets and
asserts both verify.

### F6 — Grafana webhook replay window missing *(MEDIUM; security)*
`app/webhooks/signatures/grafana.py`. In timestamp-in-signature mode the `now`
parameter was accepted but never used — no age check — so a captured delivery
could be replayed arbitrarily later (and, because `grafana:alert` external_ids
are versioned by representative-ts, a replay with a tampered `startsAt` mints
fresh observations bypassing dedup). **Fix:** 300s replay window with a
malformed-header guard, mirroring Slack/Discord; `signed_timestamp` now correctly
typed `int`. **Validation:** 2 new tests (replay rejected, malformed header →
structured error); existing timestamp test updated.

**Aggregate validation:** `277 passed` across
`services/app/webhooks/tests/`, `…/feature_flags/tests/`, `…/reconcilers/tests/`
(excluding Kafka-dependent subprocess e2e); 10 new regression tests; ruff clean.

---

## 7. Remaining Risks (report-only confirmed findings)

These are **real and verified** but were not auto-fixed because they require a
real-provider schema, credentials to validate, feature-sized work, or a product
decision — fixing them blind would risk introducing a *different* wrong mapping.
They are the primary gate on a PRODUCTION_READY verdict.

**A. Real-provider payload drift (gate-invisible — highest priority).** The
synthetic mocks match the code, so these only fail against real APIs:
- Webhook tenant-resolution reads snake_case (`business_id`, `company_uuid`,
  `organizationId`) where providers send camelCase (`businessId`, …) →
  *live webhooks always fail tenant resolution* (`webhooks/tenant_resolver.py`;
  QuickBooks/Mercury/HiBob/Ashby).
- `handlers/aws.py` + `fetchers/aws.py` read camelCase; real CloudTrail returns
  PascalCase top-level keys.
- `integrations/fireflies/client.py` is a REST client against a GraphQL-only API.
- Brex/Miro/Notion clients paginate only page 1 when the real API omits a
  `total` field → silent backfill truncation.
- `db/migrations/0098_deel.sql` is missing `contract_name`/`contract_type`
  columns the onboarding INSERT references.

**B. Auth lifecycle gaps.** No OAuth refresh-token exchange for QuickBooks /
Ramp / Gusto → access tokens die in ~60 min and terminally fail shards;
Carta has no 401 re-mint. (Feature work + real endpoints to validate.)

**C. Webhook verifier hardening.** Gusto/Figma verifiers lack replay protection;
Figma uses a static passcode-in-body. (Same class as F6; safe to fix once the
real provider signing schemes are confirmed per-source.)

**D. Idempotency / data-integrity (per-source).** GitHub `node_id`-as-external_id
may collapse PR lifecycle states; Gmail watch-renewal overwrites `history_id`;
QuickBooks/Jira webhook handlers drop entity changes/changelog entries beyond the
first; Deel contract snapshot keys on wall-clock. Each needs a careful,
source-specific dedup-key change with data-migration consideration.

**E. Notion verification-token logged in plaintext** (`integrations/notion/webhook.py`).
Real, but the log line is the *documented* operator-retrieval mechanism — naively
removing it breaks onboarding. **Proper fix** (write token → secret store; verifier
reads from store) spans router + state-wiring + the notion secret loader and is
deferred to its own change. *Do not ship the naive removal.*

---

## 8. Technical Debt Register

- **Synthetic-fidelity gap (systemic):** the overlap gate cannot catch
  real-provider drift. **Recommendation:** add a per-source *contract test*
  layer that asserts handlers/fetchers/verifiers parse captured **real** provider
  payloads (recorded fixtures), distinct from the synthetic mocks. This is the
  single highest-leverage investment for production confidence.
- **Registration drift pattern:** the 5 source-literal lists + 3 dispatch maps +
  per-service pool wiring are hand-maintained. F4 fixed the reconciler-pool
  instance with a derived helper; the same derive-don't-duplicate approach should
  be applied to the remaining hand-lists.
- 56 MEDIUM/LOW findings recorded in the swarm output for backlog grooming.

---

## 9. Production Readiness

| Dimension | Score | Notes |
|-----------|:-----:|-------|
| Reliability | 7/10 | Core paths solid; breaker + periodic reconciler now fixed; per-source auth-refresh gaps remain. |
| Security | 7/10 | Verifier framework + rotation + grafana replay solid; per-source verifier drift + Notion token exposure remain. |
| Scalability | 8/10 | Per-source lane isolation; breaker no longer blocks its loop. No scaling blocker found. |
| Observability | 7/10 | Good metrics/health surfaces; breaker sampler/metric gaps fixed. |
| Data integrity | 7/10 | Dedup invariant sound; a few per-source external_id/lifecycle bugs to resolve. |
| Recovery | 8/10 | N1 cursor + DLQ + embedding backlog drainer validated by design + tests. |
| Test coverage | 6/10 | Strong synthetic gate, but **no real-provider contract layer** — the key gap. |

**Production Readiness Score: 72 / 100**

### Verdict: **CONDITIONALLY_READY**

The control plane, shared write path, dedup, crash-safety, and (now) the circuit
breaker and steady-state reconciliation are production-grade and evidence-backed.
**Full PRODUCTION_READY is not yet supportable by evidence** because a verified
set of per-source live/backfill paths have real-provider integration defects
(§7-A) that no existing test exercises. Clearing the verdict requires: (1) a
real-provider contract-test layer; (2) resolving the §7-A drift findings; (3) the
auth-refresh work in §7-B. Each is tracked above with file:line evidence and a
remediation.

*This verdict is supported by the swarm evidence, the orchestrator's independent
re-verification, and the 277-test validation run recorded in this engagement.*
