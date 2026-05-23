# Research Plan: Real-API Sandbox for the Ingestion Pipeline

> Status: research plan (not yet executed). Authored 2026-05-22.
> Sibling docs: [production-deploy.md](./production-deploy.md), [synthetic-testing-guide.md](./synthetic-testing-guide.md), validation reports under [`docs/validation/path_i/`](../validation/path_i/).

## 1. Objective

Stand up a **sandbox environment that exercises the ingestion pipeline against real provider APIs** (GitHub, Slack, Discord, Gmail) — closing the one gap the synthetic suite explicitly does *not* cover. Per [`summary.md`](../validation/path_i/summary.md), the synthetic runs validate *internal correctness* but not "real OAuth / real provider webhook signatures / real API pagination/quotas."

Most of the *mechanism* already exists. The endpoint resolver (`lib/integrations/endpoints.py`) already lets every client target a real or mock base URL via env; [`production-deploy.md` §4](./production-deploy.md) already documents a per-source real-API validation procedure; and [`.env.production.example`](../../.env.production.example) enumerates every credential. So this is **less "build" and more "research the account/network/cost unknowns and assemble a repeatable harness."**

## 2. Confirmed decisions

| Decision | Choice | Consequence |
|---|---|---|
| Source order | **All four, easiest→hardest**: GitHub → Slack → Discord → Gmail | Full coverage; Gmail (GCP/DWD/Pub-Sub/OIDC) is the long pole |
| Env-guard profile | **Real prod guards on** (`FYRALIS_ENV=prod`, `COMPANY_OS_ENV=prod`) | Highest fidelity; exercises the same guard paths prod will |
| Ingress | **OPEN** — see §7 for the analysis to decide from | Determines how providers reach the gateway |

Implications of "real prod guards on":
- A **stable sandbox `MASTER_KEK` must be provisioned and durably stored before first boot.** Regenerating it orphans every encrypted secret in the DB.
- The gateway **refuses to start with `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`** in prod, so every source needs a real webhook secret from day one.
- `WEBHOOK_TENANT_DEFAULT_ALLOW` must stay `0` — webhooks resolve tenant from the installation payload, never a hardcoded default.

## 3. What already exists (do not re-research)

| Capability | Location | Implication |
|---|---|---|
| Per-source base-URL override | `lib/integrations/endpoints.py` (`*_API_BASE_URL` env) | Point clients at real APIs with zero code change |
| Per-source real-API validation steps | [`production-deploy.md` §4](./production-deploy.md) | Skeleton of the test procedure already written |
| Full credential inventory | [`.env.production.example`](../../.env.production.example) | Tells us exactly which sandbox creds to provision |
| Backfill harness + assertions | `services/synthetic/validation_runs/assertions.py` | Reusable correctness checks (dedup, trust tier, embedding) |
| Compose topology (infra + workers) | [`docker-compose.yml`](../../docker-compose.yml) | Sandbox reuses the same stack |

## 4. Open questions to resolve (the actual research)

**A. Ingress / networking** — webhooks and OAuth callbacks need a *publicly reachable* gateway URL; local dev can't receive provider webhooks. Biggest design fork → §7.

**B. Provider sandbox accounts** — cheapest/safest test tenant per source and its limits. Gmail push requires a *real* GCP project + domain-wide delegation — far heavier than a GitHub test App on a personal org.

**C. Secrets handling in non-prod** — confirm the sandbox runs as `FYRALIS_ENV=prod` (decided) with a stable `MASTER_KEK`; document every guard flag that must stay `0`.

**D. Cost & rate limits** — quotas, free-tier ceilings, safe backfill volume against a real account.

**E. Validation oracle** — how to assert "ingested correctly" against a real account whose contents we don't fully control (vs synthetic, where fixtures are known).

## 5. Proposed sandbox architecture (to validate during research)

```
  Provider (real API)
        │  webhook / pubsub push        ┌─────────────────────────┐
        ▼                                │  Sandbox host (compose) │
  [public ingress: tunnel or staging] ──▶│  gateway → handlers     │
        ▲  OAuth redirect / API pulls    │  kafka → normalizer     │
        └────────────────────────────────│  → writer → postgres    │
                                         │  embedding_worker        │
   backfill: fetchers ─ real *_API_BASE ─│  temporal workflows      │
                                         └─────────────────────────┘
```

Reuses the existing compose stack; the only *new* pieces are (1) a public ingress and (2) real credentials. Backfill uses the existing fetchers pointed at production base URLs (i.e. **not** `SYNTHETIC_SOURCE_API_BASE`).

## 6. Research tasks (phased)

**Phase 0 — Scope & decisions (½ day)**
- Resolve the ingress decision (§7).
- Decide sandbox identity model: shared team test accounts vs throwaway personal accounts.

**Phase 1 — Ingress & secrets foundation (1–2 days)**
- Evaluate the chosen ingress option for: stable public URL (providers dislike changing webhook URLs), TLS, and Gmail Pub/Sub OIDC-audience requirements.
- Provision and durably store the sandbox `MASTER_KEK` (password manager / secret store; never regenerate).
- Document every guard flag that must stay `0` and verify the gateway boots cleanly under prod guards.

**Phase 2 — Per-source account provisioning research** — one spike per source, easiest→hardest:

| Source | Research items | Friction |
|---|---|---|
| **GitHub** | Test App on a personal/test org; webhook URL+secret; install on 1–2 test repos; App-JWT → installation-token flow against real `api.github.com`; backfill repo enumeration | Low |
| **Slack** | Free test workspace; Events API request-URL verification handshake; signing secret; bot scopes; replay-window (`SLACK_MAX_TIMESTAMP_AGE_S`) behavior on real timestamps | Low–Med |
| **Discord** | Test app + bot; Ed25519 interactions endpoint verification; gateway message-content intent; `PYTHONHASHSEED=0` requirement | Med |
| **Gmail** | **Heaviest**: GCP project, service account + domain-wide delegation (needs a Workspace domain you control), Pub/Sub topic + push subscription, OIDC audience, watch renewal | High |

For each: document exact account-creation steps, credentials produced (map to `.env.production.example` vars), free-tier limits, and teardown.

**Phase 3 — Validation methodology (1 day)**
- Adapt [`production-deploy.md` §4](./production-deploy.md) into a repeatable checklist with explicit pass/fail oracles:
  - Backfill: assert `provider_installations` + `onboarding_triggers` rows, observation counts with correct `source_channel`, dedup (reuse `assertions.py`).
  - Live: post a known message/email → assert real signature verification passed + cross-path dedup vs backfill.
- Solve the **oracle problem**: a seed script that creates N known issues/messages/emails per source so expected counts are deterministic.

**Phase 4 — Cost, limits, teardown (½ day)**
- Document rate-limit ceilings and a safe backfill cap per source; confirm `GITHUB_MAX_BACKFILL_REPOS` / Slack private-channel flags.
- Define teardown: revoke installs, delete Pub/Sub subs, rotate/destroy sandbox secrets.

## 7. Ingress decision (open)

Webhooks, OAuth callbacks, and Gmail Pub/Sub push all need a *publicly reachable* gateway. Providers dislike changing webhook URLs, and **Gmail Pub/Sub push needs a stable HTTPS endpoint with a verifiable OIDC audience** — the failure-prone part.

| | Local + tunnel | Cloud staging | Hybrid |
|---|---|---|---|
| Setup speed | Fastest | Slowest | Medium |
| Cost | ~free | small ongoing VM | small |
| URL stability | Churns (free ngrok) / stable (named cloudflared) | Stable DNS+TLS | Stable where it matters |
| **Gmail push fit** | Painful — OIDC audience over a tunnel | Clean | Gmail → cloud only |
| Fidelity to prod | Lower | Highest | Mixed |

Given the confirmed choices (real prod guards + all four sources incl. Gmail), the internally-consistent options are **cloud staging** or **hybrid** — a pure free-tunnel setup will fight us specifically on Gmail's OIDC push, the hardest committed source. To start moving before standing up a VM, a **named cloudflared tunnel** (stable URL, free) covers GitHub/Slack/Discord and defers the Gmail/cloud question to the Phase 2 Gmail spike.

## 8. Deliverables

1. `docs/ingestion/sandbox-real-api-runbook.md` — provisioning + validation checklist per source.
2. A sandbox env template (e.g. `.env.sandbox.example`) keyed off `.env.production.example`.
3. A seed script to create known content per source (deterministic oracle).
4. Per-source validation report mirroring the `docs/validation/path_i/run*_report.md` format.

## 9. Risks

- **Gmail DWD requires a Workspace domain you control** — no domain, no domain-wide delegation; may need a paid test Workspace.
- **Webhook URL churn** breaks already-registered provider apps; favor a stable ingress before registering apps.
- **Real backfill volume** against a populated account can hit rate limits — start with capped, seeded accounts.
- **Secret loss**: regenerating `MASTER_KEK` orphans encrypted secrets; store it durably from the start.
