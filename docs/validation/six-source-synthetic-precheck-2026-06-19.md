# Six-source synthetic ingestion pre-check (Slack / Jira / Notion / GitHub / Discord / Telegram)

**Date:** 2026-06-19
**Branch:** `feat/signal-source-synthetic-precheck`
**Purpose:** before wiring these six sources with **real** credentials for our own
coordination, prove the ingestion pipeline runs correctly end-to-end under
synthetic load — backfill **and** live — with the data left in a **persistent**
database for manual cross-verification.

## TL;DR

| | |
|---|---|
| **Backfill observations** | **1200** — exactly **200 / source × 6** |
| **Live observations** | **30** — 5 / source × 6 (proves live ingress too) |
| **Total landed** | **1230** |
| **Duplicate `(tenant, source_channel, external_id, occurred_at)` groups** | **0** |
| **`source_onboarding_runs`** | 6 / 6 `completed` |
| **Data-plane subprocesses** | all `rc=0` (oauth_poller, tenant_onboarding, source_onboarding, shard_fetch, reconciler, normalizer, observation_writer) |
| **Gate verdict** | **READY ✅** |
| **Bugs found & fixed** | 2 (see below) |

The run drives the **real** subprocess + Kafka data plane (not an in-process
shortcut): `oauth_poller → tenant_onboarding → source_onboarding → shard_fetch →
normalizer → observation_writer`, with the raw tier on in-process moto-S3 and the
live edges hitting the real webhook/gateway ingress.

## Persistent databases (for cross-verification)

Both live on the dockerised Postgres `company_os_postgres` (host port **5434**),
and are **left intact** after the run:

| DB | Contents |
|---|---|
| `fyralis_signal_check` | the 1230 landed observations (the spam result) |
| `fyralis_signal_tests` | the pytest scratch DB for the per-source test suites |

Cross-verify, e.g.:

```sql
-- per-source counts (backfill vs live)
SELECT regexp_replace(t.name,'^x3-a11-([a-z]+)-.*$','\1') AS source,
       o.source_channel,
       count(*) FILTER (WHERE o.occurred_at <  '2026-02-01') AS backfill,
       count(*) FILTER (WHERE o.occurred_at >= '2026-02-01') AS live,
       count(*) AS total, count(DISTINCT o.external_id) AS distinct_eids
FROM observations o JOIN tenants t ON t.id = o.tenant_id
GROUP BY 1,2 ORDER BY 1;
```
```
  source  |  source_channel  | backfill | live | total | distinct_eids
----------+------------------+----------+------+-------+--------------
 discord  | discord:message  |      200 |    5 |   205 |          205
 github   | github:webhook   |      200 |    5 |   205 |          205
 jira     | jira:issue       |      200 |    5 |   205 |          205
 notion   | notion:object    |      200 |    5 |   205 |          205
 slack    | slack:message    |      200 |    5 |   205 |          205
 telegram | telegram:message |      200 |    5 |   205 |          205
```
Connect from the host: `psql postgresql://company_os:company_os@localhost:5434/fyralis_signal_check`
(or pgAdmin on http://localhost:5050).

## How to reproduce

The runner is `services/ingest/synthetic/validation_runs/spam_six_sources.py` —
it reuses the proven `run_all_sources` overlap-gate orchestration, narrowed to
the six sources and dialed to exactly 200 backfill observations/tenant.

> A persistent DB must be **dropped + recreated** before each run: the harness
> migrates **and `TRUNCATE`s** the DB it is given (the truncate wipes
> `schema_migrations`, so a second in-place run re-applies migrations and trips
> the `0081_grafana` source-CHECK landmine against the prior run's rows).

```bash
docker exec company_os_postgres psql -U company_os -d postgres \
  -c "DROP DATABASE IF EXISTS fyralis_signal_check;" \
  -c "CREATE DATABASE fyralis_signal_check;"
docker exec company_os_postgres psql -U company_os -d fyralis_signal_check \
  -c "CREATE EXTENSION IF NOT EXISTS vector;"

DATABASE_URL=postgresql://company_os:company_os@localhost:5434/fyralis_signal_check \
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 COMPANY_OS_ENV=test \
OBS_EMBEDDING_MODE=cutover TENANTS_PER_SOURCE=1 LIVE_PER_TENANT=5 \
./.venv/bin/python -m services.ingest.synthetic.validation_runs.spam_six_sources
```

Infra used: dev Kafka `fyralis_dev_kafka` (host **:9092**, isolated from the
dockerised `company_os_kafka`); moto-S3 spun up **in-process** by the harness;
no real external API is contacted (mock clients + fixtures + in-process ASGI).

## Per-source fixture knobs → exactly 200 backfill observations

| Source | Fixture params | Count |
|---|---|---|
| slack | `channels=1, messages_per_channel=200` | 1×200 = 200 |
| github | `repos=1, events_per_repo=100, per_page=60` (forces 2 pages/type) | 100 issues + 100 PRs = 200 |
| discord | `channels=1, messages_per_channel=200` (channels **must** be 1 — see below) | 1×200 = 200 |
| jira | `projects=1, issues_per_project=200, transitions=0, comments=0` | 1×200 = 200 |
| notion | `databases=1, pages_per_database=200, loose_pages=0, blocks=0` | 1×200 = 200 |
| telegram | `dialogs=1, messages_per_dialog=200` | 1×200 = 200 |

## One signal, end-to-end (per source)

The shared backbone for every source:
**install + `onboarding_triggers` seed → `tenant_onboarding` → `source_onboarding_run`
(planner emits shards) → `shard_fetch` (fetcher ↔ mock client) → raw envelope to
moto-S3 + Kafka `ingestion.raw.<src>` → `normalizer` (handler maps raw → draft) →
`observation_writer` (persists to partitioned `observations`)**. Per-source specifics:

- **slack** → `source_channel=slack:message`, `external_id={channel}:{ts}`.
  Planner `_plan_channel_shards` (planners/slack.py) emits 1 channel-window shard;
  fetcher paginates `conversations_history`; handler emits 1 draft/message.
- **github** → `source_channel=github:webhook`, `external_id={node_id}:{action}`
  (e.g. `I_kwDO…:opened`, `PR_kwDO…:closed`). Planner emits 5 shards/repo; only
  `issues` + `pull_requests` are fixtured → 100 + 100. Backfill fetches a page per
  fetcher call; `shard_fetch`'s loop drains pages until `end_of_data` (now derived
  from GitHub's Link `rel="next"` — see bug #3).
- **discord** → `source_channel=discord:message`, `external_id=discord:{snowflake}`.
  Gateway-style live (direct dispatch, no HTTP). Backfill planner samples
  `k=max(1,int(channels*0.05))` channels → **channels=1 always samples the one
  channel fully**; channels≥2 would sample a subset.
- **jira** → `source_channel=jira:issue`, `external_id=jira:{site}:issue:{id}:{updated}`.
  Planner emits 1 `jira_project_issues` shard/project; fetcher runs JQL; handler 1
  draft/issue (transitions/comments at 0 keep it 1:1).
- **notion** → `source_channel=notion:object`, `external_id=notion:page:{page_id}`.
  Planner enumerates databases (shards), fetcher pages DB rows → 1 page record each.
  Live ingress is the **thin webhook → retrieve_page → shadow-write** path (HTTP 200,
  not a 202 Kafka-cutover).
- **telegram** → `source_channel=telegram:message`,
  `external_id=telegram:{install}:{dialog}:{message_id}:none`. Gateway/MTProto-style
  live (direct dispatch, no HTTP). Planner emits 1 `telegram_dialog_history`
  shard/dialog; fetcher pages history backward.

Live ingress proven per source: slack/jira/github webhook → **HTTP 202** Kafka
cutover; notion webhook → **HTTP 200** shadow-write; discord/telegram → **direct
gateway dispatch**. A tampered-signature probe on the two HMAC-verified edges in
scope (jira, notion) was **rejected** (`2/2`, no 2xx) — signature gate holds.

## Tests run (all related to these six sources)

Against `fyralis_signal_tests`, `COMPANY_OS_ENV=test`:

- **planners / fetchers / handlers / reconcilers** unit tests for the six sources —
  `150 passed` (after the fix below; was `150 passed, 1 failed`).
- **synthetic generators + live generators + webhook verifiers**
  (`test_{jira,notion}_synthetic_fetch`, `test_{slack_webhook,github_webhook,discord_gateway}`,
  `test_verifier_{jira,notion}`, `test_tenant_resolver_extract`, slack-DM, notion pipeline,
  github memo) — `94 passed`.
- the six-source spam gate itself — **READY ✅**.

## Bugs found & fixed (this branch)

1. **Reconciler service crashes on startup (`ModuleNotFoundError: …reconcilers.whatsapp`).**
   `reconcilers/__init__.register_pool_provider()` iterates `RECONCILER_DISPATCH`
   and `import_module(f"…{source}")` for every key. WhatsApp (source #26) is in the
   dispatch map (live-only; backfill reconciliation deferred) but ships **no**
   `reconcilers/whatsapp.py`, so the import raised and crashed the whole
   reconciler / PeriodicReconciler service — killing steady-state gap detection for
   **all** sources. Surfaced as `reconciler: rc=1` in the first runs.
   **Fix:** guard the import (`except ModuleNotFoundError: continue`) — sources with
   a `_not_implemented_reconciler` placeholder and no module simply have no pool
   provider to register. `register_pool_provider` now registers 25 sources, skips
   whatsapp, and the run is READY with `reconciler: rc=0`.

2. **Stale drift-guard test** `fetchers/tests/test_telegram.py::test_onboarding_and_reconciler_cover_telegram`.
   It asserted the obsolete literal `telegram_reconciler_mod.set_pool_provider` in
   `reconciler.py`; that per-source block was deliberately replaced by the
   centralized `register_pool_provider(pool)` (derived from `RECONCILER_DISPATCH`,
   so it cannot drift). Telegram **is** registered (confirmed empirically: telegram
   backfill `completed`, not `failed`). **Fix:** the guard now asserts the current
   mechanism (service calls `register_pool_provider`; telegram ∈ `RECONCILER_DISPATCH`
   with a `set_pool_provider`).

3. **GitHub multi-page backfill stopped after the first page (silent truncation).**
   *Not* a missing re-queue — `shard_fetch`'s fetch loop (`while True` →
   break on `FetchResult.end_of_data`, [workflows/shard_fetch.py](../../services/ingest/ingestion/workflows/shard_fetch.py))
   already drains every page. The bug was in how the **github fetcher computed
   `end_of_data`**: `is_end = next_page is None OR len(page_records) < _DEFAULT_PER_PAGE`
   ([fetchers/github.py](../../services/ingest/ingestion/fetchers/github.py)). GitHub's
   Link-header `rel="next"` (parsed into `next_page` by the client) is the authoritative
   end signal — present iff another page exists — but the `len < _DEFAULT_PER_PAGE`
   belt **overrode** it: any short-but-not-final page (the synthetic mock caps each page
   at the fixture `per_page`, and **real GitHub can also return short non-final pages**)
   was misread as the last page, so only the first page landed (60 of 200 github obs).
   **Fix:** `is_end = next_page is None` — trust the Link header alone (regression test
   `fetchers/tests/test_github.py::test_short_nonfinal_pages_do_not_end_pagination`
   drives the drain loop over 3 short pages and asserts all are collected). The run now
   forces github onto **2 pages/type** (`per_page=60` → 60+40) and lands the full **200**
   end-to-end. This removed a latent **production data-loss** risk, not just a test artifact.

## Caveats for the real-credential wiring

- **Embeddings:** run under `OBS_EMBEDDING_MODE=cutover`, so all rows land with
  `embedding = NULL` (decommission path). Observation **landing** is independent of
  embedding mode; flip to `eager` if vectors are wanted for the real run.
- **Dedup is tenant-scoped** on `(tenant_id, source_channel, external_id, occurred_at)`,
  and the application pre-check ignores `occurred_at`. Each source's install
  identifier embeds the tenant slug, so external_ids stay tenant-distinct.
