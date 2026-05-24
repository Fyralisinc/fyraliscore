# Implementation Plan: IN-17 — Jira as an Ingestion Signal Source (Concurrent Backfill + Live)

**Branch**: `feature/IN-17-jira-integration` (off `integration/ingestion-hardening`) | **Date**: 2026-05-24
**Author**: lead engineer (Claude)

## 0. Goal

Add **Jira** as the 7th ingestion source, following the established M6 source pattern
(planner → fetcher → handler → reconciler + webhook ingress) so that Fyralis gets
**concurrent backfill + live** ingestion of real Jira signals through the **full
pipeline** (`ingestion.raw` → normalizer → `ingestion.normalized` → observation_writer),
NOT inline `ingest()` only. Deliverable stops at the point where the operator supplies
real Jira credentials in env; everything up to and including a mock-driven end-to-end
sandbox is built and verified.

## 1. Why full pipeline (not inline `ingest()`)

Jira is the same shape as GitHub: it has **both** a live push surface (Jira Cloud
webhooks for `jira:issue_created/updated`, `comment_created/updated`, etc.) **and** a
historical query surface (JQL search over the whole project history). Fyralis reasoning
needs the *backfill* (months/years of issue + transition history to learn flow/velocity
baselines) running **concurrently** with the *live* stream (new transitions as they
happen). That is exactly what the full pipeline provides and what inline-only ingest
cannot:

- Backfill requires the planner/fetcher/shard machinery (paginated JQL, per-project
  shards, cursor checkpointing, reconciler gap-detection). There is no inline path for
  it.
- Durable replay + DLQ + cross-path dedup (a backfilled issue and its live-webhook twin
  collapsing to one observation) only exist on the Kafka data plane.
- This mirrors what github/slack/discord/gmail/notion/google_calendar already do.

Decision: **full pipeline**, identical wiring to GitHub. Inline `ingest()` remains only
as the Kafka-outage fallback that the gateway already implements generically.

## 2. What Jira signals are essential for Fyralis reasoning

Fyralis reasons about team/work state — flow, velocity, blockers, commitment, ownership.
The signals that carry that, ranked:

1. **Issues** (the unit of work): key, summary, type, status, priority, assignee,
   reporter, story points, sprint, epic link, labels, resolution, created/updated. The
   *current state* of work.
2. **Changelog / status transitions** (the richest reasoning signal): every field change
   on an issue — especially `status` (To Do → In Progress → Done), `assignee`, `priority`,
   `sprint`, `resolution`. Transitions are *events with time*; they are what velocity,
   cycle-time, blocker-detection and re-open-rate are computed from. Emitted as
   first-class `state_change` observations.
3. **Comments**: discussion, decisions, blockers ("waiting on X"), context that text
   reasoning consumes.

Deferred (documented, not built in v1): worklogs (effort/time tracking), sprint
start/close events as discrete signals, board/JQL-filter metadata, attachments. These
add fidelity but are not required for the core flow/velocity/blocker reasoning and would
expand scope past the "working end-to-end" deliverable. v1 captures sprint/story-point
*as issue fields*, which is enough for commitment/velocity baselines.

### Mapping to observations
- One **`jira:issue`** handler channel (one channel, like `github:webhook`), the handler
  branches on the reshaped event/record type.
- Issue create/update → observation (kind `issue`).
- Each changelog history entry → observation; a `status`-field history → `state_change`.
- Each comment → observation (kind `comment`).

## 3. Auth model — API token + Basic auth (Jira Cloud REST v3)

Decision: **Jira Cloud REST API v3** with **HTTP Basic auth** = `base64(account_email:api_token)`.

Rationale: the deliverable is "operator gives creds → we set env → it works." That is the
API-token model (an Atlassian account email + an API token from
`id.atlassian.com/manage-profile/security/api-tokens` + the site base URL
`https://<site>.atlassian.net`). Full OAuth 2.0 (3LO) via `api.atlassian.com/ex/jira/{cloudId}`
— with the install/callback/refresh dance — is **deferred**; it adds a multi-leg OAuth
flow that is unnecessary to prove real ingestion and can layer on later behind the same
fetcher/handler. This matches how Gmail/Calendar/Drive use a non-`provider_installations`
substrate (DWD) rather than the bot-token OAuth path.

The API token is stored in `encrypted_secrets` and referenced by `secret_ref`; the email
and base_url live in the `jira_installations` row.

## 4. Install/onboarding model — dedicated tables (mirrors google_calendar)

Jira needs a workspace-scoped install plus an enumerated set of sub-resources
(projects), exactly the gcal/gmail shape, so it uses **dedicated tables**, not
`provider_installations`:

- **`jira_installations`** — one row per `(tenant, base_url)`: `base_url`,
  `account_email`, `secret_ref` (→ api_token), `cloud_id` (nullable; for webhook tenant
  resolution), `disabled_at`.
- **`jira_projects`** — one row per project the planner shards on: `project_key`,
  `project_id`, `project_name`, `updated_cursor` (high-water `updated` timestamp — the
  incremental delta primitive, analogous to gcal `sync_token`), `state`
  (`pending|active|paused|errored`).

Planner loader aggregates active projects onto the install via a JSON-aggregating LEFT
JOIN (copy of `_LOAD_GCAL_INSTALL_SQL`) → emits **one shard per project**
(`shard_kind = "jira_project_issues"`). ShardFetch loads the bare install to build the
client. No `source_client` is needed at plan time (projects are read from DB, populated
at install/seed time via `project/search`), matching gcal.

## 5. External-id scheme (mutable-source dedup, per the IN-15/16 lesson)

Jira issues and comments are **mutable**, so external_id must encode a version or a
re-edit silently dedups away and the change is lost. Use the entity's `updated`
timestamp as the version; changelog histories are immutable (history id is stable):

- Issue: `jira:{site}:issue:{issue_id}:{updated_ts}`
- Comment: `jira:{site}:comment:{comment_id}:{updated_ts}`
- Transition: `jira:{site}:transition:{issue_id}:{history_id}` (immutable; no version)

Backfill and webhook converge on the same external_id, so a backfilled issue and its
live-webhook twin collapse to one observation (github/notion/gcal parity).

## 6. Live webhook security — URL-embedded shared secret

Jira Cloud **dynamic webhooks** (registered via REST) and most automation webhooks do
**not** HMAC-sign their payloads the way GitHub does (only Connect-app JWT webhooks do).
Decision: register the webhook with a **per-installation opaque secret token embedded in
the callback URL** (`/webhooks/jira/{secret_token}` or `?token=`), and verify it
constant-time at the edge, then resolve the tenant from the payload's site/cloudId.
Documented as the deliberate deviation from the github HMAC verifier. (If/when we move to
Connect-app JWT, the verifier slot is already there.)

## 7. Full file/dispatch inventory (the allowlist tax)

Per the "adding an ingestion source" checklist, widen **all** of these together (each is a
silent-drop landmine if missed):

- Migration `0061_jira.sql`: 4 source CHECKs (`source_onboarding_runs`,
  `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`).
- `services/ingestion/raw_tier/envelope.py` `SourceLiteral`
- `services/ingestion/progress/events.py` `Source` Literal
- `services/ingestion/normalizer/channel_mapping.py` `_CHANNEL_MAP` (`("jira","backfill")`,
  `("jira","webhook")`, `("jira","poll")` → `jira:issue`)
- `services/ingestion/normalizer/invariants.py` `_S3_KEY_RE` source alternation
- `services/ingestion/raw_tier/s3.py` `build_raw_s3_key` source guard
- `services/ingestion/core.py` embedding-gate family tuple
- `services/ingestion/shadow_write.py` source Literal
- `services/ingestion/dlq/publish.py` `_VALID_SOURCES` (+ `dlq/models.py` reuses
  `SourceLiteral`)
- `services/ingestion/workflows/source_onboarding.py` `VALID_SOURCES` + `_LOAD_JIRA_INSTALL_SQL`
- `services/ingestion/workflows/tenant_onboarding.py` `VALID_SOURCES` + `_LOAD_ACTIVE_SOURCES_SQL`
- `services/ingestion/workflows/shard_fetch.py` install-load SQL (`SELECT secret_ref`)
- Dispatch tables: `planners/__init__.py`, `fetchers/__init__.py`,
  `reconcilers/__init__.py`, `handlers/__init__.py` (CHANNEL_TRUST_MAP + import).
- `fetchers/_clients.py`: `build_jira_client` + `open_jira_client`.
- Webhook edge: `services/webhooks/signatures/__init__.py` (VERIFIERS),
  `services/webhooks/router.py` (`_PROVIDER_TO_SHADOW_SOURCE`,
  `_CUTOVER_ENABLED_PROVIDERS`), `services/webhooks/tenant_resolver.py` (`_extract_jira`).

## 8. Ordered task breakdown

- **T1** — This design doc. ✔
- **T2** — Migration `0061_jira.sql`: `jira_installations` + `jira_projects` (RLS) + widen 4 source CHECKs.
- **T3** — Widen every source-name allowlist / Literal / dispatch table (§7) to admit `jira`.
- **T4** — `services/integrations/jira/client.py` (Basic-auth REST, `search_issues` JQL paginate, `list_projects`) + `_clients.build_jira_client`/`open_jira_client`.
- **T5** — `services/ingestion/planners/jira.py` (1 shard/project) + `_LOAD_JIRA_INSTALL_SQL` in source_onboarding (aggregating) and shard_fetch (bare).
- **T6** — `services/ingestion/fetchers/jira.py` (JQL paginate by `updated ASC`, `expand=changelog`, reshape issue/transition/comment → webhook-event bodies, versioned external_id, cursor = last `updated`).
- **T7** — `services/ingestion/handlers/jira.py` (`jira:issue` channel; branch on event/record_type; status history → `state_change`) + channel_mapping + trust map.
- **T8** — `services/ingestion/reconcilers/jira.py` (per-project `updated` high-water gap detection; cheap "anything newer?" probe).
- **T9** — Webhook ingress: `signatures/jira.py` (URL-token verifier) + VERIFIERS + router provider maps + `tenant_resolver._extract_jira` (site/cloudId → tenant) + gateway data-plane (reuses generic wiring).
- **T10** — Tests: planner/fetcher/handler/reconciler unit + onboarding e2e + verifier (mirror github tests; include mutable re-edit dedup test).
- **T11** — Sandbox: `scripts/sandbox_jira.py` (mock Jira server driving the real fetcher→ingest path; 1-project backfill + incremental + dedup), `services/synthetic/mock_servers/jira`, `docs/ingestion/jira-sandbox.md`, `.env.sandbox.example` entries. Stop before real creds.
- **T12** — Run migrations + full suite; document the exact env the operator must supply (`JIRA_BASE_URL`, `JIRA_ACCOUNT_EMAIL`, `JIRA_API_TOKEN`, webhook secret) and the seed command.

## 9. Operator handoff (what you supply at the end)

When you give me real creds I will set in `.env.sandbox` (or prod env):
```
JIRA_BASE_URL=https://<your-site>.atlassian.net
JIRA_ACCOUNT_EMAIL=<your-atlassian-account-email>
JIRA_API_TOKEN=<token from id.atlassian.com/manage-profile/security/api-tokens>
JIRA_WEBHOOK_SECRET=<random opaque token for the webhook URL>   # live path
```
…then run the seed script to create the `jira_installations` row + `encrypted_secrets`
entry + `jira_projects` rows (project enumeration), fire the onboarding trigger, flip
`KAFKA_PATH_ENABLED`, and you will see backfill observations land + (with ngrok) live
webhook observations land — concurrently.
