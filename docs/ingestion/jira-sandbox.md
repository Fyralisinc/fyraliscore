# Jira ingestion (IN-17) — sandbox & onboarding runbook

Jira is the 7th ingestion source. It delivers **concurrent backfill + live**
signals through the **full pipeline** (`ingestion.raw` → normalizer →
`ingestion.normalized` → observation_writer), the same shape as GitHub. Design
decisions and the signal selection live in
[specs/IN-17-jira-integration/plan.md](../../specs/IN-17-jira-integration/plan.md).

## What it ingests

- **Issues** — current field snapshot (status, type, priority, assignee,
  reporter, story points, sprint, labels, resolution). `kind=signal`.
- **Changelog transitions** — one observation per changelog history; a
  **status/resolution** change is `kind=state_change` (the flow/velocity
  signal). Other field changes are `kind=signal`.
- **Comments** — discussion/blocker context. `kind=signal`.

All land on the single `jira:issue` channel, `trust_tier=authoritative`.
external_id is versioned by the entity's `updated` timestamp for the mutable
issue/comment (`jira:{site}:issue:{id}:{updated}`) and by the immutable history
id for transitions (`jira:{site}:transition:{id}:{history_id}`), so a backfilled
record and its live-webhook twin dedup to one observation.

## Auth model

Jira Cloud REST v3, HTTP Basic = `base64(account_email:api_token)`, against the
per-tenant site `https://<site>.atlassian.net`. The API token is held in the
encrypted secret store; `base_url` + `account_email` live on the
`jira_installations` row.

## 1. Dry run with NO credentials (mock end-to-end)

Proves the whole pipeline against a local Jira mock before you have real creds:

```bash
python scripts/sandbox_jira.py          # throwaway DB, dropped on exit
python scripts/sandbox_jira.py --keep   # keep the DB to inspect
```

It exercises project enumeration → planner (1 shard/project) → backfill fan-out
(issue + transition + comment) → incremental `updated >=` delta (a status
transition → `state_change`) → cross-path dedup → the **live-webhook path
through the same handler** (asserting external_id parity with backfill) → the
reconciler gap probe, and prints the observations that landed. Expect
**12/12 checks pass**.

## 2. Onboard a REAL Jira site

### a. Supply credentials

In `.env.sandbox` (or your env):

```
JIRA_BASE_URL=https://<your-site>.atlassian.net
JIRA_ACCOUNT_EMAIL=<your-atlassian-account-email>
JIRA_API_TOKEN=<token from id.atlassian.com/manage-profile/security/api-tokens>
JIRA_WEBHOOK_SECRET=<random opaque secret>     # optional, enables the live path
```

### b. Seed the install (backfill chain + flag)

```bash
python scripts/sandbox_jira_seed.py --tenant <TENANT_UUID>
# or pin a subset:  --projects ENG,OPS
```

This verifies the creds (`GET /myself`), stores the token, enumerates projects,
writes `jira_installations` + `jira_projects` + the `onboarding_triggers` row,
and flips `ingestion.kafka_path_enabled=TRUE` so observations persist. The M6
backfill chain (`oauth_poller → tenant_onboarding → source_onboarding →
shard_fetch → reconciler`) picks up the trigger on its next tick and backfills
each project concurrently.

### c. Live webhooks (optional but recommended)

In Jira → **Settings → System → Webhooks**, create a webhook:

- **URL**: `<SANDBOX_PUBLIC_URL>/webhooks/jira/events` (use the ngrok URL from
  the sandbox; see [sandbox runbook](./real-api-sandbox.md)).
- **Secret**: the same value as `JIRA_WEBHOOK_SECRET`. Jira signs each delivery
  with HMAC-SHA256 in the `X-Hub-Signature` header (GitHub-style); the edge
  verifies it via `services/webhooks/signatures/jira.py`.
- **Events**: Issue *created* / *updated*; Comment *created* / *updated*.

`sandbox_jira_seed.py` (when `JIRA_WEBHOOK_SECRET` is set) registers the
`provider_installations` row (provider=`jira`, installation_id = the site host
from `issue.self`) so the edge resolves the tenant and loads the signing secret.
Webhook deliveries then flow through the cutover path
(`shadow_write_raw → ingestion.raw → … → observation_writer`) — concurrently
with the backfill.

## Verifying it works

```sql
SELECT kind, count(*) FROM observations
 WHERE tenant_id = '<TENANT_UUID>' AND source_channel = 'jira:issue'
 GROUP BY kind;
```

You should see `signal` (issues + comments) and `state_change` (status
transitions) rows accumulating from backfill, then new rows as live webhooks
arrive. Backfill and live twins dedup on the versioned external_id.

## Where the code lives

| Concern | File |
|---|---|
| Migration (tables + source CHECK) | `db/migrations/0061_jira.sql` |
| REST client | `services/integrations/jira/client.py` |
| Onboarding helpers | `services/integrations/jira/onboarding.py` |
| Planner (1 shard/project) | `services/ingestion/planners/jira.py` |
| Fetcher (JQL + fan-out) | `services/ingestion/fetchers/jira.py` |
| Handler (issue/transition/comment) | `services/ingestion/handlers/jira.py` |
| Reconciler (gap probe) | `services/ingestion/reconcilers/jira.py` |
| Webhook HMAC verifier | `services/webhooks/signatures/jira.py` |
| Tenant resolution | `services/webhooks/tenant_resolver.py::_extract_jira` |
| Mock server | `services/synthetic/mock_servers/jira.py` |
| Dry-run sandbox | `scripts/sandbox_jira.py` |
| Real-creds onboarding | `scripts/sandbox_jira_seed.py` |

## v1 scope notes

- `expand=changelog` and `fields.comment` from the search endpoint return the
  most-recent histories/comments inline — sufficient for the live + recent-
  history signal. Deep history would need the per-issue `/changelog` + `/comment`
  endpoints; the reconciler's incremental re-walk + the live webhook stream
  cover ongoing changes.
- Worklogs, discrete sprint start/close events, attachments are **deferred**
  (story points + sprint are captured as issue fields). OAuth 2.0 (3LO) is
  deferred in favour of the API-token model.
- Jira webhook security uses HMAC (`X-Hub-Signature`) on admin/system webhooks.
  Connect-app JWT webhooks would slot into the same verifier seam later.
