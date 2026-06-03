# Jira (IN-17)

> Issue tracking as the *declared work* layer. The 8th source: API-token Basic
> auth, dedicated tables, full-pipeline backfill + HMAC webhooks, and a webhook
> tenant-resolution path that reuses the generic `provider_installations` edge.

| Field | Value |
|---|---|
| Source | `jira` |
| Primary channel | `jira:issue` |
| Trust tier | `authoritative` |
| Live ingress | **webhook** `/webhooks/jira/events` → full pipeline (cutover-enabled) |
| Backfill | `POST /rest/api/3/search/jql` |
| Auth | API-token **Basic** auth (email + API token) |
| Signature | HMAC, GitHub-style `X-Hub-Signature` |

## Auth & install

[services/ingest/integrations/jira/onboarding.py](../../../services/ingest/integrations/jira/onboarding.py)
+ [jira/client.py](../../../services/ingest/integrations/jira/client.py). Auth is HTTP
**Basic** with `(email, api_token)`. Backfill uses dedicated `jira_*` tables;
**live webhook tenant resolution reuses the generic `provider_installations`
edge** (no Jira-specific resolver table).

## Ingress (live)

`gateway /webhooks/jira/events` → HMAC `X-Hub-Signature` verified, GitHub-style
([signatures/jira.py](../../../services/app/webhooks/signatures/jira.py)) → full
pipeline (cutover-enabled, in `_CUTOVER_ENABLED_PROVIDERS`) or inline fallback.
Channel `jira:issue`.

## Backfill

[planners/jira.py](../../../services/ingest/ingestion/planners/jira.py) +
[fetchers/jira.py](../../../services/ingest/ingestion/fetchers/jira.py) use
**`POST /rest/api/3/search/jql`** — the classic `GET /rest/api/3/search` endpoint
has returned **HTTP 410 Gone since 2025**. Incremental via the per-project
`updated` high-water cursor; produces `RawEnvelope`
(`ingress_kind="backfill"`/`"poll"`) → `jira:issue`.

- [reconcilers/jira.py](../../../services/ingest/ingestion/reconcilers/jira.py) — gap
  probe vs the `updated` high-water.
- [handlers/jira.py](../../../services/ingest/ingestion/handlers/jira.py) → `jira:issue`,
  trust `authoritative`.

## Migration & the 0061 collision history

`0062_jira.sql` — dedicated `jira_*` tables + carries every prior source forward
in the four M6 source CHECKs.

> **Numbering history.** IN-16 (Google Drive) and IN-17 (Jira) both originally
> claimed migration `0061`. Resolution: Google Drive merged first as
> `0061_google_drive`; Jira was renumbered to `0062`, and `0062` was made to
> **carry `google_drive` forward** in its CHECK constraints (the
> newest-migration-must-list-every-prior-source rule — see
> [architecture.md](../architecture.md) "Migration landmine").

## Sandbox

12/12 checks in the real-API sandbox (full pipeline backfill + HMAC webhooks).

Spec: `specs/IN-17-jira/` (off `integration/ingestion-hardening`). See
[architecture.md](../architecture.md).
