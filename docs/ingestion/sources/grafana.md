# Grafana (IN-GRAFANA)

> Observability/alerting as the *operational signal* layer. The 11th source:
> service-account **Bearer** token, a single instance-scoped install table, an
> annotations backfill, and an HMAC-signed Alerting-webhook live edge that reuses
> the generic `provider_installations` tenant-resolution edge.

| Field | Value |
|---|---|
| Source | `grafana` |
| Channels | `grafana:alert` (live webhook) · `grafana:annotation` (backfill/poll) |
| Trust tier | `authoritative` |
| Live ingress | **webhook** `/webhooks/grafana/events` → full pipeline (cutover-enabled) |
| Backfill | `GET /api/annotations` (epoch-ms windowed) |
| Auth | service-account **Bearer** token (API keys deprecated 2025) |
| Signature | HMAC-SHA256, bare hex in `X-Grafana-Alerting-Signature` (Grafana 12.0+) |

## Scope (v1)

Discrete, event-shaped signals only — **alerts** (live) and **annotations**
(backfill). Raw metric/log time-series (the `/api/ds/query` + datasource proxy)
is intentionally **out of scope**: it is a firehose that does not fit the
`observations` model, and any metric worth a signal is already expressed as a
Grafana alert rule. Backfill is **annotations-only**; the Loki-backed alert
state-history timeline is a documented v2 enhancement.

## Auth & install

[services/ingest/integrations/grafana/onboarding.py](../../../services/ingest/integrations/grafana/onboarding.py)
+ [grafana/client.py](../../../services/ingest/integrations/grafana/client.py). Auth is a
**service-account token** sent as `Authorization: Bearer <token>`. The instance
`base_url` + `org_id` live on the `grafana_installations` row (one per
`(tenant, base_url)`); the token is held in `encrypted_secrets` behind
`secret_ref`. Annotations + alert state are **org-wide**, so there is **no
per-resource child table** (unlike Jira's `jira_projects` / Mercury's
`mercury_accounts`) — the planner emits exactly one shard per install. Live
webhook tenant resolution reuses the generic `provider_installations` edge.

## Ingress (live)

`gateway /webhooks/grafana/events` → HMAC-SHA256 verified
([signatures/grafana.py](../../../services/app/webhooks/signatures/grafana.py)): bare
lowercase hex in `X-Grafana-Alerting-Signature` over the raw body (or
`"{ts}:"+body` when a timestamp header is configured via
`GRAFANA_WEBHOOK_TIMESTAMP_HEADER`). Tenant is resolved from the payload
`externalURL` host
([tenant_resolver.py `_extract_grafana`](../../../services/app/webhooks/tenant_resolver.py)).
→ full pipeline (cutover-enabled, in `_CUTOVER_ENABLED_PROVIDERS`) or inline
fallback. Channel `grafana:alert`, kind `state_change`.

One webhook POST delivers a notification **group** of alerts; v1 emits **one
`state_change` observation per delivery** (the full per-alert detail — including
each alert's `fingerprint` — is preserved in `content["alerts"]`). Per-alert
fan-out is a v2 enhancement (it needs a normalizer-level group-explode step; the
handler contract returns a single draft). external_id:
`grafana:{instance}:alert:{group_hash}:{status}:{rep_ts}` — re-notifications of
the same firing group dedup; a distinct fire/resolve cycle lands as a new row.
Alerts are **machine-generated** → `source_actor_ref=None` (the actorless path in
[ingestion/core.py](../../../services/ingest/ingestion/core.py) actor resolution).

> **Version gate.** HMAC signing requires **Grafana 12.0+** (May 2025). Older
> self-hosted instances should instead set a static `Authorization: Bearer
> <secret>` header on the contact point — supporting that verifier mode is a
> documented follow-up.

## Backfill

[planners/grafana.py](../../../services/ingest/ingestion/planners/grafana.py) +
[fetchers/grafana.py](../../../services/ingest/ingestion/fetchers/grafana.py) walk
**`GET /api/annotations`** (epoch-ms `from`/`to` + `limit`) newest-first, advancing
the upper bound backward until a short page hits the window floor
(`GRAFANA_BACKFILL_WINDOW_DAYS`, default 90; `0` = all time). Grafana auto-creates
an annotation for every alert state transition, so this stream carries historical
alert transitions (tagged `alertId`/`newState`) alongside deploy markers and
manual notes. Incremental via the `high_water_time_ms` cursor; produces
`RawEnvelope` (`ingress_kind="backfill"`/`"poll"`) → `grafana:annotation`.

- [reconcilers/grafana.py](../../../services/ingest/ingestion/reconcilers/grafana.py) —
  gap probe vs the `high_water_time_ms` (1-row `GET /api/annotations?from=hw+1ms`).
- [handlers/grafana.py](../../../services/ingest/ingestion/handlers/grafana.py) →
  `grafana:annotation` (plain = `signal`; alert-state = `state_change`), trust
  `authoritative`. external_id `grafana:{instance}:annotation:{id}:{time}`.

## Migration

[`0080_grafana.sql`](../../../db/migrations/0080_grafana.sql) — `grafana_installations`
(+ RLS) + carries every prior source forward in the four M6 source CHECKs
(`slack…quickbooks` + `grafana`, per the newest-migration-must-list-every-prior-
source rule — see [architecture.md](../architecture.md) "Migration landmine").
Validated: applies cleanly on top of `0079` (full chain) with all four CHECKs
admitting `grafana` while preserving `jira`/`quickbooks`.

## Tests

- [fetchers/tests/test_grafana.py](../../../services/ingest/ingestion/fetchers/tests/test_grafana.py)
  — backward window walk, high-water tracking, warm-start incremental floor.
- [handlers/tests/test_grafana.py](../../../services/ingest/ingestion/handlers/tests/test_grafana.py)
  — annotation signal vs alert-state state_change, actor resolution, alert-group
  external_id (fire ≠ resolve).
- [webhooks/tests/test_verifier_grafana.py](../../../services/app/webhooks/tests/test_verifier_grafana.py)
  — HMAC happy path, tamper, wrong-secret, missing header, timestamp mode.

## Real-API testing

Needs a Grafana instance + a service-account token (role with `annotations:read`)
+ an Alerting webhook contact point (Grafana 12.0+ for HMAC). Env knobs:
`GRAFANA_BACKFILL_WINDOW_DAYS`, `GRAFANA_ANNOTATIONS_PAGE_SIZE`,
`GRAFANA_RL_MAX_ATTEMPTS`, `GRAFANA_WEBHOOK_TIMESTAMP_HEADER`. See
[architecture.md](../architecture.md).
