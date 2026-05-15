# HTTP Contract: `POST /webhooks/github/events`

Inbound webhook from GitHub. Handled by the existing `services/webhooks/router.py::build_webhooks_router()`'s `/webhooks/{provider}/{subpath:path}` route with `provider='github', subpath='events'`. The router is **modified minimally** by IN-13: a new replay-cache step, a new lifecycle-event branch, and a new repo-filter step are added; existing signature-verify and tenant-resolve steps are unchanged.

## Auth

NOT Bearer. Authentication is the HMAC-SHA256 signature in `X-Hub-Signature-256` over the raw body, verified against the App-level webhook secret (single secret App-wide; Clarifications Q1). The route is on the gateway's public-path allowlist via the `_PUBLIC_PATH_PREFIXES` rule for `/webhooks/`.

## Request Headers (required)

- `X-Hub-Signature-256: sha256=<hex>` — HMAC-SHA256 of the raw body using the App-level webhook secret. Required.
- `X-GitHub-Event: <event-type>` — `pull_request | push | issues | issue_comment | pull_request_review | check_run | installation | installation_repositories | ping`. Required.
- `X-GitHub-Delivery: <uuid>` — GitHub's per-delivery UUID. Recorded in observation metadata and used as the replay-cache key. Required; missing or non-UUID → replay-cache bypass with a WARN log.

## Request Body

A JSON object per GitHub's webhook event schemas. The body's `installation.id` field is required for non-`ping` events; it drives tenant routing.

## Processing Order

The router performs the following steps in order. ANY step failing returns the response indicated; subsequent steps do not execute:

1. **Body size precheck (existing, IN-01)**: > 1 MB → 413.
2. **Best-effort JSON parse (existing)**: malformed JSON is not yet rejected; parsing failure is recorded but signature-verify still runs (no JSON-validity oracle pre-signature).
3. **Tenant-resolution attempt (existing, IN-07)**: `TenantResolver.resolve('github', payload, headers)` extracts `installation.id` via `_extract_github`. The outcome (`Resolved | UnknownInstallation | PayloadMissing`) is captured but enforcement is DEFERRED until after signature verification.
4. **Load secrets (extended in T023)**: `load_secrets('github', tenant_id_uuid, app_state=request.app.state)` returns the App-level webhook secret list (1 or 2 entries during rotation). `tenant_id_uuid` argument is ignored for GitHub.
5. **Signature verification (existing)**: `GitHubVerifier.verify(body, headers, secrets, now)` iterates the secret list with constant-time comparison. Failure → 401 with `_err_response(WebhookVerificationError)`.
6. **Ping short-circuit (NEW, FR-022)**: if `X-GitHub-Event == 'ping'`, return `200 {"handled": "ping"}`. No replay-cache entry written, no tenant enforcement.
7. **Replay-cache check (NEW, FR-008b + Clarifications Q4)**: `replay_cache.seen(installation_id, delivery_id, now)`. If TRUE → return `200 {"handled": "replay"}`, increment `github_webhook_replay_dropped_total`. If FALSE → cache the key and proceed.
8. **Tenant outcome enforcement (existing)**: `UnknownInstallation` → 401 `unknown_installation` (includes never-registered and disabled — same outcome per IN-07 FR-005); `PayloadMissing` → 400 `payload_missing`.
9. **Lifecycle event dispatch (NEW, FR-008c)**: if `X-GitHub-Event ∈ {installation, installation_repositories}`, hand off to `services.integrations.github.lifecycle.dispatch(payload, tenant_id, installation_row_id, pool, secret_store)` and return its response (typically `200 {"handled": <action>}`); NO observation is committed.
10. **Repo-filter check (NEW, FR-008e)**: read `selected_repositories` from the installation row (or use a cached value if the resolver populated one — currently it does not, so a per-delivery DB read suffices). If non-NULL and `payload.repository.full_name not in selected_repositories` → return `200 {"handled": "filtered_repo"}`, increment `github_webhook_filtered_repo_total{reason="not_selected"}`. If NULL → skip (all-repos mode).
11. **Ingest (existing)**: `services.ingestion.core.ingest('github:webhook', payload, ...)` invokes `services.ingestion.handlers.github.handle_github_webhook` which shapes the event into an `ObservationDraft` and the core commits it.

## Response

### 200 OK

The successful "the system handled this webhook" response. Body:

```json
{
  "observation_id": "<uuid7>",      // present when ingestion ran
  "deduped": true | false,           // observation-layer dedup outcome
  "trigger_queue_id": "<uuid7>",     // present when a post-commit trigger was enqueued
  "secret_label": "github_app:webhook_secret",
  "handled": "<short-form-outcome>"  // present on short-circuit paths: 'ping' | 'replay' | 'filtered_repo' | 'installation_<action>' | 'installation_repositories_<action>'
}
```

201 instead of 200 when the ingestion path inserted a new observation row (existing semantic).

### 401 Unauthorized

Signature failure OR tenant-resolution failure. Body matches the existing `WebhookVerificationError.to_dict()` shape:

```json
{
  "code": "signature_mismatch" | "malformed_signature_header" | "missing_signature" | "unknown_installation" | "secret_not_configured",
  "message": "<human-readable>",
  "context": {
    "provider": "github",
    "reason": "<reason>"
  }
}
```

### 400 Bad Request

- `payload_missing` — body did not carry a parseable `installation.id` for a non-ping event.
- `payload_too_large` — body exceeded the 1 MB cap (also raised at the precheck layer).
- `invalid_json` — verified body could not be JSON-parsed.

### 413 Payload Too Large

Same shape, status 413; raised by the body-size precheck before signature verification.

### 501 Not Implemented

`handler_not_found` — should never occur in steady state; the GitHub handler is registered. Reserved for defensive coverage.

## Metrics

Per FR-017, aggregate-only:
- `github_webhook_received_total`
- `github_webhook_verified_total{result}` where result ∈ `ok|signature_failed|unknown_installation`
- `github_webhook_signature_failure_total{reason}` 
- `github_webhook_replay_dropped_total`
- `github_webhook_replay_cache_bypass_total`
- `github_webhook_filtered_repo_total{reason}`
- `github_webhook_lifecycle_total{event,action}`

## Logging

Every processed delivery writes a structured log line with:
- `provider='github'`
- `event_type` (from `X-GitHub-Event`)
- `delivery_id` (from `X-GitHub-Delivery`)
- `installation_id_hash` (BLAKE2b 8-byte hex of `installation.id`) — NEVER the raw id
- `installation_row_id` and `tenant_id` (UUIDs) on success paths
- `outcome` (e.g. `ingested`, `replayed`, `filtered_repo`, `lifecycle_dispatched`, `signature_failed`, `unknown_installation`)

Per FR-016 and SC-008: NO log line contains the raw `installation_id`, `account.login`, `account.id`, the App's private key, OR the candidate signature value.

## Examples

### Successful PR delivery

```http
POST /webhooks/github/events HTTP/1.1
Content-Type: application/json
X-Hub-Signature-256: sha256=ab12...
X-GitHub-Event: pull_request
X-GitHub-Delivery: 11111111-2222-3333-4444-555555555555

{"action":"opened","pull_request":{"id":1,"node_id":"PR_kwDO...","number":42,"title":"Add rate limiter",...},"installation":{"id":"12345678",...},"repository":{"full_name":"org/a",...},"sender":{"login":"alice",...}}
```

Response:
```http
HTTP/1.1 201 Created
Content-Type: application/json
X-Observation-Id: 01HRYZ...
X-Deduped: false

{"observation_id":"01HRYZ...","deduped":false,"trigger_queue_id":"01HRZ0...","secret_label":"github_app:webhook_secret"}
```

### Replay drop

Same delivery re-sent within 5 minutes:
```http
HTTP/1.1 200 OK
Content-Type: application/json

{"handled":"replay"}
```

### Repo-filter drop

PR opened in `org/c` (not in `selected_repositories=['org/a','org/b']`):
```http
HTTP/1.1 200 OK
Content-Type: application/json

{"handled":"filtered_repo"}
```

### Lifecycle dispatch

`installation.deleted` webhook:
```http
HTTP/1.1 200 OK
Content-Type: application/json

{"handled":"installation_deleted"}
```
