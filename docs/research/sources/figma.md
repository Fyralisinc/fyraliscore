# Figma — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (8/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: clones Jira/Grafana (API-token Bearer archetype) · can-we-gather: YES · effort: M.**

---

## TL;DR

Figma exposes a REST/JSON API (v1) plus Webhooks V2, giving us design-file trees, named versions, comment threads, project/team enumeration, library publish events, and dev-handoff status changes for any Figma org we own. Auth is granular OAuth 2.0 per-resource (the old broad `files:read` scope is deprecated) or a long-lived org/team access token on a Dev/Full-seat service identity. Backfill enumerates teams → projects → files via `projects:read` endpoints, then fetches each file's full document tree, version history, and comments; the planner emits one shard per file key, matching the Jira/Grafana cursor model. Live capture runs through Figma Webhooks V2, which maps directly onto our HMAC-webhook → Kafka 202 ingress path except for one net-new wrinkle: Figma authenticates callbacks via a shared **PASSCODE inside the JSON body** rather than an HMAC signature header, requiring a new `FigmaVerifier` variant. All other pipeline plumbing (client, fetcher cursor, handler, migration, onboarding) is a direct clone of the Jira/Grafana slices.

---

## What companies use it for — and what signal lives there

Figma is the dominant collaborative design tool for product teams. Almost every company building a digital product runs design → review → dev-handoff cycles inside Figma. The data that lives there is a high-fidelity record of *what is being built*, *who is reviewing it*, and *when it is ready for engineering* — all cross-correlatable with GitHub and Jira.

- **Active product/UI design (team files)** — *who uses it:* designers, PMs, engineers reviewing designs. *Signal we capture:* which features/products are in active design (file + page names), design velocity (named-version cadence, `lastModified` recency), and design → dev handoff timing (`DEV_MODE_STATUS_UPDATE`). Cross-correlatable with GitHub/Jira to measure end-to-end feature throughput.
- **Design reviews and async feedback (comment threads)** — *who uses it:* designers, reviewers, stakeholders. *Signal we capture:* `FILE_COMMENT` events + `/comments` backfill as decisions/feedback: who reviews whom, blocker/feedback content (embeddable NL), and review-cycle latency from comment-created to resolved. A collaboration-graph and decision-trail signal.
- **Shared design system / component library** — *who uses it:* design-systems team, all consuming product teams. *Signal we capture:* `LIBRARY_PUBLISH` events = design-system maturity and governance; publish frequency tracks design-system investment and standardization across the org.
- **Prototyping and stakeholder/customer demos** — *who uses it:* designers, founders, sales/CS. *Signal we capture:* prototype/file existence + version snapshots tied to demo or release milestones; named versions as deliberate checkpoints aligned with launches.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| **File / document node tree** | `GET /v1/files/:key` returns the entire file as a tree rooted at a `DOCUMENT` node whose children are `CANVAS` nodes (pages); every node has id, name, visible, type, rotation | `key`, `document→children(CANVAS/PAGE)→nodes`; per-node: `id`, `name`, `type`, `visible`, `rotation`; file-level: `name`, `lastModified`, `thumbnailUrl`, `version`, `editorType`, `role` | What the org is actually designing and how complex/mature each file is; `lastModified` + `version` are a per-file activity/velocity signal; `editorType` distinguishes design vs FigJam vs Dev Mode |
| **File versions (named versions)** | `GET /v1/files/:key/versions`; `FILE_VERSION_UPDATE` fires on a deliberate named-version checkpoint | `version id`, `created_at`, `label`, `description`, `user` (creator) | Named versions are intentional milestones (design review, dev handoff, release); version cadence per file/team is a strong project-phase and release-readiness signal — far higher intent than raw autosave `FILE_UPDATE`s |
| **Comments / comment threads** | `GET /v1/files/:key/comments`; `FILE_COMMENT` fires on each new comment; threading + resolved state included | `comment id`, `file_key`, `parent_id` (thread), `message`, `user` (author), `created_at`, `resolved_at`, `client_meta` (anchor node/coords) | Richest collaboration signal: design feedback, decisions, blockers, who reviews whom; thread resolution timing = review-cycle velocity; NL `message` text is embeddable for reasoning over design decisions |
| **Projects & files index (team/project enumeration)** | `GET /v1/teams/:id/projects` + `GET /v1/projects/:id/files` enumerate the file list for backfill | `team_id`, `project_id`, project name, file key, file name, `last_modified` | Org map of design work: which teams/projects exist, how many active files, recency; this is the planner's shard list (one file = one backfill shard) and an org-structure signal |
| **Library publishes (`LIBRARY_PUBLISH`)** | Webhook event fired when a team library (components/styles/variables) is published; large publishes may split across multiple events | library file key, published components/styles/variables (created/modified/deleted ids), description, user | Design-system maturity and governance signal; publish frequency tracks design-system investment and cross-team reuse |
| **Dev Mode status (`DEV_MODE_STATUS_UPDATE`)** | Webhook event signalling a node/section is marked Ready-for-dev (or back to in-progress) | file key, node id, status (`ready_for_dev` / etc.), user, timestamp | Design → engineering handoff signal; correlating Figma handoff timing with GitHub/Jira activity is a cross-source velocity/throughput intelligence signal |

---

## API & authentication

**API style:** REST/JSON over HTTPS (`https://api.figma.com`), REST API v1 for reads, Webhooks V2 (`/v2/webhooks`) for change-data-capture. File contents are returned as a JSON node tree; all endpoints return JSON arrays/objects.

**Key endpoints** (all VERIFIED by primary docs):

| Endpoint | Purpose |
|---|---|
| `GET /v1/files/:key` | Full document tree (DOCUMENT→CANVAS/PAGE→nodes) |
| `GET /v1/files/:key/nodes` | Specific nodes by id |
| `GET /v1/files/:key/meta` | Lightweight file metadata (cheap recency probe) |
| `GET /v1/files/:key/versions` | Version history (named versions only) |
| `GET /v1/files/:key/comments` | All comments/threads for a file |
| `GET /v1/teams/:team_id/projects` | Enumerate projects for a team |
| `GET /v1/projects/:project_id/files` | Enumerate files within a project |
| `POST /v2/webhooks` | Create/manage webhooks (`webhooks:write`) |
| `GET /v2/webhooks/:webhook_id` | Read webhook metadata (`webhooks:read`) |
| `GET /v1/me` | Authenticated user / connectivity + seat probe |

**Auth mechanism:** OAuth 2.0 with granular per-resource scopes (preferred) or a Figma personal/org access token for simpler setups.

**Required scopes for v1 ingestion:**
- `file_content:read` — file nodes, editor type
- `file_metadata:read`
- `file_versions:read`
- `file_comments:read`
- `projects:read` / `project_metadata:read`
- `webhooks:read` + `webhooks:write`

**Deprecated scope to avoid:** `files:read` (broad, deprecated — must not be relied on).

**Enterprise/admin-only scopes (out of v1 scope):** `file_variables:*`, `library_analytics:read`, `org:*` admin scopes.

**Org-token vs per-user:** Org/team access token or OAuth on a single **Dev/Full-seat service identity** is strongly preferred — full coverage and avoids the ~6/month Tier-1 rate cap that applies to View/Collab seats. Per-user OAuth fragments coverage and hits seat-tier caps.

**Admin requirements:** Webhook registration and org/team-wide token issuance typically require a Figma admin (Org/Enterprise plan) and a Dev or Full seat for the token identity.

---

## Backfill (historical pull)

**Supported:** Yes — full backfill is supported and straightforward.

**Mechanism:** The planner enumerates `teams → projects → files` via `projects:read` endpoints to build the file list, then emits **one backfill shard per file key**. Each shard fetches the file's document tree once (`GET /v1/files/:key`), then walks `/versions` and `/comments` for that file. `GET /v1/files/:key/meta` provides a cheap recency probe before fetching the full tree.

**History depth:** Files return their **current state** (full node tree) in one call — there is no incremental node-history of past edits; deep edit history is not exposed. Comments and named versions carry full history (`created_at` per comment/version), so the collaboration + version-cadence signal is fully backfillable. `FILE_UPDATE` autosave history is not retrievable retroactively.

**Pagination:** A single file is fetched whole (no intra-file page cursor — payload size is the bound, not a pagination token). Pagination is at the file-list level: iterate teams → projects → files. `/comments` returns all comments at once (small per file). The exact pagination shape for `GET /v1/teams/:id/projects` and `/v1/projects/:id/files` (cursor vs full list) is unverified — see Open Questions.

**Rate limits:** Leaky-bucket, three endpoint tiers, quotas scale by seat type + plan. Tier-1 (GET file, GET file nodes, GET images): ~10/min (Starter) to ~20/min (Organization) for Dev/Full seats, but only ~**6/month** for View/Collab seats. Backfill MUST run under a Dev/Full-seat identity; pace file reads to stay under the per-minute Tier-1 bucket. 429 handling reuses the bounded Retry-After retry pattern from `JiraClient`/`GrafanaClient`.

**Maps to our pipeline:** The planner emits one `shard_kind="figma_file"` shard per file key, matching the per-resource fan-out model (analogous to `jira_issue` or `mercury_account`). The `FigmaCursor` Pydantic model (clone of `JiraCursor`) carries two dimensions: (1) file-list position from team/project enumeration and (2) per-file high-water on comment `created_at` + last-seen version id for incremental re-walk. The file tree itself is fetched whole (no intra-file page token). `FetchResult(records, next_cursor, end_of_data)` signals `end_of_data=True` when the project file list is exhausted and per-file companion calls (versions/comments) are drained. The `workflow_states.state_data["cursor"]` carries the opaque cursor between ticks per the `ShardFetch` N1 invariant: publish all records → flush → only if flush==0 advance the cursor.

---

## Live ingestion (real-time)

**Mechanism:** Figma Webhooks V2 — HTTP POST callbacks to our registered endpoint. One webhook per team/context, registered via `POST /v2/webhooks` with `webhooks:write`. Maps to our **HMAC-webhook → Kafka 202 ingress pattern** (live path (a) in the contract), with one deviation in the signature scheme (see below).

**Events:**

| Event | Description |
|---|---|
| `PING` | Registration health check |
| `FILE_UPDATE` | Fires ~30 min after editing inactivity in a file (coarse activity ping, debounced) |
| `FILE_DELETE` | File deleted |
| `FILE_VERSION_UPDATE` | User creates a named version (deliberate checkpoint) |
| `FILE_COMMENT` | User comments on a file |
| `LIBRARY_PUBLISH` | Team library published (large publishes may split into multiple events) |
| `DEV_MODE_STATUS_UPDATE` | Node/section marked ready-for-dev (or reverted) |

**Signature scheme:** NOT an HMAC header. Figma authenticates the callback by including the shared **PASSCODE** (supplied at webhook creation time) inside the request **JSON body**. Our verifier must constant-time-compare `body["passcode"]` against the per-tenant secret — a new `FigmaVerifier` variant under `services/app/webhooks/signatures/figma.py`. This differs from all existing verifiers: GitHub/Jira use `sha256=<hex>` in a header, Grafana uses a bare hex HMAC header, Linear uses bare hex + optional timestamp replay window. The `FigmaVerifier` is the only net-new infrastructure item in this implementation.

**Retry policy:** Fixed — 3 retries at exponential backoff: 5 minutes, 30 minutes, 3 hours. Endpoint MUST return HTTP 200 OK. Transient 5xx are auto-redelivered; our handler stays idempotent via versioned `external_id`.

**High-intent signals:** `FILE_COMMENT` and `FILE_VERSION_UPDATE` are the high-resolution live signals. `FILE_UPDATE` is debounced (~30 min inactivity) and is a coarse activity ping, not a per-edit stream.

**Maps to our pipeline:** Live path **(a) HMAC webhook → Kafka cutover → 202**, with the `FigmaVerifier` substituting for the header-HMAC verifiers. `figma` is added to `_CUTOVER_ENABLED_PROVIDERS` and `_PROVIDER_TO_SHADOW_SOURCE`. The router's `receive` flow: raw body → 1MB precheck → `VERIFIERS["figma"]` (passcode-in-body check, 401 on mismatch) → tenant-resolve via `_extract_figma(payload, headers)` (reads `team_id` from body — see Open Questions) → `kafka_path_enabled` gate → `_attempt_kafka_path` → 202.

---

## Can we gather this? — feasibility

**Verdict: Yes.**

As the org that owns the Figma team/org, we can issue an OAuth grant (or org/team token) on a Dev/Full-seat service identity, enumerate our teams/projects/files, read file trees + versions + comments, and register Webhooks V2 for live capture. This is the same **self-owned-account posture** as our Jira (API token), Grafana (service-account), and Mercury sources. No third-party consent is needed beyond our own Figma admin.

**Access model:** Org/team access token or OAuth on a single Dev/Full-seat service identity. Webhook creation and broad file access require a Figma admin on an Org/Enterprise plan.

**Legal/ToS:** First-party access to our own org's design data via Figma's documented REST + Webhooks API — within Figma's developer terms. No scraping, no undocumented endpoints. Standard data-processing/retention review applies since we copy file/comment content.

**Compliance/PII:** Design files and comments are generally lower sensitivity than finance/HR sources, but comment text and file/page names CAN contain PII, unreleased product/strategy information, or customer data embedded in mockups. No E2E encryption — Figma serves content in cleartext JSON to an authorized token. Treat comment NL + file content as confidential; apply the same redaction/raw-tier handling as other text sources. Enterprise-only scopes (`file_variables:*`, `library_analytics:read`, `org:*`) are out of v1 scope.

**Blockers (soft only):**
1. **Seat-tier rate caps** — the token identity MUST be a Dev/Full seat or Tier-1 file reads collapse to ~6/month.
2. **No deep edit history** — only current tree + version/comment history is backfillable; raw per-edit node history is not exposed.
3. **Passcode-in-body webhook auth** — requires a new `FigmaVerifier` variant (net-new work, not a hard blocker).

**No hard blockers.** No E2E encryption, no closed API, no undocumented endpoints, no third-party consent.

**Confidence: high.**

---

## How it maps onto our pipeline

```
SOURCE: figma

Auth shape →            API-token Bearer (org/team access token, long-lived)
                        OR OAuth2 per-resource (granular scopes, refresh path)
                        token storage: secret_ref on figma_installations
                        (if OAuth: refresh_secret_ref also on figma_installations)
Install table →         figma_installations (cols: team_id, base_url, secret_ref,
                          webhook_secret_ref, optional oauth refresh_secret_ref)
                        child resource table: figma_files (shard targets: one row
                          per file key, populated by planner from project enumeration)
Backfill cursor →       dimension: two-level — (1) file-list position from
                          team/project enumeration; (2) per-file high_water on
                          comment created_at + last-seen version id
                        high_water field: comment.created_at / version.id
                        incremental floor: team/project file list exhausted
                        rate-limit-safe empty page: y (returns FetchResult with
                          records=[], next_cursor=unadvanced, end_of_data=False on 429)
                        shard_kind: "figma_file"   per-resource fan-out (one shard per file)
Live mechanism →        HMAC webhook→202 (live path (a))
                        EXCEPT: signature is passcode-in-body, NOT an HMAC header
                        signature: body field "passcode", constant-time compare
                          (new FigmaVerifier — no header name)
                        tenant identifier in payload: team_id in webhook body
                          (extractor _extract_figma) — UNVERIFIED exact field path,
                          confirm before building
New files →             fetchers/figma.py · planners/figma.py · handlers/figma.py
                        signatures/figma.py (new FigmaVerifier, passcode-in-body)
                        _clients.py build_figma_client + open_figma_client
                        idempotency constructor figma:{team_id}:{kind}:{id}:{version}
                        _load_install branch in shard_fetch.py
                        router maps (_PROVIDER_TO_SHADOW_SOURCE, _CUTOVER_ENABLED_PROVIDERS,
                          _PROVIDER_CHANNEL) · tenant_resolver (_extract_figma)
Migration →             NNNN_figma.sql: figma_installations(+RLS)(+figma_files child)
                          + source_check widening (4 tables: source_onboarding_runs,
                          onboarding_shards, ingestion_failures, onboarding_triggers)
Observation kind(s) →   signal: file/document snapshot (object_type=document),
                          comment (object_type=comment), named version (object_type=version),
                          library publish (object_type=library_publish)
                        state_change: DEV_MODE_STATUS_UPDATE (object_type=dev_status),
                          FILE_DELETE (object_type=document, tombstone)
                        channel(s): "figma:document", "figma:comment",
                          "figma:version", "figma:library_publish", "figma:dev_status"
                        trust_tier: authoritative
                        external_id: versioned-by-version/updated_at,
                          namespaced by team_id:
                          figma:{team_id}:file:{file_key}:{version}
                          figma:{team_id}:comment:{comment_id}:{created_at}
                          figma:{team_id}:version:{file_key}:{version_id}
                          figma:{team_id}:library_publish:{file_key}:{timestamp}
                          figma:{team_id}:dev_status:{file_key}:{node_id}:{timestamp}
Rate-limit risk →       MEDIUM. Tier-1 file reads: ~10-20/min Dev/Full seat,
                          ~6/MONTH View/Collab seat. Large orgs need backfill paced
                          under per-minute bucket. Live webhooks not rate-limited inbound.
Legal/ToS risk →        LOW. First-party API access, documented endpoints, no scraping.
                          Residual: comment NL + file names may carry PII or unreleased
                          product info — handle via existing raw-tier/redaction posture.
Effort →                M. Client + cursor + handler + onboarding = direct Jira/Grafana
                          clone. Net-new: (1) passcode-in-body FigmaVerifier,
                          (2) large file-tree payload handling, (3) two-level
                          file-list+per-file cursor. No new auth infrastructure,
                          no E2E crypto, no MTProto complexity.
```

**Auth archetype:** Closest exemplar is `GrafanaClient` (Bearer token, lazy secret-store resolution, Retry-After 429 handling, per-instance `base_url`) combined with `JiraClient`'s paged-list helpers and connectivity probe pattern. If we go OAuth, the `quickbooks` refresh-token path applies for token rotation. Onboarding clones `jira/grafana` onboarding.py to register the install row + webhook `secret_ref`, then calls `POST /v2/webhooks` to register `FILE_UPDATE`, `FILE_VERSION_UPDATE`, `FILE_COMMENT`, `FILE_DELETE`, `LIBRARY_PUBLISH`, `DEV_MODE_STATUS_UPDATE`.

**Install table:** `figma_installations` (per-tenant, per-team) with a `figma_files` child table (shard targets populated at plan time from project enumeration). RLS + `tenant_isolation` policy on `current_setting('app.current_tenant')::uuid` as per all per-tenant install tables.

**Backfill cursor:** `FigmaCursor` Pydantic model (clone `JiraCursor`, `extra="forbid"`). Two-level: outer position = index into the `figma_files` child table (populated by planner); inner per-file high-water = `comment.created_at` + `version.id` for incremental re-walk on subsequent syncs. File tree itself fetched whole (no intra-file page token — payload size is the bound). `end_of_data=True` when the file list is exhausted and companions are drained.

**Live mechanism:** `FigmaVerifier` in `services/app/webhooks/signatures/figma.py` implements the `Verifier` protocol: extracts `payload_json["passcode"]`, constant-time-compares against the per-tenant `webhook_secret_ref`-resolved secret, raises `401` on mismatch. This is the only new verifier variant — all existing verifiers compare a header; `FigmaVerifier` compares a body field. The `_extract_figma` extractor in `tenant_resolver.py` reads the `team_id` (or equivalent) from the webhook body to map to `provider_installations` / `figma_installations` — exact body field path is unverified, must be confirmed against live webhook payloads before building (see Open Questions).

**Observation kinds + channels:** Five handler channels, branching on `_fyralis_record_type` for backfill vs raw body event type for webhooks. `trust_tier = "authoritative"` (owned first-party data, mirrors jira/mercury/grafana).

**external_id strategy:** Versioned (the mutable-source lesson): the mutation dimension (file `version`, comment `created_at`, version `id`, publish `timestamp`) is encoded in the key so a real change lands a new observation; re-fetching the same unchanged entity collapses. All keys are namespaced by `team_id` — globally unique across tenants since `team_id` is Figma-global (the `UNIQUE (source_channel, external_id, occurred_at)` dedup index has no `tenant_id`; namespacing is mandatory to avoid cross-tenant collisions).

**Migration note:** `NNNN_figma.sql` adds `figma` to the `source_check` on all four substrate tables (`source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`) as a strict superset of all prior sources. The source-CHECK re-run landmine applies: the newest source in a prior widening migration must be cleaned up before integration tests re-run that migration. The global `observations UNIQUE` has no `tenant_id` — use tenant-unique fixtures in integration tests (the all-11 overlap-gate gotcha).

**Rate-limit risk:** MEDIUM. The ~6/month Tier-1 cap for View/Collab seats is a hard operational constraint — the service-account token identity must be validated as Dev/Full-seat before go-live. Large orgs with many files need `FetchRateLimiter`-backed pacing under the per-minute bucket. Live webhook inbound path is not rate-limited.

**Legal risk:** LOW. First-party, documented API. Residual content sensitivity (comment NL + file/page names may carry PII or unreleased product information) handled via existing raw-tier/redaction posture.

**Effort: M.** The pipeline slice (client, fetcher cursor, handler, onboarding, migration, tests) is a direct Jira/Grafana clone. The only net-new work is: (1) the passcode-in-body `FigmaVerifier`, (2) handling potentially large file-tree JSON payloads (node tree depth), and (3) the two-level file-list + per-file backfill cursor. No new auth infrastructure, no E2E cryptography, no MTProto-style user-account complexity.

---

## Open questions

- **Webhook tenant resolution:** Figma webhooks are scoped per team/context and authenticated by a body passcode — confirm the payload carries a stable `team_id`/`webhook_id` field our `_extract_figma` extractor can map to `figma_installations`, since there is no `installation_id` in the URL path like other providers.
- **File-list pagination shape:** The exact cursor/page format for `GET /v1/teams/:id/projects` and `GET /v1/projects/:id/files` (cursor-based vs full list in one call) was not in the verified claims — confirm before sizing the shard planner and `FigmaCursor` outer dimension.
- **Org-token coverage scope:** Whether an org/team-wide access token can read ALL team files or only files the token identity has been explicitly added to — affects whether we need a coverage-gap representation in the planner for inaccessible files.
- **`FILE_UPDATE` granularity ceiling:** The 30-min-inactivity debounce means `FILE_UPDATE` is a coarse activity ping. Confirm there is no finer-grained edit/activity API, or accept that `FILE_VERSION_UPDATE` + `FILE_COMMENT` are the ceiling for high-resolution velocity signal.
- **`FILE_COMMENT` webhook payload completeness:** Does the `FILE_COMMENT` webhook body include full comment content + `client_meta` anchor, or does it require a follow-up `GET /v1/files/:key/comments` to get the full thread context? Affects whether the handler can emit a complete observation inline or needs a fetch-on-event pattern.
- **Token longevity and rotation:** Long-lived org/team access token (Jira/Grafana static-token path) vs OAuth with refresh (`quickbooks`-style refresh path) — affects install table schema (whether `refresh_secret_ref` is needed) and client-builder complexity.
- **Phase-2 scope (`file_variables:read` / `library_analytics:read`):** Enterprise-only scopes for design-token and component-usage analytics — worth evaluating as a phase-2 signal source if the tenant is on Enterprise plan.

---

## Sources

- https://developers.figma.com/docs/rest-api/ (primary) — REST API overview, endpoint listing, auth model
- https://developers.figma.com/docs/rest-api/files/ (primary) — file endpoint request/response shape, node tree structure
- https://github.com/figma/rest-api-spec (primary) — OpenAPI spec; authoritative endpoint + field enumeration
- https://developers.figma.com/docs/rest-api/file-endpoints/ (primary) — file, versions, comments endpoint details
- https://developers.figma.com/docs/rest-api/component-types/ (primary) — component/library entity types and fields
- https://developers.figma.com/docs/rest-api/webhooks/ (primary) — Webhooks V2: event types, passcode auth, retry policy
- https://developers.figma.com/docs/rest-api/plan-access-tokens/ (primary) — access token types, seat-tier rate caps, scope deprecations
