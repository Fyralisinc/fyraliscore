# Miro — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (7/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: clones the Jira/Grafana archetype (service-credential org Bearer + HMAC-signed webhooks + cursor-poll fallback) · can-we-gather: yes (conditional on Enterprise plan for full coverage) · effort: M.**

---

## TL;DR

Miro is a collaborative whiteboard platform whose v2 REST API exposes Boards, ~10–15 standardized Item types (Card, Sticky Note, Text, Shape, App Card, Document, Embed, Frame, Image), Connectors (the diagram graph), Board Members, and (Enterprise-only) Audit Logs — all behind a verified opaque-cursor pagination model. We install an org-scoped OAuth app once per tenant, resolve a single long-lived Bearer token, and drive a two-level backfill: enumerate boards into `miro_boards`, then fan out one shard per board to page items, connectors, and members. Live ingestion uses HMAC-signed webhooks routing to Kafka, with a Grafana-style periodic cursor-poll reconciler as the guaranteed correctness floor because webhook scope and event coverage are not confirmed in the verified claim set. The main constraint is plan-gating: org-wide board enumeration and Audit Logs require an Enterprise plan, and on lower plans the app must be explicitly added to each board to read its content.

---

## What companies use it for — and what signal lives there

Miro boards host persistent collaborative artefacts — architecture diagrams, roadmaps, retros, discovery workshops — that are rarely captured by code, calendar, or issue-tracker signals. Four primary usage patterns, each yielding distinct signal:

- **Engineering/architecture design** (Engineering, platform, and architecture teams): teams draw system architecture, data-flow, and sequence diagrams as Shapes + Connectors inside named Frames per service or milestone. The connector graph (verified) reconstructs system topology and dependencies; frame/shape text names services and components — a structural map complementary to the GitHub/code-intel signal showing what systems exist, how they connect, and which are being redesigned.

- **Product discovery and roadmapping** (Product managers, design, UX research): sticky-note brainstorms, opportunity-solution trees, and roadmap boards with Cards in date/phase Frames. Sticky-note and card text forms the raw idea/decision corpus; frame names encode roadmap phases and priorities. Board `modifiedAt` and member churn signal which initiatives are active vs abandoned — strong forward-looking "what is the company about to build" signal.

- **Workshops, retros, and planning ceremonies** (Whole org / cross-functional teams, scrum masters): cadence and participation (board members, audit-log activity) as a team-health/velocity proxy; retro sticky text surfaces friction and risk themes; pairs with Jira and Slack signal for a fuller delivery picture.

- **Enterprise governance and security oversight via Audit Logs** (Security, IT admin, compliance): `state_change` observations for external sharing, exports, and access changes via the cursor-paginated audit stream — a governance/leak-risk feed and a reliable activity heartbeat independent of content polling.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| **Board** | Whiteboard/canvas (GET /v2/boards, GET /v2/boards/{id}). The shard target for backfill enumeration. | `id`, `name`, `description`, `owner.id`, `team.id`, `sharingPolicy`, `createdAt`, `modifiedAt`, `lastOpenedAt` | Project/initiative inventory: which teams are actively designing, staleness/abandonment via `modifiedAt`, org-chart-adjacent ownership and collaboration surface. |
| **Board Item (widget)** | The ~10–15 standardized item types on a board: Card, Sticky Note/Sticker, Text, Shape, App Card, Document, Embed, Frame, Image (verified). Each carries a type-specific `data` object, `style`, `geometry` (w/h/rotation), and `position` (x/y/origin). | `id`, `type`, `data.content`/`data.title`, `position.x`/`y`, `geometry`, `createdAt`, `modifiedAt`, `createdBy` | The actual knowledge content: sticky-note brainstorm text, card titles/descriptions, frame names (often roadmap columns/sprint phases), embedded docs/links. Rich NL signal for entity and topic extraction — what the company is planning, deciding, and arguing about. |
| **Connector** | Directed link between two items, defined by `startItem`/`endItem` references (each `id`+`snapTo`), a `style` object, and a `shape` (e.g. `straight`) (verified). | `startItem.id`, `endItem.id`, `startItem.snapTo`, `endItem.snapTo`, `style`, `shape`, `captions` | The diagram graph — turns a board from a bag of items into a structured dependency/flow graph (architecture diagrams, process flows, decision trees). Lets us reconstruct system topology, process steps, and relationship structure the company has explicitly drawn. |
| **Board Member** | Users with access to a board via GET /v2/boards/{BOARD_ID}/members (verified), with role and sharing level. | `id` (user), `email`/`name` (plan-dependent), `role` (owner/editor/commenter/viewer) | Collaboration graph + access-control signal: who works with whom on which initiative, cross-team membership, external (guest) collaborators (possible info-sharing/leak signal). |
| **Audit Log event** (Enterprise) | Org-level audit stream; v2 uses cursor pagination, dropping offset (verified). Records board create/delete/share, member add/remove, export, sign-in, etc. | event type, actor, target object, timestamp, context | Lifecycle/state-change feed → `kind=state_change` observations. Security/governance signal (who exported/shared externally) and a reliable activity heartbeat independent of content polling. |
| **Team / Organization** | Org and its teams (org-scoped enumeration; Enterprise org token). Teams group boards and members. | org id, team id, team name, member counts | Top-level tenancy + org structure for backfill enumeration (which teams/boards to shard) and a department-activity rollup. |
| **Comments / Tags** *(unverified)* | Board item comments and tags exist in the Miro product; their v2 REST read coverage is plan/scope-dependent and not in the verified claim set. | comment text, author, resolved-state, tag labels | Discussion/decision thread on diagrams — high value if available; treat as best-effort until REST coverage is confirmed (see Open Questions). |

---

## API & authentication

**API style:** REST/JSON over HTTPS. Miro Developer Platform v2 REST API. Cursor-based pagination on all list endpoints (verified): the v2 model removes the `offset` parameter and replaces it with a `cursor` param; list response `type` becomes `cursor-list` instead of `list`. Audit Logs follow the same cursor model (verified separately). Our integration is READ-only — we use GET endpoints; create/update operations are not invoked.

**Key endpoints:**

| Endpoint | Status | Purpose |
|---|---|---|
| `GET /v2/boards` | VERIFIED | Cursor-paginated board list — backfill enumeration |
| `GET /v2/boards/{board_id}` | VERIFIED | Board detail |
| `GET /v2/boards/{board_id}/items` | VERIFIED | Board items (all widget types), cursor-paginated |
| `GET /v2/boards/{board_id}/connectors` | VERIFIED | Connector/diagram graph, cursor-paginated |
| `GET /v2/boards/{BOARD_ID}/members` | VERIFIED | Board membership |
| `GET <org audit logs endpoint>` | VERIFIED (cursor model only) | Enterprise org-level audit stream |

**Auth mechanism:** OAuth 2.0 authorization-code flow producing a Bearer access token for an org-installed app, OR a long-lived token issued to the installed app. Architecturally this resolves to a single Bearer token held in our `encrypted_secrets`, resolved once from the secret store — identical posture to Jira and Notion clients. Miro installed-app access tokens are conventionally long-lived/non-expiring; whether a refresh-token rotation step is required for the specific app type chosen is unverified (see Open Questions).

**Scopes:** Read scopes required include `boards:read` and (for membership/org-level access) `organizations:read` / board members read; Audit Logs requires an Enterprise `auditlogs:read` scope. Exact scope strings are not in the verified claim set — must be confirmed against the Miro app-scope docs before implementation.

**Org token vs per-user:** Org-level. A single app installed to the company's Miro org/team, authorized once by an admin, yields one install row per (tenant, org) and backfills all boards the app can see. This mirrors Jira's per-site install, not the Slack per-user xoxp model. Caveat: board content visibility is gated — on non-Enterprise plans the app generally sees content only for boards it has been explicitly added to; org-wide content access plus Audit Logs require an Enterprise plan.

**Admin requirements:** An org/team admin must install and authorize the app. For full coverage, the admin must either ensure the app is added to all relevant boards, or enable org-content access on an Enterprise plan. Audit Logs and org-wide board enumeration are Enterprise-only.

---

## Backfill (historical pull)

**Supported:** Yes. Full historical backfill via cursor-paginated GET endpoints, exactly the pattern our `ShardFetch` loop already drives.

**Mechanism:** Two-level fan-out.

1. **Board enumeration shard** (`miro_org_boards`): at seed time, page `GET /v2/boards` (cursor-paginated) and populate a `miro_boards` table with one row per board — analogous to `jira_projects`. This is a Grafana-style single-per-install shard that drives seeding of per-board shards.
2. **Per-board shards** (`miro_board_items`): one shard per board, paging items, connectors, and members for that board. Incremental re-walks key off board/item `modifiedAt` as the high-water mark (Jira-style `updated_cursor`), since item list endpoints are not natively delta-filtered.
3. **Audit Logs shard** (Enterprise, optional): a third cursor-tail shard that continuously advances through the org audit stream.

**Pagination:** Opaque cursor (verified): pass the prior response's `cursor` value; response `type` is `cursor-list`; absence of a next cursor signals end-of-data. This is our existing opaque-cursor contract directly — no offset arithmetic. Pydantic `MiroCursor` model round-trips through `workflow_states.state_data`.

**History depth:** Board/item/connector backfill is a point-in-time snapshot of current state for every board the token can read. Miro REST does not expose per-widget edit history, so "deep edit history" is out of scope (document like the Jira inline-window note). Historical lifecycle events come from Audit Logs (Enterprise), whose retention depth is set by Miro's policy (unverified — see Open Questions).

**Rate limits:** Miro enforces per-app rate limits (credit/Level-based; 429 with `Retry-After` expected). Reuse the bounded `Retry-After` retry budget already in `JiraClient`/`GrafanaClient`; on budget exhaustion return `FetchResult` with the cursor unadvanced and `end_of_data=False` so `ShardFetch` re-enters. Per-board fan-out across a large org is the primary throughput concern (see Rate-limit risk below).

**Maps to our pipeline:** The `MiroCursor` carries `{cursor, kind, high_water_modified, seeded}` in `workflow_states.state_data` — directly our opaque-cursor contract. Two `shard_kind` values: `miro_org_boards` (enumeration, one per install, Grafana-style) and `miro_board_items` (one per board, Jira-per-project-style, covering items + connectors + members). The `N1 invariant` (`advance_cursor_atomic_with_kafka_publish`) applies without modification: publish all records → flush → only if `flush==0` advance the cursor. End-of-data on the items shard marks that shard `done`; the enumeration shard emits a `shard_fetch_completed` that can trigger re-seeding of new boards found on the next pass.

---

## Live ingestion (real-time)

**Mechanism:** HMAC-signed webhook → `MiroVerifier` → router returns 202 → Kafka. Periodic cursor-poll reconciler (Grafana-style) is the guaranteed correctness floor because webhook scope and coverage are not in the verified claim set.

> **Important:** The claim "webhooks are a new v2 capability" was REFUTED during verification — this does not mean webhooks are absent, only that their newness/version framing is unverified. Miro does offer board-level webhook subscriptions, but their exact event coverage and org-vs-board subscription scope must be confirmed against primary docs before relying on the webhook path as the primary live mechanism.

**Events (unverified scope):**

- Board item created/updated/deleted (webhook, scope TBC)
- Connector created/updated/deleted (TBC)
- Board shared / member added/removed (likely via Audit Logs poll rather than webhook)
- Audit-log lifecycle events (Enterprise, polled via cursor)

**Signature scheme:** Assume HMAC-SHA256 in a Miro-specific header (the Grafana/Jira pattern). Implement `MiroVerifier` under `services/app/webhooks/signatures/miro.py` satisfying the `Verifier` protocol (constant-time compare, try every active secret for rotation). Register in `signatures/__init__.py::VERIFIERS`, resolve the per-tenant secret from `provider_installations(provider='miro')`. If Miro instead uses an opaque per-subscription token (no HMAC), fall back to Jira-style constant-time-token verification. Exact header name and signing bytes must be confirmed against Miro webhook docs (see Open Questions).

**Tenant resolution:** From the org/board id in the verified payload or `provider_installations.installation_id` (org id), mirroring Jira's `cloud_id` resolution path. Add `_extract_miro(payload, headers) -> str | None` to `tenant_resolver.py` and register in `PROVIDER_EXTRACTORS`.

**Maps to our pipeline:** Live path **(a) HMAC webhook → Kafka cutover → 202**. `MiroVerifier` registered in `VERIFIERS`; provider added to `_CUTOVER_ENABLED_PROVIDERS` and `_PROVIDER_TO_SHADOW_SOURCE` in `router.py`; `_EXPECTED_LIVE_STATUS["miro"] = {202}` in the validation harness; `miro` added to `_HMAC_SOURCES` (drives the tampered-signature gate probe). Because webhook scope is unconfirmed, a Grafana-style periodic reconciler (cursor sweep on `modifiedAt` + Audit Logs cursor tail) is the primary correctness guarantee; webhooks are a latency optimization.

---

## Can we gather this? — feasibility

**Verdict: Yes** — as the org that owns the Miro account we can install an org-scoped Miro app (admin-authorized OAuth) and programmatically read our own boards, items, connectors, and members via the v2 REST API, paging with verified opaque cursors. This is a normal first-party use of the documented Developer Platform, not scraping, and is ToS-compliant when done with an installed, authorized app.

**Access model:** Org-level admin-installed OAuth app → single long-lived Bearer token per (tenant, org) in `encrypted_secrets`, resolved once (Jira/Notion posture). Enterprise plan unlocks org-wide enumeration and Audit Logs.

**Legal/ToS:** First-party access to one's own org data via the official API is within Miro's Developer Terms. No scraping. Standard API ToS (rate limits, no resale of platform) apply.

**Compliance/PII:** Board content is user-generated free text and images and WILL contain PII and confidential business plans — must flow through our existing PII/trust-tier handling and tenant-isolated RLS storage. No end-to-end encryption blocker (Miro content is server-side accessible to authorized apps). Images and embeds may reference external files via signed URLs — store references only; fetch bytes only if needed (Drive-style). Audit Logs touch security data; restrict to authorized tenants. External/guest board members may put third-party data in scope.

**Blockers:** No hard blocker for core entities. Soft gates:
- Enterprise plan required for Audit Logs and org-wide board enumeration.
- Per-board app-grant required for full content coverage on non-Enterprise plans.
- Webhook scope/coverage unconfirmed — mitigated by poll-reconciler fallback.
- Token lifetime (refresh requirement) unverified.

**Confidence: high** for the core boards/items/connectors/members backfill path on a standard org token.

---

## How it maps onto our pipeline

```
SOURCE: miro

Auth shape →            OAuth2 org Bearer token (long-lived, resolved once from secret store)
                        token storage: secret_ref on miro_installations
                        (if refresh-token rotation required: add refresh_secret_ref, QBO-lite step)
Install table →         miro_installations (cols: tenant_id, org_id, base_url, secret_ref,
                          webhook_secret_ref, plan_tier)
                        child resource table: miro_boards (board_id, board_name, team_id,
                          modified_at, last_seeded_at — shard targets for per-board fan-out)
Backfill cursor →       dimension: opaque v2 cursor (verified) carried in MiroCursor Pydantic model
                        high_water field: modifiedAt   incremental floor: last high_water_modified
                        rate-limit-safe empty page: y (return FetchResult cursor unadvanced, end_of_data=False)
                        shard_kind: "miro_org_boards" (enumeration, one per install)
                                    "miro_board_items" (per-board: items+connectors+members)
                                    "miro_audit_logs" (Enterprise, cursor-tail)
                        fan-out: per-resource (boards → per-board shards, Jira-per-project-style)
Live mechanism →        HMAC webhook → 202 (Kafka cutover); periodic cursor-poll reconciler as
                        correctness floor (Grafana-style, because webhook scope is unconfirmed)
                        signature: header TBC (assume Miro-Signature or X-Miro-* — MUST confirm)
                          format: sha256=<hex> (HMAC-SHA256) OR opaque token (confirm against docs)
                        tenant identifier in payload: org_id or board_id → provider_installations
                          (provider='miro', installation_id=org_id) — extractor _extract_miro
New files →             services/ingest/integrations/miro/{client.py,onboarding.py,__init__.py}
                        services/ingest/ingestion/fetchers/miro.py
                          (fetch_page_miro, MiroCursor, SHARD_KIND_ORG_BOARDS, SHARD_KIND_BOARD_ITEMS;
                           register FETCHER_DISPATCH['miro'])
                        services/ingest/ingestion/planners/miro.py
                          (reads ctx.install["boards"] for per-board shard fan-out)
                        services/ingest/ingestion/handlers/miro.py
                          (board/item/connector/member → signal observations;
                           audit-log events → state_change observations;
                           register @register("miro:board"), @register("miro:item"),
                           @register("miro:audit"), add import to handlers/__init__.py)
                        services/app/webhooks/signatures/miro.py
                          (MiroVerifier; register in signatures/__init__.py::VERIFIERS)
                        idempotency constructor in services/ingest/ingestion/idempotency/__init__.py
                        _load_install branch in shard_fetch.py (_LOAD_MIRO_INSTALL_SQL)
                        router.py: add to _PROVIDER_TO_SHADOW_SOURCE,
                          _CUTOVER_ENABLED_PROVIDERS, _PROVIDER_CHANNEL
                        tenant_resolver.py: add 'miro' to ResolverProvider Literal,
                          _extract_miro(), register in PROVIDER_EXTRACTORS
                        build_miro_client + open_miro_client in _clients.py
Migration →             0095_miro.sql:
                          miro_installations + miro_boards tables with RLS tenant_isolation policies;
                          source-check widening: DROP+re-ADD source_check on all 4 tables
                          (source_onboarding_runs, onboarding_shards, ingestion_failures,
                           onboarding_triggers) listing the FULL superset including 'telegram'
                          (current newest at 0094) plus 'miro';
                          extend Source Literal in progress/events.py;
                          add observations partitions per adding-a-source checklist
Observation kind(s) →   signal: board snapshot, item (card/sticky_note/text/shape/app_card/
                            document/embed/frame/image) snapshot, connector snapshot, member snapshot
                          state_change: audit-log lifecycle events (share/export/member-change),
                            board deletion
                          channel(s): "miro:board", "miro:item", "miro:connector",
                            "miro:member", "miro:audit"
                          trust_tier: "authoritative" (first-party API source, like jira:issue)
                          external_id: versioned-by-modifiedAt, namespaced by org_id
                            format: miro:{org_id}:board:{board_id}:{modified_at}
                                    miro:{org_id}:item:{item_id}:{modified_at}
                                    miro:{org_id}:connector:{connector_id}:{modified_at}
                                    miro:{org_id}:member:{board_id}:{user_id}:{role}
                                    miro:{org_id}:audit:{event_id}:{timestamp}
Rate-limit risk →       Medium. Per-board fan-out multiplies calls: large orgs produce
                          (num_boards) × (items + connectors + members pages) requests.
                          Miro credit/Level-based per-app limit → 429 storms on big-org
                          initial backfill. Mitigate: bounded Retry-After retry (reuse
                          JiraClient/GrafanaClient budget), per-board shard pacing,
                          backfill window/board-priority cap. Steady-state incremental +
                          webhook load is low.
Legal/ToS risk →        Low. First-party own-org access via official API; API ToS permits
                          server-side polling with an installed app.
                          Residual: board content is high-PII/confidential free text (handle
                          via trust-tier + tenant-isolated RLS); external/guest board members
                          may put third-party data in scope; Audit Logs are security-sensitive.
                          No scraping, no ToS gray area.
Effort →                M. Reuses every existing primitive (opaque cursor, FetchResult/
                          ShardFetch loop, secret-store token client, Verifier Protocol +
                          provider_installations live path, 5-file source slice, migration
                          template). No new auth or pagination primitive. Novel work:
                          (a) two-level board-discovery/enumeration as first-class shard target,
                          (b) normalizing the diagram graph (connectors + items into coherent
                          observation set), (c) confirming + wiring the webhook signature scheme.
                          Not S: two-level fan-out + graph normalization.
                          Not L: no auth/transport novelty (unlike QBO realm or Google DWD/push).
```

**Auth archetype — exemplar: Jira (+ Grafana for live fallback).** The `MiroClient` follows `JiraClient`'s posture: a single long-lived Bearer token loaded once via `build_miro_client(install, *, pool)` from `encrypted_secrets`, with `base_url` from `miro_installations`. If the app type requires refresh-token rotation, add a `refresh_secret_ref` column and a QBO-lite refresh step in the client — otherwise pure token auth. The `integrations/miro/` package holds `onboarding.py` (OAuth callback → `finalize_install` writing to `miro_installations`) and `register_webhook_installation` (store `webhook_secret_ref` for the HMAC verifier).

**Install table.** `miro_installations(tenant_id FK, org_id, base_url, secret_ref, webhook_secret_ref, plan_tier)` + child `miro_boards(installation_id FK, board_id, board_name, team_id, modified_at, last_seeded_at)`. The `_load_install` branch in `shard_fetch.py` selects from `miro_installations JOIN miro_boards` (for board shards) or from `miro_installations` alone (for the enumeration shard), analogous to `_LOAD_JIRA_INSTALL_SQL`.

**Backfill cursor.** `MiroCursor` Pydantic model (`extra="forbid"`) carrying `{cursor: str | None, kind: str, high_water_modified: str | None, seeded: bool}`. Stored in `workflow_states.state_data["cursor"]` (never in `onboarding_shards.cursor_token`). On first call, seeds `high_water_modified` from `shard_identifier["updated_cursor"]` (warm-start floor). On rate-limit, returns `FetchResult(records=fetched_so_far, next_cursor=current_cursor_unadvanced, end_of_data=False)`.

**Live mechanism.** Path **(a) HMAC webhook → Kafka cutover → 202**. `MiroVerifier.verify()` in `signatures/miro.py` tries every active `webhook_secret_ref` secret (rotation-safe, constant-time compare). Tenant resolved from org/board id in the payload via `_extract_miro` → `provider_installations(provider='miro', installation_id=org_id)`. Because webhook scope is unconfirmed, a Grafana-style periodic reconciler (`modifiedAt` sweep + Audit Logs cursor tail) is the primary correctness guarantee; webhooks provide latency improvement once their scope is validated.

**New files summary:** `services/ingest/integrations/miro/` (3 files) + `fetchers/miro.py` + `planners/miro.py` + `handlers/miro.py` + `signatures/miro.py` + edits to `_clients.py`, `idempotency/__init__.py`, `shard_fetch.py`, `router.py`, `tenant_resolver.py`, `handlers/__init__.py`, `fetchers/__init__.py`, `progress/events.py`. Plus the migration and validation-harness additions.

**Migration note.** `0095_miro.sql` must DROP and re-ADD the same four `source_check` constraints (`source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`) listing the FULL superset including `'telegram'` (current newest at `0094`) plus `'miro'` — same last-applied-wins landmine documented in `0073_jira.sql`. Also extend the `Source Literal` in `progress/events.py` and add `observations` partitions per the adding-a-source checklist. The `_fyralis_org` namespace tag on backfill records must equal what `_extract_miro` derives from the webhook payload to satisfy the dedup invariant (no cross-tenant collision since `external_id` is always org-scoped).

**external_id strategy.** Versioned (not immutable) — Miro board items are mutable (edited in place), so encoding `modifiedAt` in the key ensures a genuine change lands a NEW observation rather than colliding. Namespaced by `org_id` per the global-UNIQUE invariant (`source_channel + external_id + occurred_at` has no `tenant_id`). The `_fyralis_org` tag on backfill records must match the `installation_id` the webhook handler derives — analogous to Jira's `_fyralis_site` == host of `issue.self`.

**Rate-limit risk.** Medium. A large org with hundreds of boards multiplies API calls significantly during initial backfill. Mitigate with the bounded `Retry-After` retry already in our clients, per-board shard pacing, and a configurable board-priority cap (recency-scored shards first). Steady-state incremental polling is low volume.

**Legal risk.** Low. No ToS gray area for first-party own-org programmatic access via the official Developer Platform. Residual: PII/confidential content in board free text requires trust-tier + RLS handling; Audit Logs are security-sensitive and must be restricted to authorized tenants.

**Effort estimate: M.** Every primitive (opaque cursor, FetchResult/ShardFetch, secret-store token client, Verifier Protocol, 5-file source slice, migration template) is reused verbatim. No new auth or transport primitive introduced. The novel work — board-discovery enumeration as a first-class shard tier, diagram/connector graph normalization, and webhook signature confirmation — is bounded and well-understood, keeping this firmly Medium rather than Large.

---

## Open questions

- **Webhook scope:** Does Miro expose webhook subscriptions at the org level or only per-board, and what is the exact event taxonomy (`item.created`/`item.updated`/`item.deleted`, connector events, share events)? The "webhooks are new-in-v2" claim was refuted, so event coverage and subscription scope must be confirmed against `developers.miro.com` before promoting the webhook live path from "latency optimization" to "primary."
- **Webhook signature scheme:** HMAC-SHA256 (which header? are timestamp bytes prepended?) or an opaque per-subscription verification token (Jira-style)? Determines whether `MiroVerifier` follows the Grafana HMAC or Jira constant-time-token shape.
- **Token lifetime:** Are Miro installed-app access tokens effectively non-expiring (Jira/Notion posture) or do they require OAuth refresh-token rotation? If the latter, add a `refresh_secret_ref` column and a QBO-lite refresh step to the client.
- **Exact read scope strings:** Confirm `boards:read`, members read, `organizations:read`, and `auditlogs:read` scope strings, and which require an Enterprise plan, against the Miro app-scope docs.
- **Content visibility on non-Enterprise plans:** Must the app be explicitly added to each board to read its items/connectors, or is there an org-wide board-enumeration endpoint available on lower tiers? This decides whether full-org backfill is achievable without Enterprise.
- **REST read coverage for Comments and Tags:** Board item comments and resolved tags exist in the Miro product (referenced in primary docs) but their v2 REST read coverage is plan/scope-dependent and not in the verified claim set. High signal value if available.
- **Audit Logs availability, retention depth, and event schema:** Drives the `state_change` observation mapping and the historical-depth claim; depth unverified.
- **Per-app rate-limit numbers/credit model:** Needed to size per-board backfill pacing and avoid 429 storms on large orgs. The credit/Level model is known to exist but exact limits are not in the verified claim set.

---

## Sources

- <https://developers.miro.com/docs/rest-api-reference-guide> (primary — 6 claims)
- <https://developers.miro.com/docs/rest-api-comparison-guide> (primary — 6 claims; v1→v2 cursor migration, verified offset-removal claim)
- <https://developers.miro.com/docs/work-with-connectors> (primary — 4 claims; connector shape + fields, verified)
- <https://developers.miro.com/docs/working-with-sticky-notes-and-tags-with-the-rest-api> (primary — 4 claims)
- <https://miroapp.github.io/api-clients/node/classes/index.Board.html> (primary — 6 claims; Board fields, item types, verified)
- <https://developers.miro.com/docs/websdk-reference-board> (primary — 6 claims)
- <https://developers.miro.com/docs/getting-started-with-oauth> (primary — 6 claims; OAuth flow + token model)
