# Carta — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the
> [Source Integration Contract](_integration-contract.md). Web-researched +
> adversarially verified (8/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict:** QuickBooks archetype clone · can-we-gather: conditional yes (self-consumption only; partner/multi-tenant gated) · effort: M.

---

## TL;DR

Carta is a cap-table / equity / fund-administration platform exposing a versioned
REST API (CRM v1 + Issuer/Investor/Portfolio v1alpha1) over OAuth 2.0 with granular
named scopes. For a company-intelligence pipeline the highest-value data is the
issuer capitalization table — share classes, option pools, warrants, convertible
notes, stakeholders, fully-diluted/outstanding shares, and cash raised — together
with CRM deal/fundraising relationships. The source maps cleanly onto the QuickBooks
archetype: OAuth Bearer access token + a company-scope id (`issuerId` / `portfolioId`
in place of `realmId`). The critical constraint is that Carta has **no documented
native webhook surface**: live ingestion is poll-incremental via `PeriodicReconciler` /
`oauth_poller`, not an HMAC webhook edge, and the cap table is a current-state
snapshot rather than an event log, so history reconstruction requires our own
periodic snapshotting and diffing.

---

## What companies use it for — and what signal lives there

Carta is the system of record for startup equity, covering cap-table management,
employee option grants, fund administration, and fundraising CRM. Signal density is
very high because every material financing or hiring event leaves a trace.

- **Venture-backed startup (issuer):** founders/CFO/legal admin maintain the cap
  table — issuing share classes, closing rounds/SAFEs, granting options.
  _Signal:_ fundraising events (`cashRaised` increases = new round), dilution and new
  share classes (financing stage Seed → A → B), option-pool top-ups (forward hiring
  intent), and convertible-note/SAFE issuance (bridge financing / runway pressure).

- **People/HR and finance ops (option grants):** track grant velocity, pool depletion,
  and equity-headcount growth through employee grants.
  _Signal:_ option-pool grant velocity and depletion as hiring-intensity proxy; pool
  refreshes that typically precede a round.

- **VC/PE fund (Investor/Portfolio APIs):** fund operators aggregate cap-table
  insights across a portfolio via `read_investor_capitalizationtables` /
  `read_portfolio_securities`.
  _Signal:_ portfolio-wide ownership and security positions — breadth of which
  companies a fund holds and summary cap-table marks.

- **Investment/BD team (CRM):** fundraising pipeline and relationship state via
  `/companies` and deal objects.
  _Signal:_ who is raising, deal stage, and fund relationship graph — a
  go-to-market / fundraising-intent signal.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| **Capitalization table (issuer)** | `GET /v1alpha1/issuers/{issuerId}/capitalizationTable` — top-level summary + typed sub-collections | `summary.fullyDilutedShares`, `summary.outstandingShares`, `summary.cashRaised`; `shareClassSummaries[]`; `optionPoolSummaries[]`; `warrantBlockSummaries[]`; `noteBlockSummaries[]` | Single richest ownership/dilution signal: fundraising (cashRaised deltas), dilution, option-pool top-ups, convertible-note issuance. Each delta is a fundable company-health event. |
| **Stakeholders** | Issuer scope (`read_issuer_stakeholders`) — people/entities holding equity | Stakeholder identity, holding type, share counts (**PII**) | Cap-table holder roster: founders, employees, investors. Equity-headcount proxy, founder vesting status, investor identity. **High PII sensitivity.** |
| **Share classes** | `shareClassSummaries[]` within the cap table | Class name, authorized/outstanding/fully-diluted shares, preferences | Round structure (Seed/Series A/B preferred), liquidation preferences, authorized-vs-issued headroom. Directly reveals financing stage. |
| **Option pools** | `optionPoolSummaries[]` — equity reserved/granted for employees | Pool size, granted, available, RSAs/options | Hiring intent and comp velocity: top-up usually precedes a hiring wave or new round; pool depletion signals aggressive hiring. |
| **Warrant blocks** | `warrantBlockSummaries[]` — derivative instruments | Warrant counts, strike, holder | Venture-debt / partnership / vendor warrant coverage — a relationship and financing-structure signal. |
| **Convertible notes / SAFEs** | `noteBlockSummaries[]` — debt/convertible instruments | Principal, cap, discount, maturity | Bridge/seed financing and runway pressure; maturity dates are a forward cash-risk signal. |
| **Investor / portfolio holdings** | Investor & Portfolio APIs (`read_investor_capitalizationtables`, `read_portfolio_securities`) | Security positions, portfolio-company cap-table summaries | For a fund-operator account: portfolio-wide ownership and mark signals; breadth of which companies a fund holds. |
| **CRM companies & deals** | CRM v1 — `GET/POST /v1/companies` and deal/fundraising relationship objects | Company records, deal/relationship state; filtering/sorting/pagination (VERIFIED) | Fundraising pipeline and relationship CRM data: who's raising, deal stage, fund relationships. |

---

## API & authentication

**API style:** REST/JSON over HTTPS, multiple versioned surfaces: CRM Base `v1/`,
and Issuer/Investor/Portfolio Base `v1alpha1/`. Resource-scoped by `issuerId` /
`portfolioId`. Supports filtering, sorting, and pagination on list endpoints.
Five named product suites: Launch, Investor, Issuer, Portfolio, CRM.

**Verified endpoints:**
- `GET /v1alpha1/issuers/{issuerId}/capitalizationTable` — cap table by share class
  (summary + shareClassSummaries / optionPoolSummaries / warrantBlockSummaries /
  noteBlockSummaries). **VERIFIED.** Mock at
  `https://mock-api.carta.com/v1alpha1/issuers/{id}/capitalizationTable`.
- `GET /v1/companies` — List Companies with filtering, sorting, pagination. **VERIFIED.**
- Investor API `v1alpha1` — `read_investor_capitalizationtables` surface. **VERIFIED (surface name).**
- Portfolio API `v1alpha1` — `read_portfolio_securities` surface. **VERIFIED (surface name).**

**Auth mechanism:** OAuth 2.0 — Authorization Code Flow, Client Credentials Flow,
and OpenID Connect. Bearer access token on every call. Scopes are granular and named
per surface:

| Scope | Surface |
|---|---|
| `read_issuer_capitalizationtablesummary` | Cap-table summary |
| `read_issuer_stakeholders` | Stakeholder roster (PII) |
| `read_investor_capitalizationtables` | Investor / portfolio cap tables |
| `read_portfolio_securities` | Portfolio securities |

All scope names VERIFIED.

**Org-token vs per-user:** Org/company-scoped. An existing Carta customer authorizes
our OAuth app to read their own issuer/company data via Client Credentials
(machine-to-machine) or Authorization Code with an admin grant. One token + one
`issuerId` per company — mirrors QuickBooks `realmId` scoping exactly.

**Admin requirements:** Company admin must approve the OAuth grant. As of 2025,
third-party partner API access is invite-only and requires Carta partner-program
admission **plus** a valid SOC 2 Type 2 certification. An existing customer reading
their own data is the lower-friction path (see Feasibility).

---

## Backfill (historical pull)

**Supported:** Yes. List endpoints support filtering, sorting, and pagination
(VERIFIED for CRM List Companies). The cap table is a single scoped `GET` returning
the full current ownership snapshot.

**Mechanism:** Per-entity backfill — one shard per resource scope (`issuerId` /
`portfolioId`) and one per CRM list. The cap table is fetched whole (a snapshot, not
paginated); CRM companies and list resources are walked page-by-page with sort +
filter. This matches the QuickBooks `ShardFetch` (install, `shard_identifier`,
cursor) loop exactly.

**Pagination:** Offset/cursor pagination on list endpoints (filtering + sorting +
pagination confirmed); exact page-token field name is **unverified**. The cap-table
object itself is not paginated — it is a point-in-time summary.

**History depth:** The cap table represents **current state, not an event log**. A
backfill captures the present ownership snapshot; history must be reconstructed by
snapshotting on a cadence and diffing. CRM may carry `created`/`updated` timestamps
for incremental filtering — **unverified**. See Open Questions.

**Rate limits:** 10 req/s burst, 300 req/min hard cap (VERIFIED). `429` response
carries `RateLimit-Reset` = seconds to wait. Generous; a single company's full cap
table + CRM is well within budget.

**Maps to our pipeline:** Each `issuerId` becomes a shard (`shard_kind =
"carta_captable"`). The cap-table shard carries a `CartaCursor{ snapshot_hash,
high_water_updated, page_offset, seeded }` — the `snapshot_hash` acts as the
`high_water_*` that the `PeriodicReconciler` uses as its warm-start reference,
exactly analogous to `Metadata.LastUpdatedTime` in the QuickBooks cursor. CRM list
shards use `page_offset` + an `updated_cursor` high-water as an incremental floor
(mirrors QuickBooks `STARTPOSITION` + `Metadata.LastUpdatedTime >`). Honor
`RateLimit-Reset` / `Retry-After` with bounded retry — directly reuse the QuickBooks
client's 429 handling. The N1 invariant (S3-write → publish → flush → advance cursor,
never advance-then-publish) applies unchanged.

---

## Live ingestion (real-time)

**Mechanism:** Poll-incremental. **No native webhook surface is present in the
verified evidence.** Carta is a request/response REST API. Live updates come from
re-running the fetcher on a cadence (`PeriodicReconciler` / `oauth_poller`) and
diffing the cap-table snapshot or filtering the CRM list by `updated-timestamp`.
There is no HMAC webhook → Kafka 202 edge to wire.

**Events produced by poll-diff:**
- Cap-table snapshot diff (new round / dilution / option-pool change / new note or
  warrant)
- New or updated CRM company / deal
- New stakeholder appearing on the cap table

**Signature scheme:** N/A — no inbound webhook, so no HMAC signature verifier is
needed. If Carta later exposes webhooks, they are not documented in the verified
evidence.

**Notes:** Because the cap table is a current-state snapshot, change detection is
diff-based: re-fetch, compare against the prior observation, emit a `state_change`
observation when `fullyDilutedShares` / `cashRaised` / share-class set changes.
`external_id` must be versioned by a snapshot hash or `LastUpdated`-equivalent so a
real change lands as a **new** observation — this mirrors the QuickBooks
`SyncToken`-versioning lesson.

**Maps to our pipeline:** Live path **(none of the four HTTP paths)** — closest
analogue is the poll-incremental arm used internally by Google Calendar / Drive, but
here it is entirely driven by `PeriodicReconciler` / `oauth_poller` with no inbound
HTTP push. Carta's source-certification evidence records a direct poll with no
HTTP response to assert. No entry in `_HMAC_SOURCES`, no entry in
`_CUTOVER_ENABLED_PROVIDERS`, no `_PROVIDER_CHANNEL` entry.

---

## Can we gather this? — feasibility

**Verdict:** Conditionally yes for our own org data. Verified: existing Carta
customers retain API access to consume their own data even after the 2025 invite-only
transition. If we (the account owner / issuer) authorize our own OAuth app with the
minimum read scopes, we can programmatically pull our own cap table, stakeholders,
and CRM data via Client Credentials or an admin Authorization-Code grant — org-level,
not per-user.

**Access model:** Org/company-scoped OAuth Bearer token scoped to our `issuerId`
(analogous to QuickBooks `realmId`). One admin grant covers the company's data. NOT
per-user.

**Legal / ToS:** Carta API ToS plus, for any partner-grade access, the invite-only
partner program. Self-consumption of one's own data is the sanctioned, lower-risk
path. Building a multi-tenant connector that ingests OTHER companies' Carta data
would require partner-program admission.

**Compliance / PII:** HIGH sensitivity. Cap-table + stakeholder data is financial
PII — named individuals, ownership percentages, equity grants (comp-adjacent). Must
be:
- Encrypted at rest via `secret_store` / `encrypted_secrets` pattern
- Tenant-isolated via RLS (jira / quickbooks template)
- Access-scoped to the minimum read scopes
- Stakeholder identities treated as sensitive PII in the observation/raw tiers

No E2E encryption blocker (REST/TLS is the transport).

**Blockers:**
1. Partner API access is invite-only as of 2025 — blocks a generic multi-tenant
   connector; does **not** block self-consumption.
2. SOC 2 Type 2 certification is required of all API partners — a material
   onboarding gate for partner-mode.
3. No native webhook = no real-time; live is poll-only.
4. Cap table is current-state, not an event log — history depth is shallow without
   our own snapshotting.

**Legal risk:** Medium-high (financial PII + invite-only partner gating + SOC 2
requirement).

**Confidence:** medium.

---

## How it maps onto our pipeline

```
SOURCE: carta

Auth shape →            OAuth2(+realm/scope-id): OAuth 2.0 Authorization Code or
                        Client Credentials; Bearer access token + issuerId (≡ realmId);
                        rotating refresh token owned by oauth_poller.
                        token storage: secret_ref + refresh_secret_ref on carta_installations
Install table →         carta_installations (cols: tenant_id, issuer_id, base_url,
                          secret_ref, refresh_secret_ref, token_expires_at)
                        child resource table: carta_resources
                          (resource_type, updated_cursor, snapshot_hash)
Backfill cursor →       dimension: snapshot-hash (cap table) + offset/high-water (CRM lists)
                        high_water field: snapshot_hash / high_water_updated
                        incremental floor: snapshot hash (cap table) or updated timestamp (CRM)
                        rate-limit-safe empty page: y (honor RateLimit-Reset header)
                        shard_kind: "carta_captable" / "carta_crm"
                        one shard per issuerId (cap table) + per-list fan-out (CRM)
Live mechanism →        NONE/poll-only — no HTTP endpoint; PeriodicReconciler/oauth_poller
                        re-runs the fetcher on a cadence; handler diffs new snapshot vs
                        prior observation and emits state_change on material change.
                        signature: none (no inbound webhook)
                        tenant identifier in payload: n/a (no webhook; no _extract_carta needed)
New files →             services/ingest/integrations/carta/__init__.py
                        services/ingest/integrations/carta/client.py
                          (CartaClient: cap_table(issuerId), list_companies(...),
                           429/RateLimit-Reset retry)
                        services/ingest/integrations/carta/oauth.py
                          (connect/preflight+finalize, operator-mediated token+issuerId,
                           verify-before-write)
                        services/ingest/integrations/carta/onboarding.py
                          (finalize_install → carta_installations + carta_resources +
                           onboarding_triggers source='carta'; NO register_webhook_installation)
                        services/ingest/integrations/carta/metrics.py
                        services/ingest/ingestion/fetchers/carta.py
                          (fetch_page_carta + FETCHER_DISPATCH['carta'])
                        services/ingest/ingestion/handlers/carta.py
                          (carta:capitalization handler, diff-based state_change/signal,
                           external_id versioned by snapshot hash)
                        services/ingest/ingestion/planners/carta.py
                          (one shard per resource)
                        idempotency constructors: carta_captable, carta_company
                          (in services/ingest/ingestion/idempotency/__init__.py)
                        _clients.py: build_carta_client + open_carta_client
                        shard_fetch.py: _LOAD_CARTA_INSTALL_SQL + _load_install branch
                        NO webhook verifier (signatures/<source>.py not needed)
                        NO tenant_resolver._extract_carta (no inbound webhook)
                        NO router.py entries (_PROVIDER_TO_SHADOW_SOURCE /
                          _CUTOVER_ENABLED_PROVIDERS / _PROVIDER_CHANNEL)
Migration →             0095_carta.sql:
                          carta_installations(tenant_id FK, issuer_id, base_url,
                            secret_ref, refresh_secret_ref, token_expires_at,
                            UNIQUE(tenant_id, base_url)) + RLS (jira template)
                          carta_resources(resource_type, updated_cursor, snapshot_hash)
                          + source_check widening on all 4 substrate tables
                          (source_onboarding_runs / onboarding_shards /
                           ingestion_failures / onboarding_triggers),
                          strict superset of all prior sources through telegram
Observation kind(s) →   signal: new cap-table snapshot (first fetch), new CRM company/deal,
                            new stakeholder
                        state_change: material ownership change (cashRaised increase = new
                            round, dilution, share-class added, option-pool top-up,
                            note maturity triggered)
                        channel(s): "carta:capitalization" (share_class / option_pool /
                            warrant_block / note / stakeholder record types),
                            optionally "carta:crm" (companies/deals)
                        trust_tier: "authoritative" (Carta is the equity system of record)
                        external_id: versioned-by-snapshot_hash, namespaced by issuer_id
                          (carta:{issuer_id}:captable:{snapshot_hash} — globally unique
                           across tenants; mirrors qbo:{realm}:{kind}:{id}:{sync_token})
Rate-limit risk →       Low. 10 req/s burst, 300 req/min (verified) — ample for a single
                        company's cap table (one fetch) + CRM lists. Bounded retry
                        honoring RateLimit-Reset reuses the QuickBooks client pattern.
Legal/ToS risk →        Medium-high: financial PII (named stakeholders, ownership %),
                        invite-only partner program for non-self data, SOC 2 Type 2
                        requirement for partner-grade access. Self-consumption of own
                        org data is sanctioned and lower risk; a multi-tenant connector
                        is gated on partner-program admission.
Effort →                M. (+) cleanly reuses the QuickBooks OAuth + scope-id archetype,
                        dedicated-table onboarding/migration template, ShardFetch fetcher
                        loop, and existing 429/Retry-After client pattern.
                        (-) added work: OAuth token refresh in oauth_poller, snapshot-diff
                        change detection for the live path (no webhook shortcut), careful
                        PII handling for stakeholder data, and versioned external_id keying.
                        NOT S: refresh-token plumbing + diff-based live + PII scoping.
                        NOT L: no new infra, no webhook-verifier subsystem.
```

**Auth archetype (exemplar: QuickBooks).** Carta maps onto the `quickbooks`
archetype: OAuth 2.0 Bearer access token with a rotating refresh token persisted in
`refresh_secret_ref`, and a company-scope identifier (`issuerId`) stored on
`carta_installations` in place of QuickBooks' `base_url`/`realmId`. The
`oauth_poller` drives token refresh using the same operator-mediated
connect/preflight+finalize surface from `quickbooks/oauth.py` — paste `issuerId` +
`access_token` + `refresh_token` + `scopes`, verify against the real Carta API
before seeding. Unlike QuickBooks, there is no webhook verifier token to store, so
`webhook_secret_ref` is absent from the install table.

**Install table.** `carta_installations` carries `(tenant_id, issuer_id, base_url,
secret_ref, refresh_secret_ref, token_expires_at)` with `UNIQUE(tenant_id,
base_url)`. A child `carta_resources` table records per-resource cursors
(`resource_type`, `updated_cursor`, `snapshot_hash`). RLS follows the jira/quickbooks
template: `ENABLE ROW LEVEL SECURITY` + `FORCE ROW LEVEL SECURITY` + a
`tenant_isolation` policy on `current_setting('app.current_tenant')::uuid`.

**Backfill cursor.** The `ShardFetch` workflow loads one shard per `issuerId` (for
the cap-table snapshot) and additional shards per CRM resource list. The cap-table
shard's cursor is `CartaCursor{ snapshot_hash, high_water_updated, seeded }`;
`snapshot_hash` acts as the incremental floor, so a re-run without a real change
produces no new observations (empty diff → no publish → cursor unadvanced, N1 safe).
CRM shards use `{ page_offset, high_water_updated, seeded }` mirroring the QuickBooks
`STARTPOSITION` + `Metadata.LastUpdatedTime >` pattern.

**Live mechanism.** No HTTP path. `PeriodicReconciler` / `oauth_poller` re-invokes
the fetcher on a cadence. The handler diffs the new snapshot against the prior
observation in the DB and emits a `state_change` observation only when
`fullyDilutedShares`, `cashRaised`, or the share-class / option-pool / note set
changes. No `signatures/carta.py`, no `_extract_carta` in `tenant_resolver.py`, no
router entries.

**New files.** `services/ingest/integrations/carta/{__init__.py, client.py, oauth.py,
onboarding.py, metrics.py}` · `services/ingest/ingestion/fetchers/carta.py` ·
`services/ingest/ingestion/handlers/carta.py` · `services/ingest/ingestion/planners/carta.py`
· idempotency constructors in `idempotency/__init__.py` · `_clients.py` additions ·
`shard_fetch.py` `_LOAD_CARTA_INSTALL_SQL` branch.

**Migration.** `db/migrations/0095_carta.sql` (the current head is `0094_telegram`).
Follow the source-CHECK re-run landmine convention: the DROP+re-ADD of the four
`source_check` constraints must list every prior source (`slack`, `discord`,
`github`, `gmail`, `google_calendar`, `google_drive`, `notion`, `jira`, `mercury`,
`quickbooks`, `grafana`, `telegram`) plus `carta` as a strict superset. Confirm
against `db/migrations` head before assigning the number.

**Observation `external_id` strategy.** Versioned, namespaced by `issuer_id`:
`carta:{issuer_id}:captable:{snapshot_hash}`. A no-op re-poll (identical snapshot)
produces the same hash and collapses at the `UNIQUE (source_channel, external_id,
occurred_at)` index. A real ownership change produces a new hash and lands a new
`state_change` observation — mirrors `qbo:{realm}:{kind}:{id}:{sync_token}`. The
`issuer_id` namespace ensures global uniqueness across tenants (no cross-tenant
dedup collision).

**Rate-limit risk:** Low — 10 req/s burst, 300 req/min (verified) is ample for a
single organization's cap table (one snapshot fetch) plus CRM list pages. Honor
`RateLimit-Reset` with bounded retry.

**Legal risk:** Medium-high. Financial PII (named stakeholders, ownership percentages,
equity grants). Multi-tenant / partner path requires SOC 2 Type 2. Self-consumption
path is sanctioned.

**Effort: M.** Reuses the QuickBooks OAuth archetype end-to-end; primary additional
work is refresh-token plumbing in `oauth_poller`, diff-based live detection (no
webhook shortcut), and careful PII scoping for stakeholder data.

---

## Open questions

- Does Carta expose **any** native webhook / event-subscription surface? None appears
  in the verified evidence — confirmed-absent would lock in the poll-only live path;
  if one exists, an HMAC webhook edge could be added and effort estimate would
  increase slightly.
- Exact **pagination token mechanics** on list endpoints (offset vs opaque cursor,
  page-size cap, field names) — verified that filtering/sorting/pagination is
  supported but not the precise contract.
- Does `capitalizationTable` (or any endpoint) support an **as-of / historical
  query**, or is it strictly current-state? This determines whether backfill captures
  history or only a present snapshot and thus how much history must be reconstructed
  via periodic snapshotting + diffing.
- Is the **rotating refresh-token behavior** (lifetime, single-use rotation) the same
  as the QuickBooks model the `oauth_poller` assumes? Need Carta's token TTL +
  rotation semantics before wiring `refresh_secret_ref`.
- For **self-consumption** (existing customer reading own data), can Client
  Credentials be used directly, or is an Authorization-Code admin grant required?
  Affects whether onboarding is fully headless.
- Concrete **scope set** required for each entity (stakeholders vs cap-table summary
  vs CRM) and whether a Carta admin can grant read-only scopes without partner-program
  admission.
- Is the **SOC 2 Type 2 requirement** enforced only for partner-program third parties,
  or also for a customer using its own API access? Verified text says "all API
  partners" — clarify whether self-service customer access is exempt.
- **Rate-limit scope**: are the 10 req/s and 300 req/min limits per-app, per-token,
  or per-issuer? Matters only at multi-tenant scale.

---

## Sources

- <https://docs.carta.com/llms.txt> (primary) — 6 claims
- <https://docs.carta.com/api-platform/docs/introduction> (primary) — 6 claims
- <https://docs.carta.com/api-platform/docs> (primary) — 6 claims
- <https://docs.carta.com/api-platform/docs/overview> (primary) — 6 claims
- <https://docs.carta.com/api-platform/docs/authorization> (primary) — 6 claims
- <https://docs.carta.com/api-platform/docs/calculating-cap-table-percentages> (primary) — 5 claims
- <https://carta.com/api/> (unreliable) — 0 claims survived verification
