# Implementation Plan: Notion as a Signal Source — OAuth Install, Poll-Based Backfill + Incremental, DB Rows / Page Content / Comments as Observations

**Branch**: `integration/notion-signal-source` (cut from `harden/ingestion-prod-readiness`) | **Date**: 2026-05-23 | **Spec**: _not yet written — this plan is the anchoring artifact pending a `spec.md` backfill_
**Input**: User request — "add Notion as a signal source so Fyralis can reason over it."

## Summary

Notion becomes the **fifth ingestion source** alongside gmail / github / slack / discord, slotting into the M6 ingestion substrate that IN-13 and its predecessors built. Unlike those four, Notion is not a conversation or code-state stream — it is the **declared-intent + canonical-state** layer (roadmaps, OKRs, task DBs, specs, PRDs, retros, comments). Its reasoning value is the contrast it creates: Notion records what the org *says* is true and planned; GitHub/Slack record what *happened*. Fyralis can then reason about intent-vs-reality drift (e.g. "task marked Done in Notion, but no PR ever merged"). See [§ Signal Value](#signal-value-what-fyralis-gets) below.

**The one structural divergence from the existing four sources**: Notion has no reliable real-time content push (its webhook product is new and event-coverage is partial). Therefore **v1 is poll-first**:

- **Backfill** rides the existing M6.2a `ShardFetch` loop via a new `FETCHER_DISPATCH["notion"]`.
- **Incremental** ("live") updates ride the **existing `PeriodicReconciler` (migration 0058) / `oauth_poller` machinery** — re-running the fetcher on a cadence with a `last_edited_time` high-water mark — **NOT** the `services/webhooks/` ingress.
- Consequently v1 makes **zero changes** to `services/webhooks/router.py` and `services/webhooks/tenant_resolver.py`. Push-webhook ingest is an explicit follow-up (see [Out of Scope](#out-of-scope)).

This task adds:

- A `services/integrations/notion/` package: `oauth.py` (install + callback, mirrors `services/integrations/slack/oauth.py`), `client.py` (async Notion REST client + pagination), `metrics.py`. **No `jwt.py`** — Notion bot tokens are long-lived; no per-request JWT minting or token-exchange cache (simpler than GitHub's IN-13 shape).
- A `services/ingestion/fetchers/notion.py` with `fetch_page_notion()` + a `NotionCursor` Pydantic model, assigned into `FETCHER_DISPATCH["notion"]` at import.
- A `services/ingestion/planners/notion.py` (`PLANNER_DISPATCH["notion"]`) that decomposes a workspace install into shards: one `notion_database` shard per database + one `notion_page_tree` shard for loose (non-DB) pages.
- A `services/ingestion/reconcilers/notion.py` (`RECONCILER_DISPATCH["notion"]`) — gap detection comparing each shard's cursor high-water `last_edited_at` against the live latest edit.
- `services/ingestion/handlers/notion.py` registering three channels: `notion:page` (DB rows + their property/status transitions), `notion:page_content` (page body blocks), `notion:comment`. Each produces an `ObservationDraft`.
- Trust-tier entries in `CHANNEL_TRUST_MAP`.
- A `notion`-client builder added to `services/ingestion/planners/context.py::PlannerContext` (parallel to how GitHub's `source_client` is built per A18.6).
- Notion install/callback routes in `services/integrations/router.py` + the gateway public-path allowlist + a prod-safety invariant for the Notion OAuth secret.
- **One migration** (`0059_notion_source_check.sql`): extend the two `CHECK (source IN ('slack','github','discord','gmail'))` constraints (on `source_onboarding_runs` and `onboarding_shards`) to include `'notion'`. No new tables — Notion reuses `provider_installations` + `encrypted_secrets` + `oauth_install_states` + `installation_audit_log` exactly as slack/github/discord do.

**Existing assets reused unchanged:**

- `provider_installations` (provider=`'notion'`, installation_id=`workspace_id`) — `provider` is free `TEXT`, no migration needed for the table itself.
- The OAuth substrate: `oauth_install_states` nonce ledger, `installation_audit_log`, `lib/shared/secrets` Fernet store, the `issue_state_token` / nonce-consume HMAC flow.
- The M6.2a `ShardFetch` fetch loop and the N1 cursor-advance primitive (`state.advance_cursor_atomic_with_kafka_publish`) — the fetcher just returns `FetchResult`; persistence is not its concern.
- `services/ingestion/core.py::ingest()` — actor resolution, entity fast-path, embedding, `observations` insert with `UNIQUE(source_channel, external_id, occurred_at)` dedup, and the `think_trigger_queue` enqueue. Notion observations flow Think downstream with no Think changes (§V N/A).

## Signal Value: What Fyralis Gets

| Notion object | Channel | Signal it carries | Reasoning use |
|---|---|---|---|
| Database row (page) + property changes | `notion:page` | Status (`Todo→Done`), owner, due date, priority transitions | Declared work-state; cross-check against GitHub actual-state for drift |
| Page body blocks | `notion:page_content` | Specs, PRDs, meeting notes, decisions | Rationale + entity grounding ("why was this decided") |
| Comments | `notion:comment` | Discussion anchored to a canonical entity | Conversation signal already entity-anchored (unlike Slack) |
| `created_by` / `last_edited_by` / `last_edited_time` | (all) | Actor attribution + activity heartbeat | Actor resolution + recency scoring |
| Relations / `@`-mentions | (entities_hint) | Page↔page, page↔person edges | Free entity-graph edges Notion already encodes |

**Trust posture**: Notion content is human-authored via an authenticated API — directly comparable to `slack:message`. All three channels are `attested_agent`, **not** `authoritative`. Notion declares *intent*; it is not a system-of-record verifying *reality* (that is GitHub merges / Stripe events). Elevating DB status-field transitions to a higher tier is deferred until there's a concrete reasoning consumer that needs it (§X — no knob without a second caller).

## Technical Context

**Language/Version**: Python 3.12 (`.venv`); `from __future__ import annotations`, full type hints, Pydantic v2 `extra="forbid"` at wire boundaries.

**Primary Dependencies**:
- **httpx** — async Notion REST client (already in project). The Notion API is plain HTTPS + `Authorization: Bearer <token>` + `Notion-Version` header; **no new dependency** (no SDK; we wrap httpx like the GitHub/Slack clients).
- **asyncpg** — factory-injected pool.
- **lib.shared.secrets** (Fernet) — bot-token storage, unchanged.

**Storage**: Postgres 16 + pgvector. Reused tables: `provider_installations`, `encrypted_secrets`, `oauth_install_states`, `installation_audit_log`, `source_onboarding_runs`, `onboarding_shards`, `observations`. One migration alters two CHECK constraints. **No new tables.**

**Testing**: pytest `integration` marker (live Postgres + Ollama, §IV). `respx` mocks `api.notion.com` HTTP only — the Postgres and `lib/shared/secrets` boundaries are real.

**Performance Goals**:
- OAuth callback wall time ≤ 2 s (state consume + `GET /v1/users/me` workspace lookup + UPSERT + redirect).
- Backfill page fetch: bounded by Notion's rate limit (~3 req/s avg); fetcher must honor `429 Retry-After` via `services.ingestion.workflows.retry`.
- Incremental poll cadence: reuse `PeriodicReconciler` default; target staleness ≤ poll interval (propose 15 min, matching greeting cadence — confirm in spec).

**Constraints**:
- Notion rate limit ~3 req/s/integration with bursts; `429` carries `Retry-After`. Fetcher returns an empty-records page and a non-advanced cursor on rate-limit rather than failing the shard.
- Pagination is cursor-based: every list endpoint returns `{results, next_cursor, has_more}`. `has_more=false` ⇒ `end_of_data=True`.
- Notion deep-paginates page bodies (blocks have children recursively). v1 caps block recursion depth (propose depth ≤ 3) and records a truncation marker rather than unbounded recursion (§X).
- `FYRALIS_ENV=prod` MUST fail-fast at startup unless the Notion OAuth secret ref is configured — extend `_assert_prod_safety_invariants` (parallel to IN-13's GitHub branch).

**Scale/Scope**: Per-tenant Notion installs in the low hundreds. A workspace has tens to low-hundreds of databases and thousands of pages; the planner shards per-database to bound each fetch unit.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-evaluated at end of Phase 1.*

| Principle | Status | Notes |
|---|---|---|
| §I Four Foundations distinct | PASS | Notion data lands as **Observations** (`kind ∈ {signal, state_change}`); a DB status-field transition is a natural `state_change`. No Model/Act/Resource writes here — Think produces those downstream. `provider_installations` is a permitted per-feature side table. |
| §II Append-only migrations | PASS | One new migration `0059_notion_source_check.sql`: `ALTER TABLE … DROP CONSTRAINT IF EXISTS … ; ADD CONSTRAINT … CHECK (source IN ('slack','github','discord','gmail','notion'))` on both `source_onboarding_runs` and `onboarding_shards`. Idempotent (DROP IF EXISTS + re-add). No applied migration edited. Not destructive (constraint widening, no DROP COLUMN/type-narrowing) — no staged plan required. |
| §III Tenant isolation structural | PASS | No new tenant-scoped tables. All new queries hand-roll `WHERE tenant_id = $1`; installs reuse `provider_installations`' existing RLS + FK + tenant-prefixed index from migration 0039. |
| §IV Integration tests, real DB | PASS | All `services/integrations/notion/tests/*` and `services/ingestion/{fetchers,planners,reconcilers,handlers}/tests/test_notion*` run on live Postgres; `respx` only for `api.notion.com`. |
| §V Reasoning vs rendering | N/A | Pure ingestion plumbing. Observations trigger Think via the existing `ingest()` → `think_trigger_queue` path. No Think/Rendering edits. |
| §VI Trust/confidence/falsifiers | PASS | Notion observations carry `trust_tier='attested_agent'` per `CHANNEL_TRUST_MAP`. No Model writes ⇒ no falsifier obligation in this task. |
| §VII Determinism + audit | PASS | `uuid7()` for every new substrate row (install/audit). Observation idempotency via the existing UNIQUE index; `external_id` chosen stable across backfill and poll-incremental (Notion page/comment `id` + `last_edited_time`). Every install/reinstall/disable writes `installation_audit_log`. |
| §VIII Structured errors | PASS | New classes derive from `CompanyOSError`: `NotionOAuthError`, `NotionApiError`. Reuse existing `InstallationConflictError`, `StateTokenInvalidError`, `SecretStoreError`. |
| §IX Dual-write until proven | PASS — N/A | No hot-path schema field, no existing Notion data, no parallel writer. The CHECK widening is backward-compatible (existing rows unaffected). Reader/writer introduced together. |
| §X Simplicity / YAGNI | PASS | No webhook ingress in v1 (no second push consumer yet). No SDK. No per-block-type schema. No knob for trust elevation. Each deferral is listed in Out of Scope with rationale. Reuses three existing dispatch tables rather than inventing a Notion-specific pipeline. |

**No NON-NEGOTIABLE violations.** Complexity Tracking table is empty.

### Complexity Tracking

(none — no deviations from the constitution require justification)

## Project Structure

### Source Code

New files:

```text
services/integrations/notion/
├── __init__.py
├── oauth.py            # GET install / GET callback; state-token + nonce-consume + UPSERT + audit (mirrors slack/oauth.py)
├── client.py           # async Notion REST client: search, databases.query, blocks.children.list, comments.list, pages/users.retrieve; 429 Retry-After handling
└── metrics.py          # notion_install_* + notion_fetch_* counters

services/ingestion/fetchers/notion.py        # fetch_page_notion + NotionCursor; FETCHER_DISPATCH["notion"]=...
services/ingestion/planners/notion.py        # plan_notion_shards; PLANNER_DISPATCH["notion"]=...
services/ingestion/reconcilers/notion.py     # reconcile_notion; RECONCILER_DISPATCH["notion"]=...
services/ingestion/handlers/notion.py        # @register("notion:page"|"notion:page_content"|"notion:comment")

db/migrations/0059_notion_source_check.sql   # widen two CHECK constraints to include 'notion'
```

Colocated tests:

```text
services/integrations/tests/test_oauth_notion.py          # install 302 + callback (first install, collision, reinstall, state-token failures)
services/integrations/tests/test_client_notion.py         # pagination, 401, 429 Retry-After backoff
services/ingestion/fetchers/tests/test_notion.py          # page/cursor round-trip, has_more terminal, rate-limit empty page
services/ingestion/planners/tests/test_notion.py          # database enumeration → shards; loose-page shard
services/ingestion/reconcilers/tests/test_notion.py       # clean vs gap-detected (cursor high-water < live latest)
services/ingestion/handlers/tests/test_notion.py          # each channel → ObservationDraft; external_id stability; entity hints from relations/mentions
```

Changed files:

```text
services/ingestion/fetchers/__init__.py       # add "notion": _not_implemented_fetcher stub + import notion module
services/ingestion/planners/__init__.py        # add "notion" stub + import
services/ingestion/reconcilers/__init__.py     # add "notion" stub + import
services/ingestion/planners/context.py         # build a Notion source_client when source=='notion' (parallel to github)
services/ingestion/handlers/__init__.py        # add notion:page / notion:page_content / notion:comment to CHANNEL_TRUST_MAP + import notion handler module
services/integrations/router.py                # add /integrations/notion/install + /callback
services/gateway/main.py                       # add /integrations/notion/callback to _PUBLIC_PATHS; extend _assert_prod_safety_invariants for NOTION_OAUTH_* secret
lib/shared/errors.py                           # add NotionOAuthError, NotionApiError
CODEBASE-ARCHITECTURE.md                       # append a section documenting IN-14 (mirror the IN-13 shape)
```

**Structure Decision**: Mirror the per-source layout the M6 substrate established. Notion touches the same four dispatch tables every source touches (planner / fetcher / reconciler / handler) plus the OAuth substrate. No new pipeline, no new top-level module.

## Phase Ordering

No dual-write / reader-cutover phase (§IX N/A — backward-compatible CHECK widening, no hot-path field). Functionality-first slices:

**Slice 1 (foundational — gated on migration + dispatch wiring)**
- T001: `db/migrations/0059_notion_source_check.sql` — widen the two `source` CHECK constraints to include `'notion'`. Idempotent (`DROP CONSTRAINT IF EXISTS` + re-add). Leading comment per §II.
- T002: Read-only assertion test: after migration, `INSERT … source='notion'` into `source_onboarding_runs` + `onboarding_shards` succeeds; pre-existing rows unaffected.
- T003: Register `"notion"` stubs in all three dispatch `__init__.py` (`FETCHER_DISPATCH`, `PLANNER_DISPATCH`, `RECONCILER_DISPATCH`) + add the `from … import notion` line to each. Reconciler stub returns default-clean (matching the existing pattern); planner/fetcher stubs raise `NotImplementedError`.
- T004: `lib/shared/errors.py` — add `NotionOAuthError`, `NotionApiError`.

**Slice 2 (Notion REST client)**
- T005: `services/integrations/notion/client.py::NotionClient` — methods `search(filter, start_cursor)`, `query_database(db_id, start_cursor)`, `list_block_children(block_id, start_cursor)`, `list_comments(block_id, start_cursor)`, `retrieve_page(id)`, `retrieve_user(id)`. Constructor takes the bot token + `Notion-Version`. All list methods return `(results, next_cursor, has_more)`.
- T006: `429 Retry-After` handling + a `_open_notion_client(install)` test seam that reads the bot token from `secret_store` via `install["secret_ref"]`.
- T007–T009: `test_client_notion.py` — pagination across two pages; `401` → `NotionApiError(reason='unauthorized')`; `429` honors `Retry-After`.

**Slice 3 (OAuth install + callback)**
- T010: `services/integrations/notion/oauth.py::install_handler` (Bearer-authed) — `issue_state_token(tenant_id, pool, provider="notion")`, 302 to `https://api.notion.com/v1/oauth/authorize?client_id=…&response_type=code&owner=user&state=<token>`.
- T011: `callback_handler` (public) — verify HMAC state, atomic nonce consume, `POST /v1/oauth/token` (Basic-auth client_id:client_secret) → bot token + `workspace_id` + `bot_id`, `secret_store.put("notion_token:{workspace_id}")`, UPSERT `provider_installations(provider='notion', installation_id=workspace_id, secret_ref=…)`, `installation_audit_log` row, 302 to success page.
- T012: Mount `/integrations/notion/install` + `/callback` in `router.py`; add callback to `_PUBLIC_PATHS`; extend `_assert_prod_safety_invariants` to require the Notion OAuth secret refs under `FYRALIS_ENV=prod`.
- T013–T016: `test_oauth_notion.py` — install 302 with tenant-bound state; first-install callback persists secret + row + audit; cross-tenant collision → `install-error?reason=installation_collision`, audit `status='rejected_collision'`; reinstall same tenant reuses row; expired/invalid/consumed state → error redirect.

**Slice 4 (Planner — shard decomposition)**
- T017: `services/ingestion/planners/context.py` — when `source=='notion'`, build a `NotionClient` as `ctx.source_client` (parallel to the GitHub branch per A18.6).
- T018: `services/ingestion/planners/notion.py::plan_notion_shards(ctx)` — `search(filter=database)` to enumerate databases → one `Shard(shard_kind="notion_database", shard_identifier={"database_id":…, "workspace_id":…})` each; plus one `Shard(shard_kind="notion_page_tree", …)` for loose pages. `recency_score` from `exp(-age_days/τ)` using each DB's `last_edited_time`.
- T019: `PLANNER_DISPATCH["notion"] = plan_notion_shards` at import.
- T020: `test_notion.py` (planners) — mocked `search` returns 2 DBs → 2 db shards + 1 page-tree shard; recency ordering.

**Slice 5 (Fetcher — backfill page loop)**
- T021: `services/ingestion/fetchers/notion.py::NotionCursor` (Pydantic, `extra="forbid"`): `start_cursor: str | None`, `last_edited_at: str | None`, `items_seen: int`, plus a `sub_phase` discriminator for the `notion_database` shard (rows → then per-row blocks → then per-row comments).
- T022: `fetch_page_notion(install, shard_identifier, cursor)` — dispatch on `shard_kind`: `notion_database` queries rows then walks blocks + comments; `notion_page_tree` walks loose pages. Returns `FetchResult(records, next_cursor, end_of_data=not has_more)`. Records shaped to match the handler's expected envelope (A27.3): `{"channel": "notion:page"|"notion:page_content"|"notion:comment", "object": <notion object>, "workspace_id":…}`.
- T023: `external_id` derivation — `notion:page:{page_id}`, `notion:comment:{comment_id}` — stable across backfill and poll re-fetch. Cursor advances `last_edited_at` high-water from `results[*].last_edited_time`.
- T024: Rate-limit path — on `429`, return `FetchResult(records=[], next_cursor=<unchanged>, end_of_data=False)` after honoring `Retry-After` (loop re-enters next tick).
- T025: `FETCHER_DISPATCH["notion"] = fetch_page_notion`.
- T026–T028: `test_notion.py` (fetchers) — two-page round-trip with cursor; `has_more=false` → `end_of_data=True`; rate-limit empty page leaves cursor unadvanced.

**Slice 6 (Handlers — normalization to ObservationDraft)**
- T029: `services/ingestion/handlers/notion.py` — `@register("notion:page")`: extract title + status/property snapshot, `occurred_at=last_edited_time`, `source_actor_ref=f"notion:{last_edited_by.id}"`, `external_id=notion:page:{id}`, `kind="state_change"` when a tracked status property is present else `"signal"`, `entities_hint` from relation properties + `@`-mentions. `@register("notion:page_content")` and `@register("notion:comment")` similarly.
- T030: Add `"notion:page"`, `"notion:page_content"`, `"notion:comment"` → `"attested_agent"` in `CHANNEL_TRUST_MAP`; add the handler-module import line.
- T031–T033: `test_notion.py` (handlers) — each channel → well-formed `ObservationDraft`; `external_id` stable; relation/mention entity hints extracted; status-change page yields `kind="state_change"`.

**Slice 7 (Reconciler — gap detection)**
- T034: `services/ingestion/reconcilers/notion.py::reconcile_notion(shards, run)` — for each completed shard, compare cursor `last_edited_at` high-water against the live latest `last_edited_time` (one cheap `query_database`/`search` with page size 1). If live > cursor, emit a `ResharedShard` for that database/page-tree with a boosted recency. Else clean.
- T035: `RECONCILER_DISPATCH["notion"] = reconcile_notion`.
- T036–T037: `test_notion.py` (reconcilers) — clean when live ≤ cursor; gap → one `ResharedShard` with `parent_shard_id`.

**Slice 8 (Incremental poll + polish)**
- T038: Confirm the existing `PeriodicReconciler` / `oauth_poller` re-runs the Notion fetcher on cadence with the saved cursor — verify no Notion-specific wiring is needed beyond the source being registered. If a cadence knob is required, add it config-driven (no hardcode).
- T039: `services/integrations/notion/metrics.py` — `notion_install_total{result}`, `notion_fetch_pages_total`, `notion_fetch_rate_limited_total`, `notion_reconcile_gap_total`.
- T040: End-to-end integration test: seed a `provider_installations(provider='notion')` row + mocked `api.notion.com`; drive a `source_onboarding_runs` row through planner → fetcher → `ingest()` and assert `observations` rows land for each channel with stable `external_id` and `trust_tier='attested_agent'`.
- T041: Append the IN-14 section to `CODEBASE-ARCHITECTURE.md`.

## Risk Register

1. **Notion rate limits (~3 req/s) make full-workspace backfill slow.** A workspace with thousands of pages × (rows + blocks + comments) is many requests. **Mitigation**: per-database sharding bounds each unit; `recency_score` runs recent DBs first so high-value signal lands early; `429 Retry-After` is honored with cursor-preserving empty pages so the shard never fails on throttle.
2. **No real-time push ⇒ staleness between polls.** Intent changes (status flips) are seen at most one poll-interval late. **Mitigation**: documented; poll cadence is the tunable. Push-webhook ingest is the named follow-up if sub-minute latency is needed.
3. **Recursive block pagination unbounded.** Deeply nested pages could explode request count. **Mitigation**: cap recursion depth (≤3) in v1; emit a truncation marker in `content`; revisit if a consumer needs deeper bodies.
4. **`external_id` stability across backfill vs poll.** If the two paths derived different keys, the dedup UNIQUE index would double-count. **Mitigation**: both paths use the same `fetch_page_notion` → same `notion:page:{id}` derivation; tested in T040.
5. **Notion API version pinning.** Notion requires a `Notion-Version` header; behavior changes across versions. **Mitigation**: pin one version as a constant in `client.py`; bump is a reviewed change.
6. **Trust over-claiming.** A status field declared "Done" is intent, not verified reality. **Mitigation**: `attested_agent`, not `authoritative`; reasoning that treats Notion as ground truth would be a downstream Think bug, not an ingestion one.

## Out of Scope (deferred, with rationale)

- **Push-webhook ingest** (`services/webhooks/router.py` + `tenant_resolver.py` extension, `ResolverProvider` Literal + `_extract_notion`): deferred — Notion's webhook coverage is partial and v1's poll path covers correctness. Adding it later is purely additive at the webhook edge.
- **Writing back to Notion** (outbound — creating/updating pages): out of scope; this is a read-only signal source.
- **Trust elevation for DB status transitions**: deferred until a Think consumer demonstrably needs a tier above `attested_agent` (§X).
- **Per-block-type structured extraction** (tables, embeds, synced blocks): v1 captures text content + properties; richer block typing deferred.
- **`spec.md` / `tasks.md` / `data-model.md`**: this plan is the anchoring artifact written ahead of the formal speckit sequence; backfill `spec.md` before implement if running the full SDD gate.

## Decisions (locked — implementation phase, 2026-05-23)

The three open questions are resolved as follows. Decision 3 was **revised** after reading the normalizer routing layer.

### D1 — Poll cadence: 15 min, env-overridable
Incremental staleness ≤ 15 min, matching `GREETING_REFRESH_INTERVAL_SECONDS=900` (no point ingesting faster than the UI renders). Implemented as env var `NOTION_POLL_INTERVAL_SECONDS` (default `900`) read by the incremental driver, so ops can tune without a deploy. **Rationale**: balances Notion's ~3 req/s rate limit against drift-detection freshness; the render cadence is the natural ceiling.

### D2 — Block-recursion depth cap: 3, with explicit truncation marker
Page-body block trees are walked to depth ≤ 3. Beyond that, the handler stamps `content["_truncated"] = {"reason": "depth_cap", "depth": 3}` rather than silently dropping. Env var `NOTION_BLOCK_DEPTH_CAP` (default `3`) for tuning. **Rationale**: the signal (title, top-level prose, first bullet layer) lives in the first 2–3 levels; deeper descent is diminishing-value API spend against the rate limit. The truncation marker preserves epistemic honesty (§VI) — a reasoner knows the body was clipped.

### D3 — Channel model: ONE channel `notion:object`, internal branching (REVISED from three channels)
**Original plan**: three channels `notion:page` / `notion:page_content` / `notion:comment`.
**Revised decision**: a **single** registered channel `notion:object`, with the handler branching on the Notion object's native `object` field (`"page"` | `"block"` | `"comment"`) to set `kind` and `content["object_type"]` per record.

**Why the revision**: the Kafka-path normalizer ([services/ingestion/normalizer/channel_mapping.py](../../services/ingestion/normalizer/channel_mapping.py)) routes `(source, ingress_kind) → exactly one channel` via `resolve_channel`. A single source+`backfill`/`poll` pair therefore *cannot* fan out to three separately-registered handler channels. The established precedent is GitHub: one channel `github:webhook` whose handler branches on `X-GitHub-Event` across 6 event types, assigning per-record `trust_tier`/`kind`. Notion follows that exactly — and Notion objects self-describe via their `object` field, so no injected header is even needed (cleaner than GitHub).

The downstream distinction I wanted (a status flip vs. a body edit vs. a comment) is preserved without fighting the router:
- `object="page"` → `content.object_type="page"`; `kind="state_change"` when a tracked status property changed, else `"signal"`.
- `object="block"` → `content.object_type="block"` (page-body content); `kind="signal"`.
- `object="comment"` → `content.object_type="comment"`; `kind="signal"`.

All carry `source_channel="notion:object"` and `trust_tier="attested_agent"`. This supersedes every "three channels" / `notion:page_content` mention elsewhere in this plan.

**Routing wiring**: `resolve_channel` gains `(notion, backfill) → "notion:object"` and `(notion, poll) → "notion:object"`. `SourceLiteral` in the raw envelope gains `"notion"`.
