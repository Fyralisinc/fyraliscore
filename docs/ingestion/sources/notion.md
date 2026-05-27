# Notion (IN-14)

> The *declared-intent / canonical-state* layer (databases, pages, comments) —
> lets Think reason about intent-vs-reality drift against GitHub/Slack actuals.
> Poll-based, with a special always-pipeline webhook handler.

| Field | Value |
|---|---|
| Source | `notion` |
| Primary channel | `notion:object` (one channel, handler branches on `object`) |
| Trust tier | `attested_agent` |
| Live ingress | **poll** via `periodic_reconciler` (`NOTION_POLL_INTERVAL_SECONDS`, default 900s) + a special webhook handler |
| Backfill | databases/pages → blocks → comments (resumable tree walk) |
| Auth | OAuth — long-lived bot token (no JWT/refresh) |

## Why it is structurally different

Notion has no reliable content push, so there is effectively **no inline webhook
path**. The webhook endpoint `/webhooks/notion/...` routes **always** to the full
pipeline via a *dedicated handler* that fetches the full page and calls
`shadow_write_raw()` directly (using the `app.state.notion_data_plane` alias,
which points at the *same* producer/S3 instances). It short-circuits the router
before the inline block — there is **no** `_PROVIDER_CHANNEL` entry for Notion.
The primary live path is a **poll**: the `periodic_reconciler` re-runs the
backfill fetcher under `ingress_kind="poll"`.

## Auth & install

[services/integrations/notion/oauth.py](../../../services/integrations/notion/oauth.py):
`/integrations/notion/install` → `/callback` (public allowlist). Long-lived bot
token, no refresh. Outbound [client.py](../../../services/integrations/notion/client.py)
(`search` / db-query / blocks / comments) honors 429 `Retry-After`.

## Backfill / poll (the quartet)

- [planners/notion.py](../../../services/ingestion/planners/notion.py) — one
  `notion_database` shard per DB + one `notion_page_tree` shard.
- [fetchers/notion.py](../../../services/ingestion/fetchers/notion.py) —
  **resumable work-stack tree walk**: the cursor *is* the stack. One Notion list
  call per invocation; `end_of_data` when the stack empties. A 429 re-pushes the
  current item with its cursor unadvanced; a 404 on one object skips it and keeps
  walking. Block recursion is depth-capped (`NOTION_BLOCK_DEPTH_CAP`, default 3)
  with an explicit `content._truncated` marker beyond the cap.
- [reconcilers/notion.py](../../../services/ingestion/reconcilers/notion.py) —
  gap probe: live latest edit vs cursor high-water.
- [handlers/notion.py](../../../services/ingestion/handlers/notion.py) — **one
  channel `notion:object`**; branches on the native `object` field
  (page/block/comment), setting `kind` + `content.object_type` per record. A DB
  row with a `status` property → `kind=state_change`.

## Dedup / external_id

`external_id = notion:{object}:{id}` — identical across backfill and poll re-runs,
so the observations unique index collapses twins.

## Migration

`0059_notion_source_check.sql` — widens the inline `source` CHECK on the four M6
tables to admit `'notion'`. Idempotent + additive.
`provider_installations.provider` is free TEXT — no change.

Spec: `specs/IN-14-notion-signal-source/plan.md`. See [architecture.md](../architecture.md).
