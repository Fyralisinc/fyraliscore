# Slack human↔human DM ingestion — test environment

Per-user-OAuth (xoxp) ingestion of **direct messages** and **group DMs**, in
addition to channel messages, through the full pipeline — **backfill + live**,
landing in Postgres where you can watch it in pgAdmin.

## TL;DR — one command

The full pipeline is already running (`docker compose -f docker-compose.yml -f
docker-compose.sandbox.yml`). Run:

```bash
scripts/slack_dm_demo.sh            # defaults: user U_ALICE, 6 live events
# or: scripts/slack_dm_demo.sh U_BOB 10
```

It drives **install → backfill → live emit → status** for a consenting user and
prints the result. Then open pgAdmin and run the query below.

## What it does

| Step | What happens |
|---|---|
| **install** | Stores a mock per-user `xoxp` token (`slack_user_token:{team}:{user}`), records the consenting user in `slack_dm_installations`, and registers a `provider_installations` row + signing secret so the **live webhook path verifies + resolves tenant**. |
| **backfill** | Synthesizes historical **DMs (im)** with colleagues + a friend, a **group DM (mpim)**, and a couple **channel** messages, and ingests each through the real handler (`ingest()` → normalize → `observations`). |
| **live/emit** | Synthesizes a fresh event and POSTs it **Slack-v0-HMAC-signed to `/webhooks/slack/events`** — the genuine live path (signature verify → tenant resolve → ingest). Rotates `message.im` / `message.mpim` / `message_changed` (an edit). Falls back to inline ingest if the self-call fails. |
| **status** | DM-vs-channel counts + the most recent rows. |

DMs and channels share one handler and one dedup key (`external_id =
"{channel}:{ts}"`), so a backfilled message and its live twin collapse to one
observation. DM-vs-channel is the `content->>'channel_type'` attribute
(`im` / `mpim` / `channel`), **not** a separate channel.

## View results in pgAdmin

Connect:

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | **`5434`** |
| Database / Username / Password | `company_os` |

Then run:

```sql
-- DM vs channel breakdown
SELECT
  CASE WHEN content->>'channel_type' IN ('im','mpim') THEN 'DM/MPIM'
       WHEN left(external_id,1) IN ('D','G')          THEN 'DM/MPIM (by id)'
       ELSE 'channel' END        AS surface,
  content->>'channel_type'       AS channel_type,
  content->>'subtype'            AS subtype,
  count(*)                       AS n
FROM observations
WHERE source_channel = 'slack:message'
GROUP BY 1,2,3 ORDER BY 1,2,3;

-- recent rows (DM + channel), newest first
SELECT external_id, content->>'channel_type' AS channel_type,
       content->>'subtype' AS subtype, content_text, occurred_at, ingested_at
FROM observations
WHERE source_channel = 'slack:message'
ORDER BY ingested_at DESC LIMIT 30;
```

Expected after one run (`n=6`): ~21 `im`, ~5 `mpim`, ~2 `channel`, 1
`message_changed` (the captured edit).

## Concurrency

`backfill` and `live/emit` are independent endpoints — run them at the same
time (e.g. loop `live/emit` in one shell while `backfill` runs in another); the
`UNIQUE(source_channel, external_id, occurred_at)` constraint absorbs any
overlap.

## How the pieces fit (code)

- Handler (DM subtypes + channel_type/subtype): [services/ingest/ingestion/handlers/slack.py](../../services/ingest/ingestion/handlers/slack.py)
- Console: [services/app/gateway/slack_router.py](../../services/app/gateway/slack_router.py) (mounted in [main.py](../../services/app/gateway/main.py), env gate `SLACK_DM_PANEL_ENABLED`)
- Per-user identity table: [db/migrations/0065_slack_dm_installations.sql](../../db/migrations/0065_slack_dm_installations.sql)
- Driver: [scripts/slack_dm_demo.sh](../../scripts/slack_dm_demo.sh)

## Edit / delete semantics

- `message_changed` is captured as a **distinct edit observation** keyed on the
  edit timestamp (dedup is insert-only, so reusing the original ts would drop
  the edited text); the original ts is preserved in `content.original_ts`.
- `message_deleted` carries no content and is **rejected** (deletion tracking
  is out of scope for this layer).

## Rebuilding after code changes

Images are baked (`build: .`). After editing handler/console code:

```bash
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml build gateway normalizer
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml up -d --no-deps --force-recreate gateway normalizer
# new migration? apply with:
docker compose -f docker-compose.yml -f docker-compose.sandbox.yml run --rm migrate
```

## Worker-fetch backfill (the production Kafka path)

The console above drives the pipeline with inline `ingest()`. The **production
worker-fetch backfill** does the same work through the genuine planner →
fetcher → raw-tier(S3) → Kafka → normalizer → observation_writer chain, in
**Provider Lab mode**, landing **identical observations** (same `slack:message`
channel, same `external_id="{channel}:{ts}"`, same `content.channel_type`):

```bash
scripts/slack_dm_worker_demo.sh            # default user U_ALICE, 6 msgs/DM
```

What it exercises:

| Stage | Code |
|---|---|
| **plan** | `plan_shards_slack` emits one `slack_channel_window` per bot channel **+** one `slack_dm_window` per im/mpim conversation **per consenting user** (`slack_dm_installations`), enumerated via `SlackUserClient.conversations_list(types=im,mpim)` under that user's xoxp token. |
| **fetch** | `fetch_page_slack` branches on `slack_dm_window`: opens the per-user client (`_open_slack_user_client`), reads `conversations.history`, and injects `channel` + `channel_type` so each record matches the live webhook + inline twin. |
| **produce** | Each record → raw tier (S3, content-addressed) → `RawEnvelope` pointer on `ingestion.raw.slack` (the real shard_fetch producer functions). |
| **consume** | The running `normalizer` → `ingestion.normalized.slack` → `observation_writer` → `observations` (gated by the tenant's `ingestion.kafka_path_enabled` flag). |

The loopback **Provider Lab** (`services/ingest/synthetic/provider_lab/`) serves the
Slack reads: a per-user token `spam-slack-user::<team>::<user>` requesting
`types=im,mpim` returns that consenting user's DM conversations; a **bot token
gets none** (the real Slack ceiling). DM fixtures come from
`make_slack_dm_workspace` — its DM channel ids use the same blake2b scheme as
the inline console, so a worker-backfilled DM and its inline twin dedup to one
observation.

The demo uses a dedicated tenant (`00000000-0000-0000-0000-0000000000d3`). It
rebuilds the app image, provisions the per-source Kafka lanes, recreates the
two consumer workers, then runs a one-shot producer
(`scripts/slack_dm_worker_fetch.py`). Re-run `scripts/sandbox_up.sh` to restore
the workers to baseline. View it the same way in pgAdmin — filter on that
tenant_id. Expected: 18 `im` + 4 `mpim` + 2 `channel` = 24 observations.

> The driver calls the real planner + fetcher + shard_fetch producer functions
> directly (rather than via the `source_onboarding`/`shard_fetch` service tick
> loops) so the run is self-contained and observable; in production those
> services invoke this identical code off an `onboarding_triggers` row.

## Production note

The inline console drives the pipeline with **mock** xoxp tokens + synthetic
data (no real Slack calls); the worker-fetch demo above adds the genuine
backfill worker chain in Provider Lab mode. In production, per-user xoxp tokens
(consent flow) replace the mock tokens and the source clients hit real Slack.
