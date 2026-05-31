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

- Handler (DM subtypes + channel_type/subtype): [services/ingestion/handlers/slack.py](../../services/ingestion/handlers/slack.py)
- Console: [services/gateway/slack_router.py](../../services/gateway/slack_router.py) (mounted in [main.py](../../services/gateway/main.py), env gate `SLACK_DM_PANEL_ENABLED`)
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

## Production note

This console drives the pipeline with **mock** xoxp tokens + synthetic data (no
real Slack calls). The production worker-fetch backfill (planner enumerates
`conversations.list(types=im,mpim)` per consenting user token → fetcher →
Kafka workers) is the next increment; the synthetic spammer already serves
Slack reads (`services/synthetic/spammer/server.py`) to exercise it in
spammer mode.
