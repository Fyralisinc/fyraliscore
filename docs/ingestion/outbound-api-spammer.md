# Outbound-API source mock ("spammer")

Goal: drive the ingestion pipeline's **outbound** path (the calls *we* make
to each source's API during backfill / fetch) against a **local mock server**
that impersonates each source — including rate limits — on synthetic data,
without touching client code. Pointing the pipeline at the mock is pure
config.

## Status

| Piece | State |
|---|---|
| Endpoint configurability (all 4 sources) | ✅ done — `lib/integrations/endpoints.py` |
| Local spammer server (all 4 sources) | ✅ runnable — `services/synthetic/spammer/server.py` |
| Real READ clients (github/slack/discord) | ✅ done — `list_repo_events`/`head_repo_events`, `conversations_list`/`conversations_history`, `list_guilds`/`list_guild_channels`/`get_messages` |
| `_open_*_client` + `_build_source_client` wired to real clients | ✅ done — `services/ingestion/fetchers/_clients.py` (env-resolved base URLs) |
| Real client → spammer outbound loop, all 4 (pagination + 429) | ✅ proven — `services/synthetic/spammer/tests/test_{gmail,github,slack,discord}_outbound_loop.py` |
| Full gmail **backfill** against a real-port spammer | ✅ proven — `BackfillHarness(real_clients=True)` + `test_harness_gmail_against_real_port_spammer` |
| discord Gateway **WSS** mock (live path) | ✅ done — `services/synthetic/spammer/discord_gateway.py` + `test_discord_gateway_loop.py` |
| **Concurrent backfill + live, 4 sources × 50 tenants, Kafka-routed, against the spammer** | ✅ **READY** — Run 5 (`--run=5`); 515 observations, all per-source counts exact, 7/7 assertions pass |

## Run 5 — the capstone (`python -m services.synthetic.validation_runs.runner --run=5`)

Concurrent backfill + live ingestion across all four sources at 50 tenants,
routed through Kafka, with **backfill driving the real source clients over
HTTP against the local spammer** (token exchange → pagination → rate-limit
backoff) instead of in-process mock clients. Live ingestion stays inbound
(webhook / gateway / pubsub) via the Kafka cutover. Verified READY:

- Per-source counts exact: gmail 150, github 165, slack 100, discord 100 (515 total).
- Peak 50 simultaneous backfills in-flight while live fired; live slack/github → HTTP 202 (Kafka cutover).
- Zero duplicate observations, zero signal leak, DLQ empty, completion fires once per tenant.

Getting there required four scaling fixes (all in this work):
1. **Shared keep-alive httpx client** (`_clients._get_http`) — a fresh client per
   `_open_*_client` floods the spammer with TCP churn and wedges its loop.
2. **Multi-worker spammer** (`SPAMMER_WORKERS`, default 4) — state is
   registry-derived/read-only so workers are consistent.
3. **Locked lazy pool init** (`_clients._get_pool`) — the unlocked `global`
   raced under fan-out and exhausted Postgres connections; spammer-mode
   clients now carry no pool at all (tokens preset).
4. **Reconciler dispatch timeout** (`RECONCILER_DISPATCH_TIMEOUT_SEC`) — the
   reconciler holds its claim transaction across the best-effort gap-check
   HTTP; bounding it stops one slow source-API call from freezing the
   single-loop reconciler tick (a timed-out check is treated as clean).

## Endpoint configuration (`lib/integrations/endpoints.py`)

Every outbound client resolves its base URL via `endpoint(name)` at
construction time. Precedence: **per-source env var → single-host spammer
base → production default.**

| Resolver name | Per-source env var | Production default |
|---|---|---|
| `gmail_api` | `GMAIL_API_BASE_URL` | `https://gmail.googleapis.com/gmail/v1` |
| `google_directory` | `GOOGLE_DIRECTORY_BASE_URL` | `https://admin.googleapis.com/admin/directory/v1` |
| `google_token` | `GOOGLE_TOKEN_URI` | `https://oauth2.googleapis.com/token` |
| `github_api` | `GITHUB_API_BASE_URL` | `https://api.github.com` |
| `slack_api` | `SLACK_API_BASE_URL` | `https://slack.com/api` |
| `discord_api` | `DISCORD_API_BASE_URL` | `https://discord.com/api/v10` |
| `discord_gateway_bot` | `DISCORD_GATEWAY_BOT_URL` | `https://discord.com/api/v10/gateway/bot` |

**Single-host shortcut:** set `SYNTHETIC_SOURCE_API_BASE=http://localhost:9100`
to point ALL sources at one spammer host; each is served under a conventional
sub-path (`/gmail`, `/github`, `/slack`, `/discord`). A per-source env var
always wins over it.

Note: the gmail DWD token endpoint is *also* data-driven — the service-account
JSON's `token_uri` field — so set that to the spammer's `/gmail/token` for the
gmail vertical.

## Running the spammer

```bash
COMPANY_OS_ENV=test SPAMMER_PORT=9100 \
  python -m services.synthetic.spammer.server
```

Rate-limit knobs (env): `SPAMMER_429_EVERY` (429 on every Nth data request;
0=off), `SPAMMER_RETRY_AFTER` (Retry-After seconds), `SPAMMER_GMAIL_MESSAGES`
(messages per mailbox). Seed deterministic fixtures by pointing
`SPAMMER_FIXTURE_REGISTRY` at a harness `registry.json` (the spammer indexes
gmail by email, github repos by `{owner}/{repo}`, slack/discord by channel id,
discord guilds globally).

Then point the pipeline at it. Single-host shortcut for ALL sources:

```bash
export SYNTHETIC_SOURCE_API_BASE=http://localhost:9100
```

or per-source, e.g. for gmail:

```bash
export GMAIL_API_BASE_URL=http://localhost:9100/gmail/gmail/v1
# + a service-account JSON whose token_uri = http://localhost:9100/gmail/token
```

The real source clients then make real HTTP calls to the spammer — token
exchange → authed request → pagination → 429 backoff — all on synthetic data.

### Driving full gmail backfill against a real-port spammer

`BackfillHarness(real_clients=True)` spawns the spammer
(`services/synthetic/spammer/process.py::SpammerProcess`), seeds it from the
same fixture registry, writes a throwaway DWD service-account JSON whose
`token_uri` is the spammer, and sets `GMAIL_API_BASE_URL` /
`SYNTHETIC_SOURCE_API_BASE` in the subprocess env. No mock client is
monkeypatched — the default `_open_*_client` builds the **real** clients
(`services/ingestion/fetchers/_clients.py`). Proven by
`test_harness_gmail_against_real_port_spammer` (gated on `X3_HARNESS_E2E=1` +
Kafka + moto).

### Driving the live discord Gateway against the WSS mock

`services/synthetic/spammer/discord_gateway.py::DiscordGatewayMock` is a real
WebSocket server speaking the v10 opcode protocol (HELLO → IDENTIFY → READY →
heartbeat/ACK → MESSAGE_CREATE DISPATCH → RESUME replay). Point the real
`DiscordGatewayClient` at it by setting `SPAMMER_DISCORD_WSS_URL` (the spammer's
`/gateway/bot` returns it) + `DISCORD_GATEWAY_BOT_URL`. Proven by
`test_discord_gateway_loop.py`.

## How rate limits are accepted (no code change needed)

Each client maps HTTP `429` (+ `Retry-After`) to its rate-limit signal:
gmail raises `GoogleRateLimited` (the gmail fetcher wraps calls in
`retry_with_backoff_on_429`); slack's `_call` and discord's `_request` honor
`Retry-After` and retry *within the client* (bounded budget); github surfaces
`GithubApiError`. The spammer returns real HTTP 429s (`SPAMMER_429_EVERY`) and
the `test_{gmail,slack,discord}_outbound_loop.py` 429 tests prove absorption /
mapping over the real httpx stack.

## Read-client surface (github / slack / discord) — IMPLEMENTED

The real clients now carry the backfill **read** methods the M6.4–M6.6
fetchers / planners / reconcilers call, mirroring the mock-client interface:

- `GithubClient.list_repo_events` / `head_repo_events` (token mint → REST page
  with ETag + `Link` pagination + `If-None-Match` 304 fast-path).
- `SlackClient.conversations_list` / `conversations_history` (cursor paging).
- `DiscordClient.list_guilds` / `list_guild_channels` / `get_messages`
  (snowflake `before`/`after` paging).

`_open_{github,slack,discord}_client` (fetchers + reconcilers) and
`source_onboarding._build_source_client` build these real clients via
`services/ingestion/fetchers/_clients.py`, resolving the base URL through
`lib.integrations.endpoints` — so production vs. spammer is config-only. The
spammer serves the matching routers (`_github_router` / `_slack_router` /
`_discord_router`), seeded from a fixture registry.

**Remaining for full multi-tenant backfill of these three against the
spammer:** per-tenant identity seeding. Gmail's identity is the impersonated
email (carried in the DWD `sub` → `spam::<email>`), so multi-tenant gmail just
works. Slack `conversations.list` is workspace-scoped by bot token and discord
list-guilds is app-bot-scoped; driving many tenants needs the spammer to
resolve team/guild from a token-encoded identity (it already honors
`spam-slack::<team>` / `spam-gh::<install>`) plus the harness seeding those
secrets. The read path (history / messages / repo events) is keyed on
globally-unique path ids and already works multi-tenant.

## Discord Gateway (live) — IMPLEMENTED

Discord live ingestion is a **WebSocket** Gateway, not REST.
`services/synthetic/spammer/discord_gateway.py::DiscordGatewayMock` is a WSS
server speaking the v10 opcode protocol (HELLO/IDENTIFY/READY/heartbeat/
DISPATCH/RESUME). The real client fetches the wss URL from `/gateway/bot`
(configurable via `DISCORD_GATEWAY_BOT_URL`; the spammer returns
`SPAMMER_DISCORD_WSS_URL`). `test_discord_gateway_loop.py` runs the real
`DiscordGatewayClient` against it end-to-end.
