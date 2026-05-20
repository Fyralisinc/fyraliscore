# Ticket #42 — Slack + GitHub live-ingestion synthetic drivers (tenant-targeted webhooks)

**Status:** Resolved with Z1 commits (Z1-slack: `5e82783`; Z1-github: this commit).
**Target milestone:** Synthetic-coverage trilogy (mega-prompt 4).
**Filed:** 2026-05-20.

## Symptom

The Path I validation audit (post-mega-prompt-3) found that Slack + GitHub *live* ingestion had no tenant-targeted synthetic driver. The synthetic suite covered:

- Backfill for all four sources — X3 harness (A22).
- Gmail live ingestion — Y1 Gmail Pub/Sub generator (A23).
- Discord live ingestion — Y2 Discord Gateway generator (A24).

The only Slack/GitHub traffic generator was `services/synthetic/cutover_load.py` (M-Load), which is an HTTP throughput load tester:

- It POSTs over real HTTP to a running webhook server at a `target_url` (no in-process ASGI path); the validation environment runs only Postgres + Kafka, no webhook server.
- Its tenant pool is a deterministic-Zipf set of *random* UUIDs whose `team_id` / `installation.id` values match **no** installed tenant. The webhook router resolves `(provider, installation_id) → tenant` via `provider_installations` and returns `UnknownInstallation → 401` for unresolved installs, so M-Load produces **zero** observations for known tenants.

Net: Slack/GitHub live ingestion could not be exercised with synthetic data attributed to specific seeded tenants, blocking validation runs that compose backfill + live across all four sources.

## Resolution

[A25 amendment](../ingestion/05-lld-amendments.md) (filed with the Z1-github commit) adds two in-process drivers:

- `SlackWebhookGenerator` — `services/synthetic/live_generators/slack_webhook.py` (Z1-slack, `5e82783`).
- `GithubWebhookGenerator` — `services/synthetic/live_generators/github_webhook.py` (Z1-github, this commit).

Both dispatch real, signed webhooks via `httpx.AsyncClient(transport=ASGITransport(app))` to the gateway app's webhook router, targeting *seeded* `provider_installations` rows (Slack by `team_id`; GitHub by `installation.id`). They coordinate X2 mock state, support per-tenant burst patterns + replay simulation, and compose with the X3 harness for backfill-then-live lifecycle tests.

Tests: 9 (Slack) + 10 (GitHub) = 19 new, all green. Coverage: happy path, signature validation, tenant-resolution gate, multi-tenant parallel dispatch, replay idempotency, mixed event types (GitHub), and X3 composition.

## Effect

The synthetic-coverage trilogy is complete — webhook (Slack + GitHub) + backfill (all four) + Pub/Sub (Gmail) + Gateway (Discord). All four sources are testable end-to-end for both backfill and live paths against specific seeded tenants. Validation runs composing all four sources × (backfill + live) can now be authored without a coverage gap.
