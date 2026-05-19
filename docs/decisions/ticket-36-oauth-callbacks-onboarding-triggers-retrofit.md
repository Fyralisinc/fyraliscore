# Ticket #36 — Retrofit OAuth callbacks to write `onboarding_triggers` rows

**Status:** Deferred. Filed in M6.3 Phase 3 closeout.
**Target milestone:** **Required before first real-customer cutover.** M7 territory or earlier if customer timeline demands.
**Filed:** 2026-05-19.

## Summary

The M6 framework's entry point is the `onboarding_triggers` table (M1-shipped schema, [migration 0047](../../db/migrations/0047_onboarding_triggers_outbox.sql)). The `oauth_poller` service (M6.1) consumes rows from this table and starts the M6 chain. Today, **no production code writes `onboarding_triggers` rows** — only tests do. Each source's OAuth callback handler must be retrofitted to insert an `onboarding_triggers` row in the same transaction as its install row.

Affects four OAuth callbacks (all four sources):

| Source | Callback location | Today writes | Needs to also write |
|---|---|---|---|
| Gmail | [services/integrations/gmail/oauth.py::connect_finalize](../../services/integrations/gmail/oauth.py) | `gmail_installations` + `gmail_install_audit` | `onboarding_triggers` (source='gmail', gmail_installation_id=<new install id>) |
| Slack | `services/integrations/slack/oauth.py` (or equivalent) | `provider_installations` (probably) | `onboarding_triggers` (source='slack', installation_row_id=<new install id>) |
| GitHub | `services/integrations/github/oauth.py` (or equivalent) | `provider_installations` (probably) | `onboarding_triggers` (source='github', ...) |
| Discord | `services/integrations/discord/oauth.py` (or equivalent) | `provider_installations` (probably) | `onboarding_triggers` (source='discord', ...) |

Each retrofit is a small addition inside the existing OAuth-callback transaction — atomically write the install row + audit + trigger together.

## Why this is needed

Without the retrofit, the M6 framework is **inert in production**:

- `oauth_poller` runs and finds no triggers → no `onboarding_runs` rows → no chain.
- Gmail/Slack/GitHub/Discord installs complete but never start backfill.
- Existing steady-state paths (push notifications, webhooks) continue to function, but new tenants never get a backfill.

This is the F4 finding from M6.3 pre-Phase-1 audit. **First real-customer cutover cannot ship without this.**

## Why this is deferred from M6.3

- OAuth callbacks are M1 substrate; modifying them is out of M6.3's per-source backfill scope.
- The retrofit is a cross-source change (touches all four OAuth flows); deserves its own work-unit.
- Some retrofits may surface their own substrate findings (e.g., Slack's OAuth flow uses a redirect that's not transactional in the same way as the Gmail wizard; investigate per-source).

## Scope of work

1. **Gmail (`services/integrations/gmail/oauth.py::connect_finalize`):**
   - Inside the existing `tenant_transaction` block (line 170-198), add:
     ```python
     await tctx.execute(
         """
         INSERT INTO onboarding_triggers
             (id, tenant_id, source, trigger_kind, gmail_installation_id, payload)
         VALUES ($1, $2, 'gmail', 'install', $3, $4::jsonb)
         """,
         uuid7(), tenant_id, install_id,
         json.dumps({"scope": scope_alias, ...}),
     )
     ```
   - Idempotency: if the user re-runs `/connect/finalize` for the same `(tenant_id, workspace_domain)`, the install row is upserted (`ON CONFLICT DO UPDATE`); we should NOT write a duplicate trigger row. Either gate the INSERT on "install was created vs updated," or use a per-(install, version) idempotency key.

2. **Slack / GitHub / Discord:** analogous retrofits in each source's OAuth-callback handler. Investigate each callback's transaction shape; use the same in-transaction INSERT pattern.

3. **Tests per source:**
   - Unit: callback writes trigger row in the same transaction as install row; idempotency on re-run.
   - Integration: completing OAuth flow → trigger row → oauth_poller consumes → run created.

4. **Update runbook §6.D-§6.G** to remove the "pre-F4-ticket inert state" notes — after this ticket lands, production triggers fire and the M6 chain runs end-to-end.

## Out of scope

- The M6 framework code itself. M6.3-M6.6 ship the dispatch entries; this ticket ships the entry-point retrofit.
- Backfill correctness validation (covered by M6.3-M6.6 E2E tests).
- Migrating existing installs (installs that exist pre-retrofit don't get a trigger; a separate one-time backfill query would handle them, but that's a deployment concern, not part of this ticket).

## Risk if deferred past first customer cutover

**Critical.** Customer onboarding flows would complete in the UI but produce no observations for several minutes (existing steady-state path catches up slowly) or never (for sources without push notifications). First-impression bug.

## Coordination

- Independent of Ticket #35 (watch scheduler) and Ticket #37 (Gmail inline-ingestion retirement).
- Should land BEFORE first customer cutover; ordering with #35/#37 is flexible.
