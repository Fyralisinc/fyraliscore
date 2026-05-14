# Phase 0 — Research

Each item is a load-bearing decision with one of: **Confirmed** (verified against current codebase or external doc), **Decided** (pinned in spec Clarifications), **Deferred** (out-of-scope follow-up).

---

## R1 — Ed25519 library and existing verifier surface

**Decision**: Reuse `services/webhooks/signatures/discord.py` verbatim. PyNaCl (`nacl.signing.VerifyKey`) is already imported lazily there.

**Rationale**: The verifier was written in IN-06 and is wired through the `services.webhooks.verifier` interface that IN-07 + IN-08 already adapted to. Verified by reading the first 40 lines of `services/webhooks/signatures/discord.py` and grepping for `pynacl` in `pyproject.toml` — the dependency is present transitively. Lazy import (`_import_nacl()`) means the module loads cleanly even in stripped-down test environments.

**Alternatives considered**: Direct use of `cryptography.hazmat.primitives.asymmetric.ed25519` — rejected because the `cryptography` library's Ed25519 API is asymmetric-key-pair-oriented and noisier than PyNaCl's `VerifyKey.verify()`, and we would diverge from the IN-06 verifier shape for no gain.

---

## R2 — Discord interaction de-dup mechanism

**Decision**: Lean on the existing UNIQUE index `observations_source_channel_external_id_occurred_at_key` on `observations(source_channel, external_id, occurred_at)`. The ingestion handler sets `external_id = f"discord:{interaction.id}"`; a duplicate POST raises `asyncpg.exceptions.UniqueViolationError` which the handler catches and treats as idempotent success (no new Observation, original ack returned).

**Rationale**: Verified the index exists by `\d observations` — it's already in place from a much earlier migration. No new migration needed. This matches Slack's existing pattern (`external_id = f"{channel}:{ts}"`). DB-level enforcement eliminates the race condition that a check-then-insert pattern would have, and Constitution §IV's "real DB in integration tests" guarantees we catch any regression in the index by way of T006.

**Alternatives considered**:
- New `seen_interaction_ids` table with TTL — rejected as a redundant new table when the existing index serves the purpose. Violates §X simplicity.
- In-process LRU cache + DB constraint — premature optimisation; Discord retries are infrequent enough that the DB constraint is the only enforcement we need.

---

## R3 — Slash-command registration verb

**Decision**: Pinned in spec Clarifications Q2 — `POST /applications/{app_id}/commands` per install. Discord auto-upserts on `name` collision since API v9, returning the same command `id` on re-install.

**Rationale**: Captured in spec Clarifications. Worth re-stating that the alternative — a one-time bootstrap script — was rejected because it breaks the self-serve "click → consent → working" contract in SC-001.

**Alternatives considered**: Documented in Clarifications Q2.

---

## R4 — State-token reuse across Slack and Discord

**Decision**: Both providers share `oauth_install_states`. The table's existing `provider` column (added in IN-08 migration 0040) disambiguates. Discord's `verify_and_consume_state` includes `WHERE provider='discord'` in the atomic UPDATE; a Slack-issued nonce will not consume against the Discord predicate.

**Rationale**: Confirmed `oauth_install_states.provider` exists and is required by inspection. Reusing the table avoids a new migration and matches the IN-08 "single source of truth" pattern for OAuth state.

**Alternatives considered**: Per-provider state tables — rejected; the provider column is the right disambiguator and is already in place.

---

## R5 — State-token issuance helper location

**Decision**: Reuse `services.integrations.slack.oauth.issue_state_token` as-is for Discord during T008. The helper takes `(tenant_id, pool)` and writes a `provider`-tagged row. If the function name reads Slack-specific (it does), an alias `services.integrations.oauth_state.issue_state_token` is added in T008 with the original function relocated; `services.integrations.slack.oauth` keeps a thin re-export for IN-08 test compatibility (SC-009).

**Rationale**: The helper is already provider-agnostic by accident — it takes a `provider` argument in its row INSERT. Avoid the cost of a name change unless it materially helps readability. Decision is deferred to the implementer in T008 with a structural-rename option clearly available.

**Alternatives considered**: Build a Discord-specific issue_state_token — rejected; trivial duplication.

---

## R6 — Bot-kick chokepoint concurrency

**Decision**: Pinned in spec Clarifications Q1 — idempotent re-runs, no row-level locking. Match `services/integrations/slack/uninstall.py::_disable_and_zeroize` byte-for-byte at the structural level.

**Rationale**: Captured in Clarifications. The Slack version has been live in IN-08's test suite for ~163 passing tests and has not flaked under concurrent test execution.

**Alternatives considered**: Documented in Clarifications Q1.

---

## R7 — Discord OAuth response shape

**Decision**: Parse `response["guild"]["id"]` defensively. If absent and the request included the `bot` scope, route to `install-error?reason=slack_oauth_error` (reusing the IN-08 error reason code is acceptable; the UI shell treats it as "OAuth provider returned an unexpected shape").

**Rationale**: Per Discord's current documented behavior, the `oauth2/token` response includes `guild.id` when `bot` scope is granted. Older flows may not. Defensive parsing keeps the OAuth callback robust without coupling us to a specific response shape; the structured error path lets the UI render an actionable message.

**Alternatives considered**: Hard-fail on missing `guild.id` with a Discord-specific error code — rejected; the IN-08 error path already exists and the UI doesn't need to distinguish per-provider for "OAuth shape unexpected."

---

## R8 — Public key per-installation mirroring vs app-level only

**Decision**: Mirror the application public key into `encrypted_secrets` with label `discord_public_key:<guild_id>`. This produces N rows holding identical plaintext, one per installation.

**Rationale**: The IN-08 `load_secrets` function resolves secrets via `provider_installations.secret_ref` → `encrypted_secrets` row. To stay on that path (so verification looks the same whether per-installation secrets differ across rows or not), we mirror. The waste is bounded (a few hundred rows at our scale) and the consistency win is significant — every provider's secret loading goes through the same DB path.

The env-var fallback `WEBHOOK_SECRET_DISCORD` remains for the PING handshake which precedes any installation.

**Alternatives considered**:
- Single per-app row with NULL `tenant_id` — would require special-casing `load_secrets` to handle "global" secrets that bypass the per-installation lookup. Rejected; adds a wart for one provider.
- Don't mirror; always use env-var path for Discord — rejected; conflicts with the IN-08 directional commitment ("env vars are dev-only fallback for prod posture").

---

## R9 — Optimistic deferred ack

**Decision**: Not implemented in v1. Rely on warm DB + sub-100ms Observation insert to stay well under Discord's 3s window.

**Rationale**: §X simplicity. The optimistic ack pattern adds branching to the handler that we don't yet have evidence we need. If integration tests show p95 latency approaching 2s we revisit by returning `{"type": 5}` early and completing the actual ack content via the deferred-message API.

**Alternatives considered**: Always deferred ack — rejected as user-facing latency penalty without justification.

---

## R10 — Slash command spec

**Decision**: One global slash command, name `fyralis`, with one required string option `query` (max length 6000 per Discord docs). Description set to a placeholder string; final wording owned by product.

**Rationale**: Spec Assumptions block pins this. Captured here as a research item so the implementer knows exactly what `commands.py::register_fyralis_command` POSTs.

**Alternatives considered**: Subcommand structure (`/fyralis ask`, `/fyralis ingest`, etc.) — deferred to IN-13.

---

## R11 — Test pollution between IN-08 and IN-09 suites

**Decision**: Add a `_unique_guild_id()` factory to `services/integrations/tests/conftest.py` that returns a per-test snowflake-like string. The factory ensures collision tests don't share guild_id with cross-tenant tests.

**Rationale**: Spec risk register item 5. Cheaper than per-suite truncation hooks.

**Alternatives considered**: Per-suite DB schema — overkill for our scale.
