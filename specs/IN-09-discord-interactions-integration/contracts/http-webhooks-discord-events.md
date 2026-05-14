# HTTP Contract — `/webhooks/discord/events` (regression-only, no code changes)

This contract documents the route IN-06 + IN-07 + IN-08 already shipped, restated for clarity because IN-09 reads against it. **No router logic changes in IN-09.**

The router is `services/webhooks/router.py::handle_webhook` mounted at `/webhooks/{provider}/{event_kind}`. Discord provider name is `discord`; the only `event_kind` Discord uses is `events`.

## Request

```http
POST /webhooks/discord/events
Content-Type: application/json
X-Signature-Ed25519: <hex-encoded 64-byte signature>
X-Signature-Timestamp: <unix-seconds>

<interaction-create payload>
```

Body is the Discord `InteractionCreate` payload. Signed across `timestamp || body` with the application's Ed25519 private key; verified against the application public key.

## Signature verification

**Source of the public key**:

1. If a `provider_installations` row exists for the payload's `guild_id`, `secret_ref` resolves via `lib/shared/secrets/.get(ref, tenant_id=...)` to the `discord_public_key:<guild_id>` row in `encrypted_secrets`. Used for normal traffic.
2. If no `guild_id` (PING — interaction type=1) OR no `provider_installations` row, fall back to `os.environ['WEBHOOK_SECRET_DISCORD']`. Used for PING handshake and for the rejection path of `unknown_installation`.

Both paths run through `services/webhooks/signatures/discord.py::verify`. The DB-backed mirror (path 1) and the env-var path (path 2) produce identical plaintext for a healthy deployment; the mirror exists so `load_secrets` resolves uniformly.

## Response shapes

| Outcome | Status | Body |
|---|---|---|
| Valid signature, PING (type=1) | 200 | `{"type": 1}` (PONG) |
| Valid signature, ApplicationCommand (type=2), valid installation | 200 | `{"type": 5}` (DEFERRED_CHANNEL_MESSAGE_WITH_SOURCE) OR `{"type": 4, "data": {...}}` — see plan.md Risk #3 |
| Valid signature, type=2, no installation (`UnknownInstallation`) | 401 | `{"code": "webhook_verification_failed", "message": "no enabled installation matches the supplied identifier", "context": {"provider": "discord", "reason": "unknown_installation"}}` |
| Invalid signature | 401 | `{"code": "webhook_verification_failed", "message": "...", "context": {"provider": "discord", "reason": "signature_mismatch"}}` |
| Missing signature header | 401 | `{"code": "webhook_verification_failed", "message": "missing signature header", "context": {"provider": "discord", "reason": "signature_header_missing"}}` |
| Stale timestamp (> 300s) | 401 | `{"code": "webhook_verification_failed", "message": "...", "context": {"provider": "discord", "reason": "timestamp_too_old"}}` |
| Body parse failure | 400 | `{"code": "webhook_payload_invalid", ...}` |

## Defensive ordering invariants (per FR-003 / IN-08)

The router MUST execute in this order:

1. Parse body (best-effort JSON load).
2. Run tenant resolver (`tenant_resolver.resolve('discord', payload, headers)`). PING returns `PayloadMissing` (no `guild_id`). Real interactions return `Resolved` or `UnknownInstallation`.
3. Load secrets (`load_secrets('discord', tenant_id, app_state=...)`).
   - For PING (`tenant_id=None`): falls through to env-var `WEBHOOK_SECRET_DISCORD`.
   - For real interactions: resolves via DB-backed path.
4. **Verify the Ed25519 signature.** Wrong-signature payloads are rejected here, NEVER falling through to step 5.
5. If `_is_discord_ping(payload)`: return `{"type": 1}` (handshake short-circuit).
6. If outcome is `UnknownInstallation`: return 401 `unknown_installation`.
7. Dispatch to `services/ingestion/handlers/discord.py::handle_discord_webhook`.

The "verify first, then route" order is a Constitution §III defense-in-depth invariant — never short-circuit before signature verification, regardless of provider. FR-003 codifies this for Discord PING; IN-08's analogous comment block in `router.py` already documents the same ordering for Slack `url_verification`.

## Ingestion handler dispatch

After step 7 the router calls `services.ingestion.handlers.discord.handle_discord_webhook(payload, headers)` which returns an `ObservationDraft`. The draft is then persisted by the ingestion pipeline (existing IN-06 machinery — unchanged).

The handler:

- Returns the draft for type=2 (ApplicationCommand).
- For type=3 (MessageComponent — button click), type=4 (ApplicationCommandAutocomplete), type=5 (ModalSubmit): NOT IMPLEMENTED in IN-09. Returns the draft with `content.metadata.unsupported_interaction_type=<n>` and `content_text=""`. Acceptance: integration test asserts these types do not raise; they produce a low-signal Observation that downstream Think will ignore. This is intentional — interaction types beyond slash commands are tracked under IN-13.

## Idempotency

`external_id = f"discord:{interaction.id}"` is enforced unique by `observations_source_channel_external_id_occurred_at_key`. A duplicate POST (Discord retry within 3 s) results in `UniqueViolationError` inside the ingestion pipeline; the handler catches and returns success with the original ack body.
