# Contract: `services.integrations.discord.gateway.dispatch`

The dispatch module is the bridge between the WSS client (which speaks Discord's opcode protocol) and the ingestion handler (which writes substrate rows). It is a pure-async function module; no class wraps it.

## Public surface

### `async def handle_dispatch(payload, deps) -> None`

Top-level entry — called by `DiscordGatewayClient` for every op 0 (DISPATCH) frame.

- `payload`: the parsed JSON message from the gateway. Has shape `{ "op": 0, "s": <int>, "t": <event_name>, "d": <event_payload> }`.
- `deps`: a `DispatchDeps` dataclass holding `pool`, `tenant_resolver`, `ingestion_handler`, `metrics`, `application_id`.

Returns `None`. Raises only on unrecoverable programmer errors; routine failures (UnknownInstallation, dedup hit) are absorbed and logged.

Dispatches on `payload["t"]`:

| event | handler |
|---|---|
| `READY` | Bind session_id + resume_gateway_url on the client (not in this module) |
| `RESUMED` | Log + metric |
| `MESSAGE_CREATE` | `handle_message_create(d, deps)` |
| `GUILD_CREATE` | metric only |
| `GUILD_DELETE` | metric only (Clarifications: no bridge to IN-09 chokepoint) |
| any other | metric `dispatch_total{event=<t>}`, no action |

### `async def handle_message_create(message, deps) -> None`

The hot path. Pure async function; no shared state.

```python
async def handle_message_create(
    message: dict[str, Any],
    deps: DispatchDeps,
) -> None:
    # 1. Author filter (R7: fastest, before any DB hit)
    if message.get("author", {}).get("bot") is True:
        source = "self" if message["author"]["id"] == deps.application_id else "other_bot"
        deps.metrics.inc("discord_gateway_filtered_bot_total", source=source)
        return
    if message.get("webhook_id") is not None:
        deps.metrics.inc("discord_gateway_filtered_bot_total", source="webhook")
        return

    # 2. Tenant resolution
    guild_id = message.get("guild_id")
    if guild_id is None:
        # DM — out of scope for v1; just log + metric.
        deps.metrics.inc("discord_gateway_dispatch_total", event="MESSAGE_CREATE_DM")
        return
    try:
        resolved = await deps.tenant_resolver.resolve(
            "discord", payload={"guild_id": guild_id},
        )
    except UnknownInstallation:
        deps.metrics.inc("discord_gateway_dropped_unknown_installation_total")
        log.info(
            "discord_gateway_dropped_unknown_installation",
            short_guild_hash=short_guild_hash(guild_id),
        )
        return

    # 3. Build ingestion payload
    payload = {
        "source_channel":   "discord:message",
        "external_id":      f"discord:{message['id']}",
        "source_actor_ref": f"discord:{message['author']['id']}",
        "content_text":     message.get("content", ""),
        "occurred_at":      _parse_timestamp(message["timestamp"]),
        "tenant_id":        resolved.tenant_id,
        "metadata": {
            "channel_id":        message.get("channel_id"),
            "short_guild_hash":  short_guild_hash(guild_id),
            "mention_user_ids":  [m["id"] for m in message.get("mentions", [])],
            "attachment_count":  len(message.get("attachments", [])),
        },
    }

    # 4. Hand to existing ingestion handler
    try:
        await deps.ingestion_handler.handle(payload)
        deps.metrics.inc("discord_gateway_messages_total")
    except DedupIgnored:
        # The handler converts UniqueViolation into this benign signal.
        deps.metrics.inc("discord_gateway_messages_dedup_total")
```

## Failure modes and invariants

- **`UnknownInstallation`** → drop silently, metric, log with short_guild_hash (NOT raw guild_id).
- **`DedupIgnored`** → metric, no error propagation (idempotency held by unique constraint).
- **`asyncpg.PostgresError` other than UniqueViolation** → propagate to caller; caller may close connection and reconnect. **Do not retry inline** — the next MESSAGE_CREATE from Discord will trigger a fresh attempt naturally.
- **`KeyError` / `ValueError` on malformed Discord payload** → log ERROR with the offending event_id (if extractable), metric `dispatch_total{event="malformed"}`, return without raising. A malformed payload is Discord's bug, not ours.

## Test contract

Tests in `services/integrations/discord/gateway/tests/test_dispatch_message_create.py` and `test_dispatch_filters.py`:

1. `test_message_create_lands_as_observation` — happy path; seeded `provider_installations` + valid payload → exactly one row.
2. `test_duplicate_message_id_is_idempotent` — two dispatches of same message.id → one row, second dedup metric increments.
3. `test_author_bot_self_drops_silently` — `author.bot=true, author.id=APP_ID` → zero rows, `filtered_bot_total{source="self"}` increments.
4. `test_author_bot_other_drops_silently` — `author.bot=true, author.id != APP_ID` → zero rows, `source="other_bot"`.
5. `test_webhook_id_drops_silently` — `webhook_id="123"` → zero rows, `source="webhook"`.
6. `test_unknown_guild_drops_silently` — guild_id with no install row → zero rows, `dropped_unknown_installation_total` increments, no raw guild_id in caplog.
7. `test_dm_message_drops_silently` — no guild_id (DM context) → zero rows, `dispatch_total{event="MESSAGE_CREATE_DM"}` increments.
8. `test_attachment_only_message_ingests_with_empty_content` — `content=""`, `attachments=[…]` → one row, `content_text=""`, `attachment_count > 0`.
9. `test_content_text_verbatim` — message with markdown (`**bold**`), URLs, mentions → `content_text` equals message.content byte-for-byte.
10. `test_no_raw_guild_id_in_logs` — any of the above paths → no log record contains the raw guild_id (use `caplog` + `assert guild_id not in record.getMessage()` for all records).

All tests use real Postgres + real Ollama (Constitution §IV). The Gateway WSS is not exercised by these tests — they call `handle_message_create` directly with fabricated payloads.
