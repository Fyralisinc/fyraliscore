# Module Contract — `services/integrations/discord/client.py`

The single chokepoint for outbound HTTP calls to `discord.com/api`. Resolves per-installation bot tokens from `lib/shared/secrets`. Honours Discord rate limits. Triggers the bot-kick chokepoint on 401/403-code-50001.

## Construction

```python
class DiscordClient:
    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        secret_store: SecretStore,
        installation_row_id: UUID,
        tenant_id: UUID,
        guild_id: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> None: ...
```

Constructed once per outbound — short-lived. The caller (typically the ingestion handler post-Observation-commit, or the OAuth callback for slash-command registration) owns the lifecycle.

`http_client` defaults to a freshly-built `httpx.AsyncClient(timeout=10.0)`; tests pass a respx-wrapped client.

## Public methods (Phase 1 surface)

### `async post_followup_message(interaction_token: str, *, content: str) -> dict`

Sends an asynchronous follow-up to a previously-acked interaction. Used by ingestion handlers that want to reply to a `/fyralis ask` invocation after the substrate has finished thinking.

Endpoint: `POST https://discord.com/api/v10/webhooks/{application_id}/{interaction_token}` (no bot token required — the interaction_token is the credential).

### `async get_guild_member(user_id: str) -> dict`

Fetches a guild member for enrichment. Endpoint: `GET https://discord.com/api/v10/guilds/{guild_id}/members/{user_id}` (bot-token-authed).

### `async get_channel(channel_id: str) -> dict`

Fetches a channel for enrichment. Endpoint: `GET https://discord.com/api/v10/channels/{channel_id}` (bot-token-authed).

### `async post_register_global_command(application_id: str, command_spec: dict) -> dict`

Registers (or upserts) the `/fyralis` global command. Endpoint: `POST https://discord.com/api/v10/applications/{application_id}/commands` (bot-token-authed). Called once per OAuth callback success.

## Internal contract

### `_resolve_bot_token() -> str`

Looks up the bot token via `secret_store.get(label=f'discord_bot_token:{self.guild_id}', tenant_id=self.tenant_id)`. Raises `DiscordApiError(code='discord_secret_unavailable')` if the secret is missing (the installation is unhealthy — likely a dangling row that should have been cleaned up by the chokepoint).

### Rate-limit handling

Every outbound goes through `_request(method, url, **kwargs)`:

```python
state = RateLimitState()
while state.should_retry(now=time.monotonic()):
    resp = await self.http_client.request(method, url, ...)
    state.attempts += 1
    state.total_wall_seconds = time.monotonic() - start

    if resp.status_code == 429:
        retry_after = float(resp.headers.get('Retry-After', '1'))
        capped = min(retry_after, max(1.0, 30.0 - state.total_wall_seconds))
        if capped <= 0:
            break
        await asyncio.sleep(capped)
        continue

    if resp.status_code in (401, 403):
        # Chokepoint check: 401 always fires; 403 fires only on code=50001 (Missing Access)
        try:
            body = resp.json()
        except Exception:
            body = {}
        if resp.status_code == 401 or body.get('code') == 50001:
            await _disable_and_zeroize_discord(
                pool=self.pool,
                secret_store=self.secret_store,
                installation_row_id=self.installation_row_id,
                tenant_id=self.tenant_id,
                guild_id=self.guild_id,
                reason='outbound_401',
            )
            raise DiscordApiError(
                code='discord_api_unauthorized',
                message='installation was disabled following an authorization failure',
                context={
                    'tenant_id': str(self.tenant_id),
                    'http_status': resp.status_code,
                },
            )

    if 200 <= resp.status_code < 300:
        return resp.json()

    # Other 4xx/5xx: don't retry, raise.
    raise DiscordApiError(
        code='discord_api_error',
        message=f'discord returned {resp.status_code}',
        context={'tenant_id': str(self.tenant_id), 'http_status': resp.status_code},
    )

raise DiscordApiError(
    code='discord_api_rate_limited',
    message='retry budget exhausted',
    context={
        'tenant_id': str(self.tenant_id),
        'attempts': state.attempts,
        'total_wall_seconds': state.total_wall_seconds,
    },
)
```

Note `_disable_and_zeroize_discord` is idempotent (Clarifications Q1). Concurrent outbounds that both 401 will both call into it; each call is safe.

## Error surface (raises)

| Exception | When |
|---|---|
| `DiscordApiError(code='discord_api_unauthorized')` | After 401 chokepoint fires. The caller should treat the installation as gone. |
| `DiscordApiError(code='discord_api_rate_limited')` | Retry budget exhausted (≤3 attempts, ≤30s wall). |
| `DiscordApiError(code='discord_secret_unavailable')` | Bot token not in secret store. |
| `DiscordApiError(code='discord_api_error', context.http_status=<n>)` | Other terminal 4xx/5xx. |

All errors structured per Constitution §VIII (`code`, `message`, `context`).

## Logging

Every request emits a single structured log line via `structlog.get_logger(__name__)`:

```python
log.info(
    "discord_api_request",
    method=method,
    endpoint=url_template,  # e.g., '/applications/{app_id}/commands' — NOT the substituted URL
    tenant_id=str(self.tenant_id),
    # NEVER guild_id directly in the structured log — SC-006 / FR-005 redaction
    http_status=resp.status_code,
    duration_ms=int((time.monotonic() - start) * 1000),
)
```

`guild_id` is intentionally elided from logs (SC-006). Operators correlate via `tenant_id` + `installation_row_id` (which IS logged in the audit row).
