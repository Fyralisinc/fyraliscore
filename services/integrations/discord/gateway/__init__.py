"""services/integrations/discord/gateway — IN-12 Discord Gateway WSS worker.

Persistent WebSocket client that connects to gateway.discord.gg, IDENTIFY-s
with the MESSAGE_CONTENT privileged intent, and dispatches MESSAGE_CREATE
events to the existing ingestion handler as observations with
`source_channel='discord:message'`.

Distinct from IN-09's Interactions HTTP path (services/integrations/discord/
oauth.py + client.py + commands.py):
  - IN-09 receives slash commands; this worker receives every guild message.
  - IN-09 uses HTTP webhook → Ed25519 verification; this worker uses WSS +
    bot-token IDENTIFY.
  - IN-09 emits `discord:interaction` observations; this worker emits
    `discord:message`.

The two surfaces share `provider_installations` (read-only here) and the
ingestion handler at `services/ingestion/handlers/discord.py`.

See specs/IN-12-discord-gateway-message-ingest/ for the full design.
"""
