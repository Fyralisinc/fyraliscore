# Webhook tenant-resolver vendor-sample fixtures

These JSON files are recorded request bodies (or synthesized
representatives) from each launch provider, used by the
`test_tenant_resolver_extract.py` and `test_tenant_resolver_lookup.py`
test modules.

| File | Provider | Carries |
|---|---|---|
| `slack_event_callback.json` | Slack | `team_id` at root |
| `github_webhook.json` | GitHub | `installation.id` (numeric, stringified by extractor) |
| `linear_webhook.json` | Linear | `organizationId` at root |
| `discord_interaction.json` | Discord | `guild_id` AND `application_id` (extractor prefers guild) |
| `discord_global_command.json` | Discord | `application_id` only (extractor falls back) |

Stripe has no body fixture: the resolver reads `Stripe-Account` from
request headers, not the JSON payload. Tests synthesize a header bag
directly.
