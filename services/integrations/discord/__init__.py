"""services.integrations.discord — Discord-specific OAuth, uninstall,
slash-command registration, and outbound REST client (Interactions
HTTP scope; Gateway WebSocket deferred to IN-12).

Sibling of services.integrations.slack with the same shape but three
load-bearing distinctions:

* Signatures use Ed25519 (not HMAC-SHA256) — verified via PyNaCl in
  services.webhooks.signatures.discord. Per-installation public key
  stored in encrypted_secrets with label `discord_public_key:<gid>`,
  env-var WEBHOOK_SECRET_DISCORD as the app-level fallback for the
  PING handshake (which precedes any provider_installations row).
* OAuth scopes are `applications.commands+bot` (not `chat.*`).
* Uninstall has no webhook event. Detection is the outbound-401
  chokepoint in client.py → uninstall._disable_and_zeroize_discord.

See specs/IN-09-discord-interactions-integration/ for the full spec.

Reuses IN-08 substrate verbatim (lib/shared/secrets, encrypted_secrets,
oauth_install_states, installation_audit_log, provider_installations)
— zero new migrations.
"""
