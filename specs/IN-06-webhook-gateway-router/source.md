**Files relevant**

- new: services/webhooks/router.py
- new: services/webhooks/signatures/{slack,discord,github,linear,stripe}.py
- services/gateway/main.py (mount the new router)
- existing services/ingestion/handlers/slack.py (reuse verifier)

**Why it is needed**
Real webhook sources don't send Bearer tokens — they sign requests with HMAC (Slack, GitHub, Linear, Stripe), ed25519 (Discord), or arrive via SNS (SES). The current `/ingest/{channel}` endpoint REQUIRES Bearer auth and is structurally incapable of accepting real webhooks. Cannot ship until this is fixed.

**How can it be done**

1. New router `/webhooks/{provider}/{path:path}` — NOT under `/ingest/`, NOT bearer-protected
2. Per-provider signature verifier modules:
   - Slack: HMAC v0 over `v0:{ts}:{body}`, `X-Slack-Signature` header (already exists)
   - GitHub: HMAC SHA-256 over body, `X-Hub-Signature-256` header
   - Linear: HMAC SHA-256 over body, `Linear-Signature` header
   - Discord: ed25519 over `X-Signature-Timestamp + body`, `X-Signature-Ed25519` header (use pynacl)
   - Stripe: HMAC SHA-256 over `t={ts}.{body}`, `Stripe-Signature` header
3. Each verifier:
   - Has its own secret from env / secrets manager
   - Enforces a replay-protection window (300s typical, configurable)
   - Uses constant-time compare (hmac.compare_digest)
   - Returns structured errors `{verifier, reason}`
4. Failed verification → 401 + emit `ingest_sig_invalid_total{provider}` metric
5. Add `pynacl` to pyproject.toml for Discord

**Acceptance criteria**

- End-to-end test from real Slack workspace works
- Spoofed signature returns 401
- Rotated secret cleanly cuts over (test secret swap)
- All 5 providers covered with unit tests against vendor sample payloads

**Estimated effort:** 5 days
