# Quickstart: GitHub Production Integration

End-to-end operator and developer flow for IN-13. This document is the runbook for register-the-App, install, observe-a-delivery, uninstall.

## Operator: One-Time App Registration

1. Sign in to GitHub as the org/user that will own the Fyralis App.
2. Go to **Settings → Developer settings → GitHub Apps → New GitHub App**.
3. Fill in the form:
   - **GitHub App name**: `Fyralis` (or env-suffixed like `Fyralis (Staging)`)
   - **Homepage URL**: `https://<your-fyralis-domain>/`
   - **Callback URL**: `https://<your-fyralis-domain>/integrations/github/callback`
   - **Webhook URL**: `https://<your-fyralis-domain>/webhooks/github/events`
   - **Webhook secret**: generate a long random string (≥ 32 bytes). Save it — you'll set it in Fyralis env in step 5. This is the **App-level webhook secret** (Clarifications Q1) and is SHARED across every customer installation.
4. Set **Repository permissions**:
   - Contents: Read
   - Issues: Read
   - Pull requests: Read
   - Metadata: Read (required)
   - Checks: Read
   - Commit statuses: Read
5. Set **Subscribe to events**:
   - Pull request, Push, Issues, Issue comment, Pull request review, Check run, Installation, Installation repositories
6. **Where can this GitHub App be installed?** Any account.
7. Click **Create GitHub App**.
8. On the resulting App page:
   - Note the **App ID** (numeric).
   - Note the **App slug** (the URL-safe name, e.g. `fyralis`).
   - Generate a **Private key** (click "Generate a private key" → downloads a `.pem` file).
9. Set the following env vars in your Fyralis deployment (`.env` or container env):
   ```bash
   GITHUB_APP_ID=123456
   GITHUB_APP_SLUG=fyralis
   GITHUB_APP_PRIVATE_KEY="$(cat /path/to/downloaded.pem)"
   # OR (file-mount variant):
   # GITHUB_APP_PRIVATE_KEY_PATH=/run/secrets/github_app_private_key.pem

   # App-level webhook secret (the value from step 3):
   GITHUB_WEBHOOK_SECRET=<the random string from step 3>
   WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1  # required under FYRALIS_ENV=prod for env-fallback path
   # OR (DB-backed variant, preferred for prod):
   # GITHUB_APP_WEBHOOK_SECRET_REF=github_app:webhook_secret   # then store the secret value via lib/shared/secrets admin CLI
   ```
10. Restart the gateway and worker processes. The startup invariant check (`_assert_prod_safety_invariants`) verifies that at least one of `GITHUB_APP_WEBHOOK_SECRET_REF` OR (`GITHUB_WEBHOOK_SECRET` + `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`) is set; it fails fast otherwise.

11. Verify the webhook endpoint by clicking **Redeliver** on the auto-generated **Ping** event in GitHub's App page → **Advanced** tab. Expect HTTP 200 in GitHub's UI and a structured log line in Fyralis: `webhook_ping_handled provider=github`.

## Customer: Self-Serve Install

Once the operator step is complete, a GitHub org admin installs the App without any operator action:

1. From Fyralis UI (or directly): `GET /integrations/github/install` while authenticated as a Fyralis tenant.
2. Fyralis 302-redirects to `https://github.com/apps/<app_slug>/installations/new?state=<signed-token>`.
3. GitHub shows the App-install consent screen: the admin picks **All repositories** OR a specific subset.
4. The admin clicks **Install**. GitHub redirects to `https://<your-fyralis-domain>/integrations/github/callback?installation_id=<id>&setup_action=install&state=<token>`.
5. Fyralis:
   - Verifies the state token (HMAC + atomic single-use nonce consume per provider).
   - UPSERTs the `provider_installations` row.
   - Mints an installation access token (App-JWT → POST `/app/installations/<id>/access_tokens`).
   - GETs `/installation/repositories` to seed `selected_repositories` (NULL if admin granted "all").
   - Writes an `installation_audit_log` row with `action='install', status='ok'`.
   - 302-redirects to `/integrations/github/installed?installation=<short-hash>`.
6. Total wall time: ≤ 2 s (test budget; live wall time depends on GitHub round-trip latency).

## Customer: First Webhook Delivery

After install, the customer creates any tracked event in a selected repository:

1. Customer opens a Pull Request in `org/a` (one of the selected repos).
2. GitHub signs the `pull_request.opened` payload with the App-level webhook secret and POSTs it to `https://<your-fyralis-domain>/webhooks/github/events` with headers:
   - `X-Hub-Signature-256: sha256=<hex>`
   - `X-GitHub-Event: pull_request`
   - `X-GitHub-Delivery: <uuid>`
3. Fyralis processes the delivery:
   - Verifies the HMAC-SHA256 signature against the App secret (rotation overlap supported via 2-element secrets list).
   - Replay-cache lookup on `(installation_id, X-GitHub-Delivery)` — first-time delivery, MISS.
   - Tenant resolution: `installation.id → tenant_id` via `TenantResolver`.
   - Repo-filter check: `payload.repository.full_name='org/a'` is in `selected_repositories=['org/a','org/b']` → proceed.
   - Calls `services/ingestion/handlers/github.py` to shape the event into an Observation under the tenant.
4. Returns HTTP 201 with `{observation_id, deduped, secret_label, ...}` body. GitHub UI shows the delivery as **succeeded**.
5. Downstream: the existing `post_commit_worker` consumes the `trigger_queue` row produced by `ingest()` and runs the per-event reasoning pipeline asynchronously.

## Customer: Uninstall

The admin removes the App from their org (Settings → Applications → Installed GitHub Apps → Fyralis → Uninstall):

1. GitHub fires `installation.action=deleted` to Fyralis's webhook URL.
2. Fyralis verifies signature, dispatches to `services/integrations/github/lifecycle.py`, which calls `_disable_installation_github(reason='installation_deleted_webhook')`:
   - `UPDATE provider_installations SET enabled=FALSE WHERE installation_id=...`
   - Invalidates the cached installation access token (if any)
   - Writes `installation_audit_log` row `action='uninstall', status='ok'`
   - **Does NOT touch the App-level webhook secret** (it's shared with every other tenant)
3. Next webhook delivery from that `installation_id` → 401 `unknown_installation` (the tenant resolver collapses disabled and never-registered into the same outcome).

If the inbound webhook is missed (network jitter, our deploy cutover), the next outbound call (e.g., `mint_installation_token` for a different feature) will return 401 `Bad credentials` or 404, and the outbound chokepoint converges on the same `_disable_installation_github` function with `reason='outbound_401_or_404_chokepoint'`. Two audit rows are an accepted property of the lock-free design.

## Operator: Webhook Secret Rotation

To rotate the App-level webhook secret without dropping in-flight deliveries:

1. Set Fyralis env: `GITHUB_APP_WEBHOOK_SECRET_PREV_REF=<current secret ref>` and `GITHUB_APP_WEBHOOK_SECRET_REF=<new secret ref>`. Deploy. Both old and new secrets are loaded by the verifier (multi-secret iteration in `GitHubVerifier`).
2. In GitHub's App developer settings, update the webhook secret to the new value.
3. Wait ≥ 5 minutes for in-flight deliveries (GitHub's retry window) to drain.
4. Set Fyralis env: `GITHUB_APP_WEBHOOK_SECRET_PREV_REF=` (unset). Deploy. Now only the new secret verifies.

## Developer: Running the Tests

```bash
# Real Postgres + Ollama required (Constitution §IV)
DOCKER_COMPOSE up -d postgres ollama
pytest -m integration services/integrations/tests/test_*_github.py
pytest -m integration services/webhooks/tests/test_verifier_github.py

# Schema drift check (after touching migrations)
python scripts/check_schema_drift.py

# Regression: ensure IN-08 / IN-09 are untouched
pytest -m integration services/integrations/tests/test_*_slack.py
pytest -m integration services/integrations/tests/test_*_discord.py
```

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Webhook deliveries 401 `signature_mismatch` for ALL installations | App-level secret mismatch between GitHub config and Fyralis env | Verify `GITHUB_WEBHOOK_SECRET` (or the secret-store ref) matches GitHub's App config. Rotate carefully per the rotation runbook above. |
| Webhook deliveries 401 `unknown_installation` | Installation row disabled or never created | Check `provider_installations.enabled` for the `installation_id`. If FALSE, the customer can re-install; if absent, the OAuth flow never completed. |
| Webhook deliveries 200 `handled='filtered_repo'` for repos the customer expects to ingest | `selected_repositories` allowlist doesn't include the repo | Customer must either re-install with "all repos" selected, OR add the specific repo via GitHub's App-settings UI (which fires `installation_repositories.added` and updates Fyralis transparently). |
| OAuth callback 302 to `install-error?reason=installation_collision` | The `installation_id` was previously associated with a different tenant | Operator audit-log review; admin must uninstall from GitHub and the operator must verify no `provider_installations` row remains for that installation_id under another tenant. |
| Outbound `_disable_installation_github` firing repeatedly for an active installation | Stale cached installation token + clock skew between Fyralis pod and GitHub | Restart the pod (clears the in-process cache); investigate clock sync. |
| `GET /installation/repositories` returns >90 repos | Pagination cap (R8) | Audit-log row carries `selected_repositories_truncated=true, total_available=<N>`. Customer can re-install with "all repos" if explicit selection of >90 is not workable. |
