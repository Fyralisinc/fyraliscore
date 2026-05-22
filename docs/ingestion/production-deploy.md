# Ingestion — production deploy & first-customer checklist

This is the operator runbook for deploying the ingestion system for a
real customer using the four sources (gmail / github / slack / discord).
It covers the deploy topology, the required configuration, the
bring-up order, and the per-source real-API validation steps that the
synthetic harness cannot exercise.

> The synthetic validation (Run 1–5) proves the pipeline's *internal*
> correctness end-to-end (backfill + live, concurrent, Kafka-routed, 50
> tenants). It does **not** exercise real OAuth, real provider webhook
> signatures, or real API pagination/quotas — that is what the §4
> per-source validation below is for.

---

## 1. Topology (what runs)

`docker-compose.yml` brings up the full data plane:

| Group | Services |
|---|---|
| infra | postgres, ollama, **kafka** (KRaft), **minio** (S3), **redis** |
| one-shots | **migrate** (applies migrations), **kafka-init** (topics), **minio-init** (bucket) |
| ingress | **gateway** (webhooks, Gmail Pub/Sub push, OAuth callbacks) |
| backfill loops | oauth_poller, tenant_onboarding, source_onboarding, shard_fetch, reconciler |
| consumer chain | normalizer, observation_writer, dlq_writer, embedding_worker, **embedding_backlog** |
| live workers | discord_gateway_worker, gmail_watch_scheduler, gmail_history_poller |
| app | think_worker, post_commit_worker, ui, nginx, acme |

Embedding has **two fill paths** and both run: `embedding_worker` drains
the `ingestion.embedding` topic (fed by the Kafka writer — backfill, and
live when the per-tenant `kafka_path_enabled` cutover is on), while
`embedding_backlog` scans `embedding_pending=TRUE` directly (the
catch-all for inline-path live observations and any lost topic message;
rate-limited via `redis`). Both are backend-agnostic via
`EMBEDDER_BACKEND` (ollama default, openai opt-in).

All app/workers gate on `migrate` + `kafka-init` + `minio-init`
(`service_completed_successfully`), so migrations + topics + the raw
bucket exist before anything produces or consumes.

**Managed Kafka / S3:** drop the `kafka` / `minio` / `kafka-init` /
`minio-init` services and point `KAFKA_BOOTSTRAP_SERVERS` + `S3_*` at the
managed endpoints. Provision the topics with
`python scripts/provision_kafka_topics.py` (or your platform's IaC) —
`ingestion.raw`, `ingestion.normalized`, `ingestion.embedding`,
`ingestion.dlq`, `ingestion.tenant_traffic_signal`.

---

## 2. Configuration

1. Copy `.env.production.example` → `.env.production` on the host and
   fill it in. Required before first boot:
   - `COMPANY_OS_ENV=prod` **and** `FYRALIS_ENV=prod` (both — the
     secret-store / webhook / OAuth guards key on either via
     `lib.shared.env.is_prod`).
   - `MASTER_KEK` — a stable 32-byte url-safe-base64 Fernet key. Generate
     **once**; store in your secret manager. If it is missing in prod the
     gateway refuses to boot (rather than minting an ephemeral key and
     making every stored secret unrecoverable after restart).
   - `OAUTH_STATE_HMAC_KEY`, `AUTH_BOOTSTRAP_SECRET`.
   - Per-source app credentials + webhook secrets (see §4).
2. `GATEWAY_MOUNT_SIM=0` (the gateway also hard-refuses to mount the
   synthetic `/simulation/*` injection router whenever `is_prod()`).
3. `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW` must be unset/0 in prod.
4. `WEBHOOK_TENANT_DEFAULT_ALLOW` must be unset/0 — webhooks resolve the
   tenant from the installation, never a hardcoded default.

---

## 3. Bring-up

```bash
# On the deploy host, in the repo root, with .env.production present:
docker compose up -d --build --remove-orphans
# migrate / kafka-init / minio-init run to completion first; then the
# gateway + workers start. Verify:
docker compose ps
curl -sf http://localhost:8000/healthz
```

Migrations apply automatically via the `migrate` one-shot. The deploy
workflow (`.github/workflows/deploy-production.yml`) does this on push to
`production`.

**Health:** each Kafka consumer exposes `/healthz` + `/metrics` on
`:9300` inside its container (compose has a healthcheck that restarts a
consumer whose loop wedges). Scrape `/metrics` for throughput / lag /
DLQ-publish counters if you run Prometheus.

**Alerts:** set `INGESTION_ALERT_WEBHOOK_URL` to receive a POST when the
per-tenant cutover circuit breaker trips.

---

## 4. Per-source real-API validation

Do this once per source in a sandbox/staging environment (or carefully
against the customer's real workspace) **before** declaring a source
live. Each follows the same arc: register the app → configure secrets →
OAuth install → confirm backfill produced observations → confirm a live
event lands → confirm dedup.

### 4.1 GitHub
1. Create a GitHub App: set the webhook URL to
   `https://<host>/webhooks/github`, a strong webhook secret, and
   permissions (issues, pull requests, metadata read). Note the App ID,
   slug, and generate a private key (PEM).
2. Config: `GITHUB_APP_ID`, `GITHUB_APP_SLUG`,
   `GITHUB_APP_PRIVATE_KEY[_PATH]`, `WEBHOOK_SECRET_GITHUB`.
3. Install the App on the customer's org/repos. Confirm the OAuth
   callback wrote a `provider_installations` row and an
   `onboarding_triggers` row.
4. Backfill: confirm shards were planned for every accessible repo
   (org-wide grants now enumerate fully; >90 repos are no longer
   truncated — raise `GITHUB_MAX_BACKFILL_REPOS` only to bound it
   deliberately). Confirm `observations` rows appear with
   `source_channel='github:webhook'`.
5. Live: open an issue/PR; confirm the webhook is signature-verified
   (a forged signature → 401) and the event lands as one observation,
   deduped against any backfilled twin.

### 4.2 Slack
1. Create a Slack app: enable Events API → request URL
   `https://<host>/webhooks/slack`, add the signing secret, scopes
   (`channels:history`, `channels:read`, `groups:read` if you set
   `SLACK_BACKFILL_INCLUDE_PRIVATE=1`).
2. Config: `SLACK_CLIENT_ID/SECRET/REDIRECT_URI`, `WEBHOOK_SECRET_SLACK`.
3. OAuth-install to the workspace; confirm the bot token is stored
   encrypted (Fernet) and the install row + trigger landed.
4. Backfill: confirm all channels enumerate (cursor-paginated; >1000 no
   longer truncated) and history produces observations.
5. Live: post a message; confirm signature + 300s replay window enforced
   and the message lands deduped.

### 4.3 Discord
1. Create a Discord app + bot. Set the interactions endpoint to
   `https://<host>/webhooks/discord` (Ed25519 public key), enable the
   gateway intents for message content.
2. Config: `DISCORD_BOT_TOKEN`, `DISCORD_APPLICATION_ID`,
   `DISCORD_CLIENT_ID/SECRET/REDIRECT_URI`, `WEBHOOK_SECRET_DISCORD`,
   and **`PYTHONHASHSEED=0`** (defence-in-depth; the planner now uses a
   stable SHA-256 sample seed regardless).
3. Invite the bot; confirm `discord_gateway_worker` connects (bot-token
   authenticated WSS) and resolves guild→tenant.
4. Backfill: confirm guilds enumerate (paginated >200) and the 5%
   channel sample is stable across restarts.
5. Live: post in a guild text channel; confirm the gateway MESSAGE_CREATE
   lands as an observation.

### 4.4 Gmail
1. Create a GCP service account with domain-wide delegation; authorize
   the client ID + scopes in the customer's Google Admin console. Create
   a Pub/Sub topic + push subscription to
   `https://<host>/webhooks/gmail/pubsub` with OIDC.
2. Config: `GMAIL_SERVICE_ACCOUNT_JSON_FILE`, `GMAIL_PUBSUB_PROJECT_ID`,
   `GMAIL_PUBSUB_PUSH_ENDPOINT`, `GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE`,
   `GMAIL_PUBSUB_PUSH_OIDC_SA`. (The Gmail routers mount only when the SA
   JSON is configured.)
3. Run the connect wizard (`/connect/preflight` → `/connect/finalize`);
   confirm `gmail_installations` + watches provisioned.
4. Backfill: confirm history drains to observations (`gmail:` channel).
5. Live: send a test email; confirm the Pub/Sub push is OIDC-verified
   (forged token → 401) and the message lands deduped. Confirm
   `gmail_watch_scheduler` renews the watch (watches expire ~7 days).

---

## 5. Known limitations (operate around these)

- **Steady-state completeness relies on live delivery.** Backfill runs a
  one-shot reconcile at the end; there is no periodic re-reconcile loop
  for github/slack/discord. If a webhook/gateway event is missed after
  onboarding, nothing re-fetches it. Gmail is the exception (durable
  `history_id` watermark + poller safety net). Mitigation: monitor
  provider webhook delivery dashboards; re-run onboarding to backfill a
  gap. A periodic reconcile is future work.
- **Per-shard gap checks are best-effort.** A transient error during the
  end-of-backfill gap check is treated as "no gap"; a real gap could be
  missed. Re-running onboarding re-checks.
- **Single-broker Kafka (RF=1).** The default compose runs one KRaft
  broker (`acks=all` guarantees only the lone leader's log). For
  durability use managed Kafka with RF≥3 / min-ISR≥2.
- **MinIO default credentials.** The single-box default uses fixed
  internal MinIO creds (not host-exposed). For real S3, set
  `S3_ENDPOINT_URL=""` + real IAM creds and drop the minio services.
- **DLQ depth.** `dlq_writer` lands failures in `ingestion_failures`.
  Query depth with
  `SELECT failure_kind, count(*) FROM ingestion_failures GROUP BY 1;`
  and alert on growth. (`/metrics` exposes DLQ-publish throughput.)
