# Instagram Direct Messages

Instagram is a Kafka-first source for Direct Messages to a connected
professional account. It uses Meta's Instagram Login / Business Login flow;
the connected account must be a professional Business or Creator account that
is eligible for the Messaging API.

## Data Path

```text
Meta webhook or Conversations API
  -> gateway raw S3 shadow write + RawEnvelope on ingestion.raw.instagram
  -> normalizer (instagram:message)
  -> observation writer
  -> observations + Think T1 trigger
```

When the Kafka path is disabled for a tenant, the verified webhook takes the
same `instagram:message` handler inline. It remains available during a Kafka
or raw-tier incident, but is not the steady-state route.

History and recovery use the Conversations API. A connected account first
discovers its active conversations, then creates a shard for each Meta
conversation ID. The default history window is 90 days and is controlled by
`history_lookback_days` on the installation. Poll reconciliation re-discovers
recent conversations and re-walks each tail, so missed webhooks and outbound
business replies land with the same observation external ID as their live
copy.

Meta may limit access to inactive request-folder conversations. Fyralis stores
the coverage limitation rather than manufacturing a completeness guarantee.

## Deployment Configuration

The Fyralis deployment, not a tenant installation row, owns the Meta app:

| Variable | Purpose |
| --- | --- |
| `INSTAGRAM_APP_ID` | Meta app ID used for OAuth. |
| `INSTAGRAM_APP_SECRET_REF` | Production secret-store reference for the Meta app secret. |
| `INSTAGRAM_VERIFY_TOKEN_REF` | Production secret-store reference for the webhook verification token. |
| `INSTAGRAM_OAUTH_REDIRECT_URI` | Exact Meta OAuth callback URL. |
| `INSTAGRAM_API_BASE_URL` | Optional API host override; defaults to `https://graph.instagram.com`. |
| `INSTAGRAM_GRAPH_VERSION` | Optional Graph version override; defaults to `v24.0`. |
| `INSTAGRAM_LOGIN_SCOPES` | Optional comma-separated scopes; defaults include messaging access. |
| `INSTAGRAM_WEBHOOK_FIELDS` | Optional subscription fields for DMs, reactions, referrals, postbacks, and seen events. |

For local development only, plaintext `INSTAGRAM_APP_SECRET` and
`INSTAGRAM_VERIFY_TOKEN` are accepted by the shared secret contract. Production
must use `*_REF` values. Do not place a tenant's Meta access token or DM text
in environment variables or logs.

The tenant-admin install endpoint is `GET /integrations/instagram/install`.
It redirects to Meta, validates the account, subscribes the app, encrypts the
user token in the tenant secret store, persists the account route, and emits
the normal source-onboarding trigger. `POST /integrations/instagram/disconnect`
revokes Fyralis-side access by disabling its installation and webhook route and
deleting the stored token.

## Webhook Configuration

Configure this callback in the Meta app:

```text
https://<fyralis-host>/integrations/instagram/webhook
```

The gateway verifies `X-Hub-Signature-256` before tenant routing. Unknown or
disabled account routes acknowledge with `200` to avoid Meta retry storms;
known installs with an invalid signature are rejected with `401`.

Meta can use an OAuth delivery identifier that differs from the canonical Graph
account ID. Fyralis persists the OAuth `user_id` as a route alias when Meta
returns it. For an older installation without that field, the first validly
signed delivery performs a bounded identity check with the installed account's
token before binding the alias; the delivery is never routed solely by an
unverified identifier.

## Identity Policy

The connected business account maps to a tenant-owned `ai_agent` actor under
`instagram:business:<ig_business_account_id>`. A DM customer uses the scoped
reference `instagram:<ig_business_account_id>:user:<instagram_scoped_user_id>`.
Customer refs never auto-merge with employee actors and do not create actor
clarifications by default. Fyralis keeps a tenant-scoped `instagram_contacts`
record, marks a repeated inbound customer as a candidate after three messages
(configurable by `INSTAGRAM_CUSTOMER_PROMOTION_MIN_MESSAGES`), and continues
to reason over the observation content even while the customer is unresolved.

## Operations

Use the dedicated source lifecycle tool to pause, resume, inspect, or uninstall
an installation. Pausing and uninstalling also disable its
`instagram_webhook_routes` entry, preventing a disconnected tenant from
receiving live events. The only tenant secret that lifecycle operations remove
is the Instagram access token; deployment-owned Meta app credentials remain
outside tenant data.

```bash
python scripts/manage_dedicated_source_installations.py \
  --dsn "$DATABASE_URL" pause \
  --tenant-id <tenant-id> --operator-actor <actor-id> \
  --source instagram --scope-id <ig-business-account-id> --reason maintenance
```

For release, apply migrations `0187` and `0188`, provision Kafka topics from
the `SourceLiteral` registry, then deploy the gateway, normalizer,
observation-writer, Think worker, source-onboarding, shard-fetch, reconciler,
and periodic-reconciler services.
