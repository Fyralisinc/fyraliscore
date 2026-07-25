# Fyralis Local BYOC Rehearsal Chart

This chart is the zero-spend rehearsal package for the Fyralis BYOC data plane.
It runs the core customer-cloud shape on a local Kubernetes cluster with bundled
Postgres/pgvector, Kafka, MinIO, Redis, gateway, migration/topic/bucket jobs,
and the default ingestion worker set.

It does not create AWS resources and it does not require source secret values.

## Runbook

Generate local BYOC artifacts:

```bash
fyralis byoc agent local-rehearsal \
  --region us-east-1 \
  --workdir .fyralis/local-rehearsal \
  --json
```

Build and load the local app image:

```bash
docker build -t fyralis/local:dev .
kind create cluster --name fyralis-byoc
kind load docker-image fyralis/local:dev --name fyralis-byoc
```

Render and install:

```bash
helm template fyralis ./deploy/helm/fyralis \
  --namespace fyralis-system \
  -f .fyralis/local-rehearsal/provider/aws-cloudformation/helm-values.json

helm upgrade --install fyralis ./deploy/helm/fyralis \
  --namespace fyralis-system \
  --create-namespace \
  -f .fyralis/local-rehearsal/provider/aws-cloudformation/helm-values.json
```

Check the local gateway:

```bash
kubectl -n fyralis-system rollout status deployment/fyralis-gateway
kubectl -n fyralis-system port-forward svc/fyralis-gateway 8000:8000
curl http://localhost:8000/healthz
```

## Figma OAuth and durable design snapshots

The default worker set includes `oauth-poller`, `tenant-onboarding`,
`source-onboarding`, `shard-fetch`, `reconciler`, `periodic-reconciler`,
`normalizer`, and `observation-writer`. Together they turn a completed Figma
OAuth callback into a first `figma:file_snapshot` observation and continue
reconciling connected files in steady state.

The chart creates separate local MinIO buckets for raw ingestion data
(`minio.bucket`, default `fyralis-raw`) and durable design artifacts
(`minio.blobBucket`, default `fyralis-blobs`). They are exposed to the runtime
as `S3_RAW_BUCKET` and `S3_BLOB_BUCKET` respectively.

Figma is disabled until the deployment administrator supplies the public
customer-app settings and names a pre-created Kubernetes Secret. Do not put a
Figma client secret or OAuth state-HMAC value in Helm values or `app.extraEnv`:
that map becomes a ConfigMap. The named Secret must contain managed-secret
*references*, not the raw secret values:

```yaml
# Created and managed outside this chart.
stringData:
  FIGMA_CLIENT_SECRET_SECRET_REF: <customer-secret-manager-reference>
  OAUTH_STATE_HMAC_KEY_SECRET_REF: <customer-secret-manager-reference>
```

For example, use an uncommitted customer override file:

```yaml
app:
  # These non-secret provider settings may remain in the ConfigMap.
  extraEnv:
    SECRET_STORE_BACKEND: fernet
    MASTER_KEK_PROVIDER: aws-secrets-manager
    SECRET_PROVIDER_REGION: us-east-1
  figmaOAuth:
    enabled: true
    clientId: <customer-figma-client-id>
    redirectUri: https://gateway.customer.example/integrations/figma/oauth/callback
    uiBaseUrl: https://app.customer.example
    allowHttpLoopback: false
    scopes: current_user:read,file_metadata:read,file_content:read,file_comments:read,file_versions:read
    existingSecret: customer-figma-oauth-runtime
```

When enabled, the chart mounts that external Secret only in the gateway,
`shard-fetch`, `reconciler`, and `periodic-reconciler`, which are the
components that exchange or refresh Figma grants. Helm rejects missing public
settings, a missing external Secret name, raw OAuth entries in `app.extraEnv`,
and identical raw/artifact bucket names.

Validate the rendered configuration before installation:

```bash
helm lint ./deploy/helm/fyralis -f customer-figma-values.yaml
helm template fyralis ./deploy/helm/fyralis \
  --namespace fyralis-system \
  -f customer-figma-values.yaml >/dev/null
```

See [Figma BYOC OAuth administration](../../../docs/operations/figma-byoc-oauth-admin.md)
for the customer-owned Figma app and managed-secret setup, and
[Figma design artifacts](../../../docs/ingestion/figma-design-artifacts.md)
for safe retrieval of the complete design JSON.

## Telegram installation workers

Telegram is disabled by default. Declare one exact worker binding per active
Fyralis installation:

```yaml
telegramGateway:
  enabled: true
  installations:
    - name: executive-account
      tenantId: 11111111-1111-4111-8111-111111111111
      installationId: 22222222-2222-4222-8222-222222222222
    - name: operations-account
      tenantId: 33333333-3333-4333-8333-333333333333
      installationId: 44444444-4444-4444-8444-444444444444
```

The chart renders one independent Deployment for each entry. Each process
loads only the installation matching both UUIDs, resolves the Telethon session
and API hash in that tenant, writes only that installation's update state, and
holds a Redis lease keyed by both UUIDs. Session and API credentials remain in
Fyralis's tenant-scoped secret store and do not belong in Helm values.

`TELEGRAM_TENANT_ID` and `TELEGRAM_INSTALLATION_ID` are reserved and rejected
in `app.extraEnv`, because global configuration would make every worker inherit
the same binding.

## Signal installation workers

Signal is disabled by default. It depends on the unofficial signal-cli
`0.14.4.1` runtime, linked to an account controlled by the customer. Run each
daemon with its HTTP transport:

```bash
signal-cli -a <number> daemon --http 0.0.0.0:8080
```

Then declare one exact worker binding per active Fyralis installation:

```yaml
signalGateway:
  enabled: true
  signalCliVersion: "0.14.4.1"
  installations:
    - name: finance-phone
      tenantId: 11111111-1111-4111-8111-111111111111
      installationId: 22222222-2222-4222-8222-222222222222
      jsonrpcEndpoint: http://signal-cli-finance:8080/api/v1/rpc
      sseEndpoint: ""
      multiAccount: false
    - name: operations-phone
      tenantId: 33333333-3333-4333-8333-333333333333
      installationId: 44444444-4444-4444-8444-444444444444
      jsonrpcEndpoint: http://signal-cli-operations:8080/api/v1/rpc
      sseEndpoint: ""
      multiAccount: false
```

The chart renders two independent Deployments in this example. Each process
loads only the installation row matching both UUIDs, resolves its session
secret in that tenant, writes only that installation's sync state, and holds a
Redis lease keyed by both UUIDs. Linked-device state and raw credentials do not
belong in Helm values; they remain in signal-cli's protected data directory and
Fyralis's tenant-scoped secret store.

`SIGNAL_TENANT_ID`, `SIGNAL_INSTALLATION_ID`, and Signal endpoint variables are
reserved and rejected in `app.extraEnv`, because global configuration would
cause every Signal worker to inherit the same binding.
