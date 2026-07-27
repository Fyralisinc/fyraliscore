# Webhook secret binding

Webhook signing-secret scope is owned by the canonical
`WebhookIngressDefinition.secret_loader_binding`. The shared gateway resolves
that callable lazily from the webhook route contract and invokes every loader
with the same arguments:

```python
await loader(
    route_id,
    tenant_id,
    installation_row_id=installation_row_id,
    app_state=request.app.state,
)
```

The default binding is
`services.app.webhooks.secrets:load_installation_secrets`. It loads only the
resolver-selected `(installation_row_id, provider, tenant_id)` row and then
decrypts that row's `secret_ref`. It never broadens a lookup to another
installation. Its plaintext environment fallback remains development-only and
is disabled in production.

GitHub and Notion deliberately override the default:

| Route | Binding | Scope and rotation |
| --- | --- | --- |
| `github` | `services.ingest.integrations.github.webhook_secrets:load_app_webhook_secrets` | One GitHub App secret; current and `WEBHOOK_SECRET_GITHUB_PREV` overlap |
| `notion` | `services.ingest.integrations.notion.webhook_secrets:load_app_webhook_secrets` | One Notion integration verification token; current and `NOTION_WEBHOOK_VERIFICATION_TOKEN_PREV` overlap |

Notion's installation `secret_ref` is its outbound workspace bot token, so it
must not be used for inbound signature verification.

Loader resolution, execution, and return-shape validation fail closed. An
exception, non-sequence result, or element that is not a `Secret` produces a
sanitized retryable `503`; no verifier or ingestion handler runs.

When adding a webhook source, use the default exact-installation binding unless
provider evidence proves the signing secret has a different scope. A
non-default loader belongs in that source's integration package and must
preserve rotation overlap explicitly.
