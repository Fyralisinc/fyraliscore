# Subprocessor And Third-Party Data Flow

Owner: Security and Platform Engineering.
Last reviewed: 2026-06-25.

This document defines the external services Fyralis may call in production and
what data can cross each boundary. It is an operational control, not a vendor
contract. Legal/DPA records must map the customer-approved vendor list to this
technical inventory before production enablement.

## Data Boundary Rules

- Raw source payloads, prompts, completions, embeddings inputs, object keys, and
  credentials are customer data unless explicitly classified otherwise.
- Support bundles and operator audit rows must contain bounded counts, states,
  IDs, timestamps, and non-secret configuration only.
- `Generated reasoning` inherits the highest sensitivity of the source evidence
  used to create it.
- External calls must use configured timeouts, bounded retries, circuit breakers
  where available, and tenant/cost budgets where applicable.
- Production configuration must reference managed secret providers for wrapping
  keys and runtime credentials; raw secret values must not be committed or
  stored in production env templates.

## External Data Flow Matrix

| Boundary | Code path | Data sent out | Data received | Production controls |
| --- | --- | --- | --- | --- |
| Codex/OpenAI Responses API for Think and question planning | `lib.llm.provider`, `services.platform.execution.question_planning_provider` | Prompt text, structured context, retrieved evidence snippets, schema instructions, tenant/product metadata needed for reasoning | Structured JSON outputs, usage metadata, error/latency signals | `LLM_PROVIDER=codex`, strict config, request/token/spend budgets, timeout/retry limits, safe error handling, no raw prompt/completion logs. |
| OpenAI Embeddings API, optional | `lib.embeddings.openai_backend` | Text to embed, model/dimension parameters | Embedding vectors and provider status/errors | Disabled by default in production template in favor of local Ollama; when enabled, use API key from secret manager, breaker/retry limits, concurrency budget, no text logging. |
| Ollama embedding service, local/private | `lib.embeddings.ollama` | Text to embed within the deployment network | Embedding vectors and local service status/errors | Preferred production template backend; keep service inside the customer/control boundary, enforce dimension checks, breaker/retry limits, bounded concurrency. |
| Object storage for raw tier | `services.ingest.ingestion.raw_tier.s3` | Raw compressed source bodies and metadata/tags including content hash, data class, retention days | Raw bytes for normalizer, replay, and recovery | Use customer-approved S3/compatible bucket, encryption at rest, IAM-scoped access, retention tags, content-addressed idempotent writes, no object keys in support bundles. |
| Managed secret provider | `lib.shared.secrets.provider_contract`, `lib.shared.secrets` | Secret reference names and auth to provider | Master wrapping key or equivalent key material | Production forbids `MASTER_KEK_PROVIDER=env`; supported providers are AWS Secrets Manager, GCP Secret Manager, and HashiCorp Vault; source credentials remain in `encrypted_secrets` behind opaque refs. |
| Source provider APIs and webhooks | `services.ingest.integrations.*`, `services.app.webhooks.*` | OAuth/client credentials, source API requests, webhook registration calls, cursor/watch renewal calls | Customer-authorized source data, webhook payloads, rate-limit responses | Least-privileged scopes, signature/OIDC verification where applicable, source rate budgets, per-source DLQs, tenant-scoped install rows, pause/resume/uninstall/rotation operator controls. |
| GitHub Actions and container/security scanners | `.github/workflows/*` | Repository source, build context, dependency metadata, container image/SBOM artifacts | CI results, vulnerability findings, signed artifact attestations | Runs before merge/deploy, does not receive customer runtime data, verifies signed SBOM/checksum artifacts before deploy. |
| Observability backends | `observability/*`, `lib.observability`, gateway/worker metrics | Bounded metrics, counters, histograms, labels, health states | Dashboards, alerts, burn-rate signals | Metrics labels are bounded and privacy-safe; no raw payloads, prompts, completions, object keys, tokens, or PII in metrics. |

## Source Provider Scope

Source-provider subprocessors include the APIs listed in
[Integration data classification](integration-data-classification.md). Their
data classes determine whether additional customer approvals are required:

- `Financial`, `HR`, `Security`, and decrypted end-to-end communications require
  explicit customer approval and access-policy review before enablement.
- Sources that expose raw files, comments, messages, transcripts, or issue text
  must be treated as `PII` and `IP` capable even when the nominal source is
  operational.
- Webhook sources must verify signatures or provider identity before tenant
  resolution proceeds.

## Prohibited Flows

- Raw source payloads to CI, support bundle exports, operator audit metadata, or
  public logs.
- Prompt/completion text in application logs or metrics.
- Production wrapping keys, provider client secrets, webhook HMAC secrets, or
  bearer tokens in committed env files.
- Object-store keys or raw DLQ payloads in first-line support outputs.
- Embedding/LLM provider calls from inside database transactions.

## Review Checklist

Before enabling a new external boundary:

1. Add or update the row in this document.
2. Add or update the source row in
   [Integration data classification](integration-data-classification.md).
3. Verify env-contract coverage for any new secret/config key.
4. Verify logs, metrics, audit metadata, and support bundles cannot include raw
   payloads or credentials.
5. Record legal/DPA approval in the customer launch evidence folder.
