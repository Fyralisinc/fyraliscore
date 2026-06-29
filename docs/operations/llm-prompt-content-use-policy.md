# LLM Prompt And Content Use Policy

Owner: Security and Reasoning Engineering.
Last reviewed: 2026-06-25.

This policy governs any Fyralis path that sends customer-derived content to an
external LLM or embedding provider. It applies to Think, Ask/query planning,
retrieval-augmented answers, summarization, extraction, forecast/recommendation
reasoning, embeddings, and any future agent/tool workflow.

This is the Fyralis engineering policy. Legal and customer-success teams must
verify provider-specific contractual terms, DPAs, retention controls, and
regional processing commitments before a provider is enabled for production.

## Default Position

- Production reasoning uses the configured provider path only after customer
  approval for the provider class and data region.
- Local/private embedding is preferred for production deployments. External
  embedding APIs are opt-in and require the same approval level as LLM prompts.
- Customer source payloads, retrieved evidence, generated prompts, completions,
  embeddings inputs, and tool-call arguments are customer data.
- Generated reasoning inherits the highest sensitivity of the source evidence
  used to create it.

## Allowed Data

Only send the minimum context required for the task:

- selected evidence snippets already authorized for the requesting actor
- bounded metadata needed for reasoning, routing, or schema validation
- stable IDs when they are needed to connect the output back to local records
- redacted or summarized content when the full payload is not required
- schema definitions and non-secret system instructions

For `Financial`, `HR`, `Security`, decrypted end-to-end communications, and
source-code/design/IP-heavy sources, prefer summarized or field-minimized
context unless a customer-approved workflow explicitly requires verbatim text.

## Prohibited Data

Never send the following to an external LLM or embedding provider:

- bearer tokens, OAuth codes, refresh tokens, webhook signatures, private keys,
  API keys, session cookies, or source credentials
- raw object-store keys, presigned URLs, bucket names that expose customer
  topology, or internal deployment secrets
- database connection strings, DSNs, tenant secret refs, or KMS/Vault references
- unrestricted raw source payloads when a smaller evidence slice is sufficient
- support-bundle internals, operator audit metadata blobs, or incident notes
  that include customer secrets
- data from a tenant, actor, source, or entity the current request is not
  authorized to access

## Required Controls

- Authorization must happen before prompt assembly. Retrieval and gateway paths
  must apply actor-scoped access checks before content reaches the provider.
- Provider calls must happen outside database transactions.
- Logs, metrics, traces, audit rows, and support bundles must not include raw prompts,
  completions, provider request bodies, provider responses, embeddings inputs, or
  token-like values.
- Provider calls must use configured timeouts, bounded retries, and circuit
  breakers or failure isolation where available.
- Think calls must respect daily per-tenant spend, token, and request budgets.
- Prompt/completion retention outside the customer boundary is not allowed
  unless covered by customer contract and provider configuration.
- Cached provider responses may be used only in test/evaluation environments or
  with explicit customer approval and bounded retention.

## Provider Enablement Checklist

Before enabling an external LLM or embedding provider for a customer:

1. Confirm the provider and region are approved by the customer contract/DPA.
2. Confirm the source data classes in
   [Integration data classification](integration-data-classification.md).
3. Confirm the data-flow row in
   [Subprocessor and third-party data flow](subprocessor-data-flow.md).
4. Confirm the production env contract does not require raw provider secrets in
   committed env files.
5. Run prompt/log/metric leak tests and safe-error tests.
6. Verify budget ceilings and provider outage behavior.
7. Record the decision in the customer launch evidence folder.

## Incident Response

If prompt, completion, embedding input, or provider payload leakage is suspected:

1. Disable the affected provider path or source workflow.
2. Preserve request IDs, tenant ID, actor ID, source, provider, model, and time
   window without copying raw leaked content into the incident ticket.
3. Rotate any possibly exposed credentials.
4. Review logs, metrics, audit rows, support bundles, provider dashboards, and
   local response caches.
5. Follow the privacy/security incident process in
   [Incident response guide](incident-response-guide.md).
