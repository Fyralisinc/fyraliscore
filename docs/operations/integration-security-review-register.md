# Integration Security Review Register

Owner: Security and Integrations Engineering.
Last reviewed: 2026-06-25.

This register is a production enablement gate for every source connector. A
new source may not be enabled for a customer until its row exists here, the
review checklist below is complete, and the approval artifact is linked from
the customer launch evidence folder.

## Required Review Checklist

Each integration review must confirm:

- data classes match
  [Integration data classification](integration-data-classification.md)
- customer contract, DPA, and subprocessor approval cover the source and region
- auth scopes are least-privileged and documented
- credentials, refresh tokens, webhook secrets, and service-account keys are
  stored only as `secret_ref` values
- webhook ingress verifies signatures or OIDC claims before ingesting payloads
- source payload minimization, redaction, retention, export, and deletion rules
  are documented
- logs, metrics, traces, support bundles, and audit metadata cannot leak raw
  payloads, prompts, credentials, URLs, object keys, or token-like values
- source API retries, rate limits, circuit breakers, and failure isolation are
  implemented or explicitly feature-gated
- uninstall disables watches/subscriptions, pauses work, and removes secret
  material where the source supports revocation
- tests cover install validation, auth failure, signature/OIDC rejection,
  secret-ref usage, safe errors, and uninstall cleanup for the source family

## Register

Status values:

- `required`: review is mandatory before any production enablement
- `approved`: review artifact exists and production enablement may proceed
- `blocked`: source is not eligible for production until listed controls are
  implemented

| Source | Risk tier | Status | Required review focus |
| --- | --- | --- | --- |
| `ashby` | High | required | Recruiting PII/HR minimization, candidate note redaction, OAuth/token storage, deletion/export coverage. |
| `aws` | High | required | Least-privileged role scopes, CloudTrail/config redaction, account topology masking, infrastructure access policy. |
| `brex` | High | required | Finance-role access, card/account masking, transaction retention, provider rate budgets. |
| `carta` | High | required | Cap-table and stakeholder confidentiality, legal approval, finance/leadership access policy, export controls. |
| `deel` | High | required | Worker/contract minimization, compensation handling, HR/finance authorization, secret revocation. |
| `discord` | Standard | required | Guild/channel scoping, bot token handling, event signature/authorization, uninstall cleanup. |
| `figma` | High | required | Design/IP scoping, comment/file minimization, binary fetch controls, guest/user PII handling. |
| `fireflies` | High | required | Transcript opt-in, summary-only defaults, meeting participant PII, retention and deletion behavior. |
| `github` | High | required | Repository scoping, webhook signature verification, code/secret redaction, installation revocation. |
| `gmail` | High | required | OAuth/DWD scope review, mailbox consent, body minimization, watch renewal and deletion cleanup. |
| `google_calendar` | High | required | Attendee/location/description minimization, calendar scoping, watch lifecycle, OIDC/webhook controls. |
| `google_drive` | High | required | File/folder scoping, object-store retention, binary fetch opt-in, permission drift and uninstall cleanup. |
| `grafana` | High | required | Alert/dashboard sensitivity, infra-role access, API token rotation, incident payload redaction. |
| `gusto` | High | blocked | SSN/bank-field exclusion proof, payroll minimization, HR/DPA approval, finance access segregation. |
| `hibob` | High | required | Employee lifecycle minimization, HR authorization, service-user scope review, retention policy. |
| `jira` | Standard | required | Project/site scoping, webhook signature verification, issue/comment secret redaction, uninstall cleanup. |
| `linkedin` | High | required | Organization/account consent, profile/people PII minimization, anti-scraping compliance, token revocation. |
| `mercury` | High | required | Bank/account masking, finance access, transaction retention, API token rotation. |
| `miro` | High | required | Board/image/comment confidentiality, board scoping, binary/attachment controls, guest PII handling. |
| `notion` | High | required | Workspace/page/database scoping, body minimization, page deletion/access revocation behavior. |
| `quickbooks` | High | required | Accounting/finance access, tax/bank identifier masking, refresh-token security, retention controls. |
| `ramp` | High | required | Card/account masking, finance access, spend/vendor retention, provider rate budgets. |
| `signal` | High | blocked | Explicit customer/legal approval, decrypted content handling, session secret storage, attachment metadata-only default. |
| `slack` | High | required | Channel/user scoping, signature verification, bot/signing secret refs, retention and uninstall cleanup. |
| `telegram` | High | blocked | MTProto session security, consent model, decrypted message sensitivity, attachment metadata-only default. |

## Production Enablement Rule

Production enablement for a source requires:

1. Register row status is `approved`.
2. Approval artifact is linked from the customer launch evidence folder.
3. Data classification and subprocessor entries are present.
4. Source install path verifies credentials before writing install rows.
5. Secrets are stored as opaque refs and can be rotated/revoked.
6. Source-specific safe-error, redaction, and uninstall tests pass in CI.

Sources with status `required` may run only in development, staging, or
customer-approved pilot environments behind feature flags. Sources with status
`blocked` must not be production-enabled until the blocking review items are
closed.
