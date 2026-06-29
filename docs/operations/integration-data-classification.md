# Integration Data Classification

Owner: Security and Integrations Engineering.
Last reviewed: 2026-06-25.

This document classifies the data Fyralis may ingest by source family. It is a
production gate: no source may be enabled for a customer until its row exists,
its controls are implemented or explicitly feature-gated, and the customer
contract covers the relevant data classes.

Classification tags:

- `PII`: names, email addresses, user IDs, comments, messages, meeting
  participants, employee/customer identifiers, or personal metadata.
- `Financial`: payments, invoices, payroll, banking/card/accounting data,
  revenue, spend, cap table, or compensation.
- `HR`: employee lifecycle, recruiting, contractor, payroll, benefits, or
  performance-adjacent data.
- `Security`: audit logs, permissions, cloud/account activity, identities, or
  incident/security event data.
- `Infrastructure`: cloud assets, deploys, metrics, logs, incidents, or system
  topology.
- `Communications`: messages, email, chat, meeting transcripts, comments, or
  collaboration text.
- `IP`: source code, design files, docs, roadmap, strategy, customer data
  embedded in files, or other proprietary business content.
- `Generated reasoning`: summaries, embeddings, extracted observations, model
  facts, recommendations, forecasts, and LLM-derived outputs created from the
  source.

## Source Matrix

| Source | Primary data classes | Required controls before production |
| --- | --- | --- |
| `ashby` | PII, HR, Communications, Generated reasoning | Minimize candidate/interview fields, redact sensitive notes from logs, enforce secret refs, scope retention to recruiting policy. |
| `aws` | Security, Infrastructure, PII, Generated reasoning | Treat raw CloudTrail/config payloads as sensitive, redact request/response fields that can contain secrets, restrict to least-privileged read roles. |
| `brex` | Financial, PII, Generated reasoning | Mask account/card identifiers to last 4, restrict finance role access, enforce provider rate budgets. |
| `carta` | Financial, PII, HR, Generated reasoning | Treat cap-table/stakeholder data as highly sensitive, restrict finance/leadership access, require customer legal approval. |
| `deel` | HR, Financial, PII, Generated reasoning | Minimize worker and contract fields, redact compensation identifiers where not needed, require HR/finance access policy. |
| `discord` | Communications, PII, Generated reasoning | Restrict guild/channel scope, store bot/session secrets by ref, honor deletion/uninstall workflows. |
| `figma` | IP, PII, Communications, Generated reasoning | Treat file/comment text and design content as confidential, scope files/projects, avoid fetching binaries unless needed. |
| `fireflies` | Communications, PII, Generated reasoning | Default to summary/action-item metadata; gate verbatim transcript ingestion behind customer opt-in and retention approval. |
| `github` | IP, Security, PII, Communications, Generated reasoning | Restrict repository selection, verify webhook signatures, avoid persisting secrets from code or issue payloads. |
| `gmail` | Communications, PII, IP, Generated reasoning | Require domain/admin consent or OAuth scope review, minimize bodies where possible, honor user/mailbox deletion policy. |
| `google_calendar` | PII, Communications, Generated reasoning | Minimize attendee/location/description fields, scope calendars, renew watches with tenant-scoped credentials. |
| `google_drive` | IP, PII, Communications, Generated reasoning | Scope folders/files, enforce object-store retention, avoid binary fetch unless explicitly enabled. |
| `grafana` | Infrastructure, Security, PII, Generated reasoning | Treat alerts/incidents/dashboard metadata as operationally sensitive, restrict to infrastructure/leadership access. |
| `gusto` | HR, Financial, PII, Generated reasoning | Never persist SSNs or raw bank/routing numbers, minimize payroll fields, require DPA/HR approval before enablement. |
| `hibob` | HR, PII, Generated reasoning | Minimize employee lifecycle fields, restrict HR/leadership access, verify service-user scopes. |
| `jira` | IP, PII, Communications, Generated reasoning | Scope projects/sites, verify webhook signatures, redact secrets from issue text/logs before support export. |
| `linkedin` | PII, Communications, HR, Generated reasoning | Scope organization/account data, avoid scraping-like behavior, require customer consent for people/profile fields. |
| `mercury` | Financial, PII, Generated reasoning | Mask routing/account identifiers, restrict finance role access, apply transaction retention limits. |
| `miro` | IP, PII, Communications, Generated reasoning | Treat boards/images/comments as confidential, store references before binaries, scope boards and guests. |
| `notion` | IP, PII, Communications, Generated reasoning | Scope workspace/pages/databases, minimize page body ingestion, honor page deletion and access revocation. |
| `quickbooks` | Financial, PII, Generated reasoning | Restrict accounting data to finance/leadership, mask bank/tax identifiers, verify refresh-token handling. |
| `ramp` | Financial, PII, Generated reasoning | Mask card/account identifiers, restrict finance role access, enforce spend/vendor retention policy. |
| `signal` | Communications, PII, IP, Generated reasoning | Treat decrypted message content as highest sensitivity, require explicit customer/legal approval, default attachments to metadata-only. |
| `slack` | Communications, PII, IP, Generated reasoning | Scope channels/users, verify signatures, store bot/signing secrets by ref, honor retention and uninstall cleanup. |
| `telegram` | Communications, PII, IP, Generated reasoning | Treat MTProto session and decrypted content as high sensitivity, require consent model and metadata-only attachment default. |

## Production Rules

- Source-specific onboarding must link to the applicable row before enablement.
- Any source carrying `Financial`, `HR`, `Security`, or decrypted
  end-to-end-message content requires explicit customer approval and access
  policy review.
- Raw payloads, object keys, prompts, completions, and credentials must not
  appear in logs, metrics, audit metadata, or support bundles.
- Derived `Generated reasoning` inherits the highest sensitivity of its source
  evidence and must follow the same tenant isolation, retention, and export
  rules.
- New ingestion flow docs must add a row here in the same change.
