# Customer Launch Evidence Folder

Owner: Security, Legal, Customer Success, and Integrations Engineering.
Last reviewed: 2026-06-25.

This folder defines the evidence pack required before a customer or source
family can be declared production-ready. It is intentionally template-only in
the repository. Real customer evidence, questionnaire answers, contracts,
DPAs, approval emails, security exports, architecture diagrams containing
customer topology, and vendor portal screenshots must not be committed here.

## Repository Policy

- Commit templates, checklists, and examples with synthetic `.example` or
  `.test` data only.
- Keep customer-specific launch folders under
  `docs/operations/customer-launch-evidence/customers/<customer-slug>/` in
  local/private storage only.
- Do not commit customer contracts, DPAs, questionnaire exports, cloud account
  IDs, diagrams with private topology, support bundle output, logs, screenshots,
  or incident notes.
- Store official legal/security evidence in the approved document repository
  named in the customer contract, and link only an internal evidence ID from
  release notes or launch reviews.

## Required Evidence

Each customer launch evidence pack must include:

- signed customer contract and DPA reference
- approved subprocessor/vendor list and region commitments
- completed security questionnaire or customer security review reference
- data classification and retention approval for enabled sources
- integration security review approval for each enabled source
- source scope, OAuth/IAM scopes, service-account scopes, and webhook/OIDC
  verification evidence
- secret-management evidence proving credentials are stored as refs only
- backup/restore, rollback, migration rehearsal, and staging smoke evidence
- observability, alert routing, and on-call ownership evidence
- launch risk acceptance and named approvers

## Customer Folder Shape

Use this shape in the private evidence repository or a local uncommitted
workspace:

```text
customers/<customer-slug>/
  README.md
  approvals/
    dpa-reference.md
    subprocessor-approval.md
    security-questionnaire.md
    risk-acceptance.md
  integrations/
    <source>/
      security-review.md
      scope-review.md
      credential-handling.md
      uninstall-plan.md
  operations/
    backup-restore.md
    migration-rehearsal.md
    rollback-rehearsal.md
    observability.md
```

Start from
[customer-launch-evidence-template.md](customer-launch-evidence-template.md)
and [integration-evidence-template.md](integration-evidence-template.md).
