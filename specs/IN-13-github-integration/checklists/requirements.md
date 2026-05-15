# Specification Quality Checklist: GitHub Production Integration

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-05-15
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs)  
      _Caveat: spec references concrete file paths (e.g. `services/integrations/github/`) and existing protocol constants (HMAC-SHA256, RS256, `X-Hub-Signature-256`). This is consistent with the IN-08/IN-09 spec house style in this repo, which treats the existing module layout and the GitHub webhook protocol surface as the contract. Pure business-stakeholder language would lose actionable signal for the plan phase._
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders (within the IN-NN house style above)
- [x] All mandatory sections completed (User Scenarios, Requirements, Success Criteria)

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (where the property is user-facing); protocol-facing criteria name the wire-level guarantee they validate
- [x] All acceptance scenarios are defined (US1–US7 each carry Given/When/Then scenarios)
- [x] Edge cases are identified (bootstrap PING, key rotation, suspended installs, large payloads, unsupported events, race-on-uninstall, missing delivery header)
- [x] Scope is clearly bounded (out-of-scope: user-OAuth, outbound product features, GraphQL, GHES)
- [x] Dependencies and assumptions identified (Assumptions section covers per-installation secret mechanism choice, private key handling, table reuse, single-App assumption)

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria (FR-001 through FR-022 each tie back to one or more user-story scenarios or success criteria)
- [x] User scenarios cover primary flows (install, deliver, uninstall, repo-select, replay, observe)
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification beyond the IN-NN house-style allowance noted above

## Notes

- The single open design question — "per-installation webhook secret via GitHub API vs. per-installation pepper over a single App-level secret" — is captured explicitly in Assumptions and FR-007 / Key Entities. The plan phase MUST resolve it; the spec preserves the security property regardless of which path the plan picks.
- The `selected_repositories` persistence choice (existing `metadata` JSONB column vs. new dedicated column) is similarly deferred to plan and called out in Assumptions and FR-018.
- All references to existing files have been verified against the working tree at spec time: `services/ingestion/handlers/github.py`, `services/webhooks/signatures/github.py`, `services/webhooks/router.py`, `services/webhooks/tenant_resolver.py::_extract_github`, `services/integrations/slack/oauth.py`, `services/integrations/slack/uninstall.py`, `services/integrations/discord/oauth.py`, `services/integrations/discord/uninstall.py`. The new package path `services/integrations/github/` does not yet exist and is created by this feature.
