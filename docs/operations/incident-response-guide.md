# Incident Response Guide

Owner: Security and Platform Engineering.
Last reviewed: 2026-06-24.

This guide defines the production process for privacy, security, availability,
and data-integrity incidents in Fyralis core.

## Severity Levels

| Severity | Definition | Response target |
| --- | --- | --- |
| SEV-1 | Confirmed or likely cross-tenant data exposure, credential leak, destructive data corruption, or sustained customer outage | Immediate paging and incident commander assigned within 15 minutes |
| SEV-2 | Customer-visible degradation, source-wide ingestion outage, failed deploy rollback, stale backups, or provider outage affecting launch workflows | Incident commander assigned within 30 minutes |
| SEV-3 | Elevated errors, isolated source backlog, non-critical worker failure, delayed telemetry, or docs/process gap | Triage during business hours or current on-call shift |

## Roles

| Role | Responsibility |
| --- | --- |
| Incident commander | Owns timeline, severity, communication, and decisions |
| Technical lead | Owns diagnosis, mitigation, rollback/forward fix |
| Customer lead | Owns customer updates and support coordination |
| Security lead | Owns privacy/security assessment, evidence preservation, and legal escalation |
| Scribe | Records timeline, commands, decisions, and follow-up items |

## First 15 Minutes

1. Declare severity and create an incident record.
2. Assign incident commander and scribe.
3. Capture request IDs, tenant IDs, deploy SHA, alert names, and dashboards.
4. Freeze risky manual actions until an owner is assigned.
5. If customer data exposure is possible, stop affected egress paths first.
6. Preserve logs and audit rows. Do not delete data to hide symptoms.
7. Decide whether to pause source ingestion, pause a feature, roll back, or
   keep investigating.

## Privacy Or Tenant Isolation Incident

Containment order:

1. Disable the affected route, source, worker, or feature flag.
2. Stop external egress that may transmit customer data.
3. Snapshot relevant audit rows and sanitized request metadata.
4. Run tenant isolation probes and route access audit.
5. Identify affected tenants and data classes.
6. Preserve evidence for legal/security review.

Useful commands:

```bash
python scripts/audit_gateway_route_access.py --production
python scripts/check_schema_drift.py "$DATABASE_URL"
```

Do not export raw logs, prompts, object payloads, access tokens, or source data
outside the customer-approved security boundary.

## Availability Incident

Mitigation order:

1. Check deployment workflow rollback status.
2. Check `/healthz`, `/readyz`, and compose process state.
3. Inspect product SLO dashboard and worker/process health.
4. If a recent deploy caused the incident, use
   [Rollback runbook](rollback-runbook.md).
5. If a source lane is noisy, use
   [Per-source worker scaling](per-source-worker-scaling-runbook.md).
6. If DLQ grows, use
   [Ingestion DLQ replay/quarantine](ingestion-dlq-replay-quarantine-runbook.md)
   or [Durable dead-letter admin](dead-letter-admin-runbook.md).

## Credential Or Secret Incident

1. Stop affected source/API calls.
2. Rotate the provider credential in the secret provider.
3. Invalidate exposed tokens with the provider when supported.
4. Verify no raw secret appears in logs, audit rows, source payloads, or issue
   trackers.
5. Re-enable the source only after secret resolution succeeds and webhooks are
   revalidated.

## Communication Cadence

| Severity | Internal updates | Customer updates |
| --- | --- | --- |
| SEV-1 | Every 15 minutes | Initial notice as soon as facts are confirmed, then agreed cadence |
| SEV-2 | Every 30 minutes | When customer impact is confirmed, then hourly or agreed cadence |
| SEV-3 | At handoff or resolution | Only if customer impact or requested support case exists |

Customer updates must contain impact, current mitigation, next update time, and
known customer action. Do not speculate about root cause before evidence is
available.

## Resolution And Follow-Up

An incident is resolved only after:

- customer-visible impact has stopped
- mitigation is verified by health checks or dashboard recovery
- audit/log evidence is preserved
- customer communication is sent if required
- follow-up issues have owners and due dates

Post-incident review must include:

- timeline
- customer impact and data classes involved
- root cause and contributing factors
- what detected the issue and what should have detected it earlier
- code, test, alert, runbook, and process follow-ups
