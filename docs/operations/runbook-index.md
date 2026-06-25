# Operator Runbook Index

Owner: Platform Engineering.
Last reviewed: 2026-06-25.

This index is the production coverage map for routine Fyralis incidents and
operator workflows. Use it as the first stop during triage; the linked runbooks
own the detailed commands, safety checks, and verification steps.

## Coverage Matrix

| Scenario | Primary runbook | First operator action | Success signal |
| --- | --- | --- | --- |
| deploy | [Deployment runbook](deployment-runbook.md) | Verify release artifact, staging evidence, and production promotion inputs. | Production health, readiness, backup freshness, and product SLO gates are green. |
| rollback | [Rollback runbook](rollback-runbook.md) | Identify last known good artifact and confirm rollback does not require schema rollback. | `/healthz` recovers and product SLO burn returns below rollback threshold. |
| migration failure | [Migration release runbook](migration-release-runbook.md) | Stop rollout, capture failed migration logs, and decide forward fix versus restore rehearsal. | Migration state is reconciled and schema drift check passes. |
| queue backlog | [Worker fabric runbook](worker-fabric-runbook.md) | Identify the queue family, tenant/source lane, depth, age, and worker saturation. | Queue drain rate exceeds arrival rate and DLQ count stays flat. |
| DLQ replay/quarantine | [Ingestion DLQ replay/quarantine runbook](ingestion-dlq-replay-quarantine-runbook.md) and [Durable dead-letter admin runbook](dead-letter-admin-runbook.md) | List sanitized failures, choose retry versus quarantine, and record operator reason. | `operator_action_log` has retry/quarantine rows and unresolved DLQ depth drops. |
| webhook verification spike | [Incident response guide](incident-response-guide.md) and [Observability and alert guide](observability-alert-guide.md) | Check signature failure alerts by provider and confirm secret/config drift before disabling traffic. | Signature failures return to baseline without accepting unsigned payloads. |
| source API outage | [Per-source worker scaling runbook](per-source-worker-scaling-runbook.md) and [Source onboarding runbook](source-onboarding-runbook.md) | Confirm provider status, breaker state, 429/5xx rate, and whether to pause the affected source lane. | Healthy sources continue draining and affected source resumes without duplicate ingestion. |
| LLM provider outage | [Incident response guide](incident-response-guide.md) and [Performance, scale, and cost targets](performance-cost-targets.md) | Check LLM breaker, daily budget state, retry exhaustion, and whether to pause Think dispatch. | Product reads stay available and deferred Think work resumes after provider recovery. |
| DB saturation | [Observability and alert guide](observability-alert-guide.md) and [Worker fabric runbook](worker-fabric-runbook.md) | Check pool saturation, long transactions, lock waits, queue writers, and hot queries. | Pool utilization and lock waits return below alert thresholds. |
| Redis/broker/object storage outage | [Incident response guide](incident-response-guide.md), [Worker fabric runbook](worker-fabric-runbook.md), and [Data retention, backup, and recovery](data-retention-backup-recovery.md) | Identify which dependency is unavailable and stop workflows that would lose durability. | Dependency health recovers and pending work resumes idempotently. |
| tenant isolation incident | [Incident response guide](incident-response-guide.md) and [Gateway route access inventory](gateway-route-access-inventory.md) | Freeze affected access paths, preserve audit evidence, and run route/RLS checks. | Cross-tenant access path is closed and audit evidence is preserved. |
| secret rotation | [Admin and role management](admin-role-management-guide.md), [Source onboarding runbook](source-onboarding-runbook.md), and [Data retention, backup, and recovery](data-retention-backup-recovery.md) | Rotate through the managed secret provider or source reinstall flow; never paste raw secrets into logs. | New `secret_ref` resolves, old credential is revoked, and source health check passes. |
| backup restore | [Data retention, backup, and recovery](data-retention-backup-recovery.md) | Confirm restore target, backup freshness, object-store state, and customer-data safety boundary. | Restore test is recorded fresh and data-integrity checks pass. |
| customer support diagnostics | [Product workflow support guide](product-workflow-support-guide.md) | Export a sanitized tenant support bundle and correlate with product workflow metrics. | Bundle excludes raw payloads/secrets and gives enough state for first-line triage. |

## Guardrails

- Do not run a destructive operation unless the scenario runbook explicitly
  lists it and a second operator has approved it.
- Do not paste raw customer payloads, prompts, provider tokens, object keys, or
  secret values into incident notes.
- Prefer scoped pauses, source-lane isolation, and deferral over deleting or
  rewriting customer data.
- Every operator action that mutates roles, queues, dead letters, source state,
  or credentials must carry an operator id, reason, tenant id, and audit row.
