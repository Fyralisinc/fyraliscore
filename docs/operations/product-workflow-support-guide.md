# Product Workflow Support Guide

Owner: Product Engineering.
Last reviewed: 2026-06-24.

This guide defines the production-critical user workflows and the support
signals operators should use when those workflows degrade.

## Critical Workflows And SLO Targets

| Workflow | User success condition | Initial p95 latency target | Initial availability/error target |
| --- | --- | ---: | ---: |
| Login/session | User obtains a valid tenant-scoped session | 1.0s | 99.9% non-5xx |
| Source install | Source reaches validated installed or safe failed state | 30s for validation step | 99% non-5xx |
| First successful backfill | At least one source item lands in substrate | 15 minutes for small sources | 99% job success after retries |
| Today/CEO view | User receives current tenant-scoped dashboard data | 2.0s | 99.5% non-5xx |
| Ask/query | User receives answer or explicit degraded response | 8.0s | 99% non-5xx |
| Model detail/map | User receives model data or explicit topology-degraded state | 3.0s | 99.5% non-5xx |
| Recommendation/decision action | User accepts, rejects, or records action safely | 2.0s | 99.5% non-5xx |
| Forecast/prediction review | User views prediction, status, and evidence | 3.0s | 99.5% non-5xx |
| Source pause/uninstall | Source stops accepting new work and records state | 30s | 99% non-5xx |
| Admin role change | Role mutation completes and audit row is present | 2.0s | 99.9% non-5xx |

These targets are launch defaults. They must be tuned after staging soak and
real design-partner traffic.

## Metrics

The gateway emits bounded workflow metrics:

```text
product_workflow_requests_total{workflow,status_class}
product_workflow_request_duration_seconds{workflow}
product_workflow_events_total{workflow,event,outcome}
```

Prometheus records request rate, 5xx ratio, p95 latency, and burn-rate style
signals. Use the Product Workflow Health Grafana dashboard first, then drill
into route-level diagnostics only when the workflow metric identifies the
affected area.

The event counter captures bounded business outcomes such as recommendation
actions and forecast review/Ask events. It must not include tenant IDs, object
IDs, prompts, source names, or free-form reasons.

## Safe Customer-Facing Diagnosis

Support responses should include:

- request ID or approximate timestamp
- workflow name
- impact and current status
- safe customer action, if any
- next update time

Support responses must not include:

- raw prompts or model payloads
- source record content
- access tokens or webhook signatures
- object-store keys
- stack traces or internal exception names
- cross-tenant identifiers

## Workflow Triage

| Workflow | First checks | Next runbook |
| --- | --- | --- |
| Login/session | Auth errors, tenant mapping, gateway 401/403 rate | [Admin and role management](admin-role-management-guide.md) |
| Source install | Credential validation, scope check, secret resolution, provider health | [Source onboarding](source-onboarding-runbook.md) |
| First backfill | Source lane lag, DLQ rate, object-store write failures | [Per-source worker scaling](per-source-worker-scaling-runbook.md) |
| Today/CEO view | Product SLO dashboard, route access, DB saturation | [Observability and alert guide](observability-alert-guide.md) |
| Ask/query | LLM/provider health, retrieval latency, explicit degraded responses | [Incident response](incident-response-guide.md) |
| Model detail/map | Topology worker health, degraded reasons in API response | [Worker fabric](worker-fabric-runbook.md) |
| Recommendation action | Audit row presence, post-commit queue, dead letters | [Durable dead-letter admin](dead-letter-admin-runbook.md) |
| Source pause/uninstall | Install status, watch cleanup, secret deletion | [Source onboarding](source-onboarding-runbook.md) |
| Admin role change | `actor_roles`, `operator_action_log`, route access policy | [Admin and role management](admin-role-management-guide.md) |

## Degraded States

Production APIs should return explicit degraded states where the workflow can
still be useful:

- topology unavailable for map/model pages
- source sync paused or stale
- Ask dependency unavailable
- partial evidence due to source outage
- worker backlog above SLO

The backend already exposes degraded reasons on selected model/map surfaces.
Remaining UI/API gaps stay tracked in the production readiness checklist.

## Support Bundle Rules

A sanitized support bundle may include:

- request IDs
- route/workflow names
- status codes
- bounded error codes
- deployment SHA
- worker health states
- metric snapshots with bounded labels

It must not include raw logs, source payloads, prompts, completions, object
keys, credentials, cookies, bearer tokens, or PII.

Generate the current tenant support bundle:

```bash
python scripts/export_support_bundle.py \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --window-hours 24 \
  --deployment-sha "$DEPLOYMENT_SHA"
```

The exporter includes only bounded counts, states, timestamps, and backup
status fields. It intentionally excludes operator metadata blobs, ingestion
error summaries, raw object keys, prompts, completions, and token-like fields.
Successful exports write a `support_bundle.export` row in
`operator_action_log` with the operator actor, window size, deployment SHA
presence, schema version, and privacy-contract flags. The operator actor must
have tenant-wide `admin` or `leadership`.
