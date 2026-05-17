# ADR 0001 — PgBouncer Deployment Mode: Sidecar per Pod

**Status:** Accepted
**Date:** 2026-05-17
**Context:** Ingestion implementation-plan Q1
**Related work:** M1.2 (`lib/shared/db.py` `pgbouncer_compatible` opt-in)
**Related LLD:** §5.2

## Context

The ingestion pipeline's Path-B writer (M5) and ShardFetchWorkflow
activity workers (M3) issue many short transactions per second per
pod, with average transaction times in the 5–20 ms range. Direct
asyncpg pools to Postgres at this fan-out require either:

1. A large `max_connections` on the Postgres server (one slot per
   pool member per pod), OR
2. A connection-pooler proxy that multiplexes server connections
   across many client connections.

Direct pools have hit `max_connections=100` in dev at as few as 5
worker pods (LLD §5.2 backing analysis). Production fan-out will be
worse. PgBouncer in transaction mode is the standard answer.

The remaining question is **where pgbouncer runs**: centralised
service (one pgbouncer cluster shared by all pods) or sidecar per
pod (each app pod runs its own pgbouncer container, connections go
pod → localhost pgbouncer → Postgres).

## Decision

**Sidecar per pod.** Each application pod that uses
`pgbouncer_compatible=True` (M3 fetchers, M5 writers, future
workers) runs a pgbouncer sidecar container in the same Pod spec.
The app talks to `127.0.0.1:6432`; pgbouncer talks to the Postgres
endpoint.

## Rationale

- **N3 fault isolation (system-design.md §2).** A centralised
  pgbouncer service is a chokepoint whose failure affects every
  pod simultaneously. A sidecar's failure affects only its pod;
  k8s restarts the sidecar and the pod resumes.
- **Latency.** pgbouncer adds ~0.1–0.3 ms to each acquire when
  running on localhost. A centralised service adds at least one
  network hop (~1–3 ms inside the same VPC, more across zones).
  At 7 queries per observation × batches of 500 (LLD §5.2), the
  hop cost compounds.
- **Operational surface.** A centralised pgbouncer is another
  service to monitor, alert on, certify, patch, fail-over. A
  sidecar is part of the Pod's lifecycle — k8s already manages
  it. No new on-call dimension.
- **Per-pod connection budgeting.** A runaway pod can exhaust its
  own sidecar's pool without diluting other pods' headroom. With
  a centralised service, the noisy-pod problem is one tier
  removed but not eliminated.

## Trade-offs Accepted

- **Higher total Postgres connections.** Each sidecar opens a
  fixed pool to Postgres (start: 20 server connections per pod).
  With N pods, Postgres needs `N × 20` connection slots plus
  headroom. At current scale (≲10 pods) this is ≤200 slots —
  well under any reasonable `max_connections` ceiling. At scale
  beyond ~100 pods this becomes a constraint; revisit then.
- **No cross-pod connection sharing.** A short-lived burst on one
  pod cannot borrow idle slots from another pod. Acceptable
  because workers are sized for steady-state load, not bursts;
  bursts queue at the asyncpg-pool layer inside each pod.

## Configuration

| Setting | Value | Notes |
|---|---|---|
| `pool_mode` | `transaction` | Matches `statement_cache_size=0` on the asyncpg side. |
| `default_pool_size` | `20` | Server connections per sidecar. Tune from `pgbouncer SHOW POOLS` metrics post-M5. |
| `max_client_conn` | `200` | Client-side (asyncpg) connection ceiling per sidecar; well above the asyncpg pool's `max_size`. |
| `server_reset_query` | `DISCARD ALL` | Cleans session state between client transactions. |
| `auth_type` | `hba` | Service-account password authentication. |
| `listen_addr` | `127.0.0.1` | Localhost only — no exposure outside the Pod. |
| `listen_port` | `6432` | Standard pgbouncer port; app DSN points here. |

## Application-side Coupling

Workers that opt into pgbouncer must set `pgbouncer_compatible=True`
on `lib.shared.db.init_pool` (M1.2). The flag forwards
`statement_cache_size=0` to asyncpg, which is mandatory for
transaction-mode pgbouncer.

Workers that do NOT opt in (the outbox poller, the reconciler —
both single-pod, long-lived) keep `pgbouncer_compatible=False` and
talk to Postgres directly. The pool-mode registry at
`services/ingestion/db_config.py` documents which is which.

## When to Revisit

- **Pod count crosses ~100.** At that scale `N × default_pool_size`
  approaches typical `max_connections` ceilings. Either lower
  `default_pool_size` per sidecar (with attendant latency cost) or
  introduce a second tier (sidecar → regional pgbouncer service →
  Postgres).
- **Postgres `max_connections` becomes binding** — symptom: an
  emergent inability to add another pod without breaching the
  ceiling.
- **A tenant-isolation incident traces to pgbouncer.** If transaction
  mode's connection multiplexing ever leaks session state across
  tenants (it shouldn't, given `DISCARD ALL` + `statement_cache_size=0`
  + RLS on `app.current_tenant`), revisit pool-mode choice.

## Out of Scope

- Provisioning the sidecar manifests, Helm charts, or k8s
  templates. That is infrastructure work, not application work.
- Postgres `max_connections` tuning. The DBA team owns the server
  parameter.
- Connection-level metrics ingestion into the observability stack.
