# Tier-1 Redaction Allowlist — the auditable I1 artifact

> **Invariant I1 — No PII at T1.** The default tier emits aggregated metrics
> only; zero PII or payload bytes leave the customer VPC. This document is the
> explicit, auditable specification of *what* the boundary OTel Collector keeps
> and *what* it drops. It is the human-readable twin of the
> `filter/allowlist` and `transform/redact-labels` processors in
> [`otel-collector-config.yaml`](./otel-collector-config.yaml). If the two ever
> disagree, **this document is the intent and the config is the bug.**

Two independent gates compose to enforce I1:

1. **Metric-family allowlist** (keep-list): only the families below survive.
2. **Label drop-list + enum allowlist**: on the surviving families, every
   high-cardinality / PII-risk label is deleted; only bounded enums remain.

A series leaves the VPC **iff** its family is on the allowlist **and** all of
its remaining labels are bounded enums. Everything else is dropped at the
boundary and never reaches the wire.

---

## Gate 1 — Metric-family allowlist (KEEP these; drop all else)

Grounded in the data-plane's existing instrumentation (`lib/observability/metrics.py`,
`observability/prometheus/`, postgres-exporter custom queries) and the golden-12
+ G1–G7 fleet signals from `docs/plans/byoc-control-plane.md` §9.

### Golden-12 + fleet families (Tier 1)

| # | Metric family (regex) | Subsystem | Why it's safe (aggregate) |
|---|---|---|---|
| 1 | `up` | worker liveness | 0/1 per `job` enum |
| 1 | `worker_heartbeat_age_seconds` | worker liveness | seconds, per `worker` enum |
| 1 | `worker_uptime_seconds` | worker liveness | seconds, per `worker` enum |
| 11 | `fyralis_schema_version` (**G1**) | schema integrity | version int, per `component` enum |
| 11 | `fyralis_partition_coverage*` | schema integrity | count of covered months |
| 11 | `fyralis_db_pool*` | DB | pool in-use/idle counts, per `pool` enum |
| 11 | `pg_*` | DB (postgres-exporter) | aggregate pg_stat_* gauges |
| 11 | `process_*` | runtime | RSS/CPU/fds — no identity |
| 5 | `fyralis_onboarding_shards` | backfill | shard counts per `status` enum |
| 3 | `fyralis_dlq_unresolved` | DLQ | unresolved depth (gauge) |
| 3 | `fyralis_dead_letter_rows` | DLQ | poison backlog per `table` enum |
| 7 | `fyralis_think_queue_pending` | reasoning | queue depth (gauge) |
| 8 | `think_runs_total\|failed\|latency*` | reasoning | counts/histogram per `trigger_kind`,`status` |
| 9 | `fyralis_embedding_backlog_pending` | embedding | backlog count (gauge) |
| 10 | `fyralis_llm_breaker*` (**G3**) | LLM | breaker state 0/1 per `provider`,`state` enum |
| 10 | `think_cost_recent_usd*` | LLM cost | rolling spend (gauge), no identity |
| 12 | `fyralis_oauth_token*` (**G2**) | auth | refresh success/fail counters + expiry-soon gauge per `source`,`provider`,`state` |
| 12 | `webhook_verification_failures_total` | ingress | counter per `source` enum |
| 12 | `webhook_resolver_outcomes_total` | ingress | counter per `outcome` enum |
| 2 | `kafka_consumergroup_lag*` | Kafka | lag per `group`,`topic`,`partition` enum |
| 2 | `normalizer_consumer_lag_seconds*` | Kafka | seconds-behind (gauge) |
| 2 | `breaker_trips_total` | Kafka | cutover counter |
| 4/6 | `writer_full_mode_writes\|full_mode_dedup_hits\|full_mode_failures\|shadow_drop\|poison_dlq\|parse_failure` | writer | per-source counters (`source`,`mode` enum) |
| 3 | `writer_poison_attempts*` (mig 0137) | writer | poison-cap counter |
| — | `reconciliation_pass_count*` | reconcile | pass counter |
| — | `fyralis_worker_expected_present*` (**G5**) | fleet | expected-vs-present worker count |
| — | `http_requests_total` | gateway | per `route`,`method`,`status` enum |
| — | `scrape_samples_scraped`,`scrape_duration_seconds` | meta | scrape health |

> **Note on `http_requests_total`:** this family is allowlisted but its `route`
> label MUST be a *templated* route (`/integrations/{source}/webhook`), never a
> resolved path with ids. The data plane already templates routes; the label
> drop-list (Gate 2) additionally removes `path`/`url` if present.

### Explicitly NOT allowlisted (examples of what gets dropped)

- Anything per-tenant-user, per-signal, per-observation, per-model, per-run.
- Raw business counters keyed by `external_id`, `installation_id`, `realm_id`.
- Any family not on the list above — **default-deny**.

---

## Gate 2 — Label drop-list (delete) + enum allowlist (keep)

After family filtering, **delete** every label below from every surviving
datapoint. This is a backstop: `lib/observability/metrics.py` already forbids
unbounded label values, but an exporter or a future regression could leak one,
so the boundary deletes them unconditionally.

### DROP — direct identifiers (unbounded cardinality)

`tenant_id_inner`, `installation_id`, `signal_id`, `observation_id`,
`model_id`, `run_id`, `external_id`, `node_id`, `shard_id`, `actor_id`,
`session_id`, `user_id`, `thread_id`, `channel_id`, `event_id`, `trace_id`,
`span_id`, `request_id`, `id`, `uuid`, `realm_id`, `team_id`, `workspace_id`

### DROP — PII / contact

`email`, `user`, `username`, `actor`, `author`, `name`, `display_name`,
`phone`, `ip`, `client_ip`, `remote_addr`

### DROP — free-form / payload text

`url`, `path`, `uri`, `endpoint`, `query`, `text`, `content`, `message`,
`error_message`, `exception`, `title`, `subject`, `body`, `payload`,
`filename`, `file`, `repo`, `repository`, `branch`, `commit`, `sha`

### KEEP — bounded operational enums (the only labels allowed to egress)

| Label | Bounded value set |
|---|---|
| `job` | the ~6 scrape-job names (`fyralis-workers`, `fyralis-gateway`, `postgres-exporter`, `kafka-exporter`, `redis-exporter`, `minio`) |
| `worker` | the fixed worker class names (normalizer, observation_writer, think_worker, …) |
| `source` | the 26 ingestion source slugs (slack, jira, github, …) |
| `provider` | LLM/embedding providers (deepseek, openai, ollama, …) |
| `state` / `status` | enum states (open/closed/half_open, pending/failed/complete, …) |
| `table` | a fixed set of DB table names |
| `trigger_kind` | think trigger enums |
| `reason` / `op_type` | validation/drop reason enums |
| `route` / `method` | templated gateway route + HTTP verb |
| `outcome` | webhook-resolver outcome enum |
| `group` / `topic` / `partition` | Kafka consumer-group / topic / partition |
| `le` / `quantile` | histogram bucket / summary quantile |
| `instance` | scrape target host:port (low cardinality, infra) |
| `pool` / `component` / `mode` | bounded subsystem enums |

### ADDED — deployment identity (low-cardinality, intentional)

Stamped by the `resource` processor from env (C4 deployment-record keys):

| Label | Source | Cardinality |
|---|---|---|
| `tenant_id` | `FYRALIS_TENANT_ID` | 1 per deployment (group-by only; authoritative tenant scoping is the proxy's `X-Scope-OrgID` from the cert SAN — C1/I4) |
| `deployment_id` | `FYRALIS_DEPLOYMENT_ID` | 1 per deployment |
| `region` | `FYRALIS_REGION` | 1 per deployment |
| `telemetry_tier` | `FYRALIS_TELEMETRY_TIER` | 1 per deployment (T1/T2/T3) |

---

## Worked example (auditor's check)

A raw data-plane series:

```
fyralis_oauth_token_refresh_failures_total{source="slack", provider="slack",
    installation_id="inst_8f3a", user_email="jane@acme.com", state="expired"} 4
```

After the boundary:

```
fyralis_oauth_token_refresh_failures_total{source="slack", provider="slack",
    state="expired", tenant_id="acme", deployment_id="acme-use1-7f3a",
    region="us-east-1", telemetry_tier="T1"} 4
```

- Family `fyralis_oauth_token*` → **kept** (Gate 1, signal #12, G2).
- `installation_id`, `user_email` → **deleted** (Gate 2 drop-list).
- `source`, `provider`, `state` → **kept** (bounded enums).
- `tenant_id/deployment_id/region/telemetry_tier` → **added** (C4 identity).

A non-allowlisted, high-cardinality series:

```
signal_processing_latency_seconds{signal_id="sig_91ab", author="bob@x.com"} 0.4
```

→ **dropped entirely** at Gate 1 (family not allowlisted). Even if a future
family leaked, `signal_id` and `author` are on the Gate-2 drop-list.

---

## Self-test mapping

`boundary/selftest.py` asserts, against this spec:

1. `filter/allowlist` exists and references the golden/G1–G7 families.
2. `transform/redact-labels` deletes a representative PII label (`email`) and a
   representative id label (`installation_id`).
3. The `resource` processor adds `tenant_id`, `deployment_id`, `region`.
4. A sample PII-ish label is in the drop-list; a sample allowlisted family
   (`fyralis_oauth_token*`) is in the keep-list.
