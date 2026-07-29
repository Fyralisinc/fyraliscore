# Source certification load runners

Fyralis has two distinct load-test boundaries. They answer different
questions and their evidence is not interchangeable.

- The provider request boundary in `execution_driver.py` and `load_search.py`
  proves exact Provider Lab operations, `ProviderTransport` behavior, quota
  use, retry behavior, and request-boundary capacity. It is a diagnostic-only
  companion to the generated `--stage load` command.
- The ingestion data-plane boundary in `pipeline_load_runner.py` can prove
  scheduled offered load through raw S3 evidence, raw Kafka, normalized
  Kafka, Observation persistence, and T1 triggering. The generated stage now
  delegates every typed workload to this runner, but no concrete exact-pipeline
  adapter is implemented yet, so applicable workloads seal blocked artifacts.

The source certification load stage now records two distinct artifact families
for every canonical source and every declared workload:

- historical pull/backfill;
- live ingress; and
- combined live, backfill, reconciliation, and renewal work.

It is deliberately fail closed. The typed pipeline runner records the exact
workload contract in both modes and returns `blocked` until an exact adapter
and verified quota input exist; WhatsApp historical is the one declared
`not_applicable` workload. The default Provider Lab measurement is a short
virtual-clock diagnostic. It cannot prove that Fyralis stored raw evidence,
consumed either Kafka topic, persisted an Observation, triggered T1, maintained
tenant isolation, or drained downstream backlog at the offered rate.

## Provider Lab request-load stage

### Request-boundary search algorithm

For each workload and envelope, the controller:

1. calibrates the quota-disabled Provider Lab;
2. runs the initial warmup;
3. raises offered load by the source-declared 25 percent step;
4. stops when it finds an unstable upper bound;
5. binary-searches the bracket to the declared 5 percent tolerance;
6. validates the maximum stable candidate; and
7. optionally runs the weekly soak.

The Provider Lab calibration must demonstrate at least twice the target
Fyralis request rate, with p99 response time below 10 percent of the configured
client timeout. Promotion requires at least 30 seconds of wall-clock
calibration.

The diagnostic runner pairs every typed per-item data operation with the exact
strict HTTP request templates exposed by Provider Lab, across the source
contract's tenant, installation, and replica topology. Multiplexed routes must
declare the operation's exact method, path values, query, headers, and body.
Calibration must successfully exercise every typed-data-operation ×
Provider-Lab-operation diagnostic case. Replicas for the same installation
share the same Provider Lab quota scope. Non-HTTP protocol surfaces remain
explicit coverage gaps until a protocol-specific runner executes them.

`--load-offer-limit-rate` is a search safety cap, not a simulated bottleneck.
If the search remains stable through the cap, the suite is blocked and no
maximum-stable-rate artifact is claimed.

The built-in boundary runner records `scheduled_data_operation:<id>` and
`scheduled_control_operation:<id>` labels, never executable receipt labels.
Therefore its executable-operation coverage is zero and it is never
promotable. Only an end-to-end runner that actually invokes each declared
callable and validates durable receipts may emit executable-operation evidence.

### Diagnostic and promotion-duration modes

The generated execution binding invokes the short diagnostic by default:

```bash
uv run python -m services.ingest.source_certification.execution_driver \
  --source slack \
  --stage load \
  --plan-sha256 "$PLAN_SHA256"
```

`FYRALIS_CERTIFICATION_RESULT_PATH` and
`FYRALIS_CERTIFICATION_ARTIFACT_DIR` must be set by the certification producer.

Use `--load-promotion` to request source-declared wall-clock durations. This
automatically enables the weekly soak and 30-second calibration:

```bash
uv run python -m services.ingest.source_certification.execution_driver \
  --source slack \
  --stage load \
  --plan-sha256 "$PLAN_SHA256" \
  --load-promotion \
  --load-initial-rate 1 \
  --load-offer-limit-rate 500
```

The flag requests promotion-grade durations for the Provider Lab diagnostic;
it does not make the result promotion eligible. The typed pipeline runner is
always invoked first, but without the R3 exact adapter it records a sealed
blocked artifact rather than a substitute Provider Lab result.

## Exact-pipeline offered-load framework

`pipeline_load_runner.py` defines the stricter orchestration boundary needed
to measure ingestion throughput. `run_pipeline_load()` accepts a source, a
declared workload, isolated loopback infrastructure, a mode, and a
source-specific `PipelineBoundaryAdapter` factory. The generated load stage
uses this API for all six source/suite/mode attempts and persists each
self-validating result beneath `pipeline_load/`.

Pipeline artifacts use
`fyralis.source-certification-pipeline-load.v2`. A workload contains typed
`executable_operations`, not the catalog's legacy string-only semantic mix.
Each operation binds an importable callable and evidence identity; declares
data versus control work, raw/normalized/observation cardinality, cursor
applicability, provider-operation/quota-bucket mappings, and required receipt
proofs; and declares one of these schedules:

- weighted `per_item` data work;
- `once_per_trial` control work positioned before or after offered load; or
- periodic control work with an explicit cadence.

The legacy `operation_mix` remains only as a read-only compatibility projection
for historical readers. It is excluded from the typed workload hash and cannot
choose a callable, alter cardinality/cursor/quota requirements, or affect an
active load decision. Planning and reconciliation are represented as typed
control operations, never round-robined as offered data work.

There is currently no production `PipelineBoundaryAdapter` factory. The
execution driver calls `run_pipeline_load()` with no adapter until R3, which
returns a self-validating blocked artifact with
`reason_code=exact_pipeline_adapter_absent` once isolated infrastructure is
available (or a stricter prerequisite reason when it is not). It does not fall
back to the Provider Lab request runner, an in-memory simulation, or the
batch-oriented backfill harness.

### Exact adapter protocol

An exact adapter must own a dedicated, source-scoped namespace and implement
four operations:

1. `begin_trial(context)` resets only that namespace and returns a zero
   cumulative `PipelineSnapshot`.
2. `offer(item)` injects one unique provider-shaped item through the real
   source ingress and returns an `OfferReceipt`.
3. `finish_trial()` drains the raw, normalized, and Observation-to-T1
   backlogs, then returns the terminal cumulative snapshot.
4. `close()` stops its workers and releases source-scoped resources.

The adapter's `PipelineBoundaryProof` binds the measurement to all of the
following:

- the canonical source and an exact hash of the loopback Postgres, Kafka, and
  S3 binding;
- the dedicated namespace;
- the historical, live, or combined workload and the hash of its complete
  typed executable-operation contract;
- `ingestion.raw.<source>` and `ingestion.normalized.<source>`;
- the `observations` and `think_trigger_queue` relations;
- strict or disabled quota mode; and
- the exact tenant, installation, and replica topology.

Every trial cross-checks offer receipts against measurements read from the
real boundary. A receipt repeats the operation-contract hash and evidence
identity, proves the bound callable invocation, records exact raw S3, raw
Kafka, normalized, and expected-observation cardinalities, and itemizes
provider requests by the declared operation/bucket/cost mapping. Accepted
counts must satisfy that operation's cardinality contract. Historical fetch
work requires one or more raw, normalized, and observation outputs; an empty
terminal page is not accepted offered load. Control work requires zero
data-plane output. Cursor checks are required only for cursor-applicable
operations.

The terminal snapshot must exactly match the receipt totals and the
self-hashed receipt ledger. Observation and T1 identities must be unique. Raw
and normalized bytes, provider-request counts, quota units, per-layer latency
samples, event, receipt, and cursor ledger hashes, Kafka lag,
Observation-to-T1 lag, DLQ entries, cross-tenant leaks, and actual
per-replica raw-record processing counts are part of the terminal snapshot.

An adapter cannot establish this proof merely by reporting counters. Its
boundary identity must match the requested source, infrastructure hash,
workload hash, topics, relations, topology, and quota mode, and every
cumulative count is cross-checked against the individual offer receipts.

### Exact 2 × 2 × 2 topology

A release-shaped run requires:

```text
2 tenants
× 2 installations per tenant
× 2 Fyralis replicas
= 8 scheduling lanes
```

Data work rotates through all eight lanes. A terminal snapshot must contain
exactly two tenant IDs, four installation IDs, and two replica IDs. Both
replicas must report processing at least one item, and their processed-item
counts must add up to the raw Kafka record count. This proves observed worker
participation rather than merely declaring `replicas=2`.

Injected topologies remain useful for unit tests, but only exactly 2 × 2 × 2
can satisfy the release-shaped configuration.

### Wall-time search and stability

The exact-pipeline runner uses the system monotonic clock for eligible
evidence. Each trial:

1. requires a zero baseline;
2. computes `floor(target_rate × duration)` weighted data operations;
3. merges those timestamps with before/after-trial and cadence-based control
   operations, using bounded in-flight offers;
4. waits for every offer receipt;
5. calls `finish_trial()` to drain the complete data plane; and
6. computes throughput using the real elapsed time from trial start through
   the completed drain.

The search warms up for 120 seconds, increases offered load by 25 percent per
120-second step until it observes instability, and binary-searches the stable
and unstable bracket to within 5 percent. It then validates the candidate for
900 seconds and includes a 3,600-second soak.

Stability requires exact counts through every layer, at least 90 percent of
the target offered rate over end-to-end wall time, zero missing records,
unexpected duplicates, cross-tenant leaks, cursor consistency errors,
cooldown violations, failed requests, DLQ entries, and terminal lag. It also
requires every cursor-required receipt to carry a positive check, full
data-plane replica participation, and Observation p99 below the configured
maximum. Cursor-free workloads are not forced to fabricate cursor activity.

`maximum_offered_rate` is only a safety cap. In the quota-disabled ceiling
mode, reaching that cap without finding an unstable upper bound fails the
search; it is not reported as the Fyralis ceiling.

### Evidence modes and artifact states

The pipeline framework has two envelope modes:

- `provider_safe` requires a source-matching, freshness-checked
  `VerifiedQuotaConfiguration`. Each constraint records its limit ID, scope,
  units per item, steady and burst windows, HTTPS evidence URI, and
  verification timestamp. The adapter must run with strict quotas, and the
  stable rate must reach at least 90 percent of the modeled limiting rate.
- `fyralis_ceiling` rejects a quota configuration and requires the adapter to
  disable provider quotas. The search must observe a real unstable bound
  before its safety cap.

The adapter also identifies its evidence class as `exact_pipeline` or
`test_double`. Injected clocks and test doubles produce diagnostic evidence
only. The artifact states are:

- `blocked` when prerequisites such as acknowledged loopback infrastructure,
  exact quota evidence, the adapter, or a required bounded renewal callable
  are absent;
- `not_applicable` when the source contract explicitly excludes the workload
  shape (currently WhatsApp historical ingestion);
- `failed` when an attempted run violates a search or correctness invariant;
- `diagnostic` for structurally non-eligible clock, timing, topology, or
  test-double runs; and
- `passed` only when the exact-pipeline, system-wall-clock, duration,
  topology, quota, correctness, and search gates all hold.

Every pipeline artifact is canonicalized, self-hashed, and validated again
when written. This makes a missing adapter, shortened run, altered topology,
tampered counter, or synthetic evidence class visible in the artifact rather
than silently promotable.

At present, the framework and its validators are available, but the concrete
adapter is absent. Therefore Fyralis has no exact-pipeline offered-load
artifact from this runner and this document makes no release-readiness claim.

## Exact provider quota configuration

The following configuration belongs to the existing Provider Lab
request-load stage. Provider-safe request load is never run from a guessed
RPS. Supply
`FYRALIS_PROVIDER_QUOTAS_JSON` with an exact, evidence-labelled budget for
every quota bucket exercised by the source.

A one-bucket source accepts one exact object:

```json
{
  "slack": {
    "bucket": "web-api",
    "limit_id": "method-minute",
    "cost": 1,
    "capacity": 50,
    "refill_per_second": 1,
    "scope": "method/workspace/app",
    "evidence_uri": "https://docs.slack.dev/apis/web-api/rate-limits/",
    "verified_at": "2026-07-27T00:00:00+00:00"
  }
}
```

A source with several used buckets or simultaneous constraints requires a
list. Several entries may target the same route bucket when they represent
independent scopes or time windows:

```json
{
  "discord": [
    {
      "bucket": "oauth",
      "limit_id": "application-hour",
      "cost": 1,
      "capacity": 10,
      "refill_per_second": 1,
      "scope": "application/route",
      "evidence_uri": "https://docs.discord.com/developers/topics/rate-limits",
      "verified_at": "2026-07-27T00:00:00+00:00"
    },
    {
      "bucket": "rest",
      "limit_id": "route-second",
      "cost": 1,
      "capacity": 50,
      "refill_per_second": 1,
      "scope": "application/route",
      "evidence_uri": "https://docs.discord.com/developers/topics/rate-limits",
      "verified_at": "2026-07-27T00:00:00+00:00"
    }
  ]
}
```

Fields are exact: unknown or missing fields, naive timestamps, non-HTTPS
evidence, duplicate `(bucket, scope, limit_id)` constraints, missing exercised
buckets, and extra buckets all block the provider-safe envelope. `cost` is the
weighted token cost charged to that independent constraint. All constraints
on one request are checked and charged atomically: if any enforced constraint
blocks, none is drained. The quota-disabled Fyralis-ceiling search still runs
so the diagnostic remains useful.

`scope` is executable configuration, not a note. It is a slash-separated
combination of `app`, `application`, `global`, `installation`, `method`,
`realm`, `region`, `route`, `tenant`, `user`, or `workspace`. For example,
`method/workspace/app` creates one shared budget for each exact
method/workspace/application tuple. `app` creates one budget shared by all
tenants, installations, and replicas. Replica identity never creates a
provider quota bucket. To model independent app and workspace limits, declare
two entries with `scope: "app"` and `scope: "workspace"`; each may have its own
`limit_id`, capacity, refill window, and cost. Unknown scope words block
provider-safe execution.

## Provider Lab request artifacts

The load stage writes separate typed-pipeline and Provider Lab diagnostic
artifacts:

```text
artifacts/
├── stage.json
├── pipeline_load/
│   ├── historical/
│   │   ├── provider_safe.json
│   │   └── fyralis_ceiling.json
│   ├── live/
│   │   ├── provider_safe.json
│   │   └── fyralis_ceiling.json
│   └── combined/
│       ├── provider_safe.json
│       └── fyralis_ceiling.json
└── provider_lab_load/
    ├── historical/
    │   ├── provider_safe.json
    │   └── fyralis_ceiling.json
    ├── live/
    │   ├── provider_safe.json
    │   └── fyralis_ceiling.json
    └── combined/
        ├── provider_safe.json
        └── fyralis_ceiling.json
```

The typed pipeline files always exist after the load stage and record a
contract-bound `blocked` result until the exact adapter is available (or
`not_applicable` for WhatsApp history). A Provider Lab envelope is omitted when
exact quota configuration is absent or invalid, request semantics are
uncovered, calibration misses any typed-data/provider-operation case, or the
search reaches its safety cap without observing a real bottleneck. Every
completed Provider Lab envelope records:

- typed workload identity, executable/control operation IDs, compatibility
  operation mix, and topology;
- clock mode and all configured durations;
- Provider Lab calibration;
- every warmup, step, binary-search, validation, and soak trial;
- status and operation counts;
- request/quota/byte rates and p50/p95/p99 latency;
- correctness and stability counters;
- verified quota evidence;
- maximum stable rate and search tolerance; and
- all reasons the artifact is not promotion eligible.

`stage.json` also records the provider-safe versus Fyralis-ceiling comparison
and headroom ratio. A ceiling below the provider-safe stable rate blocks the
suite.

## Existing stage evaluator gates

A passing provider-safe or ceiling suite must independently report:

- wall-clock timing and at least 99 percent elapsed-duration coverage;
- 120-second warmup and load steps;
- 900-second stable validation;
- 3,600-second soak;
- 25 percent stepping and at most 5 percent final tolerance;
- exact declared tenant/installation/replica topology;
- at least 30 seconds of passing Provider Lab calibration;
- complete typed executable and control-operation receipt coverage;
- end-to-end pipeline proof; and
- a promotion-eligible artifact.

Provider-safe suites additionally require verified quota configuration and at
least 90 percent utilization of the modeled limiting quota. These checks are
performed by the evaluator as well as by the artifact builder so a short or
synthetic-only result cannot be relabelled as passing. The current stage still
fails the end-to-end gate because its typed pipeline artifacts have no exact
adapter and its Provider Lab runner remains diagnostic only. Stage artifact v3
validates the nested typed workload identity but rejects any passing load claim
until a future release-capable schema independently validates exact-pipeline
receipts.
