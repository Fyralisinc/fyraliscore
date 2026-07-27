# Source certification load runners

Fyralis has two distinct load-test boundaries. They answer different
questions and their evidence is not interchangeable.

- The provider request boundary in `execution_driver.py` and `load_search.py`
  proves exact Provider Lab operations, `ProviderTransport` behavior, quota
  use, retry behavior, and request-boundary capacity. The generated
  `--stage load` command invokes this boundary.
- The ingestion data-plane boundary in `pipeline_load_runner.py` can prove
  scheduled offered load through raw S3 evidence, raw Kafka, normalized
  Kafka, Observation persistence, and T1 triggering. Its orchestration and
  validation exist, but no concrete exact-pipeline adapter is implemented or
  wired into the generated load stage.

The existing source certification load stage produces measured Provider Lab
request envelopes for every canonical source and every declared workload:

- historical pull/backfill;
- live ingress; and
- combined live, backfill, reconciliation, and renewal work.

It is deliberately fail closed. The default command is a short virtual-clock
diagnostic. It writes useful request-boundary artifacts, but it is not an
ingestion-throughput certification. Satisfying the request-boundary checks
does not prove that Fyralis stored raw evidence, consumed either Kafka topic,
persisted an Observation, triggered T1, maintained tenant isolation, or
drained downstream backlog at the offered rate.

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

The runner schedules every explicitly bound HTTP operation across the source
contract's exact tenant, installation, and replica topology. Multiplexed
routes must declare the operation's exact method, path values, query, headers,
and body; the runner never labels a route/method Cartesian product as
operation coverage. Calibration must successfully exercise every declared
operation × workload-mix case. Replicas for the same installation share the
same Provider Lab quota scope. Non-HTTP protocol surfaces remain explicit
coverage gaps until a protocol-specific runner executes them.

`--load-offer-limit-rate` is a search safety cap, not a simulated bottleneck.
If the search remains stable through the cap, the suite is blocked and no
maximum-stable-rate artifact is claimed.

The built-in boundary runner records high-level workload entries such as
`normalize` and `persist` as `scheduled_mix:*`, not `executed_mix:*`. Therefore
its operation-mix coverage is zero and it is never promotable. Only an
end-to-end runner that actually executes each declared semantic operation may
emit `executed_mix:*` evidence.

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

The flag requests promotion-grade durations; it does not make the result
promotion eligible. The built-in Provider Lab boundary runner records
`pipeline_e2e_proven=false`, so the evaluator still blocks release. It also
does not invoke `pipeline_load_runner.py`.

## Exact-pipeline offered-load framework

`pipeline_load_runner.py` defines the stricter orchestration boundary needed
to measure ingestion throughput. It is a library API, not a second CLI stage.
`run_pipeline_load()` accepts a source, a declared workload, isolated
loopback infrastructure, a mode, and a source-specific
`PipelineBoundaryAdapter` factory.

There is currently no production `PipelineBoundaryAdapter` factory and no
execution-driver binding for it. Calling `run_pipeline_load()` without the
factory returns a self-validating blocked artifact with
`reason_code=exact_pipeline_adapter_absent`. It does not fall back to the
Provider Lab request runner, an in-memory simulation, or the batch-oriented
backfill harness.

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
- the historical, live, or combined workload and the hash of its exact
  source-owned operation mix;
- `ingestion.raw.<source>` and `ingestion.normalized.<source>`;
- the `observations` and `think_trigger_queue` relations;
- strict or disabled quota mode; and
- the exact tenant, installation, and replica topology.

Every trial cross-checks offer receipts against measurements read from the
real boundary. For each accepted item, there must be one raw S3 object and one
raw Kafka record. The receipt-declared number of expected observations must
equal the normalized-record, Observation, and T1-trigger counts. Observation
and T1 identities must be unique. Raw and normalized bytes, provider-request
counts, quota units, per-layer latency samples, event and cursor ledger
hashes, cursor checks, Kafka lag, Observation-to-T1 lag, DLQ entries,
cross-tenant leaks, and actual per-replica processing counts are part of the
terminal snapshot.

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

Work items rotate through all eight lanes. A terminal snapshot must contain
exactly two tenant IDs, four installation IDs, and two replica IDs. Both
replicas must report processing at least one item, and their processed-item
counts must add up to the accepted-item count. This proves observed worker
participation rather than merely declaring `replicas=2`.

Injected topologies remain useful for unit tests, but only exactly 2 × 2 × 2
can satisfy the release-shaped configuration.

### Wall-time search and stability

The exact-pipeline runner uses the system monotonic clock for eligible
evidence. Each trial:

1. requires a zero baseline;
2. computes `floor(target_rate × duration)` unique work items;
3. schedules them at their target monotonic timestamps across the eight
   lanes, with bounded in-flight offers;
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
requires positive cursor checks, full replica participation, and Observation
p99 below the configured maximum.

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
  exact quota evidence, or the adapter are absent;
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

The load stage writes:

```text
artifacts/
├── stage.json
└── load/
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

An envelope file is omitted when exact quota configuration is absent or
invalid, request semantics are uncovered, calibration misses any
operation/mix case, or the search reaches its safety cap without observing a
real bottleneck. Every completed measured envelope records:

- operation mix and topology;
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
- complete operation-mix coverage;
- end-to-end pipeline proof; and
- a promotion-eligible artifact.

Provider-safe suites additionally require verified quota configuration and at
least 90 percent utilization of the modeled limiting quota. These checks are
performed by the evaluator as well as by the artifact builder so a short or
synthetic-only result cannot be relabelled as passing. The existing stage
still fails the end-to-end gate because its Provider Lab runner sets
`pipeline_e2e_proven=false`; the separate pipeline framework is not yet wired
to replace that value.
