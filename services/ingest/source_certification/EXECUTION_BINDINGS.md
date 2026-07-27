# Source certification execution bindings

The evidence producer requires one committed file per canonical source at:

`services/ingest/source_certification/execution_bindings/<source_id>.json`

The 27 files are generated from the certification catalog:

```bash
COMPANY_OS_ENV=test \
  python scripts/generate_source_certification_execution_bindings.py

COMPANY_OS_ENV=test \
  python scripts/generate_source_certification_execution_bindings.py --check
```

The check fails on a missing/unknown file or a stale `spec_hash`. Missing files
and `null` stages are still recorded as blocked by the producer; they never
become passing evidence. A binding is tied to the current certification
declaration with `spec_hash` and has this exact shape:

```json
{
  "schema_version": "fyralis.source-certification-execution-binding.v1",
  "source_id": "slack",
  "spec_hash": "<SourceCertificationSpec.declaration_hash()>",
  "stages": {
    "local_correctness": {
      "argv": ["{python}", "path/to/slack_certifier.py", "local"],
      "timeout_seconds": 7200,
      "required_env": ["DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS"],
      "credential_env": []
    },
    "load": {
      "argv": ["{python}", "path/to/slack_certifier.py", "load"],
      "timeout_seconds": 21600,
      "required_env": ["DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS"],
      "credential_env": []
    },
    "fault_recovery": {
      "argv": ["{python}", "path/to/slack_certifier.py", "recovery"],
      "timeout_seconds": 7200,
      "required_env": ["DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS"],
      "credential_env": []
    },
    "canary": {
      "argv": ["{python}", "path/to/slack_certifier.py", "canary"],
      "timeout_seconds": 1800,
      "required_env": [
        "FYRALIS_CANARY_SLACK_BOT_TOKEN",
        "FYRALIS_CANARY_SLACK_TEST_WORKSPACE"
      ],
      "credential_env": [
        "FYRALIS_CANARY_SLACK_BOT_TOKEN",
        "FYRALIS_CANARY_SLACK_TEST_WORKSPACE"
      ]
    }
  }
}
```

## Current shared execution driver

Every checked-in binding invokes
`services.ingest.source_certification.execution_driver` with an exact source
and stage. Each generated command also carries `--plan-sha256`: a digest over
that source's exact scenarios, load topology, callable bindings, Provider Lab
routes/protocols, live transports, normalizers, and idempotency builders. The
driver recomputes the plan before making a request and rejects a stale binding.
This prevents a generic shared command from silently ignoring source-specific
declarations.

The driver makes the strongest source-isolated local measurement that the
current credential-free, in-process infrastructure can support without
fabricating a release claim:

- `local_correctness` executes the source-owned fixture twice, checks
  determinism and sibling-installation identity, runs the exact count oracle,
  resolves every historical, installation, normalizer, idempotency, live, and
  validation callable, and constructs the source-owned live target. It then
  exercises every HTTP method through the Provider Lab ASGI boundary, verifies
  exact operation ownership, strict unknown-route rejection, source isolation,
  and a four-scope tenant/installation request ledger.
- The local artifact has one row for every declared scenario. Each row lists
  measured prerequisite probes and the exact remaining proof. Without the
  opt-in data-plane environment, all rows remain blocked. With that environment
  exactly `raw_evidence_and_normalized_topic` and
  `observation_persistence_and_t1_trigger` can pass; special source scenarios
  with no executable remain named and blocked instead of inheriting a generic
  pipeline result.
- `load` retains the short quota-disabled representative
  Provider Lab/`ProviderTransport` measurement and additionally executes one
  concurrent request for every declared HTTP method across four scopes. The
  artifact includes the exact historical/live/combined operation mixes and
  separates non-HTTP protocol operations. It runs a provider-safe diagnostic
  only when
  `FYRALIS_PROVIDER_QUOTAS_JSON` contains an exact, evidence-labelled budget
  for the source; it never guesses a provider rate.
- `fault_recovery` injects one 503 and one 429 into every retry-safe HTTP
  catalog operation and verifies that the real shared `ProviderTransport`
  retries to a terminal provider response. Non-HTTP protocol surfaces are
  recorded as unexecuted rather than silently counted.
- `canary` remains blocked even when its source-prefixed credential is
  present, because no source has a committed real-provider canary executable.
  Credential values are never written and the driver sends zero provider
  requests.

Canary mutability is operation-bound. Every required operation has a
`CanaryOperationContract` declaring `read`, `mutation` plus one exact cleanup
action, or `unclassified`. An unclassified operation is a structural blocker;
a command cannot make it read-only by labelling a ledger row `read`. Provider
requests are classified as reads only when their exact Provider Lab binding
uses `GET`, `HEAD`, or `OPTIONS` and the source request policy is not unsafe.
Non-safe methods, unsafe requests, protocol-only operations, and
webhook/subscription/gateway lifecycles remain explicitly unclassified until a
source-specific contract declares read or mutation semantics and exact cleanup.
Every cleanup provider request is recorded in the same ordered ledger and
counts toward the canary's `max_requests`; promotion requires a terminal
successful cleanup request for every executed contracted mutation.

The aggregate local stage deliberately remains `blocked` while any required
scenario lacks proof. Fixture/callable/route diagnostics are not a substitute
for provider-specific lifecycle and cursor assertions, a 15-minute/60-minute
throughput envelope, multi-replica distributed quota proof, or real-provider
evidence. The stage artifact records both the measured facts and that claim
boundary.

### Optional isolated raw-to-T1 proof

The local stage will execute the real data plane only when all five of these
variables are present:

```text
FYRALIS_CERTIFICATION_ISOLATED_INFRA_ACK=dedicated-loopback-data-plane-v1
FYRALIS_CERTIFICATION_DATABASE_URL=postgresql://...@127.0.0.1:<port>/<db>
FYRALIS_CERTIFICATION_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:<port>
FYRALIS_CERTIFICATION_S3_ENDPOINT_URL=http://127.0.0.1:<port>
FYRALIS_CERTIFICATION_S3_RAW_BUCKET=<dedicated-bucket>
```

All three service endpoints must resolve to loopback. The database must already
contain every current migration and the complete `ingestion_source_catalog`;
Kafka must already contain the contract-derived data-plane and control topics;
the S3-compatible service must be live. The harness creates the named bucket
idempotently. The probe never truncates a database, deletes/recreates topics, or
clears a bucket.

For a history source, the probe creates one unique exact-count scenario and
runs the real seven-service `BackfillHarness` against the production client and
Provider Lab. For the contract-declared live-only source, it starts the same
consumer chain, uses the catalog-owned live-only bootstrap and live generator,
and requires the Kafka-first HTTP 202 path.

The proof then:

1. Reads only `ingestion.raw.<source>` records for the generated tenant.
2. Fetches every referenced S3 object and verifies its content hash and
   source/tenant key scope.
3. Reads only `ingestion.normalized.<source>` and verifies every envelope points
   to one of those raw objects.
4. Requires the source-owned exact Observation count, allowed source channels,
   non-null external IDs, and unique idempotency identities.
5. Requires exactly one same-tenant `T1/event_arrival` row per Observation.
6. Replays the exact raw envelopes, waits for the normalizer and writer consumer
   group to drain, and requires normalized reprocessing with zero Observation or
   T1 growth.
7. Deletes only rows carrying the generated certification tenant ID, in
   bounded dependency-order passes, then deletes that tenant and records clean
   subprocess shutdown.

That evidence promotes only the two data-plane scenarios named above. It does
not promote auth refresh, source-specific lifecycle behavior, cursor recovery,
multi-install isolation, out-of-order delivery, distributed quota behavior,
load, or live-provider canaries. Missing, partial, non-loopback, or failing
infrastructure produces blocked/failed evidence with no endpoint credential
values serialized.

An optional quota entry has no defaults and must contain exactly these fields:

```json
{
  "slack": {
    "bucket": "web-api",
    "capacity": 100,
    "refill_per_second": 1,
    "scope": "certification-slack",
    "evidence_uri": "https://official-provider.example/rate-limits",
    "verified_at": "2026-07-27T00:00:00+00:00"
  }
}
```

The bucket must match the representative Provider Lab route. An absent,
malformed, non-HTTPS, un-timestamped, or mismatched entry leaves provider-safe
diagnostics blocked. Even a valid entry remains diagnostic-only until the full
duration/topology requirements are met.

Commands run directly, never through a shell. Only a small safe process
environment plus names declared in `required_env` are passed to them.
Credential values are neither serialized nor logged. Canary credential names
must use the source contract's `FYRALIS_CANARY_<SOURCE>` prefix; local and load
stages are prohibited from receiving canary credentials.

Each command must write a complete, strictly parseable `CertificationInput` to
the path in `FYRALIS_CERTIFICATION_RESULT_PATH`. The producer reads only the
fields owned by that stage:

- `local_correctness`: local state, artifact, and scenario results.
- `load`: provider-safe and Fyralis-ceiling suites.
- `fault_recovery`: fault-recovery suites.
- `canary`: real-provider canary and operation results.

Passed result artifacts must be regular files beneath
`FYRALIS_CERTIFICATION_ARTIFACT_DIR`. In the command result, refer to them as
`evidence-file:<relative-path>`. The producer hashes each file and replaces the
reference with a run-bound artifact URI. Arbitrary URI strings are rejected.

Every accepted command result must also include a typed `stage.json`. The
producer and downloaded-bundle verifier both validate its exact source, stage,
spec hash, embedded execution-plan hash, command receipt window, and
stage-owned claims. Artifact schema v1 permits only the two isolated
raw-to-T1 local scenarios and an operation/cleanup-ledger-backed real canary as
positive claims. It deliberately rejects passing load and fault claims; those
need a future schema that independently validates full end-to-end evidence.

Before any artifact or receipt hash is written, the producer scans the result,
artifact files, stdout, and stderr for the exact non-trivial credential values
provided to the command. A match discards all of those bytes and records only a
non-secret rejection reason, with no output hashes or byte lengths retained.

The producer writes blocked inputs and receipts when a binding, required
environment variable, credential, entitlement, command result, or artifact is
absent. The `Source Certification Evidence` workflow uploads those diagnostics
and then fails unless all 27 replay through the release evaluator as passed.

## Deterministic workflow sharding

Calling the producer without shard arguments still creates the original
all-source bundle. The evidence workflow derives a 27-entry matrix from the
canonical catalog and assigns exactly one source to each deterministic shard:

```text
--shard-index <0..26> --shard-count 27
```

Every shard records the same workflow-derived run ID, exact clean commit, full
ordered catalog, every `SourceCertificationSpec` hash, and a digest over that
catalog identity. Receipts and command artifacts remain under
`provenance/receipts/<source_id>/`; a shard cannot contribute provenance from a
source it does not own.

The merge step accepts only indexes `0..26` exactly once, recomputes each shard's
expected source membership, and requires all 27 canonical sources exactly once.
It rejects a missing or duplicate shard/source, a commit/run/catalog/spec
mismatch, a dirty producer checkout, changed input or provenance bytes, and
cross-source receipt paths. Only then does it create the existing all-source
bundle and replay the normal verifier. Blocked evidence remains blocked.

Each job uses the source-specific GitHub Environment
`source-certification-<source_id>`. That environment provides one JSON secret
under the common `SOURCE_CERTIFICATION_SECRET_BUNDLE_JSON` name containing only
that source's canary values and shared dedicated-infrastructure values. The
workflow writes it with mode `0600`, passes only its path and exact source ID to
the producer, and deletes it in an `always()` cleanup step before upload. The
loader rejects foreign `FYRALIS_CANARY_*` names.

Matrix parallelism is bounded to three. Provider Lab state is process-local,
but opt-in Postgres/Kafka/S3 harness workers currently subscribe broadly to
shared endpoints. Each self-hosted producer therefore holds the host-wide
`/tmp/fyralis-source-certification-data-plane.lock` for its complete
one-source run. Separate certification hosts may run in parallel only when
their data-plane service state is isolated.

The workflow intentionally runs only the short, blocked load diagnostic.
Scheduling 15-minute validations and 60-minute soaks would create no releasable
evidence today because the built-in runner records
`pipeline_e2e_proven=false` and stage artifact v1 rejects passing load claims.
A release-capable offered-load runner and typed schema must first prove S3 raw
evidence, raw/normalized Kafka, Observation/idempotency, and T1 at the measured
rate.
