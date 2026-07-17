# P0-D Telemetry Inventory

**Status:** Characterized; HG-13 currently fails

**Machine-readable source:** [telemetry-inventory.json](telemetry-inventory.json)

## Result

Current telemetry can describe successful Think usage reasonably well by
purpose, but it cannot truthfully answer how many physical LLM attempts were
made. A call is appended to `LLMUsageAggregator` only after usage is extracted
or estimated from a response. Timeouts and failures before that point disappear
from `llm_calls_count` even though they consumed wall time and may have consumed
provider capacity or money.

The same ambiguity repeats at higher levels. `retry_count` combines transaction
retries and out-of-region reruns but excludes provider parse/transport retries.
Stage timings are successful-run JSON notes without parent/exclusive semantics.
Queues and batch walls are snapshots assembled by the benchmark rather than one
causally closed receipt.

```text
run receipt                         MISSING
  batch receipt                     MISSING
    Think run                       DURABLE, asymmetric on failure
      logical LLM request           NO ID OR DURABLE ROW
        physical provider attempt   NO RECEIPT
          successful usage block    AGGREGATED BY PURPOSE
```

## What exists today

| Surface | Current evidence | Principal limitation |
| --- | --- | --- |
| Think invocation | `think_runs` | Compact failure row loses real start time and stage detail |
| Usage/tokens/cost | `think_run_costs` by purpose | Counts recorded responses, not all physical attempts |
| Retry count | Cost row plus queue attempts | Different retry classes are omitted or conflated |
| Stage timing | `ops_applied.think_stage_timings` | Successful only; no nesting/exclusive contract |
| Queue state | Separate queue rows and snapshots | No end-to-end terminal-fate receipt |
| Batch/run wall | Benchmark artifacts | No shared durable identity or reconciliation contract |

## Objective P1 exit condition

Every physical provider attempt must write a receipt, including timeout and
failure. Logical call, Think run, batch, and coherent run identities must form a
parent chain. Tokens and cost must state whether they are exact, estimated, or
unavailable. Exclusive stage totals, batch walls, and run walls must reconcile
to independent monotonic measurements within 1%, and every truth-critical queue
item must have exactly one terminal fate before its declared causal barrier.

## Learning-log entry

`2026-07-17 — P0-D found that existing LLM telemetry measures recorded usage
responses rather than physical attempts. Failed pre-usage calls are invisible;
logical calls have no identity; retry_count conflates region and transaction
retries while excluding provider retries; successful stage notes are not an
exclusive timing tree; and benchmark queue/batch/run evidence has no common
receipt. HG-13 therefore cannot currently be computed, much less passed.`
