# P0-B Benchmark-Hook Inventory

**Status:** Characterized; HG-01 currently fails

**Machine-readable source:** [benchmark-hook-inventory.json](benchmark-hook-inventory.json)

## Result

Four hook classes are reachable from production Think. Two are direct proof
contaminants: the capability-probe injector explicitly describes itself as
benchmark-only, and the pricing bridge injector recognizes distinctive phrases
also emitted by the storyline benchmark. The noise fast path likewise shares
fixture/scorer vocabulary. The entry-point augmentor is broader: it permits an
installed overlay to alter production retrieval context and mutation region
without a core-code import.

| ID | Surface | Current result | Why it matters |
| --- | --- | --- | --- |
| BH-001 | Runtime reasoning augmentors | Reachable | Installed code can mutate context and allowed region before reasoning |
| BH-002 | Capability-probe injector | Reachable, explicitly benchmark-only | Fixture requests exact operations later counted as capability |
| BH-003 | Pricing bridge injector | Reachable, fixture-phrase-sensitive | Hard-coded phrase conjunction creates a fixed semantic hypothesis |
| BH-004 | Noise fast path | Reachable, fixture-phrase-sensitive | Shared fixture/scorer vocabulary bypasses retrieval and the LLM |

## Reachability

```text
persisted normalized signal
  -> production TriggerContext
  -> context planner
       -> dynamically discovered augment_context hooks       (BH-001)
  -> Think reasoning
       -> phrase-based noise short circuit                    (BH-004)
       -> LLM or deterministic result
       -> pricing bridge injection                            (BH-003)
       -> explicit capability_probe operation injection       (BH-002)
  -> normal validator and applier
```

Passing the normal validator/applier does not restore benchmark blindness. It
only proves the injected operation satisfies downstream shape checks.

## Objective P1 exit condition

An independent static scan and sealed behavioral population must find zero
production-reachable fixture phrases, capability requests, storyline labels,
expected-output vocabulary, or unreceipted evaluator/demo augmentors. Until
then, system-level semantic results that exercise these paths are contaminated
and cannot be used as evidence for company understanding.

## Learning-log entry

`2026-07-17 — P0-B confirmed that benchmark contamination is architectural,
not merely a scoring problem. Production Think contains a direct
capability-probe output injector, two fixture-phrase-sensitive shortcuts, and a
dynamic context/authority extension seam. Downstream validation cannot make an
answer independent when the expected behavior was injected upstream.`
