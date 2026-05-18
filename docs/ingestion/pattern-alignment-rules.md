# M6 Pattern-Alignment Rules

*Status: M6.0 Phase 3 (2026-05-18). Enforced by
[`services/ingestion/workflows/tests/test_pattern_alignment.py`](../../services/ingestion/workflows/tests/test_pattern_alignment.py).*

The five rules from [04-implementation-plan.md §M6](04-implementation-plan.md#pattern-alignment-requirements-load-bearing-for-the-seven-sub-blocks),
made operational. Each rule has:

1. **What it says** — the requirement.
2. **What the analyzer checks** — the AST property the test enforces.
3. **What it does NOT check** — the deliberate scope (so future
   contributors don't expand the rule into ordering or design
   territory).
4. **Why the relaxation is acceptable** — when the AST check is
   weaker than the human reading, why that's deliberate.

The analyzer covers `services/ingestion/workflows/*.py` (excluding
`tests/`, `__init__.py`, `__pycache__/`). The retroactive smoke
suite also runs the analyzer over M3.3 (`embedding_backlog.py`) and
M5.1 (`circuit_breaker.py`) — both proven-correct, both pass by
construction.

Substrate-allowed files (each rule names these where relevant):
`state.py`, `signals.py`, `runtime.py`, `retry.py`, `__main__.py`.

---

## Rule 1 — Orchestration separated from side effects

**What it says.** The main loop reads state, decides, and calls
named side-effect functions. Class methods of an orchestrator
(e.g. `LongRunningService` subclass) MUST NOT make direct
`pool.X(...)` or `producer.X(...)` calls; those calls live in
module-level functions, called by the method.

**What the analyzer checks.** For every `ClassDef` in a non-
substrate workflow file, every method body is walked. Any
`await <expr>.<verb>(...)` where `<expr>` is `self._pool`,
`self._kafka_producer`, `pool`, or `producer` (and verb is
`execute`/`fetch`/`fetchrow`/`fetchval`/`fetchmany`/`produce`/
`flush`/`commit`/`rollback`) is flagged.

Methods MAY call named imported functions that take the pool as a
parameter — that's the substrate pattern (`persist_state(pool, ...)`).
Methods MAY call other methods on `self` — recursive walking checks
those too.

**What it does NOT check.**
- The *ordering* of calls (N1 vs. CLAIM-VIA-UPDATE — see the
  04-implementation-plan choice criterion).
- Whether a side-effect function is "really" pure of orchestration
  concerns — that's a design review.
- Function-style services (M3.3's `run_backlog_service`, M5.1's
  `run_circuit_breaker`). They have no class methods, so this rule
  is vacuously satisfied.

**Relaxation.** The analyzer is name-based: it specifically looks
for attributes named `_pool` / `_kafka_producer` / `_producer` /
`pool` / `producer`. A determined contributor could alias the pool
(`self._db = self._pool`) and bypass the check. That's an
acceptable false-negative — code review catches deliberate
circumvention; the rule's purpose is to make the right shape the
*easy* one, not to make the wrong shape impossible.

---

## Rule 2 — State in Postgres, not memory

**What it says.** Progress-bearing state goes to Postgres, not
in-process memory. The asyncio process holds nothing that wouldn't
survive a SIGTERM-restart.

**What the analyzer checks.** Every concrete (non-substrate, non-
`__main__`) workflow file MUST import at least one name from
`services.ingestion.workflows.state`. That's structural proof the
file connects to the state substrate. The N1 cursor-advance
primitive (`advance_cursor_atomic_with_kafka_publish`) and the
CLAIM-VIA-UPDATE pattern (`persist_state` after a direct UPDATE
guarded by `WHERE ... IS NULL`) BOTH satisfy this.

**What it does NOT check.**
- Whether the file uses the state substrate *correctly*. A file
  could `from services.ingestion.workflows.state import WorkflowState`
  and never call it; this passes the structural import check.
  That's a design-review concern.
- Whether per-request mutable state inside a class method is
  written to Postgres before the method returns. (Static checking
  of "every variable that crosses a `Tick` boundary lives in
  Postgres" is the kind of analysis that runs into halting-problem
  territory; the rule is enforced by the persistence-pattern test
  suite per service, not by static analysis.)
- The N1 vs. CLAIM-VIA-UPDATE choice. Both patterns satisfy
  requirement #2 at different points in the orchestration sequence
  — see 04-implementation-plan.md for the choice criterion. The
  analyzer is **ordering-agnostic** by design.

**Relaxation.** The import-presence check is weak. Per the user's
explicit guidance: rules should be structural, not ordering. A
stronger check (e.g. "every method that mutates `self.X` must call
`persist_state` before returning") would require flow analysis the
AST doesn't support without significant complexity. The retroactive
test on M3.3 / M5.1 / feels_onboarded_monitor.py is the calibrated
proof that the relaxed check is useful.

---

## Rule 3 — Retry logic in named functions

**What it says.** When a side-effect call fails and needs retrying,
the retry policy lives in a function with a name (e.g.
`retry_with_backoff_on_429`), NOT inline `try/except` blocks
scattered through the orchestrator.

**What the analyzer checks.** Every `Try` node in a workflow file
is inspected. A "retry-shaped try" is one whose `except` handler
contains an `await asyncio.sleep(...)`. The sleep is what
distinguishes a retry (wait, then the enclosing loop re-attempts)
from a skip-and-continue error-recovery pattern (M5.1's
flag-flip handler in `_process_tick` uses `log.exception(...);
continue` to advance to the NEXT tenant in the outer loop without
sleep — that's not a retry, it's error recovery for a list
traversal).

If a retry-shaped try is found, the enclosing `FunctionDef` is
inspected. The function MUST:
- live in `retry.py` (the named retry helpers module), OR
- have a name starting with `retry_` (e.g. `retry_with_backoff_on_429`).

Otherwise the analyzer flags an "inline retry loop."

**What it does NOT check.**
- `try/except` blocks that handle errors WITHOUT retrying (e.g.
  M5.1's `try: await asyncio.wait_for(stop_event.wait(), timeout=...)
  except asyncio.TimeoutError: pass` — the except body is `pass`,
  not a retry).
- `try/finally` for resource cleanup (the finally body never has
  retry semantics).
- The *ordering* of retry vs. publish. Both N1 and CLAIM-VIA-UPDATE
  may use retries; the analyzer enforces only that retries are
  named, not where in the sequence they sit.

**Relaxation (per the prompt's allowance).** This is the hardest
rule to AST-check robustly. The user's explicit relaxation:
**naming convention is the fallback** — function name starting with
`retry_` OR file being `retry.py` is sufficient. The analyzer does
NOT inspect the retry semantics (backoff strategy, max attempts,
jitter); naming is the marker that says "this function carries
retry concerns; treat it as such."

If a future contributor invents a retry pattern that the heuristic
doesn't recognise (e.g. using `await asyncio.wait_for(...)` instead
of `await asyncio.sleep(...)` for backoff), the analyzer may
false-negative. Code review and the per-helper log-shape tests are
the second line of defence.

---

## Rule 4 — Signals via Postgres polling

**What it says.** Cross-service communication uses Postgres rows as
the signal channel, not in-process events or shared queues.
`signals.emit_signal` / `signals.poll_signals` are the substrate
primitives.

**What the analyzer checks.** Workflow files MUST NOT use any of:
- `asyncio.Queue(...)` (construction or import) — the canonical
  in-process queue.
- `multiprocessing.Queue(...)` or `multiprocessing.Manager()`.
- `threading.Lock()` / `threading.RLock()` / `threading.Event()` —
  in-process synchronization for orchestration.

**What it does NOT check.**
- `asyncio.Event()` is **allowed** — it's the standard SIGTERM
  signaling primitive used by `LongRunningService.run`. The rule's
  intent is to ban cross-service in-process channels, not the
  process-internal SIGTERM event.
- `asyncio.Lock()` is allowed inside a single service for
  intra-service serialization (e.g. ensuring one tick at a time
  during a stress test); cross-service Locks are not possible in
  asyncio anyway.

**Relaxation.** The rule is a name-based ban-list. A determined
contributor could `from asyncio import Queue as Q` and bypass the
ImportFrom check. The analyzer catches both the import and the
construction call, but again, code review handles malice.

---

## Rule 5 — No cross-workflow shared in-process state

**What it says.** Each asyncio service is one process per logical
workflow. Services do NOT share Python globals, in-process queues,
or singleton objects. All cross-service handoffs go through
Postgres or Kafka.

**What the analyzer checks.** Module-level `Assign` nodes whose
value is a mutable-container literal or constructor (`{}`, `[]`,
`set()`, `dict()`, `list()`, non-empty dict/list/set literals) are
inspected. The target name MUST:
- end in `_metrics` (the A4 + M3.3 + M5.1 precedent: per-process
  observability counters that reset on restart are not progress-
  bearing state); OR
- be ALL_CAPS (a constant); OR
- be a Python dunder (e.g. `__all__`, `__slots__`) — these are
  language conventions for module exports, not orchestration state.

Otherwise the analyzer flags the assignment.

**What it does NOT check.**
- Class-level mutable defaults (`class X: items: list = []` is a
  Python footgun the analyzer doesn't enforce against — the
  pattern doesn't show up in the substrate, so the rule is
  reserved for future tightening if a violation is found).
- Mutable state inside function bodies (per-tick local variables
  are correctly scoped and reset).
- Whether the metrics dict is actually used for observability vs.
  smuggling cross-service state. The naming convention is the
  contract; a `_metrics` dict used as a hidden signal channel
  would be caught at code review or by Rule 4's lookup.

**Relaxation.** The `_metrics` allowlist is the A4 carve-out: M3.3
and M5.1 both have module-level metrics dicts, and the M3 closeout
explicitly preserved them. Without this allowlist, the retroactive
smoke check would fail and the analyzer would be (correctly)
considered over-strict.

---

## Test-synchronization pattern: Postgres state-as-checkpoint

*Documented here per Phase 2 follow-up (2). Not a Rule, but a
**precedent** for future M6.1–M6.6 SIGTERM tests.*

When a SIGTERM-handling service writes observable state (a
`workflow_states` row, a cursor table, a queue table) on every
tick, integration tests SHOULD use that production-observable
artifact as the test synchronization point. Specifically:

- Poll the DB for the state row to appear (proof of one completed
  tick), THEN send SIGTERM.
- DO NOT use timing-based delays (`asyncio.sleep(2)` and hope the
  tick fired). Timing-based synchronization is flake-prone under
  slow CI; deterministic checkpoints are not.
- Filesystem markers (writing a temp file) are an acceptable
  fallback for services with NO observable production state, but
  the asyncio substrate's `persist_state` makes filesystem markers
  unnecessary for any M6 service.

**Precedent.** M3.3 used DB-row processed-count as the
synchronization point in `test_backlog_service_resumes_from_cursor`.
M6.0 Phase 2 used `workflow_states.last_advanced_at` in
`test_feels_monitor_sigterm_subprocess`. Same shape; same rationale.

Future M6.1–M6.6 SIGTERM tests should follow this pattern. If a
contributor reaches for filesystem markers, that's a finding worth
flagging at review — most likely the service is missing a
`persist_state` call, which is itself a Rule 2 alignment concern.

---

## Calibration

The analyzer is calibrated such that ALL of the following pass by
construction:

- Every file under `services/ingestion/workflows/*.py` (excluding
  tests).
- `services/ingestion/recovery/embedding_backlog/embedding_backlog.py`
  (M3.3 precedent).
- `services/ingestion/feature_flags/circuit_breaker.py` (M5.1
  precedent).

If a future change to any of those files trips the analyzer, the
change is the likely culprit — investigate the change before
relaxing the rule. If the change is correct and the rule is
over-strict, document the relaxation here with the rationale (same
shape as the existing "Relaxation" sections).

---

## A12 substrate amendment — analyzer impact (nil)

Per [05-lld-amendments.md A12](05-lld-amendments.md#a12--executor-typed-substrate-signatures-for-transactional-participation),
the M6.0 substrate's five non-N1 functions now accept
`asyncpg.Pool | asyncpg.Connection`. The pattern-alignment analyzer
is **unchanged** by this amendment:

- **Rule 1's** name-list (`_pool`, `pool`, `_kafka_producer`,
  `_producer`, `producer`) is unchanged. Methods that receive a
  connection from `async with self._pool.acquire() as conn` and pass
  it to a NAMED substrate function (e.g.
  `await emit_signal(conn, ...)` or `await persist_state(conn, ...)`)
  are correct — the rule's intent is preserved.
- The analyzer's name-list does NOT include `conn` / `_conn`. If a
  future contributor stores a `Connection` as a class attribute
  (`self._conn`) and calls verbs directly on it (`await self._conn.
  execute(...)`), that's the same anti-pattern in different paint.
  Code review is the first line of defence; tighten the analyzer's
  name-list if such a pattern emerges in production code.
- **Rules 2-5** are unaffected: R2 checks import-presence, R3 checks
  `await asyncio.sleep` in `except`, R4 checks queue-primitive
  imports/constructions, R5 checks module-level mutable state. None
  of those touch the executor-typed surface.

The retroactive smoke check (`test_pattern_alignment_smoke_passes_against_m3_3_and_m5_1`)
continues to pass: M3.3 and M5.1 are untouched by A12 and remain
function-style services that the analyzer trivially satisfies.
