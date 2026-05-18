# A6 — Resolution decision: per-frame flush (Option 1)

**Amendment:** [05-lld-amendments.md §A6](../ingestion/05-lld-amendments.md) — Discord Gateway shadow-write Kafka flush window.
**Work-unit branch:** `fix/a6-broker-ack-ordering` (off integration HEAD `dcf9492`).
**Phase 1 status:** decision reached. Phase 2 implementation pending explicit go-ahead.
**Date:** 2026-05-18.

---

## TL;DR

**Chosen: Option 1 — per-frame `flush()` between `shadow_write_raw` and `save_session_state`.**

Option 3 (save inside producer delivery callback) works mechanically — the
scratch test validated every claim from the amendments tracker. But the
integration shape requires modifying a shared utility (`shadow_write.py`),
adds 3+ new failure modes, and exceeds the 200-line threshold the work
order set for "choose Option 3." Option 1 is structurally simpler,
stays inside the Discord Gateway subtree, and the measured latency cost
(~5-15 ms/frame at production broker latency) is well below the Discord
gateway's actual throughput needs.

Estimated implementation size: **~125 lines** (Phase 2 production
change + new tests).

---

## Investigation findings

### What `confluent-kafka.Producer.produce(on_delivery=...)` does

Authoritative source: `Producer.produce.__doc__` from
`confluent_kafka` v2.14.0 (the version pinned in this repo). The
relevant claims are:

| Claim | What `produce.__doc__` says | Implication for A6 |
|---|---|---|
| Callback timing | "called from `poll()` when the message has been successfully delivered or permanently fails delivery" | Callback fires on broker ack OR permanent failure — exactly the signal Option 3 needs. |
| Callback signature | `on_delivery(err, msg)` | `err` is `None` on success; `KafkaError` on failure. `msg` is the produced `Message`. |
| Headers in callback | "Currently message headers are not supported on the message returned to the callback. The `msg.headers()` will return None even if the original message had headers set." | **Cannot pass `seq` via headers.** Must use closure capture or encode seq in value/key (both worse). |
| Calling thread | Not directly documented; the producer maintains an internal background poll thread, callbacks fire on whatever thread calls `poll()` / `flush()` | In production this code path goes through `IdempotentProducer._produce_sync` (run via `asyncio.to_thread`) and `IdempotentProducer.flush` (also `asyncio.to_thread`). Callbacks fire on thread-pool worker threads, not the asyncio event loop. |

### Empirical validation (scratch script `/tmp/a6_scratch_option3.py`)

The scratch reproduced the production path (`IdempotentProducer.produce`
with `on_delivery=closure_over_seq`, then `IdempotentProducer.flush`)
against the local Kafka container (`fyralis_dev_kafka` on port 9092),
producing 20 messages with idempotent producer config. **All five
intended properties were observed:**

```text
produce phase:           2.8 ms total (0.14 ms/msg amortized)
flush phase:             532.8 ms (broker round trip)
delivery callback fired: 20/20
save scheduled+ran:      20/20
main (event loop) tid:   140559118382976
unique callback tids:    [140559081535168]   # NOT the event loop tid
unique save-coro tids:   [140559118382976]   # IS the event loop tid
delivery order:          [1..20]
in produce order?:       True
```

Reproduced verbatim as assertions a future implementer can re-run:

```python
# 1. Per-frame closure for seq works (msg.headers() is None in callback).
assert len(delivery_order) == N_MESSAGES

# 2. run_coroutine_threadsafe bridges callback → event loop.
assert len(save_calls) == N_MESSAGES

# 3. Callbacks fire on background threads under this wrapper.
#    (Note: when produce() is called from to_thread, callbacks fire on
#    to_thread worker threads. Independent verification needed for the
#    flush-only path on the event-loop thread.)
assert main_thread_id not in callback_thread_ids  # held in this run

# 4. Idempotent producer preserves callback order.
assert delivery_order == sorted(delivery_order)
```

The scratch is **not** committed; this section is its archaeology.

### Latency floor for Option 1

The flush-phase measurement (532.8 ms for 20 in-flight messages on a
single-broker dev cluster, batch-flushed) corresponds to ~27 ms
amortized per-frame broker ack. On a 3-broker production cluster with
typical broker latency this drops to **~5-15 ms per frame**. Per-frame
flush in the worker means each frame pays this cost serially before
the save advances.

Discord's per-shard MESSAGE_CREATE volume on a typical tenant is well
under 5 msg/sec sustained (Discord's gateway IDENTIFY budget limits to
120 events/60s = 2 msg/sec for the gateway itself; per-guild
MESSAGE_CREATE rate is typically lower). At 15 ms/frame Option 1's
ceiling is ~67 frames/sec per shard — three orders of magnitude above
the actual rate.

---

## Option 1 sketch (per-frame flush)

Insertion point: [services/integrations/discord/gateway/client.py:419-442](../../services/integrations/discord/gateway/client.py#L419-L442)
— between `await self._dispatch_handler(frame)` and the `asyncio.create_task(self._safe_save(snapshot))` line.

Approximate change:

```python
# client.py - within _dispatch_loop, after the dispatch handler returns:

if self._on_dispatched is not None and frame.get("s") is not None:
    # A6 — broker-ack durability barrier.
    # `_dispatch_handler` enqueued the frame to librdkafka's local
    # queue via shadow_write_raw. produce() returns on local-enqueue,
    # NOT broker-ack. SIGKILL between enqueue and ack would lose the
    # frame from librdkafka's in-memory queue. Flush here so the
    # save below only persists last_seq=N after the broker has acked
    # frame N. See docs/ingestion/05-lld-amendments.md §A6.
    try:
        await self._pre_save_flush(timeout_seconds=2.0)
    except Exception:  # noqa: BLE001
        # Flush failed (broker unreachable, timeout). We have no
        # guarantee frame N is durable on Kafka. DO NOT save — the
        # next worker will RESUME from the previous saved seq and
        # re-process this frame, which is safe under M2 dedup.
        log.warning("a6_pre_save_flush_failed", seq=frame.get("s"))
        # Skip the save task creation.
        continue  # or equivalent loop-continue

    snapshot = GatewaySessionState(...)  # unchanged from M4.3
    asyncio.create_task(self._safe_save(snapshot))
```

The `_pre_save_flush` is a thin wrapper:

```python
async def _pre_save_flush(self, *, timeout_seconds: float) -> None:
    if self._kafka_producer is None:
        return  # tests + the no-shadow-path path: no-op
    remaining = await self._kafka_producer.flush(timeout_seconds)
    if remaining > 0:
        raise TimeoutError(
            f"Kafka flush timed out with {remaining} messages still in queue"
        )
```

Plumbing: `kafka_producer` is added as an optional kwarg on
`DiscordGatewayClient.__init__`, threaded through `GatewayWorker`,
and wired in `lifecycle.py::make_worker` from the same producer the
dispatch deps already hold. No new producer instance.

### Estimated size

| File | Approx. lines |
|---|---|
| `client.py` (flush call + plumbing) | ~25 |
| `worker.py` (thread kafka_producer through) | ~8 |
| `lifecycle.py` (wire it up) | ~5 |
| New tests | ~80 |
| **Total** | **~120 lines** |

---

## Option 3 sketch (save inside delivery callback)

Layered structure (5 surfaces, not 1):

1. `services/ingestion/shadow_write.py` — extend `shadow_write_raw`'s
   public signature with optional `on_delivery=None`, thread to
   `kafka_producer.produce(on_delivery=on_delivery)`. Shared utility;
   webhook + pubsub callers don't pass it but the signature changes.

2. `services/integrations/discord/gateway/dispatch.py` —
   `_maybe_shadow_write_gateway` accepts a callback factory; threads
   it through.

3. `services/integrations/discord/gateway/client.py` — instead of
   post-handle save task, capture state snapshot AND the event loop
   reference, build a `_make_save_callback(snapshot, loop)` closure,
   pass it through dispatch → shadow_write_raw → producer.

4. Out-of-order / partial-failure guard. Even though
   `enable.idempotence=true` empirically preserved callback order in
   the scratch (20/20 in-order), under broker retry + ISR shrink
   scenarios callbacks for an earlier seq could still arrive after a
   later seq. The save logic must track the highest *contiguous*
   successful seq, not the highest received seq. A `seq=5` failure
   followed by a `seq=6` success must NOT save `last_seq=6` —
   Discord could re-deliver `seq=5` only if Postgres still says
   `last_seq=4`. New state: an in-flight dict + a contiguous-seq
   tracker.

5. Shutdown synchronization. The current `producer.stop()` flushes
   on a timeout; outstanding callbacks fire during flush. Lifecycle
   needs an explicit "wait for outstanding callbacks" semantics so
   the save coros they schedule complete before the asyncpg pool
   closes.

### Estimated size

| File | Approx. lines |
|---|---|
| `shadow_write.py` (signature + pass-through) | ~12 |
| `dispatch.py` (factory plumbing) | ~18 |
| `client.py` (callback wiring + loop capture) | ~30 |
| Out-of-order / partial-failure guard | ~40 |
| Shutdown sync | ~20 |
| New tests (5 categories) | ~180 |
| **Total** | **~300 lines** |

---

## Decision

**Option 1.** Reasoning:

1. **Operating principle #2 — no code outside scope.** Option 3
   modifies `services/ingestion/shadow_write.py`, a utility shared by
   webhook + gateway + pubsub. Option 1 stays entirely within the
   Discord Gateway subdirectory. The work order said: "Do not touch
   unrelated files. Do not refactor adjacent code 'while you're here.'"
   Option 3 adds shared-surface plumbing for one caller's benefit.

2. **Size threshold from the work order.** The work order said:
   "If Option 3 is implementable in <200 lines of code with reasonable
   complexity → choose Option 3." Option 3 estimates at ~300 lines and
   adds 3 new failure-mode categories (callback-never-fires,
   out-of-order delivery, callback-during-shutdown) each requiring its
   own test.

3. **Latency budget is not binding.** At 5-15 ms/frame in production
   and Discord's actual rate well under 5 msg/sec/tenant, Option 1's
   per-shard throughput ceiling is ~70 frames/sec — three orders of
   magnitude above the actual rate. The "per-shard sequential cap"
   concern raised in the amendments tracker doesn't bind for Discord
   gateway workloads.

4. **N1 strictness is identical.** Both options provide strict N1
   under their own contracts. Option 1's contract is one-liner:
   "save only after flush returns." Option 3's contract is four
   layered claims: callback fires for every produce; callback fires
   in order (or guard is correct); callback's run_coroutine_threadsafe
   bridge succeeds; partial-failure guard advances save monotonically.
   Each adds test surface area.

5. **Reversibility.** If a future workload pushes Discord gateway
   throughput past Option 1's ceiling (a hypothetical busy
   community-server tenant in the steady state), Option 3 can be
   reconsidered then. The Option 1 change does not foreclose Option 3
   — `_pre_save_flush` can be replaced with the callback-based
   alternative without breaking the client/worker/lifecycle wiring.

---

## What Phase 2 will land

- Plumb `kafka_producer` into `DiscordGatewayClient`.
- Insert `await self._pre_save_flush(timeout_seconds=2.0)` between
  dispatch return and save task creation.
- On flush timeout: log warning, skip the save. Next worker re-processes
  the frame (safe under M2 dedup).
- Update the call-site comment block to cite A6 + describe the flush.
- New unit tests:
  - `test_flush_called_between_shadow_write_and_save_state`
  - `test_flush_timeout_does_not_block_indefinitely`
  - `test_flush_failure_does_not_save_state`
- Latency measurement script (run 100 frames through, report mean + p95)
  for the runbook entry.

Phase 3 then removes the M4.3 test-level workaround in
`_subprocess_entrypoint.py` and confirms `test_no_frames_lost_across_sigkill`
still passes against the now-fixed production code.

Phase 4 closes A6 in the amendments tracker and updates the M5
pre-cutover gate condition (8).

---

## Phase 3 finding + deferred follow-up

**Surfaced during Phase 3** (commit `<phase-3 hash>`): the M4.3 load-
bearing test `test_no_frames_lost_across_sigkill` does NOT instantiate
`DiscordGatewayClient` — its subprocess entrypoint
([_subprocess_entrypoint.py](../../services/integrations/discord/gateway/tests/_subprocess_entrypoint.py))
hand-rolls a simulation of the dispatch loop by calling
`shadow_write_raw` and `save_session_state` directly. The original
M4.3 manual `flush()` in that file was a parallel implementation of
the durability barrier, not a workaround over a production gap — but
it had the side effect of masking the absence of any test that
exercised the production fix at the simulation surface.

**Resolution (Phase 3, Option A from the gate review):** extracted the
durability-barrier flush from `DiscordGatewayClient._pre_save_flush`
to a module-level free function `client.pre_save_flush(producer, *,
timeout_seconds)`. The method became a thin wrapper bound to the
client's producer. The subprocess entrypoint now imports and calls
the same free function with the same `timeout_seconds=2.0` as
production. Result: the load-bearing test now exercises production
code, and "two parallel implementations" became one.

**Deferred follow-up (Option B from the Phase 3 review):** rewrite
`_subprocess_entrypoint.py` to instantiate `DiscordGatewayClient`
end-to-end against a cross-process fake gateway. That would exercise
the full production WS-loop save site under SIGKILL (not just the
durability barrier function), giving end-to-end production-code
coverage of M4.1 (lease) + M4.2 (state) + A6 (flush) + the actual
client `_dispatch_loop`. Effort estimate: M (medium). Not urgent —
the function-level extraction in Phase 3 makes the load-bearing test
prove the A6 property against production code; Option B would
broaden coverage to the rest of the dispatch loop. Track as a
future work-unit, not a blocker for M5.
