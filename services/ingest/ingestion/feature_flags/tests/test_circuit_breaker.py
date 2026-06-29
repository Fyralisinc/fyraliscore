"""M5.1 — Ingestion cutover circuit breaker tests.

Test categories:

  1. State machine unit tests (in-process, fresh_db, injected mock
     Kafka readers). Drive `_process_tick` deterministically.

  2. Pool-config check (assertion that `make_breaker_pool` produces
     the same pgbouncer-compatible shape as M3.1 / M4.2).

  3. SUBPROCESS test: real `python -m services.ingest.ingestion.feature_flags`
     with synthetic Kafka injection via env vars; SIGTERM the
     subprocess after some ticks; restart; assert breach-window
     state survived across the SIGTERM → restart cycle.
     **LOAD-BEARING — mirrors M3.3's test_backlog_service_resumes_from_cursor
     and M4.3's test_no_frames_lost_across_sigkill.**

The tests do NOT instantiate Temporal — M5.1 ships as an asyncio
service (Option B from the M5 Phase 0 finding); M-Temporal will
port to Temporal Schedule later.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.db import configure_connection_timeouts
from services.ingest.ingestion.feature_flags.circuit_breaker import (
    BreakerConfig,
    _process_tick,
    _TenantBreachState,
    _load_state,
    get_metrics,
    make_breaker_pool,
    reset_metrics,
)
from services.ingest.ingestion.feature_flags.client import (
    KAFKA_PATH_ENABLED,
    TenantFlags,
)


pytestmark = [pytest.mark.timeout(120)]


@pytest.fixture(autouse=True)
def _reset_breaker_metrics() -> None:
    reset_metrics()


# =====================================================================
# Helpers.
# =====================================================================

async def _seed_tenant(pool: asyncpg.Pool, name: str | None = None) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, name or f"breaker-test-{tid.hex[:8]}",
    )
    return tid


def _make_lag_fn(lag_per_partition: dict[int, float], source: str = "slack"):
    """Wrap a single-lane ``{partition: lag}`` map as the per-source reader the
    breaker now expects: ``{source: {partition: lag}}``. Defaulting to one
    source keeps the single-lane state-machine tests focused; the multi-source
    worst-case behaviour has its own test (`_make_lag_fn_multi`)."""
    async def _f(**_kwargs: Any) -> dict[str, dict[int, float]]:
        return {source: dict(lag_per_partition)}
    return _f


def _make_lag_fn_multi(lag_by_source: dict[str, dict[int, float]]):
    """Explicit per-source lag reader: ``{source: {partition: lag}}``."""
    async def _f(**_kwargs: Any) -> dict[str, dict[int, float]]:
        return {s: dict(parts) for s, parts in lag_by_source.items()}
    return _f


def _make_active_fn(active: dict[UUID, int], source: str = "slack"):
    """Wrap ``{tenant: partition}`` as ``{tenant: {source: partition}}``."""
    async def _f(**_kwargs: Any) -> dict[UUID, dict[str, int]]:
        return {tid: {source: part} for tid, part in active.items()}
    return _f


def _make_active_fn_multi(active: dict[UUID, dict[str, int]]):
    """Explicit per-source active map: ``{tenant: {source: partition}}``."""
    async def _f(**_kwargs: Any) -> dict[UUID, dict[str, int]]:
        return {tid: dict(lanes) for tid, lanes in active.items()}
    return _f


def _make_alert_recorder() -> tuple[list, Any]:
    """Returns (alerts_received_list, alert_fn)."""
    alerts: list[tuple[UUID, dict]] = []

    async def _alert(tenant_id: UUID, payload: dict[str, Any]) -> None:
        alerts.append((tenant_id, payload))
    return alerts, _alert


async def _read_flag(
    pool: asyncpg.Pool, tenant_id: UUID, flag_name: str = KAFKA_PATH_ENABLED,
) -> bool | None:
    row = await pool.fetchrow(
        "SELECT flag_value FROM tenant_flags WHERE tenant_id = $1 AND flag_name = $2",
        tenant_id, flag_name,
    )
    return row["flag_value"] if row is not None else None


_INSTANCE = "m5-test"


def _config(**overrides: Any) -> BreakerConfig:
    base = {
        "instance_name": _INSTANCE,
        "tick_interval_sec": 0.01,    # tests don't sleep
        "breach_threshold_sec": 60,
        "breach_window_ticks": 5,
        "normalizer_group_base": "normalizer",
        "signal_topic": "ingestion.tenant_traffic_signal",
        "signal_lookback_sec": 90,
        "kafka_bootstrap": "irrelevant-for-test",
    }
    base.update(overrides)
    return BreakerConfig(**base)


# =====================================================================
# 1. Trips on sustained lag.  LOAD-BEARING (state-observable in PG).
# =====================================================================

async def test_breaker_trips_on_sustained_lag(fresh_db: asyncpg.Pool) -> None:
    """5 consecutive ticks of lag>60s on the tenant's partition → flag
    flipped to FALSE in `tenant_flags` AND `tripped=TRUE` in
    `circuit_breaker_state` AND an alert was emitted.

    The flag flip is asserted via direct Postgres SELECT — observable
    state, not internal call order (matches the A6 Phase 1
    reinforcement pattern).
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-a")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()

    state: dict[UUID, _TenantBreachState] = {}
    lag_fn = _make_lag_fn({0: 120.0})           # partition 0 is breached
    active_fn = _make_active_fn({tenant_a: 0})  # tenant_a's traffic lands on partition 0

    # Five consecutive breaching ticks → trip on the 5th.
    for i in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=lag_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    # ---- Observable state #1: flag flipped to FALSE in tenant_flags ----
    flag = await _read_flag(fresh_db, tenant_a)
    assert flag is False, (
        f"After 5 consecutive breached ticks, tenant_flags.flag_value "
        f"for {tenant_a} is {flag}; expected False. The circuit "
        f"breaker did not flip the flag — N1 cutover-safety violated."
    )

    # ---- Observable state #2: tripped=TRUE in circuit_breaker_state ----
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert tenant_a in loaded
    assert loaded[tenant_a].tripped is True
    assert loaded[tenant_a].tripped_at is not None
    assert loaded[tenant_a].consecutive_breach_ticks >= config.breach_window_ticks

    # ---- Observable state #3: set_by audit field is the breaker ----
    row = await fresh_db.fetchrow(
        "SELECT set_by FROM tenant_flags WHERE tenant_id = $1 AND flag_name = $2",
        tenant_a, KAFKA_PATH_ENABLED,
    )
    assert row is not None
    assert row["set_by"] == "auto:circuit_breaker"

    # ---- Internal-call assertion: alert emitted exactly once ----
    assert len(alerts) == 1
    assert alerts[0][0] == tenant_a
    assert alerts[0][1]["lag_seconds"] == 120.0


# =====================================================================
# 2. Brief spike does NOT trip.
# =====================================================================

async def test_breaker_does_not_trip_on_brief_spike(
    fresh_db: asyncpg.Pool,
) -> None:
    """Lag>60s for 2 ticks then drops to 5s for the rest. The
    "5 consecutive" requirement means the counter resets on tick 3,
    no trip happens.
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-spike")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    # Breach for 2 ticks…
    breach_fn = _make_lag_fn({0: 120.0})
    healthy_fn = _make_lag_fn({0: 5.0})
    active_fn = _make_active_fn({tenant_a: 0})

    for _ in range(2):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=breach_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )
    # …then recover for 5 ticks.
    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=healthy_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    # ---- Observable state: flag UNCHANGED (no row exists; default ----
    # behaviour is "missing row" which the FlagCache treats as default).
    flag = await _read_flag(fresh_db, tenant_a)
    assert flag is None, (
        f"After a brief spike + recovery, tenant_flags row exists "
        f"(flag_value={flag}). The circuit breaker flipped a flag "
        f"that should have stayed at its default."
    )

    # ---- Observable state: counter reset to 0 after recovery ----
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].consecutive_breach_ticks == 0
    assert loaded[tenant_a].tripped is False

    assert len(alerts) == 0


# =====================================================================
# 3. Per-tenant isolation.
# =====================================================================

async def test_breaker_per_tenant_isolation(
    fresh_db: asyncpg.Pool,
) -> None:
    """Tenant A's partition is breached; tenant B's partition is
    healthy. Only A's flag flips; B is unaffected.
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-iso-a")
    tenant_b = await _seed_tenant(fresh_db, "tenant-iso-b")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    # Partition 0 breached, partition 1 healthy.
    lag_fn = _make_lag_fn({0: 120.0, 1: 5.0})
    # Tenant A → partition 0; tenant B → partition 1.
    active_fn = _make_active_fn({tenant_a: 0, tenant_b: 1})

    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=lag_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    # Tenant A: tripped + flag flipped.
    assert await _read_flag(fresh_db, tenant_a) is False
    # Tenant B: no flag row at all.
    assert await _read_flag(fresh_db, tenant_b) is None

    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is True
    assert loaded[tenant_b].tripped is False
    assert loaded[tenant_b].consecutive_breach_ticks == 0

    # Only A's alert fired.
    assert len(alerts) == 1
    assert alerts[0][0] == tenant_a


# =====================================================================
# 4. No auto-recovery.  LOAD-BEARING (operator-only re-enable).
# =====================================================================

async def test_breaker_does_not_auto_recover(
    fresh_db: asyncpg.Pool,
) -> None:
    """Tenant tripped at tick 5. Lag drops to 0 for the next 20 ticks.
    The flag stays FALSE; the breaker does NOT auto-flip back. Only
    an operator clearing `tripped=FALSE` and re-flipping the flag
    can re-enable the Kafka path.
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-no-recover")
    flags = TenantFlags(fresh_db)
    _, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    breach_fn = _make_lag_fn({0: 120.0})
    healthy_fn = _make_lag_fn({0: 5.0})
    active_fn = _make_active_fn({tenant_a: 0})

    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=breach_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )
    # Trip is confirmed.
    assert await _read_flag(fresh_db, tenant_a) is False

    # Now run 20 ticks of healthy lag.
    for _ in range(20):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=healthy_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    # Flag is STILL false. tripped is STILL true. No auto-recovery.
    assert await _read_flag(fresh_db, tenant_a) is False
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is True


# =====================================================================
# 5. Pool uses pgbouncer_compatible config.
# =====================================================================

async def test_breaker_uses_pgbouncer_compatible_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`make_breaker_pool` MUST construct an asyncpg.Pool with
    `statement_cache_size=0` — the fourth activation of M1.3's
    ADR Q1 pgbouncer-transaction-mode flag (after M3.1 DLQ writer,
    M3.3 backlog drainer, M4.2 session-state pool).
    """
    captured: dict[str, Any] = {}

    async def _spy(dsn: str, **kwargs: Any) -> Any:
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        return object()  # sentinel; we only inspect the args

    monkeypatch.setattr(asyncpg, "create_pool", _spy)
    await make_breaker_pool("postgresql://x@y/z")

    assert captured["kwargs"]["statement_cache_size"] == 0, (
        f"make_breaker_pool did NOT set statement_cache_size=0; "
        f"got {captured['kwargs'].get('statement_cache_size')}. "
        f"Will not be pgbouncer-compatible in transaction mode."
    )
    assert "min_size" in captured["kwargs"]
    assert "max_size" in captured["kwargs"]
    assert captured["kwargs"]["init"] is configure_connection_timeouts


# =====================================================================
# 6. Tripped state freezes the counter (no double-trip).
# =====================================================================

async def test_breaker_tripped_state_freezes_counter(
    fresh_db: asyncpg.Pool,
) -> None:
    """After a trip, additional breaching ticks must NOT re-trigger
    the alert or re-fire `set_bool`. The state row's last_tick_at
    is still updated (for stale-state GC) but the counter and
    tripped flag stay put.
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-frozen")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}
    lag_fn = _make_lag_fn({0: 120.0})
    active_fn = _make_active_fn({tenant_a: 0})

    # Trip via 5 breaching ticks.
    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=lag_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )
    assert len(alerts) == 1

    # 10 more breaching ticks — should NOT re-fire the alert.
    for _ in range(10):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=lag_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    assert len(alerts) == 1, (
        f"After trip, additional breaching ticks re-fired the alert. "
        f"Got {len(alerts)} alerts; expected 1."
    )


# =====================================================================
# 7. Non-cutover filter: tenants with flag=FALSE are skipped.
# =====================================================================

async def test_breaker_skips_tenants_with_flag_already_disabled(
    fresh_db: asyncpg.Pool,
) -> None:
    """A tenant whose ingestion.kafka_path_enabled is already FALSE
    (e.g. pre-cutover or operator-disabled) must NOT be tracked by the
    breaker — no state row created, no flag re-write, no alert.

    Re-writing FALSE-on-FALSE would clobber the existing set_by audit
    (e.g. an "operator:alice" flip becoming "auto:circuit_breaker").
    """
    tenant_disabled = await _seed_tenant(fresh_db, "tenant-disabled")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    # Operator (or earlier op) set the flag FALSE with an audit field
    # we want to PRESERVE across breaker ticks.
    await flags.set_bool(
        tenant_disabled, KAFKA_PATH_ENABLED, False,
        set_by="operator:alice", note="pre-cutover hold",
    )

    lag_fn = _make_lag_fn({0: 120.0})
    active_fn = _make_active_fn({tenant_disabled: 0})

    for _ in range(10):  # well past the 5-tick window
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=lag_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    # ---- Audit row preserved: set_by is still operator:alice ----
    row = await fresh_db.fetchrow(
        "SELECT flag_value, set_by FROM tenant_flags "
        "WHERE tenant_id = $1 AND flag_name = $2",
        tenant_disabled, KAFKA_PATH_ENABLED,
    )
    assert row["flag_value"] is False
    assert row["set_by"] == "operator:alice", (
        f"Breaker overwrote operator audit field: set_by is {row['set_by']!r}; "
        f"expected 'operator:alice'. The flag-disabled filter is missing."
    )

    # ---- No state row created — tenant was filtered out ----
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert tenant_disabled not in loaded, (
        f"Breaker created state row for a flag-disabled tenant: "
        f"{loaded.get(tenant_disabled)}. Non-cutover filter is missing."
    )
    # No alert.
    assert len(alerts) == 0
    # Metric counted the skip.
    assert get_metrics()["breaker.skipped_flag_disabled"] >= 1


# =====================================================================
# 8. Operator re-enable auto-resets breaker bookkeeping.
# =====================================================================

async def test_breaker_resets_bookkeeping_on_operator_reenable(
    fresh_db: asyncpg.Pool,
) -> None:
    """After a trip, an operator manually flips the flag back to TRUE.
    On the next breaker tick, the state row's tripped/counter must
    reset so a future sustained breach can re-trip — without this,
    a forgotten state-row cleanup leaves the breaker blind to the
    tenant forever.

    Behaviour under test (option 2 of the M5.1 verification):
      1. Trip via 5 breached ticks.
      2. Operator flips flag back to TRUE (set_by="operator:bob").
      3. Run one tick with lag healthy. Breaker observes flag=TRUE +
         existing tripped state → resets bookkeeping. Tenant traffic
         is healthy so no re-trip on this tick.
      4. Run 5 more breached ticks. Breaker re-trips (counter was 0).
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-reenable")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    breach_fn = _make_lag_fn({0: 120.0})
    healthy_fn = _make_lag_fn({0: 5.0})
    active_fn = _make_active_fn({tenant_a: 0})

    # ---- Step 1: trip the breaker. ----
    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=breach_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )
    assert await _read_flag(fresh_db, tenant_a) is False
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is True
    assert len(alerts) == 1

    # ---- Step 2: operator manually re-enables. ----
    await flags.set_bool(
        tenant_a, KAFKA_PATH_ENABLED, True,
        set_by="operator:bob", note="recovered, re-enabling",
    )

    # ---- Step 3: one healthy tick. Breaker resets bookkeeping. ----
    await _process_tick(
        config=config, pool=fresh_db, tenant_flags=flags,
        state=state, measure_lag_fn=healthy_fn,
        active_tenants_fn=active_fn, alert_fn=alert_fn,
    )

    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is False, (
        f"Operator re-enabled flag but breaker still says tripped=True: "
        f"{loaded[tenant_a]}. Auto-reset on operator re-enable is missing."
    )
    assert loaded[tenant_a].consecutive_breach_ticks == 0
    assert loaded[tenant_a].tripped_at is None
    # Metric counted the reset.
    assert get_metrics()["breaker.bookkeeping_reset_on_operator_reenable"] == 1
    # Flag must still reflect the operator's TRUE flip — breaker did
    # NOT touch the flag during the reset.
    row = await fresh_db.fetchrow(
        "SELECT flag_value, set_by FROM tenant_flags "
        "WHERE tenant_id = $1 AND flag_name = $2",
        tenant_a, KAFKA_PATH_ENABLED,
    )
    assert row["flag_value"] is True
    assert row["set_by"] == "operator:bob"

    # ---- Step 4: 5 more breached ticks → breaker re-trips. ----
    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=breach_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )
    assert await _read_flag(fresh_db, tenant_a) is False, (
        "Breaker did not re-trip after operator re-enable + sustained "
        "breach. Bookkeeping reset may be incomplete (counter not at 0)."
    )
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is True
    assert len(alerts) == 2  # original trip + re-trip


# =====================================================================
# 9. State survives a real subprocess SIGTERM + restart.
#    LOAD-BEARING — mirrors M3.3's test_backlog_service_resumes_from_cursor.
# =====================================================================

async def test_breaker_state_survives_restart(fresh_db: asyncpg.Pool) -> None:
    """Real subprocess SIGTERM and restart. The breach-window counter
    persists across the process death so a SIGTERM at counter=3 does
    NOT reset the counter to 0 on restart.

    Test shape:
      - Seed one tenant.
      - Run subprocess A with fake lag = 120s on partition 0 and the
        seeded tenant on partition 0. tick_interval=0.5s, window=5.
      - Wait until breach counter reaches 3 in
        circuit_breaker_state.
      - SIGTERM subprocess A.
      - Restart subprocess B with the SAME fake inputs.
      - Wait until the flag flips to FALSE in tenant_flags.
      - Assert: subprocess B completed the trip — i.e. only 2 more
        ticks (not 5) were needed because counter was preserved at
        3 from subprocess A.
    """
    tenant_id = await _seed_tenant(fresh_db, "subproc-test")
    instance_name = f"subproc-{tenant_id.hex[:8]}"

    fake_lag = json.dumps({"slack": {"0": 120.0}})
    fake_active = json.dumps({str(tenant_id): {"slack": 0}})

    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["BREAKER_INSTANCE_NAME"] = instance_name
    env["BREAKER_TICK_INTERVAL_SEC"] = "0.3"
    env["BREAKER_THRESHOLD_SEC"] = "60"
    env["BREAKER_WINDOW_TICKS"] = "5"
    env["M5_BREAKER_FAKE_LAG_PARTITIONS"] = fake_lag
    env["M5_BREAKER_FAKE_ACTIVE_TENANTS"] = fake_active
    env["CIRCUIT_BREAKER_LOG_LEVEL"] = "WARNING"

    # ---- Run 1: let counter reach 3, then SIGTERM. -------------------
    proc_a = subprocess.Popen(
        [sys.executable, "-m", "services.ingest.ingestion.feature_flags"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 20.0
        observed_counter = 0
        while time.monotonic() < deadline:
            state = await _load_state(fresh_db, instance_name)
            entry = state.get(tenant_id)
            if entry is not None and entry.consecutive_breach_ticks >= 3:
                observed_counter = entry.consecutive_breach_ticks
                break
            await asyncio.sleep(0.1)
        else:
            proc_a.kill()
            proc_a.wait(timeout=5)
            raise AssertionError(
                f"subprocess A did not reach counter>=3 within 20s "
                f"(got {observed_counter}). "
                f"stderr: {proc_a.stderr.read().decode()[:500]}"
            )

        # SIGTERM and confirm clean exit.
        proc_a.send_signal(signal.SIGTERM)
        rc = proc_a.wait(timeout=15)
        assert rc == 0, (
            f"subprocess A exit code {rc}; "
            f"stderr: {proc_a.stderr.read().decode()[:500]}"
        )
    finally:
        if proc_a.poll() is None:
            proc_a.kill()
            proc_a.wait(timeout=5)

    # ---- Confirm state was persisted before SIGTERM. -----------------
    state_after_a = await _load_state(fresh_db, instance_name)
    counter_after_a = state_after_a[tenant_id].consecutive_breach_ticks
    assert counter_after_a >= 3, (
        f"After subprocess A's SIGTERM, counter is {counter_after_a}; "
        f"expected >=3 (subprocess A had reached >=3 before SIGTERM)."
    )
    # Flag NOT yet flipped — subprocess A was killed before tick 5.
    assert await _read_flag(fresh_db, tenant_id) is None, (
        "Flag flipped during subprocess A's run — A reached tick 5 "
        "before the SIGTERM landed. Reduce the breach_window_ticks "
        "test parameter or speed up SIGTERM."
    )

    # ---- Run 2: restart subprocess; trip completes. ------------------
    proc_b = subprocess.Popen(
        [sys.executable, "-m", "services.ingest.ingestion.feature_flags"],
        env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    try:
        # The flag should flip within at most (5 - counter_after_a) ticks.
        # tick_interval=0.3s so 5 ticks = 1.5s. Generous deadline.
        deadline = time.monotonic() + 15.0
        flipped = False
        while time.monotonic() < deadline:
            if await _read_flag(fresh_db, tenant_id) is False:
                flipped = True
                break
            await asyncio.sleep(0.1)
        if not flipped:
            proc_b.kill()
            proc_b.wait(timeout=5)
            raise AssertionError(
                f"subprocess B did not flip the flag within 15s. "
                f"Counter after A was {counter_after_a}; subprocess B "
                f"should have needed only {5 - counter_after_a} more "
                f"ticks. State preservation across SIGTERM is broken. "
                f"stderr: {proc_b.stderr.read().decode()[:500]}"
            )

        # Cleanly stop subprocess B.
        proc_b.send_signal(signal.SIGTERM)
        rc = proc_b.wait(timeout=15)
        assert rc == 0
    finally:
        if proc_b.poll() is None:
            proc_b.kill()
            proc_b.wait(timeout=5)

    # ---- Final assertions: trip is durable + audit row is correct ----
    final_state = await _load_state(fresh_db, instance_name)
    assert final_state[tenant_id].tripped is True
    assert await _read_flag(fresh_db, tenant_id) is False

    row = await fresh_db.fetchrow(
        "SELECT set_by FROM tenant_flags WHERE tenant_id = $1 AND flag_name = $2",
        tenant_id, KAFKA_PATH_ENABLED,
    )
    assert row["set_by"] == "auto:circuit_breaker"


# =====================================================================
# 10. Worst-case across source lanes: a tenant lagging on ONE source
#     lane trips even while healthy on another (source isolation).
#     This is the behaviour the source-isolation follow-up added —
#     before it, a single legacy `ingestion.raw` topic was measured and
#     the breaker was inert post per-source split.
# =====================================================================

async def test_breaker_trips_on_worst_source_lane(
    fresh_db: asyncpg.Pool,
) -> None:
    """A tenant active on two source lanes — `github` healthy (5s) but
    `slack` breached (120s) — trips on the worst lane. The alert records
    the breached source, and a tenant healthy on ALL its lanes never trips.
    """
    tenant_worst = await _seed_tenant(fresh_db, "tenant-worst-lane")
    tenant_healthy = await _seed_tenant(fresh_db, "tenant-all-healthy")
    flags = TenantFlags(fresh_db)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    # slack lane partition 0 is breached; github lane partition 0 healthy.
    lag_fn = _make_lag_fn_multi({
        "slack": {0: 120.0},
        "github": {0: 5.0},
    })
    active_fn = _make_active_fn_multi({
        # active on BOTH lanes — the worst (slack) governs the decision.
        tenant_worst: {"slack": 0, "github": 0},
        # active only on the healthy github lane.
        tenant_healthy: {"github": 0},
    })

    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=lag_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    # Worst-lane tenant tripped; the all-healthy tenant did not.
    assert await _read_flag(fresh_db, tenant_worst) is False, (
        "Tenant lagging on its slack lane did not trip — worst-case "
        "across source lanes is not being computed."
    )
    assert await _read_flag(fresh_db, tenant_healthy) is None, (
        "Tenant healthy on all its active lanes was tripped — false trip."
    )

    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_worst].tripped is True
    assert loaded[tenant_healthy].consecutive_breach_ticks == 0
    assert loaded[tenant_healthy].tripped is False

    # Exactly one alert, naming the breached lane + its lag.
    assert len(alerts) == 1
    assert alerts[0][0] == tenant_worst
    assert alerts[0][1]["source"] == "slack"
    assert alerts[0][1]["lag_seconds"] == 120.0
    assert alerts[0][1]["active_lanes"] == 2


# =====================================================================
# 11. Hardening regressions (ingestion-hardening program).
# =====================================================================

# --- #14: blocking Kafka probes must not run on the event loop. --------

async def test_lag_probe_offloaded_to_worker_thread(monkeypatch: Any) -> None:
    """#14 regression. `_measure_kafka_lag_default` is awaited directly from
    the breaker tick that also runs the heartbeat ticker; its body is blocking
    confluent_kafka C-calls. It MUST offload to a worker thread (asyncio.
    to_thread) so a slow broker can't wedge the event loop (and thus the
    /healthz heartbeat). We assert the sync body executes off the main thread.
    """
    import threading
    from services.ingest.ingestion.feature_flags import circuit_breaker as cb

    main_thread = threading.current_thread()
    seen: dict[str, Any] = {}

    def _fake_sync(*, bootstrap: str, normalizer_group_base: str):
        seen["thread"] = threading.current_thread()
        seen["args"] = (bootstrap, normalizer_group_base)
        return {"slack": {0: 1.5}}

    monkeypatch.setattr(cb, "_measure_kafka_lag_sync", _fake_sync)
    result = await cb._measure_kafka_lag_default(
        bootstrap="b", normalizer_group_base="normalizer",
    )
    assert result == {"slack": {0: 1.5}}
    assert seen["args"] == ("b", "normalizer")
    assert seen["thread"] is not main_thread, (
        "Kafka lag probe ran on the event-loop thread — blocking confluent "
        "calls would stall the breaker heartbeat (#14)."
    )


async def test_active_sampler_offloaded_to_worker_thread(monkeypatch: Any) -> None:
    """#14 regression for the active-tenant sampler — same rationale."""
    import threading
    from services.ingest.ingestion.feature_flags import circuit_breaker as cb

    main_thread = threading.current_thread()
    seen: dict[str, Any] = {}

    def _fake_sync(*, bootstrap: str, signal_topic: str, lookback_sec: int):
        seen["thread"] = threading.current_thread()
        return {}

    monkeypatch.setattr(cb, "_sample_active_tenants_sync", _fake_sync)
    result = await cb._sample_active_tenants_default(
        bootstrap="b", signal_topic="t", lookback_sec=90,
    )
    assert result == {}
    assert seen["thread"] is not main_thread, (
        "Active-tenant sampler ran on the event-loop thread (#14)."
    )


# --- #13: sampler must drain the read budget, not break on first empty poll.

def test_active_sampler_keeps_polling_past_empty_poll(monkeypatch: Any) -> None:
    """#13 regression. `confluent Consumer.poll()` returning None means "no
    message delivered in this poll window", NOT "end of partition". Breaking
    on the first None silently dropped tenants that emitted earlier in the
    lookback window. The sampler must keep polling until its deadline. We feed
    a poll sequence [msg_A, None, msg_B, None, None] and assert BOTH tenants
    are captured (the old `break` would have stopped after msg_A).
    """
    pytest.importorskip("confluent_kafka")
    from services.ingest.ingestion.feature_flags.circuit_breaker import (
        _sample_active_tenants_sync,
    )

    tenant_a, tenant_b = uuid4(), uuid4()

    class _FakeMsg:
        def __init__(self, tenant_id: UUID, source: str, raw_partition: int):
            self._payload = json.dumps({
                "tenant_id": str(tenant_id),
                "source": source,
                "raw_partition": raw_partition,
            }).encode()

        def error(self):  # noqa: ANN202
            return None

        def timestamp(self):  # noqa: ANN202
            return (1, 950_000)  # > cutoff_ms below, so not skipped

        def value(self):  # noqa: ANN202
            return self._payload

    poll_seq = iter([
        _FakeMsg(tenant_a, "slack", 0),
        None,
        _FakeMsg(tenant_b, "github", 1),
        None,
        None,
    ])

    class _FakePartMeta:  # noqa: D401
        pass

    class _FakeTopicMeta:
        def __init__(self) -> None:
            self.error = None
            self.partitions = {0: _FakePartMeta()}

    class _FakeClusterMeta:
        def __init__(self, topic: str) -> None:
            self.topics = {topic: _FakeTopicMeta()}

    class _FakeTP:
        def __init__(self, topic: str, partition: int, offset: int = 0):
            self.topic = topic
            self.partition = partition
            self.offset = offset

    class _FakeConsumer:
        def __init__(self, _conf: dict) -> None:
            pass

        def list_topics(self, topic: str, timeout: float = 0.0):  # noqa: ANN202
            return _FakeClusterMeta(topic)

        def offsets_for_times(self, tps, timeout: float = 0.0):  # noqa: ANN202
            return [_FakeTP(tp.topic, tp.partition, 0) for tp in tps]

        def assign(self, _tps) -> None:
            pass

        def poll(self, timeout: float = 0.0):  # noqa: ANN202
            return next(poll_seq)

        def close(self) -> None:
            pass

    monkeypatch.setattr("confluent_kafka.Consumer", _FakeConsumer, raising=False)
    monkeypatch.setattr("confluent_kafka.TopicPartition", _FakeTP, raising=False)
    # Deterministic, fast clock: fix wall time (cutoff) and step the monotonic
    # deadline clock so the 5s read budget yields exactly 5 polls then exits.
    monkeypatch.setattr("time.time", lambda: 1000.0)
    _mono = iter([100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 106.0])
    monkeypatch.setattr("time.monotonic", lambda: next(_mono))

    out = _sample_active_tenants_sync(
        bootstrap="b", signal_topic="ingestion.tenant_traffic_signal",
        lookback_sec=90,
    )

    assert out == {tenant_a: {"slack": 0}, tenant_b: {"github": 1}}, (
        "Sampler dropped a tenant that emitted after an empty poll — the "
        "break-on-None truncation (#13) has regressed."
    )


# --- #18: a failed flag flip must be retried, not self-reset to "re-enabled".

class _FlipFailingFlags(TenantFlags):
    """TenantFlags whose breaker auto-flip (value=False, set_by=auto:...)
    raises for the first `fail_times` attempts — simulating a Postgres /
    TenantFlags outage during the trip."""

    def __init__(self, pool: asyncpg.Pool, *, fail_times: int) -> None:
        super().__init__(pool)
        self._fail_times = fail_times
        self.flip_attempts = 0

    async def set_bool(  # type: ignore[override]
        self, tenant_id: UUID, flag_name: str, value: bool, *,
        set_by: str, note: str | None = None,
    ) -> None:
        if value is False and set_by == "auto:circuit_breaker":
            self.flip_attempts += 1
            if self.flip_attempts <= self._fail_times:
                raise RuntimeError("simulated TenantFlags outage during flip")
        return await super().set_bool(
            tenant_id, flag_name, value, set_by=set_by, note=note,
        )


async def test_breaker_retries_flip_after_flag_flip_failure(
    fresh_db: asyncpg.Pool,
) -> None:
    """#18 regression. If `set_bool` fails on the trip (DB outage), the breaker
    must NOT mark the tenant tripped — otherwise the next tick sees flag=TRUE
    (flip never happened) + tripped=TRUE and misreads it as an *operator
    re-enable*, resetting the breach counter and letting the lagging tenant
    evade the breaker for the entire outage. The fix flips first and only
    records tripped on success, leaving the counter pinned so the next tick
    RETRIES the flip. This test would fail on the pre-fix code (flag never
    flips because the counter is reset every tick).
    """
    tenant_a = await _seed_tenant(fresh_db, "tenant-flip-fail")
    flags = _FlipFailingFlags(fresh_db, fail_times=1)
    alerts, alert_fn = _make_alert_recorder()
    config = _config()
    state: dict[UUID, _TenantBreachState] = {}

    breach_fn = _make_lag_fn({0: 120.0})
    active_fn = _make_active_fn({tenant_a: 0})

    # ---- 5 breached ticks → window reached on tick 5; the flip raises. ----
    for _ in range(5):
        await _process_tick(
            config=config, pool=fresh_db, tenant_flags=flags,
            state=state, measure_lag_fn=breach_fn,
            active_tenants_fn=active_fn, alert_fn=alert_fn,
        )

    assert flags.flip_attempts == 1
    # Flip failed → no flag row written, NOT marked tripped, counter pinned.
    assert await _read_flag(fresh_db, tenant_a) is None
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is False
    assert loaded[tenant_a].consecutive_breach_ticks >= config.breach_window_ticks
    assert get_metrics()["breaker.flag_flip_failures"] == 1
    assert get_metrics()["breaker.trips"] == 0
    assert len(alerts) == 0  # no successful trip → no alert yet

    # ---- One more breached tick: flip now succeeds → tenant trips. ----
    await _process_tick(
        config=config, pool=fresh_db, tenant_flags=flags,
        state=state, measure_lag_fn=breach_fn,
        active_tenants_fn=active_fn, alert_fn=alert_fn,
    )

    assert flags.flip_attempts == 2, "breaker did not retry the failed flip"
    assert await _read_flag(fresh_db, tenant_a) is False, (
        "Breaker never flipped the flag after a transient flip failure — the "
        "flip-failure evasion bug (#18) has regressed."
    )
    loaded = await _load_state(fresh_db, _INSTANCE)
    assert loaded[tenant_a].tripped is True
    assert get_metrics()["breaker.trips"] == 1
    assert len(alerts) == 1
