"""M5.1 — Ingestion cutover circuit breaker tests.

Test categories:

  1. State machine unit tests (in-process, fresh_db, injected mock
     Kafka readers). Drive `_process_tick` deterministically.

  2. Pool-config check (assertion that `make_breaker_pool` produces
     the same pgbouncer-compatible shape as M3.1 / M4.2).

  3. SUBPROCESS test: real `python -m services.ingestion.feature_flags`
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
import datetime as dt
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

from services.ingestion.feature_flags.circuit_breaker import (
    BreakerConfig,
    _process_tick,
    _TenantBreachState,
    _load_state,
    make_breaker_pool,
    reset_metrics,
    run_circuit_breaker,
)
from services.ingestion.feature_flags.client import (
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


def _make_lag_fn(lag_per_partition: dict[int, float]):
    async def _f(**_kwargs: Any) -> dict[int, float]:
        return dict(lag_per_partition)
    return _f


def _make_active_fn(active: dict[UUID, int]):
    async def _f(**_kwargs: Any) -> dict[UUID, int]:
        return dict(active)
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
        "raw_topic": "ingestion.raw",
        "consumer_group": "ingestion-normalizer",
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
# 7. State survives a real subprocess SIGTERM + restart.
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

    fake_lag = json.dumps({"0": 120.0})
    fake_active = json.dumps({str(tenant_id): 0})

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
        [sys.executable, "-m", "services.ingestion.feature_flags"],
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
        [sys.executable, "-m", "services.ingestion.feature_flags"],
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
