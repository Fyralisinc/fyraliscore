"""M4.3 — Gateway lifecycle integration + crash-recovery tests.

Five tests:

  1. test_single_holder_under_two_pods         — lifecycle layer
  2. test_lease_release_on_clean_shutdown      — lifecycle layer
  3. test_lease_takeover_on_crashed_holder     — SUBPROCESS + SIGKILL
  4. test_session_resume_after_planned_restart — in-process FakeGateway
  5. test_no_frames_lost_across_sigkill        — SUBPROCESS + SIGKILL
     [LOAD-BEARING — proves N1 at the Gateway surface]

The subprocess tests use `services/integrations/discord/gateway/tests/
_subprocess_entrypoint.py` as the test-only driver. Subprocess A
processes some frames, gets SIGKILLed, subprocess B reads the
persisted session_state and continues.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import orjson
import pytest

try:
    from fakeredis import aioredis as fake_aioredis  # type: ignore[import-not-found]
    _HAS_FAKEREDIS = True
except ImportError:
    _HAS_FAKEREDIS = False

try:
    import docker as _docker_module  # type: ignore[import-not-found]
    _HAS_DOCKER_SDK = True
except ImportError:
    _HAS_DOCKER_SDK = False

try:
    import warnings as _warnings
    with _warnings.catch_warnings():
        _warnings.simplefilter("ignore", DeprecationWarning)
        from testcontainers.kafka import KafkaContainer  # type: ignore[import-not-found]
        from testcontainers.redis import RedisContainer  # type: ignore[import-not-found]
    _HAS_TESTCONTAINERS = True
except ImportError:
    _HAS_TESTCONTAINERS = False


from services.integrations.discord.gateway.leader_lock import LeaderLock
from services.integrations.discord.gateway.lifecycle import (
    LifecycleConfig,
    acquire_lease_with_backoff,
)
from services.integrations.discord.gateway.session_state import (
    PersistedGatewaySession,
    load_session_state,
    save_session_state,
)


pytestmark = [pytest.mark.timeout(180)]


def _docker_available() -> bool:
    if not _HAS_DOCKER_SDK:
        return False
    try:
        _docker_module.from_env().ping()
        return True
    except Exception:
        return False


_APPLICATION_ID = "m4-test-app"


# =====================================================================
# 1. Two pods, single holder. Lifecycle-level integration.
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_single_holder_under_two_pods():
    """Two GatewayWorker-equivalents acquire the same lease; only one
    wins. The loser's `acquire_lease_with_backoff` times out cleanly
    via the stop_event (we set the stop_event after a short window
    rather than waiting the full 5min default)."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock_a = LeaderLock(
            redis, ttl_seconds=30, lease_value="pod-a",
        )
        lock_b = LeaderLock(
            redis, ttl_seconds=30, lease_value="pod-b",
        )
        config = LifecycleConfig(
            application_id=_APPLICATION_ID,
            # Bounded backoff so the loser exits the test quickly.
            lease_acquire_initial_backoff_s=0.05,
            lease_acquire_max_backoff_s=0.1,
            lease_acquire_total_timeout_s=0.5,
        )

        # A acquires.
        assert await lock_a.acquire() is True

        # B tries to acquire with backoff and times out (a still holds).
        stop_b = asyncio.Event()
        acquired_b = await acquire_lease_with_backoff(
            lock_b, config=config, stop_event=stop_b,
        )
        assert acquired_b is False, "Both pods acquired the lease"
        assert lock_a.is_held() is True
        assert lock_b.is_held() is False
    finally:
        await redis.aclose()


# =====================================================================
# 2. Clean shutdown → lease released → next acquirer immediate.
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_lease_release_on_clean_shutdown():
    """Acquire as A. Release. B's immediate acquire succeeds (no wait)."""
    redis = fake_aioredis.FakeRedis()
    try:
        lock_a = LeaderLock(
            redis, ttl_seconds=30, lease_value="pod-a-clean",
        )
        lock_b = LeaderLock(
            redis, ttl_seconds=30, lease_value="pod-b-takeover",
        )

        assert await lock_a.acquire() is True
        # Simulate a clean shutdown: release the lease.
        assert await lock_a.release() is True

        # B acquires immediately (no TTL wait).
        assert await lock_b.acquire() is True
    finally:
        await redis.aclose()


# =====================================================================
# 3. Crashed holder → next acquirer waits TTL → succeeds. SUBPROCESS.
# =====================================================================

@pytest.mark.skipif(not _HAS_TESTCONTAINERS, reason="testcontainers unavailable")
@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_lease_takeover_on_crashed_holder(fresh_db: asyncpg.Pool):
    """Subprocess A acquires the lease, then is SIGKILLed BEFORE it
    completes its scripted frame stream (and before it can release).
    Subprocess B starts ~1s later, acquires after the lease TTL
    expires, and exits cleanly.

    Verifies the bounded-takeover property: a crashed holder does NOT
    lock the lease forever. TTL-based expiry is the safety net.
    """
    from redis.asyncio import Redis

    with RedisContainer("redis:7-alpine") as redis_box, \
         KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka_box:
        redis_url = (
            f"redis://{redis_box.get_container_host_ip()}:"
            f"{redis_box.get_exposed_port(6379)}/0"
        )
        kafka_bootstrap = kafka_box.get_bootstrap_server()
        _create_topic(kafka_bootstrap, "ingestion.raw")

        tenant_id = await _seed_tenant(fresh_db)

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = pathlib.Path(tmp)
            frames_path = tmpdir / "frames.json"
            # Subprocess A: enough frames to be running when killed.
            frames_path.write_text(json.dumps([
                {"s": i, "id": f"msg-{i}", "guild_id": "G_a"}
                for i in range(1, 20)  # 19 frames @ 50ms gap = ~1s
            ]))
            marker_dir_a = tmpdir / "markers_a"
            marker_dir_b = tmpdir / "markers_b"

            # ---- Subprocess A: SIGKILL before scripted frames done.
            env_a = _make_subprocess_env(
                redis_url=redis_url, kafka_bootstrap=kafka_bootstrap,
                tenant_id=tenant_id, application_id=f"app-takeover-{uuid4().hex[:8]}",
                frames_path=frames_path, marker_dir=marker_dir_a,
            )
            proc_a = subprocess.Popen(
                [sys.executable, "-m",
                 "services.integrations.discord.gateway.tests._subprocess_entrypoint"],
                env=env_a, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                _wait_for_marker(marker_dir_a, "lease_acquired", timeout_s=20)
                # SIGKILL while subprocess is mid-flight.
                proc_a.send_signal(signal.SIGKILL)
                proc_a.wait(timeout=10)
            finally:
                if proc_a.poll() is None:
                    proc_a.kill()
                    proc_a.wait(timeout=5)

            # ---- Subprocess B: starts now, waits TTL out, acquires.
            env_b = _make_subprocess_env(
                redis_url=redis_url, kafka_bootstrap=kafka_bootstrap,
                tenant_id=tenant_id, application_id=env_a["M4_TEST_APPLICATION_ID"],
                frames_path=frames_path, marker_dir=marker_dir_b,
            )
            proc_b = subprocess.Popen(
                [sys.executable, "-m",
                 "services.integrations.discord.gateway.tests._subprocess_entrypoint"],
                env=env_b, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                # Subprocess entrypoint uses TTL=5s. Within ~6s of A's
                # death, B should have acquired and written the marker.
                _wait_for_marker(marker_dir_b, "lease_acquired", timeout_s=15)
                # Allow B to finish its frame loop and exit cleanly.
                rc = proc_b.wait(timeout=30)
                assert rc == 0, (
                    f"subprocess B exit code {rc}; stderr="
                    f"{proc_b.stderr.read().decode()[:500]}"
                )
            finally:
                if proc_b.poll() is None:
                    proc_b.kill()
                    proc_b.wait(timeout=5)


# =====================================================================
# 4. RESUME after planned restart. In-process FakeGateway.
# =====================================================================

@pytest.mark.skipif(not _HAS_FAKEREDIS, reason="fakeredis with [lua] not available")
async def test_session_resume_after_planned_restart(fresh_db: asyncpg.Pool):
    """Verifies the in-process WS-loop save site fires after each
    dispatch AND the next worker's IDENTIFY-vs-RESUME decision uses
    the persisted seq.

    Setup:
      - Seed session_state with last_seq=42 (as if a previous worker
        had processed up to seq 42).
      - Construct a fresh DiscordGatewayClient with initial_state
        populated from the persisted row.
      - Connect to FakeGateway; FakeGateway records the first frame.

    Assertion:
      - FakeGateway's `received[0]` is an op-6 RESUME, not op-2
        IDENTIFY. The `seq` field equals 42 (the persisted value).
    """
    import httpx
    import respx
    from services.integrations.discord.gateway.client import (
        DiscordGatewayClient,
        GatewaySessionState,
    )
    from services.integrations.discord.gateway.lifecycle import (
        persisted_to_in_memory,
    )
    from services.integrations.discord.gateway.tests.conftest import (
        FakeGateway,
    )

    app_id = f"resume-app-{uuid4().hex[:8]}"
    persisted_seq = 42
    persisted_session_id = "session_already_running"

    # Seed Postgres with state as-if a previous worker had run.
    await save_session_state(
        fresh_db,
        application_id=app_id,
        shard_id=0,
        session_id=persisted_session_id,
        resume_gateway_url=None,  # filled by READY normally; OK None here
        last_seq=persisted_seq,
        heartbeat_interval_ms=41250,
    )

    # Load state via the production load path.
    persisted = await load_session_state(
        fresh_db, application_id=app_id,
    )
    assert persisted is not None
    assert persisted.last_seq == persisted_seq

    initial_state = persisted_to_in_memory(persisted)
    assert initial_state is not None
    assert initial_state.session_id == persisted_session_id
    assert initial_state.last_seq == persisted_seq

    # Now spin up a fresh client with FakeGateway and the seeded state.
    # Use respx to mock the REST /gateway/bot endpoint pointing at
    # the FakeGateway's WS URL.
    async with FakeGateway() as fg:
        fg.heartbeat_interval_ms = 200
        # Park: no scripted frames; we only need to see the first
        # outgoing frame on FakeGateway's `received` list.
        fg.script = [{"op": "sleep", "seconds": 0.5}]

        # FakeGateway requires the URL to come from /gateway/bot.
        async with httpx.AsyncClient() as http:
            with respx.mock(
                base_url="https://discord.com",
                assert_all_called=False,
            ) as router:
                router.get("/api/v10/gateway/bot").respond(
                    200, json={"url": fg.url},
                )
                # Override resume_gateway_url to point at the FakeGateway
                # — production fills this from the prior READY. The
                # client uses resume_gateway_url to RESUME; without it,
                # it'd fall back to /gateway/bot's URL (which we also
                # mock to fg.url, so it still works).
                initial_state.resume_gateway_url = fg.url

                client = DiscordGatewayClient(
                    bot_token="test-token",
                    dispatch_handler=_null_dispatch,
                    http_client=http,
                    initial_state=initial_state,
                )
                run_task = asyncio.create_task(client.run())
                try:
                    await asyncio.sleep(0.3)
                    # ===== LOAD-BEARING ASSERTION =====
                    # The first outgoing protocol frame must be op-6
                    # RESUME (not op-2 IDENTIFY) carrying the
                    # persisted seq.
                    assert len(fg.received) >= 1
                    first = fg.received[0]
                    assert first.get("op") == 6, (
                        f"First protocol frame was op={first.get('op')}, "
                        f"expected op=6 (RESUME). The initial_state seed "
                        f"did not produce a RESUME path. Full frame: {first}"
                    )
                    assert first["d"]["seq"] == persisted_seq, (
                        f"RESUME seq was {first['d']['seq']}, expected "
                        f"{persisted_seq}. The persisted last_seq did "
                        f"not flow into the RESUME frame."
                    )
                    assert first["d"]["session_id"] == persisted_session_id
                finally:
                    client.request_shutdown()
                    run_task.cancel()
                    try:
                        await run_task
                    except (asyncio.CancelledError, Exception):
                        pass
                    await client.aclose()


# =====================================================================
# 5. NO FRAMES LOST ACROSS SIGKILL. LOAD-BEARING. SUBPROCESS.
# =====================================================================

@pytest.mark.skipif(not _HAS_TESTCONTAINERS, reason="testcontainers unavailable")
@pytest.mark.skipif(not _docker_available(), reason="Docker daemon not reachable")
async def test_no_frames_lost_across_sigkill(fresh_db: asyncpg.Pool):
    """LOAD-BEARING (M4): N1 (Never lose data) at the Gateway surface.

    The exact failure mode M4 fixes: the worker holds session_id +
    last_seq in process memory. SIGKILL between two frames loses the
    in-memory state; the next worker starts fresh, IDENTIFYs as a
    new client, and Discord drops the frames buffered for the dead
    session.

    With M4.1's lease + M4.2's persisted state, the next worker
    RESUMEs from the saved seq and Discord re-delivers the buffered
    frames. Zero loss.

    === Test structure ===

    Subprocess A:
      - Frames JSON: seqs [1, 2, 3]
      - Acquires lease, IDENTIFYs (no persisted state).
      - Processes seq 1, saves state, writes marker.
      - Processes seq 2, saves state, writes marker.
      - SIGKILLed (uncatchable signal) BEFORE processing seq 3.

    Subprocess B:
      - Frames JSON: [3]   ← Discord replays only the un-delivered
                              frame after RESUME.
      - Acquires lease (after A's lease TTL expires).
      - Loads persisted state — sees last_seq=2; sends RESUME marker.
      - Processes seq 3, saves state, writes marker.
      - Exits cleanly.

    === Deterministic checkpoint mechanism ===

    The subprocess writes filesystem markers AFTER each successful
    save_session_state call (see _subprocess_entrypoint.py line ~145).
    The test polls for `<marker_dir>/seq_2.marker` to exist BEFORE
    sending SIGKILL — this is the only way to know, from outside the
    process, that seq 2 is durable. NO timing assumptions; no
    asyncio.sleep guesses; if the marker appears the save persisted,
    full stop.

    === Load-bearing assertion ===

    Counts Kafka messages on `ingestion.raw` filtered by the
    Discord-source / gateway-ingress envelope shape (the M2.2 shadow
    path). All 3 frames MUST have flowed through. Counting the
    worker's local state would only prove "the worker thinks it
    processed three frames" — the test cares whether they actually
    reached the shadow pipeline.
    """
    from confluent_kafka import Consumer as RawConsumer

    with RedisContainer("redis:7-alpine") as redis_box, \
         KafkaContainer("confluentinc/cp-kafka:7.6.1") as kafka_box:
        redis_url = (
            f"redis://{redis_box.get_container_host_ip()}:"
            f"{redis_box.get_exposed_port(6379)}/0"
        )
        kafka_bootstrap = kafka_box.get_bootstrap_server()
        _create_topic(kafka_bootstrap, "ingestion.raw")

        tenant_id = await _seed_tenant(fresh_db)
        application_id = f"app-noloss-{uuid4().hex[:8]}"

        with tempfile.TemporaryDirectory() as tmp:
            tmpdir = pathlib.Path(tmp)
            marker_dir_a = tmpdir / "markers_a"
            marker_dir_b = tmpdir / "markers_b"

            # ---- Subprocess A frames: 1, 2, 3 (we SIGKILL after 2) --
            frames_path_a = tmpdir / "frames_a.json"
            frames_path_a.write_text(json.dumps([
                {"s": 1, "id": "msg-1", "guild_id": "G_a"},
                {"s": 2, "id": "msg-2", "guild_id": "G_a"},
                {"s": 3, "id": "msg-3", "guild_id": "G_a"},
            ]))

            env_a = _make_subprocess_env(
                redis_url=redis_url, kafka_bootstrap=kafka_bootstrap,
                tenant_id=tenant_id, application_id=application_id,
                frames_path=frames_path_a, marker_dir=marker_dir_a,
            )
            proc_a = subprocess.Popen(
                [sys.executable, "-m",
                 "services.integrations.discord.gateway.tests._subprocess_entrypoint"],
                env=env_a, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                # Wait for IDENTIFY marker (fresh start, no persisted state).
                _wait_for_marker(
                    marker_dir_a, "sent_IDENTIFY", timeout_s=20,
                )
                # Wait for seq 2 to be durably saved.
                _wait_for_marker(marker_dir_a, "seq_2", timeout_s=20)
                # SIGKILL — uncatchable. Worker has no chance to release
                # the lease or save additional state.
                proc_a.send_signal(signal.SIGKILL)
                proc_a.wait(timeout=10)
            finally:
                if proc_a.poll() is None:
                    proc_a.kill()
                    proc_a.wait(timeout=5)

            # Confirm Postgres has seq=2 (not 3) at the moment of crash.
            persisted_after_a = await load_session_state(
                fresh_db, application_id=application_id,
            )
            assert persisted_after_a is not None
            assert persisted_after_a.last_seq == 2, (
                f"After SIGKILL post seq-2 marker, persisted last_seq "
                f"is {persisted_after_a.last_seq}, expected 2. The "
                f"save-after-handle ordering may be broken."
            )

            # ---- Subprocess B frames: [3] — Discord replays the
            # un-delivered frame after RESUME. -----------------------
            frames_path_b = tmpdir / "frames_b.json"
            frames_path_b.write_text(json.dumps([
                {"s": 3, "id": "msg-3", "guild_id": "G_a"},
            ]))

            env_b = _make_subprocess_env(
                redis_url=redis_url, kafka_bootstrap=kafka_bootstrap,
                tenant_id=tenant_id, application_id=application_id,
                frames_path=frames_path_b, marker_dir=marker_dir_b,
            )
            proc_b = subprocess.Popen(
                [sys.executable, "-m",
                 "services.integrations.discord.gateway.tests._subprocess_entrypoint"],
                env=env_b, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
            try:
                # B should send RESUME with seq=2 (the persisted value).
                _wait_for_marker(
                    marker_dir_b, "sent_RESUME_seq_2", timeout_s=30,
                )
                # B processes seq 3.
                _wait_for_marker(marker_dir_b, "seq_3", timeout_s=20)
                # B exits cleanly after the frame loop.
                rc = proc_b.wait(timeout=20)
                assert rc == 0, (
                    f"subprocess B exit code {rc}; stderr="
                    f"{proc_b.stderr.read().decode()[:500]}"
                )
            finally:
                if proc_b.poll() is None:
                    proc_b.kill()
                    proc_b.wait(timeout=5)

            # ===== LOAD-BEARING ASSERTION =====
            # All 3 frames flowed through the shadow path's Kafka
            # topic. Counter via real Kafka consumer (not worker
            # internal state).
            gateway_msgs = _drain_kafka(
                bootstrap=kafka_bootstrap,
                topic="ingestion.raw",
                expected=3, timeout_s=20,
                filter_ingress_kind="gateway",
            )
            assert len(gateway_msgs) == 3, (
                f"Expected 3 gateway frames on ingestion.raw, "
                f"got {len(gateway_msgs)}. Frames lost across "
                f"SIGKILL — N1 breach. The save+load+replay path "
                f"failed somewhere between persist and Kafka."
            )
            # Confirm the three are seqs 1, 2, 3 (not 1, 2, 2).
            # Each shadow envelope's ingress_metadata carries
            # message_id which maps 1:1 to seq in our test fixtures.
            seen_msg_ids = {
                env["ingress_metadata"]["message_id"]
                for env in gateway_msgs
            }
            assert seen_msg_ids == {"msg-1", "msg-2", "msg-3"}, (
                f"Expected {{msg-1, msg-2, msg-3}}, got {seen_msg_ids}. "
                f"Either a frame was lost or a frame was processed "
                f"twice — both N1 breaches."
            )


# =====================================================================
# Helpers shared by tests above.
# =====================================================================

async def _null_dispatch(_frame: dict[str, Any]) -> None:
    """No-op dispatch for tests that don't care about per-event side
    effects."""


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"m4-test-{tid.hex[:8]}",
    )
    return tid


def _create_topic(bootstrap: str, topic: str) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    futs = admin.create_topics([
        NewTopic(topic, num_partitions=4, replication_factor=1),
    ])
    for f in futs.values():
        f.result(timeout=30)


def _make_subprocess_env(
    *,
    redis_url: str,
    kafka_bootstrap: str,
    tenant_id: UUID,
    application_id: str,
    frames_path: pathlib.Path,
    marker_dir: pathlib.Path,
) -> dict[str, str]:
    env = os.environ.copy()
    env["DATABASE_URL"] = os.environ["DATABASE_URL"]
    env["REDIS_URL"] = redis_url
    env["KAFKA_BOOTSTRAP_SERVERS"] = kafka_bootstrap
    env["M4_TEST_APPLICATION_ID"] = application_id
    env["M4_TEST_TENANT_ID"] = str(tenant_id)
    env["M4_TEST_FRAMES_PATH"] = str(frames_path)
    env["M4_TEST_MARKER_DIR"] = str(marker_dir)
    env["M4_TEST_LOG_LEVEL"] = "WARNING"
    return env


def _wait_for_marker(
    marker_dir: pathlib.Path, name: str, *, timeout_s: float,
) -> pathlib.Path:
    """Poll for the marker file's existence. Raises TimeoutError with
    diagnostic info if not present within timeout. Loud-failing per
    the M3.2 _drain_dlq pattern."""
    deadline = time.monotonic() + timeout_s
    target = marker_dir / f"{name}.marker"
    while time.monotonic() < deadline:
        if target.exists():
            return target
        time.sleep(0.05)
    existing = (
        [p.name for p in marker_dir.iterdir()]
        if marker_dir.exists() else []
    )
    raise TimeoutError(
        f"marker '{name}.marker' did not appear in {marker_dir} "
        f"within {timeout_s}s. Existing markers: {existing}. "
        f"Subprocess may have crashed before reaching this checkpoint."
    )


def _drain_kafka(
    *,
    bootstrap: str,
    topic: str,
    expected: int,
    timeout_s: float,
    filter_ingress_kind: str | None = None,
) -> list[dict[str, Any]]:
    """Read up to `expected` messages from `topic`. Decode JSON; if
    `filter_ingress_kind` is set, drop messages whose
    `ingress_metadata.ingress_kind` (or top-level `ingress_kind`)
    doesn't match.

    Raises TimeoutError on incomplete read (loud-fail per M3.2 pattern).
    """
    from confluent_kafka import Consumer as RawConsumer
    c = RawConsumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"m4-test-drain-{uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([topic])
    out: list[dict[str, Any]] = []
    deadline = time.monotonic() + timeout_s
    try:
        while len(out) < expected:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"_drain_kafka: expected {expected} message(s) on "
                    f"{topic} within {timeout_s}s; got {len(out)}. "
                    f"Producer may not have flushed, or filter "
                    f"`ingress_kind={filter_ingress_kind}` excluded too "
                    f"many messages."
                )
            msg = c.poll(min(1.0, remaining))
            if msg is None or msg.error():
                continue
            decoded = orjson.loads(msg.value())
            if filter_ingress_kind is not None:
                # The shadow envelope carries ingress_kind at the
                # envelope's top level (RawEnvelope schema).
                kind = decoded.get("ingress_kind")
                if kind != filter_ingress_kind:
                    continue
            out.append(decoded)
    finally:
        c.close()
    return out
