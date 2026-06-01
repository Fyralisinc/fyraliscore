"""Test-only subprocess entrypoint for M4.3 SIGKILL + restart tests.

NOT a production module. Lives under `tests/` so it ships with the
test code, not the runtime. Invoked by `subprocess.Popen([sys.executable,
"-m", "services.integrations.discord.gateway.tests._subprocess_entrypoint"])`
inside the load-bearing test.

What it simulates:

  - The gateway worker's lifecycle WITHOUT a real Discord WSS
    connection. Exercises the M4.1 lease + M4.2 save/load primitives
    end-to-end through a process death + restart cycle, plus the M2.2
    shadow_write_raw call so the test can count Kafka messages.

  - The full production code paths (LeaderLock, load_session_state,
    save_session_state, shadow_write_raw) — not stubs.

  - A "scripted frame stream" loaded from JSON. Each frame is treated
    as already-dispatched by the (here-absent) WS client: the
    subprocess shadow-writes it, then saves session_state, then writes
    a filesystem marker.

What it doesn't test:

  - The real DiscordGatewayClient WS loop. That code path is tested
    in-process by `test_session_resume_after_planned_restart` via the
    existing FakeGateway in `conftest.py`. Combining the two gives
    end-to-end coverage: in-process verifies the WS-loop save site,
    cross-process verifies the data-loss property under SIGKILL.

Env vars expected:
  DATABASE_URL              — Postgres DSN (must already have migrations)
  REDIS_URL                 — Redis DSN
  KAFKA_BOOTSTRAP_SERVERS   — Kafka brokers
  M4_TEST_APPLICATION_ID    — fake Discord app id (any UUID-ish string)
  M4_TEST_TENANT_ID         — UUID of a seeded tenant in DB
  M4_TEST_FRAMES_PATH       — JSON file: list of {"s": int, "id": str, "guild_id": str}
  M4_TEST_MARKER_DIR        — directory the subprocess writes markers into
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import os
import pathlib
import sys
from uuid import UUID

import orjson
from redis.asyncio import Redis as AsyncRedis

from services.ingestion.kafka.producer import IdempotentProducer, ProducerConfig
from services.ingestion.shadow_write import shadow_write_raw
from services.integrations.discord.gateway._durability import pre_save_flush
from services.integrations.discord.gateway.leader_lock import LeaderLock
from services.integrations.discord.gateway.session_state import (
    load_session_state,
    make_session_state_pool,
    save_session_state,
)


log = logging.getLogger("m4_test_subprocess")


class _InMemoryS3:
    """Minimal S3 stub — same surface as the M2 e2e test's InMemoryS3.

    Each subprocess owns its own _InMemoryS3 instance. The S3 backing
    is not asserted by the test — only the Kafka shadow-path counter
    is — so per-subprocess S3 state is fine.
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self._store.setdefault(key, body)

    async def get(self, key: str) -> bytes:
        return self._store[key]


def _marker_path(marker_dir: pathlib.Path, name: str) -> pathlib.Path:
    return marker_dir / f"{name}.marker"


async def _main() -> int:
    logging.basicConfig(
        level=os.environ.get("M4_TEST_LOG_LEVEL", "WARNING"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    application_id = os.environ["M4_TEST_APPLICATION_ID"]
    tenant_id = UUID(os.environ["M4_TEST_TENANT_ID"])
    frames_path = pathlib.Path(os.environ["M4_TEST_FRAMES_PATH"])
    marker_dir = pathlib.Path(os.environ["M4_TEST_MARKER_DIR"])
    marker_dir.mkdir(parents=True, exist_ok=True)

    with frames_path.open() as f:
        frames = json.load(f)

    # ---- Connect dependencies ---------------------------------------
    pool = await make_session_state_pool(os.environ["DATABASE_URL"])
    redis = AsyncRedis.from_url(
        os.environ["REDIS_URL"], decode_responses=False,
    )
    kafka_producer = IdempotentProducer(ProducerConfig(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        client_id=f"m4-test-subprocess-{os.getpid()}",
    ))
    await kafka_producer.start()
    s3 = _InMemoryS3()

    # ---- Acquire lease ----------------------------------------------
    # Short TTL so the test can move quickly. Production uses 30s.
    lease = LeaderLock(redis, ttl_seconds=5)
    while True:
        if await lease.acquire():
            break
        # Couldn't acquire — another holder is alive. Wait briefly
        # then retry. The acquire-with-backoff is in lifecycle.py;
        # here we keep it simple because tests drive process timing
        # via explicit Popen + SIGKILL, not via the orchestrator.
        await asyncio.sleep(0.5)

    _marker_path(marker_dir, "lease_acquired").write_text(
        lease.lease_value
    )

    # ---- Load state → RESUME vs IDENTIFY decision -------------------
    persisted = await load_session_state(
        pool, application_id=application_id, shard_id=0,
    )
    if persisted is not None and persisted.last_seq is not None:
        _marker_path(
            marker_dir, f"sent_RESUME_seq_{persisted.last_seq}"
        ).write_text(
            f"session_id={persisted.session_id}\nlast_seq={persisted.last_seq}\n"
        )
        # Replay model: subprocess B sees only frames after the
        # persisted seq. (Discord buffers and replays them.)
        start_seq = persisted.last_seq + 1
    else:
        _marker_path(marker_dir, "sent_IDENTIFY").write_text("")
        start_seq = 1

    # ---- Process frames ---------------------------------------------
    # Standard "fake dispatch": shadow_write_raw to Kafka, save state,
    # write marker. The order is shadow-then-save (M4.2 save-after-
    # handle ordering — see session_state.py module docstring).
    try:
        for frame in frames:
            seq = frame["s"]
            if seq < start_seq:
                continue

            raw_body = orjson.dumps(frame, option=orjson.OPT_SORT_KEYS)
            await shadow_write_raw(
                tenant_id=tenant_id,
                source="discord",
                ingress_kind="gateway",
                raw_body=raw_body,
                s3_client=s3,   # type: ignore[arg-type]
                kafka_producer=kafka_producer,
                ingress_metadata={
                    "event_type": "MESSAGE_CREATE",
                    "message_id": frame.get("id"),
                    "channel_id": frame.get("channel_id"),
                },
            )

            # A6 — broker-ack durability barrier. Uses the production
            # `pre_save_flush` from `..._durability`; this is the same
            # function `DiscordGatewayClient._pre_save_flush` calls.
            # Refactored in A6 Phase 3 to make this load-bearing test
            # actually exercise production code (previously this site
            # had a parallel manual flush that masked the absence of
            # production-code coverage at the simulation surface). The
            # function lives in `_durability.py` rather than `client.py`
            # so the subprocess does not pull httpx + websockets into
            # its import graph just to access this one function.
            # timeout_seconds matches the production value at
            # client.py::_dispatch_loop.
            await pre_save_flush(kafka_producer, timeout_seconds=2.0)

            # SAVE-AFTER-HANDLE — see session_state.py "Save-after-
            # handle ordering (N1 contract)." Saving here means a
            # crash AFTER this save loses no frames; a crash BETWEEN
            # the shadow_write and this save means re-processing the
            # frame on the next run, which is safe under M2 dedup.
            await save_session_state(
                pool,
                application_id=application_id,
                shard_id=0,
                session_id=f"test-session-{lease.lease_value[:8]}",
                resume_gateway_url="wss://resume.test.example/",
                last_seq=seq,
                heartbeat_interval_ms=41250,
                last_heartbeat_ack_at=dt.datetime.now(tz=dt.timezone.utc),
                last_dispatched_at=dt.datetime.now(tz=dt.timezone.utc),
                leader_lease_holder=lease.lease_value,
            )

            # AFTER save persists, write the checkpoint marker. Tests
            # poll for this file's existence as the deterministic
            # "seq N is durable now" signal — no timing assumptions.
            _marker_path(marker_dir, f"seq_{seq}").write_text(
                f"session_id=test-session-{lease.lease_value[:8]}\n"
                f"last_seq={seq}\n"
            )

            # Brief inter-frame gap so the test's SIGKILL can land
            # between two frames (not in the middle of one). 50ms is
            # plenty for the SIGKILL signal delivery + pytest poll loop.
            await asyncio.sleep(0.05)
    finally:
        await kafka_producer.stop()
        await lease.release()
        await redis.aclose()
        await pool.close()

    _marker_path(marker_dir, "clean_exit").write_text("")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main()))
