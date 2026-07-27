from __future__ import annotations

import json

from fakeredis import FakeServer
from fakeredis import aioredis as fake_aioredis
from redis.asyncio import Redis

from services.ingest.source_certification.distributed_transport_diagnostic import (
    DISTRIBUTED_TRANSPORT_REDIS_ENV,
    _delete_diagnostic_keys,
    _run_distributed_transport_diagnostic,
    run_distributed_transport_diagnostic_from_env,
)


async def test_missing_isolated_redis_is_explicitly_blocked() -> None:
    result = await run_distributed_transport_diagnostic_from_env(
        "slack",
        ambient_env={},
    )

    assert result["state"] == "blocked"
    assert result["exact_assertions_passed"] is False
    assert result["failed_assertions"] == ["diagnostic_not_executed"]
    assert DISTRIBUTED_TRANSPORT_REDIS_ENV in str(result["reason"])
    assert result["synthetic_promotion_allowed"] is False


async def test_two_replica_exact_assertions_cover_cooldown_and_weighted_fairness(
) -> None:
    server = FakeServer()
    replica_a = fake_aioredis.FakeRedis(server=server)
    replica_b = fake_aioredis.FakeRedis(server=server)
    namespace = "test:provider-transport:two-replica"
    try:
        result = await _run_distributed_transport_diagnostic(
            source_id="slack",
            replica_a=replica_a,
            replica_b=replica_b,
            namespace=namespace,
            cooldown_seconds=0.01,
            connection_ids=(101, 102),
            ping_results=(True, True),
        )

        assert result["state"] == "passed"
        assert result["exact_assertions_passed"] is True
        assert result["failed_assertions"] == []
        assert all(result["assertions"].values())
        assert result["replica_count"] == 2
        assert result["quota_scopes"] == [
            {"scope": "app", "cost": 2, "capacity": 100},
            {"scope": "installation", "cost": 1, "capacity": 100},
        ]
        cooldown = result["cooldown"]
        assert cooldown["observer_callback_count_before_deadline"] == 0
        assert (
            cooldown["recovery_callback_at_ms"]
            >= cooldown["shared_deadline_ms"]
        )
        fairness = result["weighted_tenant_isolation"]
        assert fairness["tenant_a_extra_callback_count"] == 0
        assert fairness["tenant_a_blocked_scope"] == "tenant"
        assert fairness["tenant_b_second_result"] == "tenant-b-2"
        assert result["fairness_boundary"] == {
            "tenant_quota_isolation_proved": True,
            "queue_scheduler_fairness_proved": False,
            "reason": (
                "ProviderTransport owns atomic quota admission, not worker "
                "queue ordering. Fair backfill/live scheduling requires its "
                "separate durable-scheduler diagnostic."
            ),
        }
        assert result["synthetic_promotion_allowed"] is False
    finally:
        await _delete_diagnostic_keys(replica_a, namespace=namespace)
        await replica_a.aclose()
        await replica_b.aclose()


async def test_no_pass_when_runtime_replicas_are_not_independent() -> None:
    server = FakeServer()
    replica_a = fake_aioredis.FakeRedis(server=server)
    replica_b = fake_aioredis.FakeRedis(server=server)
    namespace = "test:provider-transport:same-connection"
    try:
        result = await _run_distributed_transport_diagnostic(
            source_id="github",
            replica_a=replica_a,
            replica_b=replica_b,
            namespace=namespace,
            cooldown_seconds=0.01,
            connection_ids=(1, 1),
            ping_results=(True, True),
        )

        assert result["state"] == "failed"
        assert result["exact_assertions_passed"] is False
        assert result["failed_assertions"] == [
            "independent_redis_connections"
        ]
        assert result["assertions"]["independent_redis_connections"] is False
        assert result["synthetic_promotion_allowed"] is False
    finally:
        await _delete_diagnostic_keys(replica_a, namespace=namespace)
        await replica_a.aclose()
        await replica_b.aclose()


async def test_redis_credentials_are_never_written_to_failure_artifact(
    monkeypatch,
) -> None:
    secret = "must-not-appear"

    class _FailingRedis:
        async def ping(self) -> bool:
            raise RuntimeError(secret)

        async def aclose(self) -> None:
            return None

        async def scan_iter(self, **_kwargs):
            if False:
                yield b""

    monkeypatch.setattr(
        Redis,
        "from_url",
        staticmethod(lambda *_args, **_kwargs: _FailingRedis()),
    )

    result = await run_distributed_transport_diagnostic_from_env(
        "slack",
        ambient_env={
            DISTRIBUTED_TRANSPORT_REDIS_ENV: (
                f"redis://diagnostic:{secret}@redis.invalid:6379/15"
            ),
        },
    )

    assert result["state"] == "failed"
    assert result["error_type"] == "RuntimeError"
    assert result["synthetic_promotion_allowed"] is False
    assert secret not in json.dumps(result, sort_keys=True)
