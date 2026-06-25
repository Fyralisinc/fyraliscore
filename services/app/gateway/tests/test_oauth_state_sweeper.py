from __future__ import annotations

from unittest.mock import AsyncMock

from services.app.gateway import oauth_state_sweeper


class _FakeLease:
    acquire_result = True
    instances: list["_FakeLease"] = []

    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.released = False
        _FakeLease.instances.append(self)

    async def acquire(self) -> bool:
        return self.acquire_result

    async def release(self) -> bool:
        self.released = True
        return True


async def test_protected_oauth_sweep_runs_under_lease(monkeypatch) -> None:
    _FakeLease.acquire_result = True
    _FakeLease.instances = []
    pool = AsyncMock()
    pool.execute.return_value = "DELETE 3"
    monkeypatch.setattr(oauth_state_sweeper, "PostgresLease", _FakeLease)

    deleted = await oauth_state_sweeper.sweep_oauth_install_states_once_protected(
        pool,
        lease_ttl_seconds=5,
    )

    assert deleted == "DELETE 3"
    pool.execute.assert_awaited_once()
    assert _FakeLease.instances[0].released is True
    assert _FakeLease.instances[0].kwargs["lease_name"] == (
        oauth_state_sweeper.OAUTH_SWEEPER_LEASE_NAME
    )
    assert _FakeLease.instances[0].kwargs["ttl_seconds"] == 5


async def test_protected_oauth_sweep_skips_when_lease_busy(monkeypatch) -> None:
    _FakeLease.acquire_result = False
    _FakeLease.instances = []
    pool = AsyncMock()
    monkeypatch.setattr(oauth_state_sweeper, "PostgresLease", _FakeLease)

    deleted = await oauth_state_sweeper.sweep_oauth_install_states_once_protected(pool)

    assert deleted is None
    pool.execute.assert_not_called()
    assert _FakeLease.instances[0].released is False
