from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from scripts.run_signal_gateway_worker import (
    load_signal_runtime_binding,
    persist_signal_sync_cursor,
    required_runtime_identity,
    signal_lease_key,
    signal_worker_identity,
)


TENANT_A = UUID("11111111-1111-4111-8111-111111111111")
INSTALLATION_A = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
TENANT_B = UUID("22222222-2222-4222-8222-222222222222")
INSTALLATION_B = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")


class _Executor:
    def __init__(self) -> None:
        self.installations = {
            (TENANT_A, INSTALLATION_A): {
                "id": INSTALLATION_A,
                "tenant_id": TENANT_A,
                "account_label": "+15550000001",
                "session_secret_ref": "session-a",
            },
            (TENANT_B, INSTALLATION_B): {
                "id": INSTALLATION_B,
                "tenant_id": TENANT_B,
                "account_label": "+15550000002",
                "session_secret_ref": "session-b",
            },
        }
        self.threads = {
            (TENANT_A, INSTALLATION_A): [
                {"thread_id": 101, "thread_kind": "direct", "title": "A"}
            ],
            (TENANT_B, INSTALLATION_B): [
                {"thread_id": 202, "thread_kind": "group", "title": "B"}
            ],
        }
        self.cursors = {
            (TENANT_A, INSTALLATION_A): None,
            (TENANT_B, INSTALLATION_B): None,
        }
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object):
        self.calls.append(("fetchrow", args))
        assert "tenant_id = $1" in query
        assert "id = $2" in query
        return self.installations.get((args[0], args[1]))

    async def fetch(self, query: str, *args: object):
        self.calls.append(("fetch", args))
        assert "tenant_id = $1" in query
        assert "signal_installation_id = $2" in query
        return self.threads.get((args[0], args[1]), [])

    async def execute(self, query: str, *args: object) -> str:
        self.calls.append(("execute", args))
        assert "tenant_id = $1" in query
        assert "signal_installation_id = $2" in query
        identity = (args[0], args[1])
        if identity not in self.cursors:
            return "UPDATE 0"
        self.cursors[identity] = args[2]
        return "UPDATE 1"


class _SecretStore:
    def __init__(self) -> None:
        self.values = {
            (TENANT_A, "session-a"): b"tenant-a-session",
            (TENANT_B, "session-b"): b"tenant-b-session",
        }
        self.calls: list[tuple[UUID, str]] = []

    async def get(self, ref: str, *, tenant_id: UUID) -> bytes:
        self.calls.append((tenant_id, ref))
        return self.values[(tenant_id, ref)]


class _MisdirectedExecutor(_Executor):
    async def fetchrow(self, query: str, *args: object):
        row = await super().fetchrow(query, *args)
        if row is None:
            return None
        return {
            **row,
            "tenant_id": TENANT_B,
            "id": INSTALLATION_B,
            "session_secret_ref": "session-b",
        }


def test_runtime_identity_requires_two_non_nil_uuids() -> None:
    assert required_runtime_identity(
        {
            "SIGNAL_TENANT_ID": str(TENANT_A),
            "SIGNAL_INSTALLATION_ID": str(INSTALLATION_A),
        }
    ) == (TENANT_A, INSTALLATION_A)

    with pytest.raises(ValueError, match="SIGNAL_TENANT_ID is required"):
        required_runtime_identity({})
    with pytest.raises(ValueError, match="SIGNAL_INSTALLATION_ID must be a UUID"):
        required_runtime_identity(
            {
                "SIGNAL_TENANT_ID": str(TENANT_A),
                "SIGNAL_INSTALLATION_ID": "latest",
            }
        )
    with pytest.raises(ValueError, match="must not be the nil UUID"):
        required_runtime_identity(
            {
                "SIGNAL_TENANT_ID": str(TENANT_A),
                "SIGNAL_INSTALLATION_ID": str(UUID(int=0)),
            }
        )


@pytest.mark.asyncio
async def test_two_tenants_load_only_their_exact_installation_credentials() -> None:
    executor = _Executor()
    secrets = _SecretStore()

    binding_a = await load_signal_runtime_binding(
        executor,
        secrets,
        tenant_id=TENANT_A,
        installation_id=INSTALLATION_A,
    )
    binding_b = await load_signal_runtime_binding(
        executor,
        secrets,
        tenant_id=TENANT_B,
        installation_id=INSTALLATION_B,
    )

    assert binding_a.session == "tenant-a-session"
    assert binding_a.thread_rows[0]["thread_id"] == 101
    assert binding_b.session == "tenant-b-session"
    assert binding_b.thread_rows[0]["thread_id"] == 202
    assert secrets.calls == [
        (TENANT_A, "session-a"),
        (TENANT_B, "session-b"),
    ]

    with pytest.raises(RuntimeError, match="different tenant"):
        await load_signal_runtime_binding(
            _MisdirectedExecutor(),
            secrets,
            tenant_id=TENANT_A,
            installation_id=INSTALLATION_A,
        )
    assert secrets.calls == [
        (TENANT_A, "session-a"),
        (TENANT_B, "session-b"),
    ]

    # Installation B exists, but not inside tenant A. The exact-pair lookup
    # fails before any credential can be resolved.
    with pytest.raises(LookupError, match="exact tenant"):
        await load_signal_runtime_binding(
            executor,
            secrets,
            tenant_id=TENANT_A,
            installation_id=INSTALLATION_B,
        )
    assert secrets.calls == [
        (TENANT_A, "session-a"),
        (TENANT_B, "session-b"),
    ]


@pytest.mark.asyncio
async def test_two_installations_have_independent_state_and_leases() -> None:
    executor = _Executor()

    await persist_signal_sync_cursor(
        executor,
        tenant_id=TENANT_A,
        installation_id=INSTALLATION_A,
        cursor=1001,
    )
    await persist_signal_sync_cursor(
        executor,
        tenant_id=TENANT_B,
        installation_id=INSTALLATION_B,
        cursor=2002,
    )

    assert executor.cursors == {
        (TENANT_A, INSTALLATION_A): 1001,
        (TENANT_B, INSTALLATION_B): 2002,
    }
    assert signal_lease_key(TENANT_A, INSTALLATION_A) != signal_lease_key(
        TENANT_B,
        INSTALLATION_B,
    )
    assert signal_worker_identity(
        TENANT_A,
        INSTALLATION_A,
    ) != signal_worker_identity(TENANT_B, INSTALLATION_B)

    with pytest.raises(RuntimeError, match="update state is missing"):
        await persist_signal_sync_cursor(
            executor,
            tenant_id=TENANT_A,
            installation_id=INSTALLATION_B,
            cursor=9999,
        )
    assert executor.cursors[(TENANT_B, INSTALLATION_B)] == 2002


def test_launcher_contains_no_first_install_or_global_lease_fallback() -> None:
    source = (
        Path(__file__).resolve().parents[1] / "run_signal_gateway_worker.py"
    ).read_text(encoding="utf-8")

    assert "LIMIT 1" not in source
    assert '_LEASE_KEY = "gateway:signal:leader_lock"' not in source
    assert "SIGNAL_TENANT_ID" in source
    assert "SIGNAL_INSTALLATION_ID" in source
