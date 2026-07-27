"""Exact-installation Facebook Page token lifecycle tests."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from lib.shared.provider_transport import (
    ProviderPermanentError,
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.facebook_pages.token_lifecycle import (
    CONNECTED,
    DEGRADED,
    REAUTHORIZATION_REQUIRED,
    recover_page_access_token,
    schedule_page_token_recovery,
)


pytestmark = pytest.mark.asyncio
NOW = datetime(2026, 7, 27, 10, 0, tzinfo=timezone.utc)


def _row(
    *,
    tenant_id: UUID,
    installation_id: UUID,
    page_id: str,
    enabled: bool = True,
    state: str = CONNECTED,
    user_expires_at: datetime | None = None,
) -> dict[str, object]:
    return {
        "id": installation_id,
        "tenant_id": tenant_id,
        "page_id": page_id,
        "page_access_token_ref": f"secret://old-page:{page_id}",
        "user_access_token_ref": f"secret://user:{page_id}",
        "user_token_expires_at": (
            user_expires_at or NOW + timedelta(days=30)
        ),
        "connection_state": state,
        "enabled": enabled,
        "reauthorization_required_at": None,
        "page_token_recovery_next_attempt_at": (
            NOW if state == DEGRADED else None
        ),
        "page_token_recovery_attempts": 0,
        "page_token_recovery_last_attempt_at": None,
        "page_recovery_last_error_code": None,
        "page_recovery_lease_owner": None,
        "page_token_recovery_lease_until": None,
    }


class _Pool:
    def __init__(self, *rows: dict[str, object]) -> None:
        self.rows = {
            (row["id"], row["tenant_id"]): row
            for row in rows
        }
        self.calls: list[tuple[str, tuple[object, ...]]] = []
        self.fail_swap = False

    async def fetchrow(self, sql: str, *args):
        self.calls.append((sql, args))
        key = (args[0], args[1])
        row = self.rows.get(key)
        if sql.lstrip().startswith("SELECT"):
            return dict(row) if row is not None else None

        if "graph_access_token_invalid" not in sql and "CASE" in sql:
            # Recovery scheduling query carries the controlled code as $5.
            if (
                row is None
                or not row["enabled"]
                or row["page_access_token_ref"] != args[2]
            ):
                return None
            now = args[3]
            user_missing = not row["user_access_token_ref"]
            user_expired = (
                row["user_token_expires_at"] is None
                or row["user_token_expires_at"] <= now
            )
            if user_missing or user_expired:
                row["connection_state"] = REAUTHORIZATION_REQUIRED
                row["reauthorization_required_at"] = now
                row["page_token_recovery_next_attempt_at"] = None
            else:
                row["connection_state"] = DEGRADED
                row["page_token_recovery_next_attempt_at"] = now
            row["page_recovery_last_error_code"] = args[4]
            return {
                "connection_state": row["connection_state"],
                "page_token_recovery_next_attempt_at": (
                    row["page_token_recovery_next_attempt_at"]
                ),
            }

        if "make_interval" in sql:
            if (
                row is None
                or not row["enabled"]
                or row["connection_state"] != DEGRADED
                or row["page_token_recovery_next_attempt_at"] is None
                or row["page_token_recovery_next_attempt_at"] > args[3]
                or (
                    row["page_token_recovery_lease_until"] is not None
                    and row["page_token_recovery_lease_until"] > args[3]
                    and row["page_recovery_lease_owner"] != args[2]
                )
            ):
                return None
            row["page_recovery_lease_owner"] = args[2]
            row["page_token_recovery_lease_until"] = (
                args[3] + timedelta(seconds=args[4])
            )
            row["page_token_recovery_attempts"] += 1
            return dict(row)

        if "SET page_access_token_ref = $3" in sql:
            if self.fail_swap:
                raise RuntimeError("database commit failed")
            if (
                row is None
                or not row["enabled"]
                or row["page_access_token_ref"] != args[3]
                or row["user_access_token_ref"] != args[4]
            ):
                return None
            row["page_access_token_ref"] = args[2]
            row["connection_state"] = CONNECTED
            row["page_token_recovery_next_attempt_at"] = None
            row["page_token_recovery_attempts"] = 0
            row["page_recovery_last_error_code"] = None
            row["page_recovery_lease_owner"] = None
            row["page_token_recovery_lease_until"] = None
            return {"page_access_token_ref": args[2]}
        raise AssertionError(f"unexpected fetchrow SQL: {sql}")

    async def execute(self, sql: str, *args):
        self.calls.append((sql, args))
        row = self.rows.get((args[0], args[1]))
        if row is None or not row["enabled"]:
            return "UPDATE 0"
        if "SET connection_state = 'reauthorization_required'" in sql:
            row["connection_state"] = REAUTHORIZATION_REQUIRED
            row["reauthorization_required_at"] = (
                row["reauthorization_required_at"] or args[2]
            )
            row["page_token_recovery_next_attempt_at"] = None
            row["page_recovery_last_error_code"] = args[3]
            row["page_recovery_lease_owner"] = None
            row["page_token_recovery_lease_until"] = None
            return "UPDATE 1"
        if "SET connection_state = 'degraded'" in sql:
            row["connection_state"] = DEGRADED
            row["page_token_recovery_next_attempt_at"] = args[2]
            row["page_recovery_last_error_code"] = args[4]
            row["page_recovery_lease_owner"] = None
            row["page_token_recovery_lease_until"] = None
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute SQL: {sql}")


class _Secrets:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.values = {
            str(row["page_access_token_ref"]): b"old-page-token"
            for row in rows
        }
        self.values.update(
            {
                str(row["user_access_token_ref"]): b"long-user-token"
                for row in rows
                if row["user_access_token_ref"]
            }
        )
        self.put_refs: list[str] = []
        self.deleted: list[str] = []

    async def get(self, ref: str, *, tenant_id: UUID):
        del tenant_id
        return self.values[ref]

    async def put(self, value: str, *, label: str, tenant_id: UUID) -> str:
        del tenant_id
        ref = f"secret://new:{label}:{len(self.put_refs)}"
        self.values[ref] = value.encode()
        self.put_refs.append(ref)
        return ref

    async def delete(self, ref: str, *, tenant_id: UUID) -> None:
        del tenant_id
        self.deleted.append(ref)
        self.values.pop(ref, None)


class _Client:
    def __init__(self, pages=None, *, error: Exception | None = None) -> None:
        self.pages = pages or []
        self.error = error
        self.calls: list[str] = []
        self.closed = False

    async def list_pages(self, token: str):
        self.calls.append(token)
        if self.error is not None:
            raise self.error
        return self.pages

    async def aclose(self) -> None:
        self.closed = True


async def _schedule(pool: _Pool, row: dict[str, object]) -> None:
    result = await schedule_page_token_recovery(
        pool,
        tenant_id=row["tenant_id"],
        installation_row_id=row["id"],
        expected_page_token_ref=row["page_access_token_ref"],
        graph_error_subcode=463,
        now=NOW,
    )
    assert result.state in {DEGRADED, REAUTHORIZATION_REQUIRED}


async def test_success_rederives_exact_page_and_swaps_secret_before_cleanup() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    row = _row(
        tenant_id=tenant_id,
        installation_id=installation_id,
        page_id="page-a",
    )
    pool = _Pool(row)
    secrets = _Secrets([row])
    await _schedule(pool, row)
    client = _Client(
        [
            {"id": "wrong-page", "access_token": "wrong-token"},
            {"id": "page-a", "access_token": "replacement-token"},
        ],
    )

    token = await recover_page_access_token(
        pool,
        secrets,
        tenant_id=tenant_id,
        installation_row_id=installation_id,
        operation="conversations.list",
        now=NOW,
        client_factory=lambda *_: client,
    )

    assert token == "replacement-token"
    assert row["connection_state"] == CONNECTED
    assert row["page_access_token_ref"] == secrets.put_refs[0]
    assert secrets.deleted == ["secret://old-page:page-a"]
    assert client.calls == ["long-user-token"]
    assert client.closed is True


async def test_database_swap_failure_deletes_replacement_but_preserves_old_secret() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    row = _row(
        tenant_id=tenant_id,
        installation_id=installation_id,
        page_id="page-a",
    )
    pool = _Pool(row)
    pool.fail_swap = True
    secrets = _Secrets([row])
    await _schedule(pool, row)
    client = _Client([{"id": "page-a", "access_token": "replacement-token"}])

    with pytest.raises(RuntimeError, match="database commit failed"):
        await recover_page_access_token(
            pool,
            secrets,
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            operation="messages.list",
            now=NOW,
            client_factory=lambda *_: client,
        )

    assert row["page_access_token_ref"] == "secret://old-page:page-a"
    assert "secret://old-page:page-a" in secrets.values
    assert secrets.deleted == secrets.put_refs


async def test_recovery_isolated_by_exact_tenant_and_installation() -> None:
    tenant_a, tenant_b = uuid4(), uuid4()
    install_a, install_b = uuid4(), uuid4()
    row_a = _row(
        tenant_id=tenant_a,
        installation_id=install_a,
        page_id="page-a",
    )
    row_b = _row(
        tenant_id=tenant_b,
        installation_id=install_b,
        page_id="page-b",
    )
    pool = _Pool(row_a, row_b)
    secrets = _Secrets([row_a, row_b])
    await _schedule(pool, row_a)

    await recover_page_access_token(
        pool,
        secrets,
        tenant_id=tenant_a,
        installation_row_id=install_a,
        operation="conversations.list",
        now=NOW,
        client_factory=lambda *_: _Client(
            [{"id": "page-a", "access_token": "replacement-a"}],
        ),
    )

    assert row_b["connection_state"] == CONNECTED
    assert row_b["page_access_token_ref"] == "secret://old-page:page-b"
    assert all(
        args[:2] != (install_b, tenant_a)
        for _, args in pool.calls
        if len(args) >= 2
    )


async def test_expired_user_token_requires_reauthorization_without_provider_call() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    row = _row(
        tenant_id=tenant_id,
        installation_id=installation_id,
        page_id="page-a",
        user_expires_at=NOW - timedelta(seconds=1),
    )
    pool = _Pool(row)
    old_ref = str(row["page_access_token_ref"])

    result = await schedule_page_token_recovery(
        pool,
        tenant_id=tenant_id,
        installation_row_id=installation_id,
        expected_page_token_ref=old_ref,
        graph_error_subcode=463,
        now=NOW,
    )

    assert result.state == REAUTHORIZATION_REQUIRED
    assert result.not_before is None
    assert row["page_access_token_ref"] == old_ref
    assert row["page_recovery_last_error_code"].endswith("_463")


async def test_provider_cooldown_is_persisted_and_released_without_hot_loop() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    row = _row(
        tenant_id=tenant_id,
        installation_id=installation_id,
        page_id="page-a",
    )
    pool = _Pool(row)
    secrets = _Secrets([row])
    await _schedule(pool, row)
    retry_at = NOW + timedelta(minutes=5)
    cooldown = RetryLater(
        request_context=RequestContext(
            source="facebook_pages",
            operation="pages.list",
            tenant_id=str(tenant_id),
            installation_id=str(installation_id),
        ),
        not_before=retry_at,
        reason=RetryReason.RATE_LIMIT,
        retry_after_seconds=300,
    )

    with pytest.raises(RetryLater) as caught:
        await recover_page_access_token(
            pool,
            secrets,
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            operation="messages.list",
            now=NOW,
            client_factory=lambda *_: _Client(error=cooldown),
        )

    assert caught.value is cooldown
    assert row["page_token_recovery_next_attempt_at"] == retry_at
    assert row["page_recovery_lease_owner"] is None
    assert not secrets.put_refs


async def test_disabled_installation_never_schedules_or_calls_provider() -> None:
    tenant_id, installation_id = uuid4(), uuid4()
    row = _row(
        tenant_id=tenant_id,
        installation_id=installation_id,
        page_id="page-a",
        enabled=False,
    )
    pool = _Pool(row)

    with pytest.raises(ProviderPermanentError):
        await schedule_page_token_recovery(
            pool,
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            expected_page_token_ref=str(row["page_access_token_ref"]),
            graph_error_subcode=463,
            now=NOW,
        )

    assert row["connection_state"] == CONNECTED
    assert row["page_token_recovery_next_attempt_at"] is None
