"""services/ingest/integrations/google_calendar/watch.py — events.watch channel.

The per-source `WatchSpec` + scheduler entrypoint for Google Calendar's native
push channel. The generic engine (`_google_watch`) owns register / renew /
push-resolve / drain; this file only supplies the Calendar-specific client
calls + shard shape.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from services.ingest.integrations._google_watch import (
    WatchSpec,
    renew_exact_resource,
    run_watch_scheduler as _run_scheduler,
)
from services.ingest.integrations.bounded_renewal import (
    RenewalInvocation,
    RenewalOutcome,
)
from services.ingest.integrations.google_calendar.client import (
    GoogleCalendarClient,
    resolve_scope,
)
from services.ingest.source_contract.catalog import dedicated_ingress_definition


_INGRESS = dedicated_ingress_definition("google_calendar_push")


async def _make_client(
    scope: str, *, tenant_id: Any, installation_id: Any,
):  # noqa: ANN202
    from services.ingest.integrations.gmail.client import build_google_http_client
    from services.ingest.integrations.gmail.dwd import get_minter

    http = build_google_http_client(
        get_minter(),
        source="google_calendar",
        tenant_id=str(tenant_id),
        installation_id=str(installation_id),
    )
    await http.__aenter__()
    client = GoogleCalendarClient(http, scope=resolve_scope(scope))

    async def close() -> None:
        await http.__aexit__(None, None, None)

    return client, close


async def _do_watch(client, *, row, channel_id, address, token, ttl_seconds):  # noqa: ANN001
    return await client.watch_events(
        calendar_id=row["calendar_id"],
        user_email=row["owner_email"],
        channel_id=channel_id,
        address=address,
        token=token,
        ttl_seconds=ttl_seconds,
    )


async def _do_stop(client, *, row):  # noqa: ANN001
    await client.stop_channel(
        user_email=row["owner_email"],
        channel_id=row["watch_channel_id"],
        resource_id=row["watch_resource_id"],
    )


async def _fetch(install, shard_identifier, cursor):  # noqa: ANN001
    from services.ingest.ingestion.fetchers.google_calendar import (
        fetch_page_google_calendar,
    )
    return await fetch_page_google_calendar(install, shard_identifier, cursor)


def _build_shard(row: asyncpg.Record | dict) -> dict[str, Any]:
    return {
        "calendar_id": row["calendar_id"],
        "owner_email": row["owner_email"],
        "installation_id": str(row["installation_id"]),
        "sync_token": row["cursor_token"],
    }


SPEC = WatchSpec(
    source=_INGRESS.source_id,
    table="google_calendar_calendars",
    install_table="google_calendar_installations",
    install_fk="google_calendar_installation_id",
    cursor_col="sync_token",
    cursor_next_key="next_sync_token",
    channel=_INGRESS.channel,
    id_cols=("calendar_id", "owner_email"),
    push_path=_INGRESS.route_path,
    make_client=_make_client,
    do_watch=_do_watch,
    do_stop=_do_stop,
    fetcher=_fetch,
    build_shard=_build_shard,
)


async def run_forever(pool: asyncpg.Pool, **kw) -> None:
    await _run_scheduler(pool, SPEC, **kw)


async def renew_exact_installation(
    invocation: RenewalInvocation,
) -> RenewalOutcome:
    """Contract binding for one exact Calendar watch resource renewal."""

    return await renew_exact_resource(invocation, SPEC)


__all__ = ["SPEC", "renew_exact_installation", "run_forever"]
