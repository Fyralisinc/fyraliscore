"""services/ingest/integrations/google_drive/watch.py — changes.watch channel.

The per-source `WatchSpec` + scheduler entrypoint for Google Drive's native
push channel. The generic engine (`_google_watch`) owns register / renew /
push-resolve / drain; this file only supplies the Drive-specific client calls +
shard shape.
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
from services.ingest.integrations.google_drive.client import (
    GoogleDriveClient,
    resolve_scope,
)
from services.ingest.source_contract.catalog import dedicated_ingress_definition


_INGRESS = dedicated_ingress_definition("google_drive_push")


async def _make_client(
    scope: str, *, tenant_id: Any, installation_id: Any,
):  # noqa: ANN202
    from services.ingest.integrations.gmail.client import build_google_http_client
    from services.ingest.integrations.gmail.dwd import get_minter

    http = build_google_http_client(
        get_minter(),
        source="google_drive",
        tenant_id=str(tenant_id),
        installation_id=str(installation_id),
    )
    await http.__aenter__()
    client = GoogleDriveClient(http, scope=resolve_scope(scope))

    async def close() -> None:
        await http.__aexit__(None, None, None)

    return client, close


async def _do_watch(client, *, row, channel_id, address, token, ttl_seconds):  # noqa: ANN001
    return await client.watch_changes(
        user_email=row["owner_email"],
        page_token=row["cursor_token"],
        channel_id=channel_id,
        address=address,
        token=token,
        drive_id=row["drive_id"],
        ttl_seconds=ttl_seconds,
    )


async def _do_stop(client, *, row):  # noqa: ANN001
    await client.stop_channel(
        user_email=row["owner_email"],
        channel_id=row["watch_channel_id"],
        resource_id=row["watch_resource_id"],
    )


async def _fetch(install, shard_identifier, cursor):  # noqa: ANN001
    from services.ingest.ingestion.fetchers.google_drive import fetch_page_google_drive
    return await fetch_page_google_drive(install, shard_identifier, cursor)


def _build_shard(row: asyncpg.Record | dict) -> dict[str, Any]:
    return {
        "drive_kind": row["drive_kind"],
        "drive_id": row["drive_id"],
        "owner_email": row["owner_email"],
        "installation_id": str(row["installation_id"]),
        "start_page_token": row["cursor_token"],
    }


SPEC = WatchSpec(
    source=_INGRESS.source_id,
    table="google_drive_targets",
    install_table="google_drive_installations",
    install_fk="google_drive_installation_id",
    cursor_col="start_page_token",
    cursor_next_key="next_start_page_token",
    channel=_INGRESS.channel,
    id_cols=("drive_kind", "drive_id", "owner_email"),
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
    """Contract binding for one exact Drive watch resource renewal."""

    return await renew_exact_resource(invocation, SPEC)


__all__ = ["SPEC", "renew_exact_installation", "run_forever"]
