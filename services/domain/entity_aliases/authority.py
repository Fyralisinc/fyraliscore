"""Single transaction-local authority for canonical entity identity mutation."""

from __future__ import annotations

from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, AsyncIterator, Callable
from uuid import uuid4


SETTING = "app.entity_identity_command"


@asynccontextmanager
async def identity_command_authority(conn: Any) -> AsyncIterator[str]:
    """Mint one transaction-local identity command capability."""
    command_id = str(uuid4())
    await conn.execute(
        f"SELECT set_config('{SETTING}', $1, true)", f"identity:{command_id}"
    )
    try:
        yield command_id
    except BaseException:
        try:
            await conn.execute(f"SELECT set_config('{SETTING}', '', true)")
        except Exception:
            pass
        raise
    else:
        await conn.execute(f"SELECT set_config('{SETTING}', '', true)")


def governed_identity_writer(operation: Callable[..., Any]) -> Callable[..., Any]:
    """Decorate the existing validated repository operation; add no second writer."""
    @wraps(operation)
    async def wrapped(conn: Any, *args: Any, **kwargs: Any) -> Any:
        async with identity_command_authority(conn):
            return await operation(conn, *args, **kwargs)
    return wrapped


__all__ = ["SETTING", "governed_identity_writer", "identity_command_authority"]
