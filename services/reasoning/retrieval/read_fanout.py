from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator

import asyncpg


def _pool_max_size(pool: asyncpg.Pool) -> int:
    get_max_size = getattr(pool, "get_max_size", None)
    if callable(get_max_size):
        try:
            return max(1, int(get_max_size()))
        except (TypeError, ValueError):
            return 1
    return 1


@dataclass(frozen=True)
class ReadFanoutSnapshot:
    max_concurrency: int
    in_use: int
    peak_in_use: int
    acquired: int
    denied: int


class ReadFanoutBudget:
    """Shared read-pool gate for nested retrieval fanout.

    Outer fanout stages may wait for a slot. Nested fanout stages should use
    ``connection_if_available`` and fall back to their current connection when
    no spare slot exists.
    """

    def __init__(self, pool: asyncpg.Pool, *, max_concurrency: int) -> None:
        self.pool = pool
        self.max_concurrency = max(1, int(max_concurrency))
        self._condition = asyncio.Condition()
        self._in_use = 0
        self._peak_in_use = 0
        self._acquired = 0
        self._denied = 0

    @classmethod
    def from_pool(
        cls,
        pool: asyncpg.Pool,
        *,
        reserve_connections: int = 0,
        max_concurrency: int | None = None,
    ) -> "ReadFanoutBudget":
        if max_concurrency is None:
            max_concurrency = _pool_max_size(pool) - max(0, int(reserve_connections))
        return cls(pool, max_concurrency=max(1, int(max_concurrency)))

    @property
    def available_estimate(self) -> int:
        return max(0, self.max_concurrency - self._in_use)

    def snapshot(self) -> ReadFanoutSnapshot:
        return ReadFanoutSnapshot(
            max_concurrency=self.max_concurrency,
            in_use=self._in_use,
            peak_in_use=self._peak_in_use,
            acquired=self._acquired,
            denied=self._denied,
        )

    async def _reserve(self, *, wait: bool) -> bool:
        async with self._condition:
            if not wait and self._in_use >= self.max_concurrency:
                self._denied += 1
                return False
            while self._in_use >= self.max_concurrency:
                await self._condition.wait()
            self._in_use += 1
            self._acquired += 1
            self._peak_in_use = max(self._peak_in_use, self._in_use)
            return True

    async def _release(self) -> None:
        async with self._condition:
            self._in_use = max(0, self._in_use - 1)
            self._condition.notify()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        await self._reserve(wait=True)
        try:
            async with self.pool.acquire() as conn:
                yield conn
        finally:
            await self._release()

    @asynccontextmanager
    async def connection_if_available(self) -> AsyncIterator[Any | None]:
        reserved = await self._reserve(wait=False)
        if not reserved:
            yield None
            return
        try:
            async with self.pool.acquire() as conn:
                yield conn
        finally:
            await self._release()

