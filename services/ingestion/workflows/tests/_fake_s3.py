"""In-memory `S3Client` stand-in for ShardFetch backfill-producer tests.

Mirrors the `services.ingestion.raw_tier.s3.S3Client` surface the
backfill producer uses: `connect`, `close`, `put_if_absent`, `get`.
PutIfAbsent is content-addressed-idempotent (a second write to the
same key is a no-op), matching the real client's 412-as-success
semantics — so the N1-retry idempotency property is testable without
moto.
"""
from __future__ import annotations


class FakeS3Client:
    """Dict-backed S3 stand-in. `puts` counts every put_if_absent call
    (including idempotent no-ops) so tests can assert write attempts."""

    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.puts = 0
        self.fail_next_put = False

    async def connect(self) -> None:  # noqa: D401 — interface parity
        return None

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> "FakeS3Client":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def put_if_absent(self, key: str, body: bytes) -> None:
        self.puts += 1
        if self.fail_next_put:
            self.fail_next_put = False
            raise RuntimeError("injected S3 put_if_absent failure")
        # Content-addressed: only the first write for a key lands; a
        # repeat (Kafka-retry) is a no-op success.
        self.store.setdefault(key, body)

    async def get(self, key: str) -> bytes:
        return self.store[key]


__all__ = ["FakeS3Client"]
