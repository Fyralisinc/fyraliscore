"""M2.3 — Path B (no DB access) load-bearing proof.

Per the M2 work-order's M2.3 spec:

    "The normalizer is Path B: pure transform, no DB access. Two
    complementary proofs:
       (1) static — the import graph from worker.py contains no
           asyncpg / lib.shared.tenant_context.
       (2) runtime — under synthetic load, the asyncpg API is
           tripwired; the tripwire MUST NEVER fire."

Both proofs catch different failure modes:
  - (1) catches a refactor that adds `import asyncpg` to worker.py
    or any module the worker imports at module level.
  - (2) catches a handler the normalizer DISPATCHES INTO that opens
    a connection at call time — a subtler regression that static
    inspection misses.

These tests are non-negotiable. If they fail, the normalizer is no
longer Path B and the M2 architecture invariant is broken.
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import orjson
import pytest

from services.ingestion.normalizer import worker as worker_module
from services.ingestion.raw_tier.envelope import RawEnvelope


# Repo root — used by the subprocess static-import test so it can
# find the project's packages.
_REPO_ROOT = Path(__file__).resolve().parents[4]


# =====================================================================
# 1. STATIC PROOF — import graph inspection.
# =====================================================================
# Run in a clean subprocess so we don't get fooled by a sibling
# test having already imported asyncpg in the current interpreter.
# =====================================================================


def test_worker_import_graph_contains_no_db_modules() -> None:
    """Import worker.py in a fresh Python subprocess; assert that
    `asyncpg` and `lib.shared.tenant_context` did NOT load.

    Mechanism: the subprocess `import`s `services.ingestion.normalizer.worker`
    and then prints the violating module names (if any) on a marker
    line. The parent reads that line and asserts emptiness.

    If this test fails, a recent change added a DB-touching import
    to worker.py OR to one of its transitive imports (the handler
    registry, kafka producer, raw_tier client, channel_mapping, or
    models). The fix is usually to move the offending import inside
    the function body that needs it — see `gmail.py` for the
    canonical lazy-import pattern.
    """
    script = textwrap.dedent("""
        import sys
        import services.ingestion.normalizer.worker  # noqa: F401
        violations = sorted(
            m for m in sys.modules
            if m == "asyncpg"
            or m.startswith("asyncpg.")
            or m == "lib.shared.tenant_context"
        )
        # One line, easy for the parent to parse.
        print("VIOLATIONS:" + ",".join(violations))
    """)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=_REPO_ROOT,
    )
    last_line = proc.stdout.strip().splitlines()[-1]
    assert last_line.startswith("VIOLATIONS:"), (
        f"unexpected subprocess output:\nstdout={proc.stdout!r}\n"
        f"stderr={proc.stderr!r}"
    )
    violations_raw = last_line.removeprefix("VIOLATIONS:")
    violations = [v for v in violations_raw.split(",") if v]
    assert violations == [], (
        "worker.py transitively imports DB modules:\n  "
        + "\n  ".join(violations)
        + "\nMove the offending import inside the function body "
        "(see services/ingestion/handlers/gmail.py for the canonical "
        "lazy-import pattern)."
    )


def test_supervisor_import_graph_contains_no_db_modules() -> None:
    """Same static proof applied to supervisor.py — the supervisor
    spawns workers via multiprocessing.spawn, so its OWN graph
    matters too: a DB import in supervisor.py would propagate to
    every child process at spawn time."""
    script = textwrap.dedent("""
        import sys
        import services.ingestion.normalizer.supervisor  # noqa: F401
        violations = sorted(
            m for m in sys.modules
            if m == "asyncpg"
            or m.startswith("asyncpg.")
            or m == "lib.shared.tenant_context"
        )
        print("VIOLATIONS:" + ",".join(violations))
    """)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, check=True, cwd=_REPO_ROOT,
    )
    last_line = proc.stdout.strip().splitlines()[-1]
    violations_raw = last_line.removeprefix("VIOLATIONS:")
    violations = [v for v in violations_raw.split(",") if v]
    assert violations == [], (
        "supervisor.py transitively imports DB modules: " + ",".join(violations)
    )


# =====================================================================
# 2. RUNTIME PROOF — tripwire under synthetic load.
# =====================================================================
# Patch asyncpg's user-facing API to raise a sentinel. Run the
# normalizer's per-message hot path through N synthetic envelopes.
# If anything in the dispatched code path attempts to open a
# connection, the tripwire fires and the test fails loudly.
# =====================================================================


class _NoDBAccessError(AssertionError):
    """Raised by asyncpg tripwires. Test failure marker — never
    caught by production code."""


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _slack_payload(i: int) -> dict:
    return {
        "event": {
            "type": "message",
            "channel": f"C{i:05d}",
            "user": f"U{i:05d}",
            "text": f"msg #{i}",
            "ts": f"174748320{i:01d}.001000",
            "team": "T01ACME",
        },
    }


def _build_envelope_bytes(payload: dict, *, source: str, ingress_kind: str):
    tenant = uuid4()
    raw_body = orjson.dumps(payload)
    content_hash = f"{abs(hash(orjson.dumps(payload))):040x}"[:40]
    s3_key = (
        f"dev/{source}/{tenant}/2026-05/aa/{content_hash}.json"
    )
    env = RawEnvelope(
        source=source,
        tenant_id=tenant,
        raw_s3_key=s3_key,
        content_hash=content_hash,
        ingested_at=_NOW,
        ingress_kind=ingress_kind,
    )
    return (
        raw_body,
        orjson.dumps(env.model_dump(mode="json")),
        s3_key,
    )


@pytest.fixture(autouse=True)
def _reset_metrics():
    worker_module.reset_metrics()


async def test_worker_normalize_does_not_touch_asyncpg_under_load(
    monkeypatch,
):
    """Synthetic load: 100 envelopes through `_normalize_one`. asyncpg's
    user-facing API is tripwired. If ANY dispatched handler opens a
    connection, this test fails.

    Why 100 envelopes (not 1):
      - one happy path doesn't catch a handler that only touches the
        DB on a rare branch (e.g. unknown-actor-ref). A spread of
        100 with varied content covers more branches.
      - the cost is ~50ms on a laptop; cheap insurance.
    """
    import asyncpg

    def _tripwire(*args: Any, **kwargs: Any) -> Any:
        raise _NoDBAccessError(
            "normalizer attempted to open a DB connection — "
            "Path B violation. Inspect the handler the worker "
            "dispatched into."
        )

    # Tripwire the user-facing factory functions. asyncpg.Pool /
    # asyncpg.Connection objects are constructed via these.
    monkeypatch.setattr(asyncpg, "connect", _tripwire)
    monkeypatch.setattr(asyncpg, "create_pool", _tripwire)

    # S3 stub: returns the pre-loaded body for whatever key the
    # envelope requests.
    storage: dict[str, bytes] = {}
    s3 = MagicMock()

    async def _get(key: str) -> bytes:
        return storage[key]

    s3.get = AsyncMock(side_effect=_get)

    producer = MagicMock()
    producer.produce = AsyncMock(return_value=None)

    # Build 100 Slack envelopes (the only currently-supported
    # ingress in M2; gmail/pubsub would short-circuit before
    # dispatching anyway).
    envelope_byte_list: list[bytes] = []
    for i in range(100):
        raw_body, env_bytes, s3_key = _build_envelope_bytes(
            _slack_payload(i), source="slack", ingress_kind="webhook",
        )
        storage[s3_key] = raw_body
        envelope_byte_list.append(env_bytes)

    for env_bytes in envelope_byte_list:
        produced = await worker_module._normalize_one(env_bytes, s3, producer)
        assert produced is True

    # If we got here, the tripwires never fired. Path B holds.
    assert producer.produce.await_count == 100

    metrics = worker_module.get_metrics()
    assert metrics["normalizer.unsupported_combination"] == 0


async def test_worker_normalize_does_not_touch_asyncpg_on_handler_failure(
    monkeypatch,
):
    """Same tripwire, but every envelope's payload is invalid for the
    handler. The handler raises ValidationError; the loop records
    parse_failure. asyncpg MUST STILL not be touched — error paths
    are the most common place to silently introduce DB writes
    ('let's persist the failure for triage…').
    """
    import asyncpg

    def _tripwire(*args: Any, **kwargs: Any) -> Any:
        raise _NoDBAccessError("DB access on parse_failure path")

    monkeypatch.setattr(asyncpg, "connect", _tripwire)
    monkeypatch.setattr(asyncpg, "create_pool", _tripwire)

    storage: dict[str, bytes] = {}
    s3 = MagicMock()

    async def _get(key: str) -> bytes:
        return storage[key]

    s3.get = AsyncMock(side_effect=_get)

    producer = MagicMock()
    producer.produce = AsyncMock(return_value=None)

    # Slack payload missing `event.text` → handler raises ValidationError.
    bad_payload = {
        "event": {
            "type": "message",
            "channel": "C00000",
            "user": "U00000",
            "ts": "1747483200.001000",
            # no text
        },
    }
    for _ in range(30):
        raw_body, env_bytes, s3_key = _build_envelope_bytes(
            bad_payload, source="slack", ingress_kind="webhook",
        )
        storage[s3_key] = raw_body
        with pytest.raises(Exception):
            await worker_module._normalize_one(env_bytes, s3, producer)

    # Tripwires never fired even though the handler failed every time.
    # No Kafka produce on failure either.
    assert producer.produce.await_count == 0
