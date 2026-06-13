"""lib.extensions.migrations — discover + apply extension-owned schema.

The platform seam that lets an installed extension **own its database schema**
instead of smuggling its tables into the host's global migration line. An
extension declares a ``company_os.migrations`` entry point resolving to the path
of its own ``*.sql`` migrations directory; the host discovers every such
directory and applies it **after** the core schema, each under its **own
namespaced ledger** (``schema_migrations_ext_<id>``) so the extension's filenames
can never collide with the host's in the shared ``schema_migrations`` PK.

Ordering is load-bearing: extension tables reference ``tenants(id)`` and rely on
the ``app.current_tenant`` RLS context, so core must be applied first. Discovery
mirrors the other ``company_os.*`` seams (cached, failure-isolated). Lives under
``lib`` and uses only ``lib.shared.migrations`` + stdlib + asyncpg — no
``services`` import (import-linter floor).
"""
from __future__ import annotations

import hashlib
import importlib.metadata as importlib_metadata
import logging
import pathlib
import re

import asyncpg

from lib.shared.migrations import apply_migrations_dir

log = logging.getLogger("extensions.migrations")

_ENTRY_POINT_GROUP = "company_os.migrations"
_SANITIZE_RE = re.compile(r"[^a-z0-9_]+")
_LEDGER_PREFIX = "schema_migrations_ext_"
# Postgres truncates identifiers at 63 bytes; keep the whole ledger name within it.
_MAX_SAFE = 63 - len(_LEDGER_PREFIX)

_discovered: list[tuple[str, pathlib.Path]] | None = None


def _ledger_for(extension_id: str) -> str:
    """Per-extension ledger table name — sanitized AND length-capped.

    Long ids would otherwise be silently truncated by Postgres at 63 bytes,
    which could collide two distinct extensions onto one ledger; append a short
    stable hash of the FULL id so the cap preserves uniqueness."""
    safe = _SANITIZE_RE.sub("_", extension_id.lower()).strip("_") or "unknown"
    if len(safe) > _MAX_SAFE:
        digest = hashlib.sha1(extension_id.encode()).hexdigest()[:8]
        safe = f"{safe[: _MAX_SAFE - 9]}_{digest}"
    return f"{_LEDGER_PREFIX}{safe}"


def discover_migration_dirs() -> list[tuple[str, pathlib.Path]]:
    """Resolve ``(extension_id, migrations_dir)`` for every contributing
    extension, once per process (cached).

    The entry-point **name** is the extension id (used for the ledger
    namespace); the entry point resolves to a directory path (a ``str``,
    ``pathlib.Path``, or a zero-arg callable returning one). A non-existent or
    non-directory path is logged and skipped — discovery never raises.
    """
    global _discovered
    if _discovered is not None:
        return _discovered
    found: list[tuple[str, pathlib.Path]] = []
    try:
        entry_points = importlib_metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must never block startup/migrate
        log.warning("extension_migrations_discovery_failed", exc_info=True)
        _discovered = found
        return found
    for ep in entry_points:
        try:
            obj = ep.load()
            value = obj() if callable(obj) else obj
            path = pathlib.Path(value)
            if not path.is_dir():
                log.error(
                    "extension_migrations_bad_path source=%s path=%s", ep.name, path
                )
                continue
            found.append((ep.name, path))
            log.info("extension_migrations_discovered ext=%s dir=%s", ep.name, path)
        except Exception:  # noqa: BLE001 - one bad extension must not break others
            log.error("extension_migrations_load_failed source=%s", ep.name, exc_info=True)
    _discovered = found
    return found


async def apply_extension_migrations(
    conn: asyncpg.Connection, *, on_error: str = "stop"
) -> dict[str, list[str]]:
    """Apply every discovered extension's migrations, each under its own ledger.

    Returns ``{extension_id: [applied_filenames...]}``. Call this **after** the
    core migration set (extension tables FK to ``tenants`` and use the core RLS
    context). Each extension's set is idempotent and ledger-tracked, so re-runs
    are no-ops.
    """
    dirs = discover_migration_dirs()
    # Pre-pass: reject ledger collisions BEFORE applying anything (two distinct
    # extension ids sharing a ledger would let one's filename silently mark the
    # other's migrations as already-applied).
    seen_ledgers: dict[str, str] = {}
    for ext_id, _ in dirs:
        ledger = _ledger_for(ext_id)
        if ledger in seen_ledgers and seen_ledgers[ledger] != ext_id:
            raise RuntimeError(
                f"extension ledger collision: {ext_id!r} and "
                f"{seen_ledgers[ledger]!r} both map to {ledger!r}"
            )
        seen_ledgers[ledger] = ext_id

    results: dict[str, list[str]] = {}
    for ext_id, mig_dir in dirs:
        ledger = _ledger_for(ext_id)
        applied = await apply_migrations_dir(
            conn, mig_dir, on_error=on_error, ledger_table=ledger,
            ensure_partitions=False,  # extension schema must not touch core partitions
        )
        results[ext_id] = applied
        log.info(
            "extension_migrations_applied ext=%s applied=%d ledger=%s",
            ext_id, len(applied), ledger,
        )
    return results


def reset_for_tests() -> None:
    """Force re-discovery (test isolation only)."""
    global _discovered
    _discovered = None


__all__ = [
    "discover_migration_dirs",
    "apply_extension_migrations",
    "reset_for_tests",
]
