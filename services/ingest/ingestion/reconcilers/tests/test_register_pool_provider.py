"""Regression: `register_pool_provider` wires EVERY reconciler source.

Guards the ingestion-hardening #19 drift bug: the PeriodicReconciler startup
hand-listed `set_pool_provider` for only 7 of 25 sources, so every steady-state
gap re-check of the other 18 (jira/mercury/brex/…) called `_get_pool()` and
raised RuntimeError — silently swallowed as a dispatch exception, permanently
disabling periodic gap detection for those sources. Both the at-completion
Reconciler and the PeriodicReconciler now derive their registration set from
RECONCILER_DISPATCH via the shared helper, so the two cannot drift apart.
"""
from __future__ import annotations

import importlib

from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    register_pool_provider,
)


def test_register_pool_provider_covers_every_dispatch_source() -> None:
    sentinel = object()
    sources = list(RECONCILER_DISPATCH)

    # Some dispatch entries are live-only placeholders (_not_implemented_reconciler)
    # that ship NO per-source reconciler module — e.g. whatsapp, whose backfill
    # reconciliation is a deferred phase. They have no pool provider to register;
    # register_pool_provider must skip them (rather than crash the whole service
    # on the missing import). The drift guard covers every source that DOES have a
    # module: no hand-maintained subset of those can silently fall behind.
    mods: dict[str, object] = {}
    module_less: list[str] = []
    for source in sources:
        try:
            mods[source] = importlib.import_module(
                f"services.ingest.ingestion.reconcilers.{source}"
            )
        except ModuleNotFoundError:
            module_less.append(source)
    with_modules = list(mods)
    originals = {s: getattr(mods[s], "_pool_provider", None) for s in with_modules}

    # Clear every per-source provider so we prove registration actually reached
    # each module (not a leftover pool from another test).
    for s in with_modules:
        mods[s].set_pool_provider(None)

    try:
        registered = register_pool_provider(sentinel)

        # Every dispatch source that has a reconciler module must be registered —
        # no hand-maintained subset that can silently fall behind. Module-less
        # placeholders (whatsapp) are skipped, not registered.
        assert set(registered) == set(with_modules)
        assert set(registered).isdisjoint(module_less)

        # And every per-source reconciler now resolves its pool without raising
        # — exactly what failed (RuntimeError) for the unregistered sources in
        # the periodic service before the fix.
        for source, mod in mods.items():
            assert mod._get_pool() is sentinel, source
    finally:
        for s in with_modules:
            mods[s].set_pool_provider(originals[s])
