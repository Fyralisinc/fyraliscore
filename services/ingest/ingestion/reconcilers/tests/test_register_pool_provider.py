"""Regression: `register_pool_provider` wires EVERY reconciler source.

Guards the ingestion-hardening #19 drift bug: the PeriodicReconciler startup
hand-listed `set_pool_provider` for only 7 of 25 sources, so every steady-state
gap re-check of the other 18 (jira/mercury/brex/…) called `_get_pool()` and
raised RuntimeError — silently swallowed as a dispatch exception, permanently
disabling periodic gap detection for those sources. Both the at-completion
Reconciler and the PeriodicReconciler now derive their registration set from
the source contract via the shared helper, so the two cannot drift apart.
"""
from __future__ import annotations

import importlib

from services.ingest.ingestion.reconcilers import (
    register_pool_provider,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS


def test_register_pool_provider_covers_every_historical_source() -> None:
    sentinel = object()
    sources = [
        source.source_id
        for source in SOURCE_DEFINITIONS
        if source.history is not None
    ]
    assert len(sources) == 26

    mods = {
        source: importlib.import_module(
            f"services.ingest.ingestion.reconcilers.{source}"
        )
        for source in sources
    }
    originals = {s: getattr(mods[s], "_pool_provider", None) for s in sources}

    # Clear every per-source provider so we prove registration actually reached
    # each module (not a leftover pool from another test).
    for s in sources:
        mods[s].set_pool_provider(None)

    try:
        registered = register_pool_provider(sentinel)

        # Every historical source must be registered — no
        # hand-maintained subset that can silently fall behind.
        assert set(registered) == set(sources)
        assert len(registered) == len(sources)

        # And every per-source reconciler now resolves its pool without raising
        # — exactly what failed (RuntimeError) for the unregistered sources in
        # the periodic service before the fix.
        for source, mod in mods.items():
            assert mod._get_pool() is sentinel, source
    finally:
        for s in sources:
            mods[s].set_pool_provider(originals[s])
