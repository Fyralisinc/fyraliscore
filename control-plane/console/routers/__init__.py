"""console.routers — the FEATURE router plugin directory.

Each feature adds ONE module here exposing a module-level::

    def register(app, deps): ...

``app.py`` scans this directory at startup and calls every ``register(app, deps)``
(see ``app._mount_feature_routers``). A feature NEVER edits ``app.py``; it just
drops a file here and wires its endpoints onto ``app`` using ``deps`` (the store,
signer, audit, require_operator, require_agent_write, settings).

A broken router (import error / register() raising) is logged and SKIPPED — it can
never take the console down. See ``routers/example_router.py`` for the contract.
"""
