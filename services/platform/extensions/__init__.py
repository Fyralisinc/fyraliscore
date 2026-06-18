"""services.platform.extensions — host-side enforcement for the extension API.

The concrete, DB-touching half of the host API (the contract lives in
``lib.extensions.host_api``):

  - :mod:`substrate_reader` — the capability-checked ``SubstrateReader``
    implementation (returns view types, runs under the ``fyralis_ext_readonly``
    RLS role).

These live under ``services`` (not ``lib``) because they import ``can_read`` /
asyncpg — ``lib`` cannot import ``services``.
"""
from __future__ import annotations
