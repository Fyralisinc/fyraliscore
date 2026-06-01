"""Gmail integration — Workspace Domain-Wide Delegation (DWD).

specs/003-gmail-integration/plan.md

Public surface intentionally kept narrow. Most modules are internal to
the gmail subpackage; only the connect/uninstall HTTP handlers, the
push-handler entry point, the worker bodies, and the ingest handler
registration are imported from outside.
"""
from __future__ import annotations

__all__: list[str] = []
