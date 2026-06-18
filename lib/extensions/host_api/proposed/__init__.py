"""lib.extensions.host_api.proposed — opt-in, may-break surfaces.

The analogue of VS Code's ``vscode.proposed.d.ts``: contracts that are NOT
SemVer-guaranteed and may change or be withdrawn. Extensions opt in explicitly
and must expect breakage. Today this holds the **reasoning-write** point
(``submit_diff``) — which is **first-party only, indefinitely** (ADR-0004 INV-1):
third parties write at the ingestion edge, never into the synthesis loop.
"""
from __future__ import annotations

from lib.extensions.host_api.proposed.diff import ProposedDiff, submit_diff

__all__ = ["ProposedDiff", "submit_diff"]
