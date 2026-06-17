"""lib.extensions.host_api — versioned host API surface.

The current published surface is :mod:`lib.extensions.host_api.v1`. New major
versions land as sibling ``vN`` packages; a manifest pins the range it supports
via ``engines.fyralis_host_api`` (validated at discovery). A ``proposed``
sub-surface (opt-in, may break) is reserved for points that are not yet stable
(e.g. the first-party-only reasoning-write/``submit_diff`` path — ADR-0004 INV-1).
"""
from __future__ import annotations

from lib.extensions.host_api.v1 import HOST_API_VERSION

__all__ = ["HOST_API_VERSION"]
