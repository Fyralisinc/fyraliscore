"""
services/access_control/ — Wave 5-A full access control implementation.

Spec refs: ARCHITECTURE-FINAL.md §26 (five-layer model), §11 (first-
person override interacts with Layer 5), §21 (realtime filtering).

Public surface (do NOT import internals directly):

  roles:
    - grant_role
    - revoke_role
    - roles_for_actor
    - has_role

  checks:
    - can_read
    - AccessDecision

  hierarchy:
    - manager_chain_of
    - is_in_manager_chain
    - is_shared_channel

  materialized:
    - refresh_all
    - refresh_one
    - enqueue_refresh
"""
from __future__ import annotations

from .checks import AccessDecision, can_read
from .hierarchy import is_in_manager_chain, is_shared_channel, manager_chain_of
from .materialized import (
    MATERIALIZED_VIEWS,
    enqueue_refresh,
    refresh_all,
    refresh_one,
)
from .roles import grant_role, has_role, revoke_role, roles_for_actor


__all__ = [
    "AccessDecision",
    "MATERIALIZED_VIEWS",
    "can_read",
    "enqueue_refresh",
    "grant_role",
    "has_role",
    "is_in_manager_chain",
    "is_shared_channel",
    "manager_chain_of",
    "refresh_all",
    "refresh_one",
    "revoke_role",
    "roles_for_actor",
]
