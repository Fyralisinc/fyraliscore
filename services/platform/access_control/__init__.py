"""
services/platform/access_control/ — Wave 5-A full access control implementation.

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

from .authority import (
    AccessLabel,
    AuthorityDecision,
    AuthorityFingerprint,
    AuthorityGrantError,
    AuthorizedReader,
    ObjectRef,
    Principal,
    ProvenanceEdge,
    authorize_read,
    authorized_reader,
    authority_fingerprint,
    current_grant_epoch,
    grant_read_authority,
    labels_for_observation_channel,
    labels_for_resource_kind,
    principal_for_actor,
    record_access_label,
    record_derived_access_labels,
    record_observation_access_labels,
    record_provenance_edge,
    record_resource_access_labels,
    revoke_read_authority,
)
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
    "AccessLabel",
    "AuthorityDecision",
    "AuthorityFingerprint",
    "AuthorityGrantError",
    "AuthorizedReader",
    "MATERIALIZED_VIEWS",
    "ObjectRef",
    "Principal",
    "ProvenanceEdge",
    "authorize_read",
    "authorized_reader",
    "authority_fingerprint",
    "can_read",
    "current_grant_epoch",
    "enqueue_refresh",
    "grant_read_authority",
    "grant_role",
    "has_role",
    "is_in_manager_chain",
    "is_shared_channel",
    "labels_for_observation_channel",
    "labels_for_resource_kind",
    "manager_chain_of",
    "principal_for_actor",
    "record_access_label",
    "record_derived_access_labels",
    "record_observation_access_labels",
    "record_provenance_edge",
    "record_resource_access_labels",
    "refresh_all",
    "refresh_one",
    "revoke_read_authority",
    "revoke_role",
    "roles_for_actor",
]
