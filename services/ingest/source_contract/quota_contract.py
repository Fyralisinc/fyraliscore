"""Contract-derived identities for deployment-supplied provider quota limits.

``SourceDefinition.operation_policies`` is the sole owner of source and
operation membership.  Deployments still own verified provider-specific quota
numbers and quota dimensions, but refer to operations through opaque references
derived here instead of maintaining another source -> operation registry.

The catalog digest covers every source-owned ``RequestPolicy`` field.  A
deployment configuration created for an older retry/concurrency contract
therefore fails at startup even when its operation references still exist.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from enum import Enum
from types import MappingProxyType
from typing import Any

from lib.shared.provider_transport import RequestPolicy
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.source_contract.models import SourceDefinition


PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION = "1"
_OPERATION_REFERENCE_PREFIX = "qop_v1_"


@dataclass(frozen=True, slots=True)
class ProviderQuotaOperationIdentity:
    """Opaque deployment reference for one source-owned provider operation."""

    reference: str
    source_id: str
    operation_id: str


@dataclass(frozen=True, slots=True)
class ProviderQuotaContract:
    """Immutable identity/hash projection of source-owned operation policies."""

    schema_version: str
    catalog_sha256: str
    operations: tuple[ProviderQuotaOperationIdentity, ...]
    operations_by_reference: Mapping[str, ProviderQuotaOperationIdentity]

    def reference_for(self, source_id: str, operation_id: str) -> str:
        """Return the opaque reference for one exact source operation."""

        for identity in self.operations:
            if (
                identity.source_id == source_id
                and identity.operation_id == operation_id
            ):
                return identity.reference
        raise KeyError(
            f"provider quota contract has no operation "
            f"{source_id!r}/{operation_id!r}"
        )


def _json_value(value: object) -> object:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, tuple):
        return [_json_value(item) for item in value]
    return value


def _request_policy_payload(policy: RequestPolicy) -> dict[str, object]:
    return {
        field.name: _json_value(getattr(policy, field.name))
        for field in fields(RequestPolicy)
    }


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _operation_reference(source_id: str, operation_id: str) -> str:
    digest = hashlib.sha256(
        _canonical_json(
            {
                "operation_id": operation_id,
                "schema_version": PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
                "source_id": source_id,
            }
        )
    ).hexdigest()
    return f"{_OPERATION_REFERENCE_PREFIX}{digest}"


def build_provider_quota_contract(
    sources: Sequence[SourceDefinition],
) -> ProviderQuotaContract:
    """Build the exact quota-operation identity catalog from source contracts."""

    policy_rows: list[dict[str, Any]] = []
    identities: list[ProviderQuotaOperationIdentity] = []
    seen_pairs: set[tuple[str, str]] = set()
    seen_references: set[str] = set()

    for source in sorted(sources, key=lambda item: item.source_id):
        for operation in sorted(
            source.operation_policies,
            key=lambda item: item.operation_id,
        ):
            pair = (source.source_id, operation.operation_id)
            if pair in seen_pairs:
                raise ValueError(
                    "provider quota contract contains duplicate source operation "
                    f"{source.source_id!r}/{operation.operation_id!r}"
                )
            seen_pairs.add(pair)
            reference = _operation_reference(*pair)
            if reference in seen_references:
                raise ValueError(
                    "provider quota operation reference collision for "
                    f"{source.source_id!r}/{operation.operation_id!r}"
                )
            seen_references.add(reference)
            identity = ProviderQuotaOperationIdentity(
                reference=reference,
                source_id=source.source_id,
                operation_id=operation.operation_id,
            )
            identities.append(identity)
            policy_rows.append(
                {
                    "operation_id": operation.operation_id,
                    "operation_reference": reference,
                    "request_policy": _request_policy_payload(operation.request_policy),
                    "source_id": source.source_id,
                }
            )

    if not identities:
        raise ValueError("provider quota contract must contain operations")

    digest = hashlib.sha256(
        _canonical_json(
            {
                "operations": policy_rows,
                "schema_version": PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
            }
        )
    ).hexdigest()
    by_reference = MappingProxyType(
        {identity.reference: identity for identity in identities}
    )
    return ProviderQuotaContract(
        schema_version=PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION,
        catalog_sha256=digest,
        operations=tuple(identities),
        operations_by_reference=by_reference,
    )


PROVIDER_QUOTA_CONTRACT = build_provider_quota_contract(SOURCE_DEFINITIONS)


__all__ = [
    "PROVIDER_QUOTA_CONFIG_SCHEMA_VERSION",
    "PROVIDER_QUOTA_CONTRACT",
    "ProviderQuotaContract",
    "ProviderQuotaOperationIdentity",
    "build_provider_quota_contract",
]
