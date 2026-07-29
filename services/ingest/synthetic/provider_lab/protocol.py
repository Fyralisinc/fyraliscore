"""Provider Lab adapter contract and validated local registry.

The lab deliberately owns a *test-only* coverage baseline while the production
source catalog is being migrated.  ``validate_expected_sources`` is the parity
hook for callers that own the canonical catalog; a missing lab adapter is a
startup error, not an implicit generic mock.
"""
from __future__ import annotations

import json
from dataclasses import KW_ONLY, dataclass, field
from datetime import datetime
from typing import Any, Collection, Literal, Mapping, Protocol, runtime_checkable

from starlette.routing import compile_path

from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    SOURCE_OPERATION_POLICY_CATALOG,
)

_SUPPORTED_METHODS = (
    "DELETE",
    "GET",
    "HEAD",
    "OPTIONS",
    "PATCH",
    "POST",
    "PUT",
)
_ROUTE_TRANSPORTS = frozenset(
    {
        "aws_sigv4",
        "graphql",
        "http",
        "json_rpc",
        "sse",
    }
)
_PROTOCOL_TRANSPORTS = frozenset({"injected_transport", "websocket"})


def _validate_operation_ids(operation_ids: tuple[str, ...]) -> tuple[str, ...]:
    normalized = tuple(dict.fromkeys(operation_ids))
    if normalized != operation_ids:
        raise ValueError("Provider Lab operation_ids must be unique and ordered")
    if any(
        not operation_id or operation_id.strip() != operation_id
        for operation_id in normalized
    ):
        raise ValueError(
            "Provider Lab operation_ids must be non-empty, trimmed strings"
        )
    return normalized


@dataclass(frozen=True)
class ProviderOperationBinding:
    """Exact request semantics for one operation multiplexed by a route.

    Provider routes such as GraphQL, JSON-RPC, AWS Query/JSON, and Gmail
    Pub/Sub share a URL across several source-contract operations.  A load
    harness may credit an operation only by issuing this exact method, query,
    headers, and body.  Static byte payloads keep the contract deterministic
    and dependency-light; dynamic provider-client behavior belongs in the
    source-specific certification runner.
    """

    operation_id: str
    method: str
    path_values: tuple[tuple[str, str], ...] = ()
    query_items: tuple[tuple[str, str], ...] = ()
    headers: tuple[tuple[str, str], ...] = ()
    body: bytes | None = None

    def __post_init__(self) -> None:
        operation_ids = _validate_operation_ids((self.operation_id,))
        method = self.method.upper()
        if method not in _SUPPORTED_METHODS:
            raise ValueError(
                f"unsupported Provider Lab operation method: {self.method!r}"
            )
        for name, pairs in (
            ("path_values", self.path_values),
            ("query_items", self.query_items),
            ("headers", self.headers),
        ):
            if any(
                not isinstance(key, str)
                or not key
                or not isinstance(value, str)
                for key, value in pairs
            ):
                raise ValueError(
                    f"Provider Lab operation {name} must contain string pairs"
                )
        header_names = [name.casefold() for name, _value in self.headers]
        if len(header_names) != len(set(header_names)):
            raise ValueError(
                "Provider Lab operation headers must be unique "
                "case-insensitively"
            )
        if self.body is not None and not isinstance(self.body, bytes):
            raise TypeError("Provider Lab operation body must be bytes or None")
        object.__setattr__(self, "operation_id", operation_ids[0])
        object.__setattr__(self, "method", method)


@dataclass(frozen=True)
class ProviderRoute:
    """One provider-shaped request endpoint implemented by a lab adapter.

    ``operation_ids`` names the exact source-contract operations exercised by
    this endpoint. A route may own several operations when the provider
    multiplexes them through one URL (GraphQL, JSON-RPC, AWS SigV4, or an HTTP
    route whose semantics differ by method).
    """

    route_id: str
    path_template: str
    _: KW_ONLY
    operation_ids: tuple[str, ...] = ()
    operation_bindings: tuple[ProviderOperationBinding, ...] = ()
    methods: tuple[str, ...] = ("GET",)
    quota_bucket: str | None = "default"
    quota_cost: float = 1.0
    transport: Literal[
        "aws_sigv4",
        "graphql",
        "http",
        "json_rpc",
        "sse",
    ] = "http"
    _path_regex: Any = field(init=False, repr=False, compare=False)
    _param_converters: Mapping[str, Any] = field(
        init=False, repr=False, compare=False
    )

    def __post_init__(self) -> None:
        if not self.route_id or "." not in self.route_id:
            raise ValueError(
                "route_id must be globally namespaced, for example "
                "'slack.conversations_list'"
            )
        if not self.path_template.startswith("/"):
            raise ValueError("provider route path_template must start with '/'")
        if self.path_template.startswith("/_lab"):
            raise ValueError("provider routes may not occupy the control namespace")
        methods = tuple(dict.fromkeys(method.upper() for method in self.methods))
        if not methods or any(method not in _SUPPORTED_METHODS for method in methods):
            raise ValueError(f"unsupported HTTP method set: {self.methods!r}")
        if self.quota_cost <= 0:
            raise ValueError("quota_cost must be greater than zero")
        if self.transport not in _ROUTE_TRANSPORTS:
            raise ValueError(
                f"unsupported Provider Lab route transport: {self.transport!r}"
            )
        operation_ids = _validate_operation_ids(self.operation_ids)
        operation_bindings = tuple(self.operation_bindings)
        if operation_bindings:
            binding_operation_ids = tuple(
                binding.operation_id for binding in operation_bindings
            )
            if binding_operation_ids != operation_ids:
                raise ValueError(
                    "Provider Lab operation_bindings must cover operation_ids "
                    "exactly once and in declared order"
                )
            binding_methods = {binding.method for binding in operation_bindings}
            if binding_methods != set(methods):
                raise ValueError(
                    "Provider Lab operation_bindings must cover every declared "
                    "HTTP method exactly"
                )
        elif operation_ids and (
            len(operation_ids) != 1 or len(methods) != 1
        ):
            raise ValueError(
                "multiplexed Provider Lab routes require exact "
                "operation_bindings"
            )
        regex, _path_format, converters = compile_path(self.path_template)
        path_parameter_names = set(converters)
        for binding in operation_bindings:
            bound_names = [name for name, _value in binding.path_values]
            if len(bound_names) != len(set(bound_names)):
                raise ValueError(
                    "Provider Lab operation path_values must be unique"
                )
            unknown_names = sorted(set(bound_names) - path_parameter_names)
            if unknown_names:
                raise ValueError(
                    "Provider Lab operation path_values reference unknown "
                    f"parameters: {unknown_names}"
                )
        object.__setattr__(self, "methods", methods)
        object.__setattr__(self, "operation_ids", operation_ids)
        object.__setattr__(self, "operation_bindings", operation_bindings)
        object.__setattr__(self, "_path_regex", regex)
        object.__setattr__(self, "_param_converters", converters)

    def binding_for(self, operation_id: str) -> ProviderOperationBinding:
        """Return the exact request binding or infer the unambiguous default."""

        for binding in self.operation_bindings:
            if binding.operation_id == operation_id:
                return binding
        if self.operation_ids == (operation_id,) and len(self.methods) == 1:
            return ProviderOperationBinding(
                operation_id=operation_id,
                method=self.methods[0],
            )
        raise KeyError(
            f"{operation_id!r} has no exact request binding on {self.route_id!r}"
        )

    def match(self, method: str, path: str) -> dict[str, Any] | None:
        if method.upper() not in self.methods:
            return None
        matched = self._path_regex.match(path)
        if matched is None:
            return None
        return {
            key: self._param_converters[key].convert(value)
            for key, value in matched.groupdict().items()
        }


@dataclass(frozen=True)
class ProviderProtocolSurface:
    """A used provider boundary that is not dispatched as an HTTP route.

    Discord's Gateway WebSocket and Telegram's deliberately finite injected
    transport are examples. ``operation_ids`` is empty only when the source
    contract has no quota-bearing operation for the boundary itself (Discord
    Gateway discovery remains owned by its REST route).
    """

    surface_id: str
    transport: Literal["injected_transport", "websocket"]
    operation_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.surface_id or "." not in self.surface_id:
            raise ValueError(
                "surface_id must be globally namespaced, for example "
                "'discord.gateway'"
            )
        if self.transport not in _PROTOCOL_TRANSPORTS:
            raise ValueError(
                f"unsupported Provider Lab protocol transport: {self.transport!r}"
            )
        object.__setattr__(
            self,
            "operation_ids",
            _validate_operation_ids(self.operation_ids),
        )


@dataclass(frozen=True)
class ProviderRequest:
    """Sanitized request context handed to a provider adapter.

    ``headers`` still contains Authorization so an adapter can reproduce a
    provider's identity behavior.  The request ledger performs its own
    redaction and never stores that value.
    """

    source: str
    route: ProviderRoute
    method: str
    path: str
    url: str
    path_params: Mapping[str, Any]
    query_items: tuple[tuple[str, str], ...]
    headers: Mapping[str, str]
    body: bytes
    scope: str
    source_state: Mapping[str, Any]
    # The Provider Lab owns this deterministic virtual time. Adapters must not
    # read wall-clock time when modeling expiry, renewal, quotas, or retries.
    now: datetime

    def query_one(self, name: str, default: str | None = None) -> str | None:
        for key, value in self.query_items:
            if key == name:
                return value
        return default

    def query_all(self, name: str) -> list[str]:
        return [value for key, value in self.query_items if key == name]

    def json(self) -> Any:
        if not self.body:
            return None
        return json.loads(self.body)


@dataclass(frozen=True)
class ProviderResponse:
    """Adapter-neutral response.

    JSON bodies are serialized canonically by the app.  ``raw_body`` exists
    for endpoints that intentionally return malformed provider payloads.
    """

    status_code: int = 200
    json_body: Any = None
    raw_body: bytes | None = None
    headers: Mapping[str, str] = field(default_factory=dict)
    media_type: str | None = None

    def __post_init__(self) -> None:
        if not 100 <= self.status_code <= 599:
            raise ValueError("status_code must be between 100 and 599")
        if self.raw_body is not None and self.json_body is not None:
            raise ValueError("set json_body or raw_body, not both")

    @classmethod
    def json(
        cls,
        body: Any,
        *,
        status_code: int = 200,
        headers: Mapping[str, str] | None = None,
    ) -> "ProviderResponse":
        return cls(
            status_code=status_code,
            json_body=body,
            headers=dict(headers or {}),
            media_type="application/json",
        )

    @classmethod
    def empty(
        cls,
        *,
        status_code: int = 204,
        headers: Mapping[str, str] | None = None,
    ) -> "ProviderResponse":
        return cls(
            status_code=status_code,
            raw_body=b"",
            headers=dict(headers or {}),
        )


@runtime_checkable
class ProviderAdapter(Protocol):
    """Minimal protocol implemented by all Provider Lab adapters."""

    source: str
    routes: tuple[ProviderRoute, ...]
    protocol_surfaces: tuple[ProviderProtocolSurface, ...]

    def default_state(self) -> Mapping[str, Any]:
        """Return JSON-compatible state used after app creation/reset."""

    def resolve_scope(self, request: ProviderRequest) -> str:
        """Return the quota/ledger identity for this request."""

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        """Handle a route previously matched from ``routes``."""


class AdapterRegistry:
    """Immutable, fail-fast adapter registry."""

    def __init__(
        self,
        adapters: Mapping[str, ProviderAdapter],
        *,
        expected_sources: tuple[str, ...] = CANONICAL_SOURCE_IDS,
        expected_operations: Mapping[str, Collection[str]] | None = None,
    ) -> None:
        self._adapters = dict(adapters)
        self._expected_sources = expected_sources
        operation_catalog = (
            expected_operations
            if expected_operations is not None
            else SOURCE_OPERATION_POLICY_CATALOG
        )
        self._expected_operations = {
            source: frozenset(operation_ids)
            for source, operation_ids in operation_catalog.items()
        }
        self._validate()

    def _validate(self) -> None:
        expected = set(self._expected_sources)
        actual = set(self._adapters)
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        if missing or unexpected:
            raise ValueError(
                "invalid Provider Lab adapter coverage: "
                f"missing={missing}, unexpected={unexpected}"
            )

        unexpected_operation_sources = sorted(
            set(self._expected_operations) - set(self._expected_sources)
        )
        missing_operation_sources = sorted(
            set(self._expected_sources) - set(self._expected_operations)
        )
        if unexpected_operation_sources or missing_operation_sources:
            raise ValueError(
                "invalid Provider Lab expected-operation coverage: "
                f"missing_sources={missing_operation_sources}, "
                f"unexpected_sources={unexpected_operation_sources}"
            )

        seen_surface_ids: set[str] = set()
        for source in self._expected_sources:
            adapter = self._adapters[source]
            if not isinstance(adapter, ProviderAdapter):
                raise TypeError(f"{source!r} does not implement ProviderAdapter")
            if adapter.source != source:
                raise ValueError(
                    f"adapter key/source mismatch: key={source!r}, "
                    f"adapter.source={adapter.source!r}"
                )
            seen_signatures: set[tuple[str, str]] = set()
            declared_operations: set[str] = set()
            for route in adapter.routes:
                if not route.route_id.startswith(f"{source}."):
                    raise ValueError(
                        f"{route.route_id!r} must be namespaced by {source!r}"
                    )
                if route.route_id in seen_surface_ids:
                    raise ValueError(f"duplicate Provider Lab route_id {route.route_id!r}")
                seen_surface_ids.add(route.route_id)
                duplicate_operations = declared_operations.intersection(
                    route.operation_ids
                )
                if duplicate_operations:
                    raise ValueError(
                        f"Provider Lab operations for {source!r} have more than "
                        f"one owner: {sorted(duplicate_operations)!r}"
                    )
                declared_operations.update(route.operation_ids)
                for method in route.methods:
                    signature = method, route.path_template
                    if signature in seen_signatures:
                        raise ValueError(
                            f"duplicate route for {source}: {method} "
                            f"{route.path_template}"
                        )
                    seen_signatures.add(signature)

            for surface in adapter.protocol_surfaces:
                if not isinstance(surface, ProviderProtocolSurface):
                    raise TypeError(
                        f"{source!r} protocol surface does not implement "
                        "ProviderProtocolSurface"
                    )
                if not surface.surface_id.startswith(f"{source}."):
                    raise ValueError(
                        f"{surface.surface_id!r} must be namespaced by {source!r}"
                    )
                if surface.surface_id in seen_surface_ids:
                    raise ValueError(
                        "duplicate Provider Lab surface_id "
                        f"{surface.surface_id!r}"
                    )
                seen_surface_ids.add(surface.surface_id)
                duplicate_operations = declared_operations.intersection(
                    surface.operation_ids
                )
                if duplicate_operations:
                    raise ValueError(
                        f"Provider Lab operations for {source!r} have more than "
                        f"one owner: {sorted(duplicate_operations)!r}"
                    )
                declared_operations.update(surface.operation_ids)

            expected_operations = self._expected_operations[source]
            missing_operations = sorted(
                expected_operations - declared_operations
            )
            unknown_operations = sorted(
                declared_operations - expected_operations
            )
            if missing_operations or unknown_operations:
                raise ValueError(
                    f"Provider Lab operation coverage mismatch for {source!r}: "
                    f"missing={missing_operations}, unknown={unknown_operations}"
                )

            # Validate reset state eagerly, including JSON serializability.
            try:
                json.dumps(adapter.default_state(), sort_keys=True)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"default state for {source!r} is not JSON-compatible"
                ) from exc

    def validate_expected_sources(self, expected_source_ids: tuple[str, ...]) -> None:
        """Fail if a caller-owned source catalog and this registry diverge."""

        expected = set(expected_source_ids)
        actual = set(self._adapters)
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        if missing or unexpected:
            raise ValueError(
                "Provider Lab/source catalog parity failure: "
                f"missing={missing}, unexpected={unexpected}"
            )

    @property
    def sources(self) -> tuple[str, ...]:
        return self._expected_sources

    def get(self, source: str) -> ProviderAdapter | None:
        return self._adapters.get(source)

    def require(self, source: str) -> ProviderAdapter:
        adapter = self.get(source)
        if adapter is None:
            raise KeyError(source)
        return adapter

    def match(
        self, source: str, method: str, path: str
    ) -> tuple[ProviderAdapter, ProviderRoute, dict[str, Any]] | None:
        adapter = self.get(source)
        if adapter is None:
            return None
        for route in adapter.routes:
            params = route.match(method, path)
            if params is not None:
                return adapter, route, params
        return None

    def inventory(self) -> list[dict[str, Any]]:
        return [
            {
                "source": source,
                "implemented": bool(self._adapters[source].routes),
                "expected_operation_ids": sorted(
                    self._expected_operations[source]
                ),
                "owned_operation_ids": sorted(
                    {
                        operation_id
                        for route in self._adapters[source].routes
                        for operation_id in route.operation_ids
                    }
                    | {
                        operation_id
                        for surface in self._adapters[
                            source
                        ].protocol_surfaces
                        for operation_id in surface.operation_ids
                    }
                ),
                "routes": [
                    {
                        "route_id": route.route_id,
                        "path": route.path_template,
                        "methods": list(route.methods),
                        "operation_ids": list(route.operation_ids),
                        "operation_bindings": [
                            {
                                "operation_id": binding.operation_id,
                                "method": binding.method,
                                "query_items": [
                                    list(item) for item in binding.query_items
                                ],
                                "path_values": [
                                    list(item) for item in binding.path_values
                                ],
                                "header_names": sorted(
                                    name.casefold()
                                    for name, _value in binding.headers
                                ),
                                "body_bytes": (
                                    len(binding.body)
                                    if binding.body is not None
                                    else 0
                                ),
                            }
                            for binding in route.operation_bindings
                        ],
                        "transport": route.transport,
                        "quota_bucket": route.quota_bucket,
                        "quota_cost": route.quota_cost,
                    }
                    for route in self._adapters[source].routes
                ],
                "protocol_surfaces": [
                    {
                        "surface_id": surface.surface_id,
                        "transport": surface.transport,
                        "operation_ids": list(surface.operation_ids),
                    }
                    for surface in self._adapters[
                        source
                    ].protocol_surfaces
                ],
            }
            for source in self._expected_sources
        ]


__all__ = [
    "AdapterRegistry",
    "ProviderAdapter",
    "ProviderOperationBinding",
    "ProviderRequest",
    "ProviderResponse",
    "ProviderRoute",
    "ProviderProtocolSurface",
]
