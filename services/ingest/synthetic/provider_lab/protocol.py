"""Provider Lab adapter contract and validated local registry.

The lab deliberately owns a *test-only* coverage baseline while the production
source catalog is being migrated.  ``validate_expected_sources`` is the parity
hook for callers that own the canonical catalog; a missing lab adapter is a
startup error, not an implicit generic mock.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, runtime_checkable

from starlette.routing import compile_path

from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS

_SUPPORTED_METHODS = (
    "DELETE",
    "GET",
    "HEAD",
    "OPTIONS",
    "PATCH",
    "POST",
    "PUT",
)


@dataclass(frozen=True)
class ProviderRoute:
    """One provider-shaped endpoint implemented by a lab adapter."""

    route_id: str
    path_template: str
    methods: tuple[str, ...] = ("GET",)
    quota_bucket: str | None = "default"
    quota_cost: float = 1.0
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
        regex, _path_format, converters = compile_path(self.path_template)
        object.__setattr__(self, "methods", methods)
        object.__setattr__(self, "_path_regex", regex)
        object.__setattr__(self, "_param_converters", converters)

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
    ) -> None:
        self._adapters = dict(adapters)
        self._expected_sources = expected_sources
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

        seen_route_ids: set[str] = set()
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
            for route in adapter.routes:
                if not route.route_id.startswith(f"{source}."):
                    raise ValueError(
                        f"{route.route_id!r} must be namespaced by {source!r}"
                    )
                if route.route_id in seen_route_ids:
                    raise ValueError(f"duplicate Provider Lab route_id {route.route_id!r}")
                seen_route_ids.add(route.route_id)
                for method in route.methods:
                    signature = method, route.path_template
                    if signature in seen_signatures:
                        raise ValueError(
                            f"duplicate route for {source}: {method} "
                            f"{route.path_template}"
                        )
                    seen_signatures.add(signature)

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
                "routes": [
                    {
                        "route_id": route.route_id,
                        "path": route.path_template,
                        "methods": list(route.methods),
                        "quota_bucket": route.quota_bucket,
                        "quota_cost": route.quota_cost,
                    }
                    for route in self._adapters[source].routes
                ],
            }
            for source in self._expected_sources
        ]


__all__ = [
    "AdapterRegistry",
    "ProviderAdapter",
    "ProviderRequest",
    "ProviderResponse",
    "ProviderRoute",
]
