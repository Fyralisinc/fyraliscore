"""Dependency-light declarations for ingestion source contracts.

The types use the shared provider-transport ``RequestPolicy`` and otherwise
describe wiring with stable strings instead of importing planner, fetcher,
handler, gateway, or worker implementations.  That keeps the catalog safe to
import from tooling, tests, and future control-plane code without pulling the
ingestion runtime into the import graph.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from lib.shared.provider_transport import RequestPolicy, RetrySafety


HistoryKind = Literal["api", "session"]
IngressKind = Literal["webhook", "gateway", "pubsub", "backfill", "poll"]
LiveTransportKind = Literal[
    "webhook",
    "pubsub",
    "websocket",
    "mtproto",
    "json_rpc",
    "api_poll",
    "queue_poll",
]
AcknowledgementPolicy = Literal[
    "durable_before_ack",
    "checkpoint_after_durable",
    "cursor_after_durable",
]
DeliveryPolicy = Literal[
    "at_least_once",
    "replayable_stream",
    "replayable_pull",
]
AllowedObservationKind = Literal[
    "signal",
    "state_change",
    "anomaly_flagged",
    "contestation",
    "prediction_resolution",
    "transaction",
]
AllowedTrustTier = Literal[
    "authoritative",
    "attested_agent",
    "authoritative_external",
    "reputable",
    "inferential",
    "inferential_external",
    "unvetted",
    "derived",
]
ChannelOwnerKind = Literal["provider", "platform"]
CertificationStatus = Literal["unverified", "verified", "suspended"]
WebhookTenantBinding = Literal["payload", "path_then_payload"]
WebhookHandlerMode = Literal["generic", "dedicated"]
WebhookAcknowledgementPolicy = Literal[
    "observation_response",
    "synchronous_provider_response",
    "dedicated_handler",
]
WebhookKafkaMode = Literal[
    "inline_only",
    "inline_then_shadow",
    "flagged_kafka_first_with_inline_fallback",
    "dedicated_shadow_then_ack",
]
DedicatedIngressMethod = Literal["GET", "POST"]
DedicatedAcknowledgementPolicy = Literal[
    "ack_and_reconcile_on_failure",
    "durable_or_inline_before_ack",
]
DedicatedKafkaMode = Literal[
    "hydrated_messages_handler_managed",
    "reconciled_delta_drain",
    "flagged_kafka_first_with_inline_fallback",
]
OAuthIngressMountMode = Literal["shared_router", "native_router"]
CredentialGrantType = Literal["refresh_token", "client_credentials"]
CredentialAuthStyle = Literal["basic", "body"]
SourceCategory = Literal[
    "Communication",
    "Engineering",
    "Productivity",
    "Knowledge",
    "Meetings",
    "Finance",
    "People",
    "Cloud",
    "Design",
    "Operations",
    "CRM",
]
SourceConnectionMethod = Literal[
    "OAuth",
    "Webhook",
    "API token",
    "Gateway",
    "IAM role",
    "Workspace DWD",
    "Poll",
]
SourceSyncMode = Literal[
    "Dry run",
    "Limited backfill",
    "Live events",
    "Backfill plus live",
    "Backfill plus polling",
]


_HISTORY_KINDS = frozenset({"api", "session"})
_INGRESS_KINDS = frozenset({"webhook", "gateway", "pubsub", "backfill", "poll"})
_LIVE_TRANSPORT_KINDS = frozenset(
    {
        "webhook",
        "pubsub",
        "websocket",
        "mtproto",
        "json_rpc",
        "api_poll",
        "queue_poll",
    }
)
_ACKNOWLEDGEMENT_POLICIES = frozenset(
    {
        "durable_before_ack",
        "checkpoint_after_durable",
        "cursor_after_durable",
    }
)
_DELIVERY_POLICIES = frozenset(
    {"at_least_once", "replayable_stream", "replayable_pull"}
)
_OBSERVATION_KINDS = frozenset(
    {
        "signal",
        "state_change",
        "anomaly_flagged",
        "contestation",
        "prediction_resolution",
        "transaction",
    }
)
_TRUST_TIERS = frozenset(
    {
        "authoritative",
        "attested_agent",
        "authoritative_external",
        "reputable",
        "inferential",
        "inferential_external",
        "unvetted",
        "derived",
    }
)
_CHANNEL_OWNER_KINDS = frozenset({"provider", "platform"})
_CERTIFICATION_STATUSES = frozenset({"unverified", "verified", "suspended"})
_WEBHOOK_TENANT_BINDINGS = frozenset({"payload", "path_then_payload"})
_WEBHOOK_HANDLER_MODES = frozenset({"generic", "dedicated"})
_WEBHOOK_ACKNOWLEDGEMENT_POLICIES = frozenset(
    {
        "observation_response",
        "synchronous_provider_response",
        "dedicated_handler",
    }
)
_WEBHOOK_KAFKA_MODES = frozenset(
    {
        "inline_only",
        "inline_then_shadow",
        "flagged_kafka_first_with_inline_fallback",
        "dedicated_shadow_then_ack",
    }
)
_DEDICATED_INGRESS_METHODS = frozenset({"GET", "POST"})
_DEDICATED_ACKNOWLEDGEMENT_POLICIES = frozenset(
    {
        "ack_and_reconcile_on_failure",
        "durable_or_inline_before_ack",
    }
)
_DEDICATED_KAFKA_MODES = frozenset(
    {
        "hydrated_messages_handler_managed",
        "reconciled_delta_drain",
        "flagged_kafka_first_with_inline_fallback",
    }
)
_OAUTH_INGRESS_MOUNT_MODES = frozenset({"shared_router", "native_router"})
_CREDENTIAL_GRANT_TYPES = frozenset({"refresh_token", "client_credentials"})
_CREDENTIAL_AUTH_STYLES = frozenset({"basic", "body"})
_SOURCE_CATEGORIES = frozenset(
    {
        "Communication",
        "Engineering",
        "Productivity",
        "Knowledge",
        "Meetings",
        "Finance",
        "People",
        "Cloud",
        "Design",
        "Operations",
        "CRM",
    }
)
_SOURCE_CONNECTION_METHODS = frozenset(
    {
        "OAuth",
        "Webhook",
        "API token",
        "Gateway",
        "IAM role",
        "Workspace DWD",
        "Poll",
    }
)
_SOURCE_SYNC_MODES = frozenset(
    {
        "Dry run",
        "Limited backfill",
        "Live events",
        "Backfill plus live",
        "Backfill plus polling",
    }
)

_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_BINDING_RE = re.compile(r"^[a-z][a-z0-9_.:-]*$")
_OPERATION_ID_RE = re.compile(
    r"^(?:[A-Za-z][A-Za-z0-9_.:@{}-]*|/[A-Za-z0-9_.:@{}/-]+)$"
)
_CALLABLE_REF_RE = re.compile(
    r"^[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*:"
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
_FIELD_PATH_RE = re.compile(r"^[a-z][a-z0-9_.]*$")
_HTTP_HEADER_NAME_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
_NON_IDENTIFIER_RUN_RE = re.compile(r"[^a-z0-9]+")
_ENV_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def normalize_catalog_name(value: str) -> str:
    """Normalize a canonical name, display name, or alias for lookup.

    Lookup is deliberately forgiving at the human-facing boundary: case,
    whitespace, dashes, dots, and repeated separators all converge on the
    catalog's snake-case namespace.  Stored canonical identifiers remain
    strict and are never rewritten.
    """

    if not isinstance(value, str):
        raise TypeError("catalog name must be a string")
    return _NON_IDENTIFIER_RUN_RE.sub("_", value.strip().casefold()).strip("_")


def _require_identifier(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a lowercase snake-case identifier; got {value!r}"
        )


def _require_binding(value: str | None, *, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not _BINDING_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a stable lowercase binding identifier; "
            f"got {value!r}"
        )


def _require_operation_id(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not _OPERATION_ID_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a stable semantic operation identifier; "
            f"got {value!r}"
        )


def _require_callable_ref(value: str | None, *, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not _CALLABLE_REF_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must be a colon-qualified module:callable "
            f"reference; got {value!r}"
        )


def _require_nonempty_text(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _require_route_path(value: str | None, *, field_name: str) -> None:
    if value is None:
        return
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or value == "/"
        or any(character.isspace() for character in value)
        or "?" in value
        or "#" in value
    ):
        raise ValueError(
            f"{field_name} must be an absolute route path without query, "
            f"fragment, or whitespace; got {value!r}"
        )


def _require_tuple(
    value: object,
    *,
    field_name: str,
    allow_empty: bool = False,
) -> tuple[object, ...]:
    if not isinstance(value, tuple):
        raise TypeError(f"{field_name} must be a tuple")
    if not allow_empty and not value:
        raise ValueError(f"{field_name} must not be empty")
    return value


def _require_unique_strings(
    values: tuple[str, ...],
    *,
    field_name: str,
    allow_empty: bool = False,
) -> None:
    _require_tuple(values, field_name=field_name, allow_empty=allow_empty)
    seen: set[str] = set()
    for value in values:
        _require_nonempty_text(value, field_name=f"{field_name} entry")
        if value in seen:
            raise ValueError(f"{field_name} contains duplicate {value!r}")
        seen.add(value)


@dataclass(frozen=True, slots=True)
class Certification:
    """Governance declaration for certifying a source implementation.

    A catalog entry declares the stable IDs of the test kit, evidence bundle,
    and canary it will use.  Those declarations are requirements, not proof
    that the source passed them, so ``status`` intentionally defaults to
    ``unverified``.  A later certification runner owns state transitions.
    """

    status: CertificationStatus = "unverified"
    require_test_kit: bool = True
    require_evidence: bool = True
    require_canary: bool = True
    test_kit_id: str | None = None
    evidence_id: str | None = None
    canary_id: str | None = None
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status not in _CERTIFICATION_STATUSES:
            raise ValueError(f"unknown certification status {self.status!r}")
        for field_name in (
            "require_test_kit",
            "require_evidence",
            "require_canary",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a boolean")
        for field_name in ("test_kit_id", "evidence_id", "canary_id"):
            _require_binding(getattr(self, field_name), field_name=field_name)
        _require_unique_strings(
            self.notes,
            field_name="certification notes",
            allow_empty=True,
        )
        if self.status == "verified" and self.missing_required_declarations():
            missing = ", ".join(self.missing_required_declarations())
            raise ValueError(
                "verified certification is missing required declarations: " f"{missing}"
            )

    def missing_required_declarations(self) -> tuple[str, ...]:
        """Return required certification declarations that are absent."""

        missing: list[str] = []
        if self.require_test_kit and self.test_kit_id is None:
            missing.append("test_kit_id")
        if self.require_evidence and self.evidence_id is None:
            missing.append("evidence_id")
        if self.require_canary and self.canary_id is None:
            missing.append("canary_id")
        return tuple(missing)


@dataclass(frozen=True, slots=True)
class WebhookIngressDefinition:
    """Contract for one public provider-webhook route.

    The route identity intentionally differs from ``ProviderDefinition`` for
    providers whose product-facing installation key is more precise than the
    vendor identity (for example ``atlassian`` → ``jira`` and ``intuit`` →
    ``quickbooks``).  Bindings remain dependency-light strings and are
    resolved lazily at the gateway boundary.
    """

    route_id: str
    source_id: str | None
    route_path: str
    channel: str
    verifier_binding: str
    tenant_extractor_binding: str
    ingress_metadata_binding: str
    normalizer_header_projection: tuple[tuple[str, str], ...] = ()
    tenant_binding: WebhookTenantBinding = "payload"
    handler_mode: WebhookHandlerMode = "generic"
    acknowledgement_policy: WebhookAcknowledgementPolicy = "observation_response"
    kafka_mode: WebhookKafkaMode = "inline_only"
    verification_handshake_binding: str | None = None
    verification_handshake_handler_binding: str | None = None
    dedicated_handler_binding: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.route_id, field_name="webhook route_id")
        if self.source_id is not None:
            _require_identifier(self.source_id, field_name="webhook source_id")
        _require_route_path(self.route_path, field_name="webhook route_path")
        route_prefix = f"/webhooks/{self.route_id}"
        if self.route_path != route_prefix and not self.route_path.startswith(
            f"{route_prefix}/"
        ):
            raise ValueError(
                f"webhook route_path {self.route_path!r} must be owned by "
                f"route_id {self.route_id!r} below {route_prefix!r}"
            )
        _require_binding(self.channel, field_name="webhook channel")
        _require_callable_ref(
            self.verifier_binding,
            field_name="webhook verifier binding",
        )
        _require_callable_ref(
            self.tenant_extractor_binding,
            field_name="webhook tenant extractor binding",
        )
        _require_callable_ref(
            self.ingress_metadata_binding,
            field_name="webhook ingress metadata binding",
        )
        _require_tuple(
            self.normalizer_header_projection,
            field_name="webhook normalizer header projection",
            allow_empty=True,
        )
        metadata_keys: set[str] = set()
        header_names: set[str] = set()
        for index, projection in enumerate(self.normalizer_header_projection):
            if not isinstance(projection, tuple) or len(projection) != 2:
                raise TypeError(
                    "webhook normalizer header projection entries must be "
                    "(metadata_key, header_name) tuples"
                )
            metadata_key, header_name = projection
            if (
                not isinstance(metadata_key, str)
                or _FIELD_PATH_RE.fullmatch(metadata_key) is None
            ):
                raise ValueError(
                    "webhook normalizer header projection metadata key "
                    f"{index} is invalid: {metadata_key!r}"
                )
            if (
                not isinstance(header_name, str)
                or _HTTP_HEADER_NAME_RE.fullmatch(header_name) is None
            ):
                raise ValueError(
                    "webhook normalizer header projection header name "
                    f"{index} is invalid: {header_name!r}"
                )
            normalized_header = header_name.casefold()
            if metadata_key in metadata_keys:
                raise ValueError(
                    "webhook normalizer header projection contains duplicate "
                    f"metadata key {metadata_key!r}"
                )
            if normalized_header in header_names:
                raise ValueError(
                    "webhook normalizer header projection contains duplicate "
                    f"header name {header_name!r}"
                )
            metadata_keys.add(metadata_key)
            header_names.add(normalized_header)
        if self.tenant_binding not in _WEBHOOK_TENANT_BINDINGS:
            raise ValueError(f"unknown webhook tenant binding {self.tenant_binding!r}")
        if self.handler_mode not in _WEBHOOK_HANDLER_MODES:
            raise ValueError(f"unknown webhook handler mode {self.handler_mode!r}")
        if self.acknowledgement_policy not in _WEBHOOK_ACKNOWLEDGEMENT_POLICIES:
            raise ValueError(
                "unknown webhook acknowledgement policy "
                f"{self.acknowledgement_policy!r}"
            )
        if self.kafka_mode not in _WEBHOOK_KAFKA_MODES:
            raise ValueError(f"unknown webhook Kafka mode {self.kafka_mode!r}")
        _require_callable_ref(
            self.verification_handshake_binding,
            field_name="webhook verification handshake binding",
        )
        _require_callable_ref(
            self.verification_handshake_handler_binding,
            field_name="webhook verification handshake handler binding",
        )
        _require_callable_ref(
            self.dedicated_handler_binding,
            field_name="webhook dedicated handler binding",
        )

        dedicated_bindings = (
            self.verification_handshake_binding,
            self.verification_handshake_handler_binding,
            self.dedicated_handler_binding,
        )
        if self.handler_mode == "dedicated":
            if any(binding is None for binding in dedicated_bindings):
                raise ValueError(
                    "dedicated webhook ingress requires handshake and handler "
                    "bindings"
                )
            if self.acknowledgement_policy != "dedicated_handler":
                raise ValueError(
                    "dedicated webhook ingress requires dedicated_handler ACK"
                )
            if self.kafka_mode != "dedicated_shadow_then_ack":
                raise ValueError(
                    "dedicated webhook ingress requires dedicated Kafka mode"
                )
        elif any(binding is not None for binding in dedicated_bindings):
            raise ValueError(
                "generic webhook ingress cannot declare dedicated bindings"
            )

        if (
            self.acknowledgement_policy == "synchronous_provider_response"
            and self.kafka_mode == "flagged_kafka_first_with_inline_fallback"
        ):
            raise ValueError(
                "synchronous provider responses cannot use the asynchronous "
                "202 Kafka cutover"
            )

    @property
    def shadow_write_enabled(self) -> bool:
        return self.kafka_mode in {
            "inline_then_shadow",
            "flagged_kafka_first_with_inline_fallback",
        }

    @property
    def kafka_cutover_enabled(self) -> bool:
        return self.kafka_mode == "flagged_kafka_first_with_inline_fallback"


@dataclass(frozen=True, slots=True)
class OAuthIngressDefinition:
    """Exact public OAuth/App-install ingress owned by a provider.

    ``shared_router`` entries are mounted by the common integrations router.
    ``native_router`` entries are mounted by the source module because their
    route family has additional provider-specific endpoints (Figma is the
    current example).  Both modes remain catalogued so callback discovery,
    route-access policy, and onboarding handoff data share one source of truth.
    """

    source_id: str
    install_path: str
    callback_path: str
    install_handler_binding: str
    callback_handler_binding: str
    mount_mode: OAuthIngressMountMode
    public_result_paths: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.source_id, field_name="OAuth source_id")
        _require_route_path(self.install_path, field_name="OAuth install_path")
        _require_route_path(
            self.callback_path,
            field_name="OAuth callback_path",
        )
        if self.install_path == self.callback_path:
            raise ValueError("OAuth install and callback paths must differ")
        _require_callable_ref(
            self.install_handler_binding,
            field_name="OAuth install handler binding",
        )
        _require_callable_ref(
            self.callback_handler_binding,
            field_name="OAuth callback handler binding",
        )
        if self.mount_mode not in _OAUTH_INGRESS_MOUNT_MODES:
            raise ValueError(f"unknown OAuth ingress mount mode {self.mount_mode!r}")
        _require_unique_strings(
            self.public_result_paths,
            field_name="OAuth public result paths",
            allow_empty=True,
        )
        for path in self.public_result_paths:
            _require_route_path(path, field_name="OAuth public result path")


@dataclass(frozen=True, slots=True)
class DedicatedIngressDefinition:
    """Contract for a provider-specific public ingress route.

    Dedicated routes cover protocols that cannot use the generic signed JSON
    webhook dispatcher: Pub/Sub envelopes, content-less Google watch pings,
    and Meta GET-handshake plus batched POST delivery endpoints.
    """

    ingress_id: str
    source_id: str
    route_path: str
    methods: tuple[DedicatedIngressMethod, ...]
    ingress_kind: IngressKind
    channel: str | None
    verification_policy: str
    verification_bindings: tuple[str, ...]
    tenant_binding_policy: str
    tenant_resolver_binding: str
    acknowledgement_policy: DedicatedAcknowledgementPolicy
    kafka_mode: DedicatedKafkaMode
    dispatcher_binding: str
    router_factory_binding: str
    router_factory_accepts_debug_endpoints: bool = False

    def __post_init__(self) -> None:
        _require_identifier(self.ingress_id, field_name="dedicated ingress_id")
        _require_identifier(self.source_id, field_name="dedicated source_id")
        if (
            not isinstance(self.route_path, str)
            or not re.fullmatch(r"/[a-z0-9_/-]+", self.route_path)
            or "//" in self.route_path
        ):
            raise ValueError(
                "dedicated route_path must be an exact lowercase absolute "
                f"path; got {self.route_path!r}"
            )
        _require_unique_strings(self.methods, field_name="dedicated methods")
        unknown_methods = set(self.methods) - _DEDICATED_INGRESS_METHODS
        if unknown_methods:
            raise ValueError(
                f"unknown dedicated ingress methods: {sorted(unknown_methods)}"
            )
        if self.ingress_kind not in _INGRESS_KINDS:
            raise ValueError(f"unknown dedicated ingress kind {self.ingress_kind!r}")
        _require_binding(self.channel, field_name="dedicated ingress channel")
        _require_binding(
            self.verification_policy,
            field_name="dedicated verification policy",
        )
        _require_unique_strings(
            self.verification_bindings,
            field_name="dedicated verification bindings",
        )
        for binding in self.verification_bindings:
            _require_callable_ref(
                binding,
                field_name="dedicated verification binding",
            )
        _require_binding(
            self.tenant_binding_policy,
            field_name="dedicated tenant binding policy",
        )
        _require_callable_ref(
            self.tenant_resolver_binding,
            field_name="dedicated tenant resolver binding",
        )
        if self.acknowledgement_policy not in _DEDICATED_ACKNOWLEDGEMENT_POLICIES:
            raise ValueError(
                "unknown dedicated acknowledgement policy "
                f"{self.acknowledgement_policy!r}"
            )
        if self.kafka_mode not in _DEDICATED_KAFKA_MODES:
            raise ValueError(
                f"unknown dedicated ingress Kafka mode {self.kafka_mode!r}"
            )
        _require_callable_ref(
            self.dispatcher_binding,
            field_name="dedicated dispatcher binding",
        )
        _require_callable_ref(
            self.router_factory_binding,
            field_name="dedicated router factory binding",
        )
        if not isinstance(self.router_factory_accepts_debug_endpoints, bool):
            raise TypeError("router_factory_accepts_debug_endpoints must be a boolean")


@dataclass(frozen=True, slots=True)
class OperationPolicyDefinition:
    """Immutable execution policy for one exact provider operation."""

    operation_id: str
    request_policy: RequestPolicy

    def __post_init__(self) -> None:
        _require_operation_id(
            self.operation_id,
            field_name="operation_policy_id",
        )
        if not isinstance(self.request_policy, RequestPolicy):
            raise TypeError("request_policy must be a RequestPolicy")
        if self.request_policy.max_concurrency is None:
            raise ValueError("operation request_policy must declare max_concurrency")
        if self.request_policy.retryable_status_codes is None:
            raise ValueError(
                "operation request_policy must declare retryable_status_codes"
            )
        if self.request_policy.retryable_error_codes is None:
            raise ValueError(
                "operation request_policy must declare retryable_error_codes"
            )
        if self.request_policy.retry_safety is RetrySafety.UNSAFE and (
            self.request_policy.max_attempts != 1
            or self.request_policy.retryable_status_codes
            or self.request_policy.retryable_error_codes
        ):
            raise ValueError(
                "unsafe operation policy must use max_attempts=1 and empty "
                "retry allowlists"
            )


@dataclass(frozen=True, slots=True)
class ProviderDefinition:
    """Provider-level identity, auth, and ingress adapter."""

    provider_id: str
    display_name: str
    aliases: tuple[str, ...]
    source_ids: tuple[str, ...]
    auth_strategies: tuple[str, ...]
    oauth_ingresses: tuple[OAuthIngressDefinition, ...] = ()
    webhook_ingresses: tuple[WebhookIngressDefinition, ...] = ()
    dedicated_ingresses: tuple[DedicatedIngressDefinition, ...] = ()
    data_plane: bool = True
    operation_policy_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.provider_id, field_name="provider_id")
        _require_nonempty_text(self.display_name, field_name="display_name")
        if not isinstance(self.data_plane, bool):
            raise TypeError("data_plane must be a boolean")
        _require_unique_strings(
            self.aliases,
            field_name="provider aliases",
            allow_empty=True,
        )
        _require_unique_strings(
            self.source_ids,
            field_name="source_ids",
            allow_empty=not self.data_plane,
        )
        if not self.data_plane and self.source_ids:
            raise ValueError(
                "an ingress-only provider cannot declare data-plane source_ids"
            )
        for source_id in self.source_ids:
            _require_identifier(source_id, field_name="source_id")
        _require_unique_strings(
            self.auth_strategies,
            field_name="auth_strategies",
        )
        for auth_strategy in self.auth_strategies:
            _require_identifier(auth_strategy, field_name="auth_strategy")
        _require_tuple(
            self.oauth_ingresses,
            field_name="oauth_ingresses",
            allow_empty=True,
        )
        oauth_sources: set[str] = set()
        oauth_paths: set[str] = set()
        for ingress in self.oauth_ingresses:
            if not isinstance(ingress, OAuthIngressDefinition):
                raise TypeError(
                    "oauth_ingresses entries must be OAuthIngressDefinition"
                )
            if ingress.source_id in oauth_sources:
                raise ValueError(
                    f"duplicate OAuth ingress for source " f"{ingress.source_id!r}"
                )
            oauth_sources.add(ingress.source_id)
            if ingress.source_id not in self.source_ids:
                raise ValueError(
                    f"OAuth ingress references source "
                    f"{ingress.source_id!r}, which is not owned by provider "
                    f"{self.provider_id!r}"
                )
            for path in (
                ingress.install_path,
                ingress.callback_path,
                *ingress.public_result_paths,
            ):
                if path in oauth_paths:
                    raise ValueError(
                        f"duplicate OAuth route path {path!r} for provider "
                        f"{self.provider_id!r}"
                    )
                oauth_paths.add(path)
        _require_tuple(
            self.webhook_ingresses,
            field_name="webhook_ingresses",
            allow_empty=True,
        )
        route_ids: set[str] = set()
        for ingress in self.webhook_ingresses:
            if not isinstance(ingress, WebhookIngressDefinition):
                raise TypeError(
                    "webhook_ingresses entries must be WebhookIngressDefinition"
                )
            if ingress.route_id in route_ids:
                raise ValueError(
                    f"duplicate webhook route {ingress.route_id!r} for "
                    f"provider {self.provider_id!r}"
                )
            route_ids.add(ingress.route_id)
            if ingress.source_id is not None:
                if not self.data_plane:
                    raise ValueError(
                        "ingress-only provider cannot route a webhook to a "
                        "data-plane source"
                    )
                if ingress.source_id not in self.source_ids:
                    raise ValueError(
                        f"webhook route {ingress.route_id!r} references "
                        f"source {ingress.source_id!r}, which is not owned by "
                        f"provider {self.provider_id!r}"
                    )
            elif self.data_plane:
                raise ValueError(
                    f"data-plane provider webhook {ingress.route_id!r} must "
                    "declare its source_id"
                )
        _require_tuple(
            self.dedicated_ingresses,
            field_name="dedicated_ingresses",
            allow_empty=True,
        )
        ingress_ids: set[str] = set()
        for ingress in self.dedicated_ingresses:
            if not isinstance(ingress, DedicatedIngressDefinition):
                raise TypeError(
                    "dedicated_ingresses entries must be " "DedicatedIngressDefinition"
                )
            if ingress.ingress_id in ingress_ids:
                raise ValueError(
                    f"duplicate dedicated ingress {ingress.ingress_id!r} for "
                    f"provider {self.provider_id!r}"
                )
            ingress_ids.add(ingress.ingress_id)
            if not self.data_plane:
                raise ValueError(
                    "ingress-only provider cannot declare dedicated "
                    "data-plane ingress"
                )
            if ingress.source_id not in self.source_ids:
                raise ValueError(
                    f"dedicated ingress {ingress.ingress_id!r} references "
                    f"source {ingress.source_id!r}, which is not owned by "
                    f"provider {self.provider_id!r}"
                )
        _require_unique_strings(
            self.operation_policy_ids,
            field_name="operation_policy_ids",
            allow_empty=True,
        )
        for operation_id in self.operation_policy_ids:
            _require_operation_id(
                operation_id,
                field_name="operation_policy_id",
            )
        normalized_aliases: set[str] = set()
        for alias in self.aliases:
            normalized = normalize_catalog_name(alias)
            if not normalized:
                raise ValueError(f"provider alias {alias!r} normalizes to empty")
            if normalized in normalized_aliases:
                raise ValueError(
                    "provider aliases collide after normalization: " f"{alias!r}"
                )
            normalized_aliases.add(normalized)


@dataclass(frozen=True, slots=True)
class InstallationManagementDefinition:
    """Persistence metadata used by the dedicated installation lifecycle CLI.

    These values are SQL identifiers, so keeping them in the immutable source
    contract both removes the parallel operator registry and gives startup
    validation a chance to reject unsafe or incomplete persistence bindings.
    """

    source: str
    table: str
    scope_column: str
    ref_columns: tuple[str, ...]
    entity_table: str | None
    entity_install_column: str | None
    base_url_column: str | None = "base_url"
    extra_output_columns: tuple[str, ...] = ()
    webhook_installation_id_column: str | None = None
    webhook_installation_id_transform: str | None = None
    enabled_column: str | None = None
    updated_at_column: str | None = None
    native_google_watch_table: bool = False
    status_detail_columns: tuple[str, ...] = ()
    status_presence_columns: tuple[tuple[str, str], ...] = ()
    status_credential_column_groups: tuple[tuple[str, ...], ...] = ()
    entity_status_columns: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.source, field_name="installation management source")
        for field_name in (
            "table",
            "scope_column",
            "entity_table",
            "entity_install_column",
            "base_url_column",
            "webhook_installation_id_column",
            "enabled_column",
            "updated_at_column",
        ):
            value = getattr(self, field_name)
            if value is not None:
                _require_identifier(
                    value,
                    field_name=f"installation management {field_name}",
                )
        _require_unique_strings(
            self.ref_columns,
            field_name="installation management ref_columns",
            allow_empty=True,
        )
        _require_unique_strings(
            self.extra_output_columns,
            field_name="installation management extra_output_columns",
            allow_empty=True,
        )
        for field_name, values in (
            ("ref_columns", self.ref_columns),
            ("extra_output_columns", self.extra_output_columns),
            ("status_detail_columns", self.status_detail_columns),
            ("entity_status_columns", self.entity_status_columns),
        ):
            _require_unique_strings(
                values,
                field_name=f"installation management {field_name}",
                allow_empty=True,
            )
            for value in values:
                _require_identifier(
                    value,
                    field_name=f"installation management {field_name} entry",
                )
        presence_names: set[str] = set()
        for output_name, column_name in self.status_presence_columns:
            _require_identifier(
                output_name,
                field_name="installation management status presence name",
            )
            _require_identifier(
                column_name,
                field_name="installation management status presence column",
            )
            if output_name in presence_names:
                raise ValueError(
                    "installation management status presence names must be unique"
                )
            presence_names.add(output_name)
        for group in self.status_credential_column_groups:
            if not group:
                raise ValueError(
                    "installation management credential groups cannot be empty"
                )
            _require_unique_strings(
                group,
                field_name="installation management credential group",
            )
            for column_name in group:
                _require_identifier(
                    column_name,
                    field_name=("installation management credential group column"),
                )
        if (self.entity_table is None) != (self.entity_install_column is None):
            raise ValueError(
                "installation management entity_table and "
                "entity_install_column must be declared together"
            )
        if self.webhook_installation_id_transform is not None:
            _require_binding(
                self.webhook_installation_id_transform,
                field_name=(
                    "installation management " "webhook_installation_id_transform"
                ),
            )
            if self.webhook_installation_id_column is None:
                raise ValueError(
                    "installation management webhook installation transform "
                    "requires webhook_installation_id_column"
                )
            if self.webhook_installation_id_transform != "host":
                raise ValueError(
                    "unknown installation management webhook installation "
                    f"transform {self.webhook_installation_id_transform!r}"
                )
        if (
            "webhook_secret_ref" in self.ref_columns
            and self.webhook_installation_id_column is None
        ):
            raise ValueError(
                "installation management with webhook_secret_ref requires "
                "webhook_installation_id_column"
            )
        if not isinstance(self.native_google_watch_table, bool):
            raise TypeError(
                "installation management native_google_watch_table must be " "a boolean"
            )
        if self.native_google_watch_table and self.entity_table is None:
            raise ValueError(
                "installation management native Google watch state requires "
                "an entity table binding"
            )


@dataclass(frozen=True, slots=True)
class InstallationAdapter:
    """Lazy runtime and management bindings for a source installation.

    ``loader_binding`` must resolve to an async callable accepting an
    executor plus the authoritative ``tenant_id`` and Fyralis installation
    row UUID when historical ingestion is supported. It must return only that
    exact enabled installation or ``None``. ``management`` may exist without
    a historical loader for a live-only source such as WhatsApp.
    ``planner_client_builder_binding`` is optional because most planners
    consume installation/child-table state without making provider calls
    while planning.
    """

    loader_binding: str | None
    status_loader_binding: str | None = None
    planner_client_builder_binding: str | None = None
    onboarding_failure_binding: str | None = None
    management: InstallationManagementDefinition | None = None

    def __post_init__(self) -> None:
        _require_callable_ref(
            self.loader_binding,
            field_name="installation loader binding",
        )
        _require_callable_ref(
            self.status_loader_binding,
            field_name="installation status loader binding",
        )
        _require_callable_ref(
            self.planner_client_builder_binding,
            field_name="planner client builder binding",
        )
        _require_callable_ref(
            self.onboarding_failure_binding,
            field_name="onboarding failure binding",
        )
        if self.management is not None and not isinstance(
            self.management,
            InstallationManagementDefinition,
        ):
            raise TypeError(
                "installation management must be an " "InstallationManagementDefinition"
            )
        if (
            self.loader_binding is None
            and self.status_loader_binding is None
            and self.planner_client_builder_binding is None
            and self.onboarding_failure_binding is None
            and self.management is None
        ):
            raise ValueError("installation adapter must declare a binding")


@dataclass(frozen=True, slots=True)
class NativeConnectDefinition:
    """Browser-facing contract for one source-native connection flow."""

    kind: str
    payload_fields: tuple[str, ...]
    preflight_path: str | None = None
    finalize_path: str | None = None
    start_path: str | None = None
    status_path: str | None = None
    retry_path: str | None = None
    disconnect_path: str | None = None
    preflight_payload_fields: tuple[str, ...] = ()
    scope_aliases: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.kind, field_name="native connect kind")
        _require_unique_strings(
            self.payload_fields,
            field_name="native connect payload_fields",
        )
        _require_unique_strings(
            self.preflight_payload_fields,
            field_name="native connect preflight_payload_fields",
            allow_empty=True,
        )
        _require_unique_strings(
            self.scope_aliases,
            field_name="native connect scope_aliases",
            allow_empty=True,
        )
        for field_name, values in (
            ("payload_fields", self.payload_fields),
            ("preflight_payload_fields", self.preflight_payload_fields),
            ("scope_aliases", self.scope_aliases),
        ):
            for value in values:
                if not _FIELD_PATH_RE.fullmatch(value):
                    raise ValueError(
                        f"native connect {field_name} entries must be "
                        f"lowercase field paths; got {value!r}"
                    )
        standard_paths = (self.preflight_path, self.finalize_path)
        callback_paths = (
            self.start_path,
            self.status_path,
            self.retry_path,
            self.disconnect_path,
        )
        if any(standard_paths) != all(standard_paths):
            raise ValueError(
                "native connect preflight_path and finalize_path must be "
                "declared together"
            )
        if any(callback_paths) != all(callback_paths):
            raise ValueError("native connect callback paths must be declared together")
        if all(standard_paths) == all(callback_paths):
            raise ValueError("native connect must declare exactly one route family")
        for field_name in (
            "preflight_path",
            "finalize_path",
            "start_path",
            "status_path",
            "retry_path",
            "disconnect_path",
        ):
            _require_route_path(
                getattr(self, field_name),
                field_name=f"native connect {field_name}",
            )
        paths = self.route_paths()
        if len(paths) != len(set(paths)):
            raise ValueError("native connect route paths must be unique")

    def route_paths(self) -> tuple[str, ...]:
        """Return public native-connect paths in declaration order."""

        return tuple(
            path
            for path in (
                self.preflight_path,
                self.finalize_path,
                self.start_path,
                self.status_path,
                self.retry_path,
                self.disconnect_path,
            )
            if path is not None
        )

    def as_payload(self) -> dict[str, object]:
        """Return the legacy browser payload without exposing mutable state."""

        payload: dict[str, object] = {"kind": self.kind}
        for field_name in (
            "preflight_path",
            "finalize_path",
            "start_path",
            "status_path",
            "retry_path",
            "disconnect_path",
        ):
            value = getattr(self, field_name)
            if value is not None:
                payload[field_name] = value
        if self.preflight_payload_fields:
            payload["preflight_payload_fields"] = list(self.preflight_payload_fields)
        payload["payload_fields"] = list(self.payload_fields)
        if self.scope_aliases:
            payload["scope_aliases"] = list(self.scope_aliases)
        return payload


@dataclass(frozen=True, slots=True)
class BrowserAgentDefinition:
    """Immutable browser-automation steps owned by a source contract.

    Provider console discovery belongs to :class:`OnboardingDefinition`, so
    this declaration intentionally does not repeat the console URL or source
    ID.  Runtime consumers combine those source-owned values when producing
    the legacy browser-agent payload.
    """

    settings_targets: tuple[str, ...]
    agent_collects: tuple[str, ...]
    agent_generates: tuple[str, ...]
    human_gates: tuple[str, ...]
    completion_checks: tuple[str, ...]

    def __post_init__(self) -> None:
        for field_name in (
            "settings_targets",
            "agent_collects",
            "agent_generates",
            "human_gates",
            "completion_checks",
        ):
            _require_unique_strings(
                getattr(self, field_name),
                field_name=f"browser agent {field_name}",
            )

    def as_payload(
        self,
        *,
        source: str,
        provider_console_url: str,
    ) -> dict[str, object]:
        """Return the stable browser-agent wire payload."""

        _require_identifier(source, field_name="browser agent source")
        if not isinstance(
            provider_console_url, str
        ) or not provider_console_url.startswith("https://"):
            raise ValueError("browser agent provider_console_url must use HTTPS")
        return {
            "source": source,
            "provider_console_url": provider_console_url,
            "settings_targets": list(self.settings_targets),
            "agent_collects": list(self.agent_collects),
            "agent_generates": list(self.agent_generates),
            "human_gates": list(self.human_gates),
            "completion_checks": list(self.completion_checks),
        }


@dataclass(frozen=True, slots=True)
class LocalRehearsalDefinition:
    """Customer-local provider setup used by the BYOC rehearsal workflow.

    Mappings are represented as ordered tuples so the declaration is deeply
    immutable. ``as_payload`` returns a fresh legacy-shaped dictionary for
    CLI consumers.
    """

    kind: str
    needs_public_url: bool
    env: tuple[str, ...]
    required_env: tuple[str, ...]
    manual_gate_names: tuple[str, ...]
    install_endpoint: str | None = None
    callback_path: str | None = None
    preflight_endpoint: str | None = None
    finalize_endpoint: str | None = None
    webhook_path: str | None = None
    derived_env: tuple[tuple[str, str], ...] = ()
    default_env: tuple[tuple[str, str], ...] = ()
    runtime_components: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _require_identifier(self.kind, field_name="local rehearsal kind")
        if not isinstance(self.needs_public_url, bool):
            raise TypeError("local rehearsal needs_public_url must be a boolean")
        for field_name in ("env", "required_env"):
            values = getattr(self, field_name)
            _require_unique_strings(
                values,
                field_name=f"local rehearsal {field_name}",
            )
            for value in values:
                if not _ENV_KEY_RE.fullmatch(value):
                    raise ValueError(
                        f"local rehearsal {field_name} entries must be uppercase "
                        f"environment keys; got {value!r}"
                    )
        unknown_required = set(self.required_env) - set(self.env)
        if unknown_required:
            raise ValueError(
                "local rehearsal required_env must be a subset of env; "
                f"got {sorted(unknown_required)!r}"
            )
        _require_unique_strings(
            self.manual_gate_names,
            field_name="local rehearsal manual_gate_names",
        )
        for value in self.manual_gate_names:
            _require_identifier(value, field_name="local rehearsal manual gate")
        _require_unique_strings(
            self.runtime_components,
            field_name="local rehearsal runtime_components",
            allow_empty=True,
        )
        for field_name in (
            "install_endpoint",
            "callback_path",
            "preflight_endpoint",
            "finalize_endpoint",
            "webhook_path",
        ):
            _require_route_path(
                getattr(self, field_name),
                field_name=f"local rehearsal {field_name}",
            )
        for field_name in ("derived_env", "default_env"):
            pairs = getattr(self, field_name)
            _require_tuple(
                pairs,
                field_name=f"local rehearsal {field_name}",
                allow_empty=True,
            )
            keys: list[str] = []
            for pair in pairs:
                if (
                    not isinstance(pair, tuple)
                    or len(pair) != 2
                    or not all(isinstance(value, str) for value in pair)
                ):
                    raise TypeError(
                        f"local rehearsal {field_name} entries must be string pairs"
                    )
                key, value = pair
                if not _ENV_KEY_RE.fullmatch(key):
                    raise ValueError(
                        f"local rehearsal {field_name} key must be an uppercase "
                        f"environment key; got {key!r}"
                    )
                _require_nonempty_text(
                    value,
                    field_name=f"local rehearsal {field_name} value",
                )
                keys.append(key)
            if len(keys) != len(set(keys)):
                raise ValueError(
                    f"local rehearsal {field_name} contains duplicate keys"
                )
        if set(key for key, _ in self.default_env) - set(self.env):
            raise ValueError("local rehearsal default_env keys must be declared in env")

    def as_payload(self) -> dict[str, object]:
        """Return a fresh legacy CLI payload in deterministic field order."""

        payload: dict[str, object] = {
            "kind": self.kind,
            "needs_public_url": self.needs_public_url,
        }
        for field_name in (
            "install_endpoint",
            "callback_path",
            "preflight_endpoint",
            "finalize_endpoint",
            "webhook_path",
        ):
            value = getattr(self, field_name)
            if value is not None:
                payload[field_name] = value
        payload["env"] = list(self.env)
        payload["required_env"] = list(self.required_env)
        if self.derived_env:
            payload["derived_env"] = dict(self.derived_env)
        if self.default_env:
            payload["default_env"] = dict(self.default_env)
        if self.runtime_components:
            payload["runtime_components"] = list(self.runtime_components)
        payload["manual_gate_names"] = list(self.manual_gate_names)
        return payload


@dataclass(frozen=True, slots=True)
class OnboardingDefinition:
    """Contract-owned metadata used by the BYOC source connection portal."""

    method: str
    discovery_target: str
    native_connect: NativeConnectDefinition
    browser_agent: BrowserAgentDefinition
    default_scopes: tuple[str, ...] = ()
    provider_permissions: tuple[str, ...] = ()
    ingress_paths: tuple[str, ...] = ()
    required_refs: tuple[str, ...] = ()
    no_ingress_reason: str | None = None
    local_rehearsal: LocalRehearsalDefinition | None = None
    required_inputs: tuple[str, ...] | None = None
    optional_inputs: tuple[str, ...] | None = None
    provider_console_url: str | None = None
    generic_authorization_mode: str | None = None

    def __post_init__(self) -> None:
        for field_name in (
            "default_scopes",
            "provider_permissions",
            "required_refs",
        ):
            _require_unique_strings(
                getattr(self, field_name),
                field_name=f"onboarding {field_name}",
                allow_empty=True,
            )
        for value in self.required_refs:
            if not _FIELD_PATH_RE.fullmatch(value):
                raise ValueError(
                    "onboarding required_refs entries must be lowercase field "
                    f"paths; got {value!r}"
                )
        _require_unique_strings(
            self.ingress_paths,
            field_name="onboarding ingress_paths",
            allow_empty=True,
        )
        for value in self.ingress_paths:
            _require_route_path(value, field_name="onboarding ingress path")
        has_cli_profile = any(
            (
                self.default_scopes,
                self.provider_permissions,
                self.ingress_paths,
                self.required_refs,
            )
        )
        if (
            has_cli_profile
            and not self.ingress_paths
            and self.no_ingress_reason is None
        ):
            raise ValueError(
                "onboarding without ingress_paths must declare no_ingress_reason"
            )
        if self.no_ingress_reason is not None:
            _require_nonempty_text(
                self.no_ingress_reason,
                field_name="onboarding no_ingress_reason",
            )
        if self.local_rehearsal is not None and not isinstance(
            self.local_rehearsal,
            LocalRehearsalDefinition,
        ):
            raise TypeError(
                "onboarding local_rehearsal must be a LocalRehearsalDefinition"
            )
        for field_name in ("required_inputs", "optional_inputs"):
            values = getattr(self, field_name)
            if values is None:
                continue
            _require_unique_strings(
                values,
                field_name=f"onboarding {field_name}",
                allow_empty=True,
            )
            for value in values:
                if not _FIELD_PATH_RE.fullmatch(value):
                    raise ValueError(
                        f"onboarding {field_name} entries must be lowercase "
                        f"field paths; got {value!r}"
                    )
        overlap = set(self.required_inputs or ()) & set(self.optional_inputs or ())
        if overlap:
            raise ValueError(
                "onboarding required_inputs and optional_inputs overlap: "
                f"{sorted(overlap)!r}"
            )
        if self.provider_console_url is not None and (
            not isinstance(self.provider_console_url, str)
            or not self.provider_console_url.startswith("https://")
        ):
            raise ValueError("onboarding provider_console_url must use HTTPS")
        if self.generic_authorization_mode is not None:
            _require_identifier(
                self.generic_authorization_mode,
                field_name="onboarding generic_authorization_mode",
            )
        _require_identifier(self.method, field_name="onboarding method")
        _require_nonempty_text(
            self.discovery_target,
            field_name="onboarding discovery_target",
        )
        if not isinstance(self.native_connect, NativeConnectDefinition):
            raise TypeError("onboarding native_connect must be NativeConnectDefinition")
        if not isinstance(self.browser_agent, BrowserAgentDefinition):
            raise TypeError("onboarding browser_agent must be BrowserAgentDefinition")
        if self.provider_console_url is None:
            raise ValueError(
                "onboarding with browser automation must declare "
                "provider_console_url"
            )

    def as_cli_source_profile(self) -> dict[str, object]:
        """Return a fresh payload compatible with the legacy source CLI."""

        payload: dict[str, object] = {
            "method": self.method,
            "default_scopes": list(self.default_scopes),
            "provider_permissions": list(self.provider_permissions),
            "ingress_paths": list(self.ingress_paths),
            "required_refs": list(self.required_refs),
        }
        if self.no_ingress_reason is not None:
            payload["no_ingress_reason"] = self.no_ingress_reason
        payload["native_connect"] = self.native_connect.as_payload()
        return payload


@dataclass(frozen=True, slots=True)
class CredentialRefreshDefinition:
    """Provider token refresh/re-mint policy owned by one source."""

    operation_id: str
    default_token_url: str
    token_url_env: str
    grant_type: CredentialGrantType
    auth_style: CredentialAuthStyle
    rotates_refresh_token: bool
    install_table: str
    default_expires_in: int = 3600
    client_secret_from_install: bool = False
    client_credentials_from_install: bool = False
    scope_env: str | None = None
    default_scope: str | None = None

    def __post_init__(self) -> None:
        _require_operation_id(
            self.operation_id,
            field_name="credential refresh operation_id",
        )
        if not isinstance(
            self.default_token_url, str
        ) or not self.default_token_url.startswith("https://"):
            raise ValueError("credential refresh default_token_url must use HTTPS")
        for field_name in ("token_url_env", "scope_env"):
            value = getattr(self, field_name)
            if value is not None and not _ENV_KEY_RE.fullmatch(value):
                raise ValueError(f"{field_name} must be an uppercase environment key")
        if self.grant_type not in _CREDENTIAL_GRANT_TYPES:
            raise ValueError(f"unknown credential grant type {self.grant_type!r}")
        if self.auth_style not in _CREDENTIAL_AUTH_STYLES:
            raise ValueError(f"unknown credential auth style {self.auth_style!r}")
        if not isinstance(self.rotates_refresh_token, bool):
            raise TypeError("rotates_refresh_token must be a boolean")
        _require_identifier(
            self.install_table,
            field_name="credential refresh install_table",
        )
        if (
            isinstance(self.default_expires_in, bool)
            or not isinstance(self.default_expires_in, int)
            or self.default_expires_in <= 0
        ):
            raise ValueError("credential refresh default_expires_in must be positive")
        for field_name in (
            "client_secret_from_install",
            "client_credentials_from_install",
        ):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a boolean")
        if self.client_secret_from_install and self.client_credentials_from_install:
            raise ValueError(
                "credential refresh cannot declare both install credential " "modes"
            )
        if self.default_scope is not None:
            _require_nonempty_text(
                self.default_scope,
                field_name="credential refresh default_scope",
            )
        if (self.scope_env is None) != (self.default_scope is None):
            raise ValueError(
                "credential refresh scope_env and default_scope must be "
                "declared together"
            )


@dataclass(frozen=True, slots=True)
class IngressRoute:
    """One raw-envelope ingress kind routed to one source-owned channel."""

    ingress_kind: IngressKind
    channel: str

    def __post_init__(self) -> None:
        if self.ingress_kind not in _INGRESS_KINDS:
            raise ValueError(f"unknown ingress kind {self.ingress_kind!r}")
        _require_binding(self.channel, field_name="ingress route channel")


@dataclass(frozen=True, slots=True)
class SourceDisplayDefinition:
    """Onboarding-marketplace presentation owned by one source contract."""

    order: int
    category: SourceCategory
    description: str
    connection_method: SourceConnectionMethod
    setup_requirements: str
    supported_sync_modes: tuple[SourceSyncMode, ...]
    display_name_override: str | None = None
    notice: str | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.order, bool)
            or not isinstance(self.order, int)
            or self.order < 0
        ):
            raise ValueError("source display order must be an integer >= 0")
        if self.category not in _SOURCE_CATEGORIES:
            raise ValueError(f"unknown source display category {self.category!r}")
        _require_nonempty_text(
            self.description,
            field_name="source display description",
        )
        if self.connection_method not in _SOURCE_CONNECTION_METHODS:
            raise ValueError(
                "unknown source display connection method "
                f"{self.connection_method!r}"
            )
        _require_nonempty_text(
            self.setup_requirements,
            field_name="source display setup_requirements",
        )
        _require_unique_strings(
            self.supported_sync_modes,
            field_name="source display supported_sync_modes",
        )
        unknown_sync_modes = set(self.supported_sync_modes) - _SOURCE_SYNC_MODES
        if unknown_sync_modes:
            raise ValueError(
                "unknown source display sync modes: " f"{sorted(unknown_sync_modes)!r}"
            )
        for field_name in ("display_name_override", "notice"):
            value = getattr(self, field_name)
            if value is not None:
                _require_nonempty_text(
                    value,
                    field_name=f"source display {field_name}",
                )


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    """Complete declarative contract for one canonical ingestion source.

    The four live-policy tuples are positional: index ``i`` describes one
    transport, its acknowledgement boundary, its delivery semantics, and its
    runtime binding.  ``live_contracts()`` exposes the zipped form.
    """

    source_id: str
    ui_slug: str
    provider_id: str
    display_name: str
    display: SourceDisplayDefinition
    aliases: tuple[str, ...]
    data_objects: tuple[str, ...]
    history: HistoryKind | None
    installation_identifiers: tuple[str, ...]
    runtime_identifiers: tuple[str, ...]
    ingress_routes: tuple[IngressRoute, ...]
    normalization_inputs: tuple[str, ...]
    normalizer_bindings: tuple[str, ...]
    idempotency_builder_bindings: tuple[str, ...]
    allowed_observation_kinds: tuple[AllowedObservationKind, ...]
    trust_tiers: tuple[AllowedTrustTier, ...]
    live_transports: tuple[LiveTransportKind, ...]
    acknowledgement_policies: tuple[AcknowledgementPolicy, ...]
    delivery_policies: tuple[DeliveryPolicy, ...]
    live_bindings: tuple[str, ...]
    planner_binding: str | None
    fetcher_binding: str | None
    reconciler_binding: str | None
    installation_adapter: InstallationAdapter | None
    connect_router_binding: str
    onboarding: OnboardingDefinition
    certification: Certification
    credential_refresh: CredentialRefreshDefinition | None = None
    capability_flags: tuple[str, ...] = ()
    operation_policies: tuple[OperationPolicyDefinition, ...] = ()
    provider_transport_enforced: bool = False
    operator_live_ingress: str | None = None

    def __post_init__(self) -> None:
        _require_identifier(self.source_id, field_name="source_id")
        if (
            not isinstance(self.ui_slug, str)
            or not self.ui_slug
            or normalize_catalog_name(self.ui_slug) != self.source_id
        ):
            raise ValueError(
                "ui_slug must normalize to SourceDefinition source_id; got "
                f"{self.ui_slug!r} for {self.source_id!r}"
            )
        _require_identifier(self.provider_id, field_name="provider_id")
        _require_nonempty_text(self.display_name, field_name="display_name")
        if not isinstance(self.display, SourceDisplayDefinition):
            raise TypeError("display must be a SourceDisplayDefinition")
        _require_unique_strings(
            self.aliases,
            field_name="source aliases",
            allow_empty=True,
        )
        _require_unique_strings(self.data_objects, field_name="data_objects")
        if self.history is not None and self.history not in _HISTORY_KINDS:
            raise ValueError(f"unknown history kind {self.history!r}")

        for field_name in (
            "installation_identifiers",
            "runtime_identifiers",
        ):
            values = getattr(self, field_name)
            _require_unique_strings(values, field_name=field_name)
            for value in values:
                if not _FIELD_PATH_RE.fullmatch(value):
                    raise ValueError(
                        f"{field_name} entries must be lowercase field paths; "
                        f"got {value!r}"
                    )

        _require_tuple(self.ingress_routes, field_name="ingress_routes")
        seen_ingress_kinds: set[IngressKind] = set()
        normalization_input_set = frozenset(self.normalization_inputs)
        for route in self.ingress_routes:
            if not isinstance(route, IngressRoute):
                raise TypeError("ingress_routes entries must be IngressRoute")
            if route.ingress_kind in seen_ingress_kinds:
                raise ValueError(
                    "ingress_routes contains duplicate ingress kind "
                    f"{route.ingress_kind!r}"
                )
            seen_ingress_kinds.add(route.ingress_kind)
            if route.channel not in normalization_input_set:
                raise ValueError(
                    f"ingress route {route.ingress_kind!r} points to "
                    f"{route.channel!r}, which is not a normalization input "
                    f"owned by source {self.source_id!r}"
                )

        _require_unique_strings(
            self.normalization_inputs,
            field_name="normalization_inputs",
        )
        _require_tuple(
            self.normalizer_bindings,
            field_name="normalizer_bindings",
        )
        if len(self.normalization_inputs) != len(self.normalizer_bindings):
            raise ValueError(
                "normalization_inputs and normalizer_bindings must have equal " "length"
            )
        for channel in self.normalization_inputs:
            _require_binding(channel, field_name="normalization input")
        for binding in self.normalizer_bindings:
            _require_callable_ref(binding, field_name="normalizer binding")
        _require_unique_strings(
            self.idempotency_builder_bindings,
            field_name="idempotency_builder_bindings",
        )
        for binding in self.idempotency_builder_bindings:
            _require_callable_ref(
                binding,
                field_name="idempotency builder binding",
            )

        _require_unique_strings(
            self.allowed_observation_kinds,
            field_name="allowed_observation_kinds",
        )
        unknown_kinds = set(self.allowed_observation_kinds) - _OBSERVATION_KINDS
        if unknown_kinds:
            raise ValueError(
                f"unknown allowed observation kinds: {sorted(unknown_kinds)}"
            )
        _require_unique_strings(self.trust_tiers, field_name="trust_tiers")
        unknown_trust = set(self.trust_tiers) - _TRUST_TIERS
        if unknown_trust:
            raise ValueError(f"unknown trust tiers: {sorted(unknown_trust)}")

        _require_unique_strings(
            self.live_transports,
            field_name="live_transports",
        )
        unknown_transports = set(self.live_transports) - _LIVE_TRANSPORT_KINDS
        if unknown_transports:
            raise ValueError(f"unknown live transports: {sorted(unknown_transports)}")
        _require_tuple(
            self.acknowledgement_policies,
            field_name="acknowledgement_policies",
        )
        unknown_ack = set(self.acknowledgement_policies) - _ACKNOWLEDGEMENT_POLICIES
        if unknown_ack:
            raise ValueError(f"unknown acknowledgement policies: {sorted(unknown_ack)}")
        _require_tuple(self.delivery_policies, field_name="delivery_policies")
        unknown_delivery = set(self.delivery_policies) - _DELIVERY_POLICIES
        if unknown_delivery:
            raise ValueError(f"unknown delivery policies: {sorted(unknown_delivery)}")
        _require_unique_strings(self.live_bindings, field_name="live_bindings")
        for binding in self.live_bindings:
            _require_binding(binding, field_name="live binding")
        live_lengths = {
            len(self.live_transports),
            len(self.acknowledgement_policies),
            len(self.delivery_policies),
            len(self.live_bindings),
        }
        if len(live_lengths) != 1:
            raise ValueError(
                "live_transports, acknowledgement_policies, "
                "delivery_policies, and live_bindings must have equal length"
            )

        for field_name in (
            "planner_binding",
            "fetcher_binding",
            "reconciler_binding",
            "connect_router_binding",
        ):
            _require_callable_ref(
                getattr(self, field_name),
                field_name=field_name,
            )
        if not isinstance(self.onboarding, OnboardingDefinition):
            raise TypeError("onboarding must be an OnboardingDefinition")
        history_bindings = (
            self.planner_binding,
            self.fetcher_binding,
            self.reconciler_binding,
        )
        if self.history is None and any(history_bindings):
            raise ValueError(
                "a source with history=None cannot declare planner, fetcher, "
                "or reconciler bindings"
            )
        if self.history is not None and not all(history_bindings):
            raise ValueError(
                "a source with history support must declare planner, fetcher, "
                "and reconciler bindings"
            )
        has_backfill_route = "backfill" in seen_ingress_kinds
        if self.history is None and has_backfill_route:
            raise ValueError(
                "a source with history=None cannot declare a backfill " "ingress route"
            )
        if self.history is not None and not has_backfill_route:
            raise ValueError(
                "a source with history support must declare a backfill " "ingress route"
            )
        adapter = self.installation_adapter
        if adapter is not None and not isinstance(adapter, InstallationAdapter):
            raise TypeError("installation_adapter must be an InstallationAdapter")
        if self.history is not None and adapter is None:
            raise TypeError(
                "a source with history support must declare an " "InstallationAdapter"
            )
        if self.history is not None and adapter.loader_binding is None:
            raise ValueError(
                "a source with history support must declare an installation "
                "loader binding"
            )
        if adapter is not None and adapter.status_loader_binding is None:
            raise ValueError(
                "an installation adapter must declare a collection/exact-row "
                "status loader binding"
            )
        if (
            self.history is None
            and adapter is not None
            and any(
                (
                    adapter.loader_binding,
                    adapter.planner_client_builder_binding,
                    adapter.onboarding_failure_binding,
                )
            )
        ):
            raise ValueError(
                "a source with history=None cannot declare historical "
                "installation bindings"
            )
        if (
            adapter is not None
            and adapter.management is not None
            and adapter.management.source != self.source_id
        ):
            raise ValueError(
                "installation management source must match SourceDefinition "
                f"source_id {self.source_id!r}"
            )
        if not isinstance(self.certification, Certification):
            raise TypeError("certification must be a Certification")
        if self.credential_refresh is not None and not isinstance(
            self.credential_refresh,
            CredentialRefreshDefinition,
        ):
            raise TypeError("credential_refresh must be CredentialRefreshDefinition")
        if (
            self.credential_refresh is not None
            and self.credential_refresh.operation_id not in self.operation_policy_ids
        ):
            raise ValueError(
                "credential refresh operation must be declared in "
                "operation_policy_ids"
            )
        _require_unique_strings(
            self.capability_flags,
            field_name="capability_flags",
            allow_empty=True,
        )
        for capability in self.capability_flags:
            _require_binding(capability, field_name="capability flag")
        no_outbound_requests = "no_outbound_provider_requests" in self.capability_flags
        if no_outbound_requests and (
            self.history is not None
            or self.credential_refresh is not None
            or self.operation_policy_ids
        ):
            raise ValueError(
                "no_outbound_provider_requests requires history=None, no "
                "credential refresh, and no provider operation IDs"
            )
        _require_tuple(
            self.operation_policies,
            field_name="operation_policies",
            allow_empty=(not self.provider_transport_enforced or no_outbound_requests),
        )
        operation_ids: list[str] = []
        for operation_policy in self.operation_policies:
            if not isinstance(operation_policy, OperationPolicyDefinition):
                raise TypeError(
                    "operation_policies entries must be " "OperationPolicyDefinition"
                )
            operation_ids.append(operation_policy.operation_id)
        if len(operation_ids) != len(set(operation_ids)):
            raise ValueError("operation_policies contains duplicate operation IDs")
        if not isinstance(self.provider_transport_enforced, bool):
            raise TypeError("provider_transport_enforced must be a boolean")
        if self.operator_live_ingress is not None:
            _require_nonempty_text(
                self.operator_live_ingress,
                field_name="operator_live_ingress",
            )
            if self.operator_live_ingress.startswith("/"):
                raise ValueError(
                    "operator_live_ingress describes a non-HTTP runtime; "
                    "public route paths belong to provider ingress contracts"
                )
        if (
            self.provider_transport_enforced
            and not self.operation_policy_ids
            and not no_outbound_requests
        ):
            raise ValueError(
                "provider_transport_enforced requires operation_policy_ids"
            )
        normalized_aliases: set[str] = set()
        for alias in self.aliases:
            normalized = normalize_catalog_name(alias)
            if not normalized:
                raise ValueError(f"source alias {alias!r} normalizes to empty")
            if normalized in normalized_aliases:
                raise ValueError(
                    f"source aliases collide after normalization: {alias!r}"
                )
            normalized_aliases.add(normalized)

    def live_contracts(
        self,
    ) -> tuple[
        tuple[
            LiveTransportKind,
            AcknowledgementPolicy,
            DeliveryPolicy,
            str,
        ],
        ...,
    ]:
        """Return transport policy rows in their deterministic declared order."""

        return tuple(
            zip(
                self.live_transports,
                self.acknowledgement_policies,
                self.delivery_policies,
                self.live_bindings,
                strict=True,
            )
        )

    def normalization_contracts(self) -> tuple[tuple[str, str], ...]:
        """Return ``(input channel, normalizer binding)`` rows."""

        return tuple(
            zip(
                self.normalization_inputs,
                self.normalizer_bindings,
                strict=True,
            )
        )

    @property
    def operation_policy_ids(self) -> tuple[str, ...]:
        """Return exact operation IDs in their declared policy order."""

        return tuple(definition.operation_id for definition in self.operation_policies)

    def request_policy_for_operation(self, operation_id: str) -> RequestPolicy:
        """Resolve one exact source-owned operation policy."""

        for definition in self.operation_policies:
            if definition.operation_id == operation_id:
                return definition.request_policy
        raise KeyError(
            f"source {self.source_id!r} has no request policy for "
            f"operation {operation_id!r}"
        )

    def channel_for_ingress(self, ingress_kind: str) -> str | None:
        """Return the declared channel for an ingress kind, if supported."""

        for route in self.ingress_routes:
            if route.ingress_kind == ingress_kind:
                return route.channel
        return None

    def as_ui_catalog_entry(self) -> dict[str, object]:
        """Return the deterministic TypeScript onboarding catalog shape."""

        payload: dict[str, object] = {
            "id": self.ui_slug,
            "canonicalId": self.source_id,
            "providerId": self.provider_id,
            "aliases": list(self.aliases),
            "name": (self.display.display_name_override or self.display_name),
            "category": self.display.category,
            "description": self.display.description,
            "method": self.display.connection_method,
            "requiredPermissions": list(self.onboarding.provider_permissions),
            "setupRequirements": self.display.setup_requirements,
            "supportedSyncModes": list(self.display.supported_sync_modes),
            "providerIngressPaths": list(self.onboarding.ingress_paths),
        }
        notice = self.display.notice or self.onboarding.no_ingress_reason
        if notice is not None:
            payload["noIngressReason"] = notice
        return payload

    @property
    def default_trust_tier(self) -> AllowedTrustTier:
        """Return the normalizer's default tier.

        ``trust_tiers`` is ordered deliberately: the first tier is the
        channel default and any remaining tiers are explicit handler-level
        overrides allowed for provider-asserted event subtypes.
        """

        return self.trust_tiers[0]


@dataclass(frozen=True, slots=True)
class NonSourceChannelDefinition:
    """Contract for a channel not owned by a canonical ingestion source.

    Provider-edge channels such as ``stripe:webhook`` and Fyralis platform
    channels such as ``internal:state_change`` still need the same immutable
    normalizer and trust declarations as data-plane sources.  A ``None``
    normalizer binding means the channel is a trust-classified observation
    channel but does not enter through the in-process normalization router.
    """

    channel: str
    owner_kind: ChannelOwnerKind
    owner_id: str
    normalizer_binding: str | None
    allowed_observation_kinds: tuple[AllowedObservationKind, ...]
    trust_tiers: tuple[AllowedTrustTier, ...]

    def __post_init__(self) -> None:
        _require_binding(self.channel, field_name="channel")
        if self.owner_kind not in _CHANNEL_OWNER_KINDS:
            raise ValueError(f"unknown channel owner kind {self.owner_kind!r}")
        _require_identifier(self.owner_id, field_name="owner_id")
        _require_callable_ref(
            self.normalizer_binding,
            field_name="normalizer_binding",
        )

        _require_unique_strings(
            self.allowed_observation_kinds,
            field_name="allowed_observation_kinds",
        )
        unknown_kinds = set(self.allowed_observation_kinds) - _OBSERVATION_KINDS
        if unknown_kinds:
            raise ValueError(
                f"unknown allowed observation kinds: {sorted(unknown_kinds)}"
            )
        _require_unique_strings(self.trust_tiers, field_name="trust_tiers")
        unknown_trust = set(self.trust_tiers) - _TRUST_TIERS
        if unknown_trust:
            raise ValueError(f"unknown trust tiers: {sorted(unknown_trust)}")

    @property
    def default_trust_tier(self) -> AllowedTrustTier:
        """Return the declared default; later tiers are allowed overrides."""

        return self.trust_tiers[0]


__all__ = [
    "AcknowledgementPolicy",
    "AllowedObservationKind",
    "AllowedTrustTier",
    "BrowserAgentDefinition",
    "ChannelOwnerKind",
    "Certification",
    "CertificationStatus",
    "DeliveryPolicy",
    "DedicatedAcknowledgementPolicy",
    "DedicatedIngressDefinition",
    "DedicatedIngressMethod",
    "DedicatedKafkaMode",
    "HistoryKind",
    "InstallationAdapter",
    "LiveTransportKind",
    "LocalRehearsalDefinition",
    "NativeConnectDefinition",
    "NonSourceChannelDefinition",
    "OnboardingDefinition",
    "ProviderDefinition",
    "RequestPolicy",
    "SourceCategory",
    "SourceConnectionMethod",
    "SourceDefinition",
    "SourceDisplayDefinition",
    "SourceSyncMode",
    "WebhookAcknowledgementPolicy",
    "WebhookHandlerMode",
    "WebhookIngressDefinition",
    "WebhookKafkaMode",
    "WebhookTenantBinding",
    "normalize_catalog_name",
]
