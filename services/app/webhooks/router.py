"""Catalog-owned FastAPI routes for provider webhooks.

Mounted by `services/app/gateway/main.py`. The Bearer middleware in the
gateway skips this path prefix (see `_PUBLIC_PATH_PREFIXES`), so the
only authentication is the cryptographic signature check below.

Request flow:

    1. Capture raw body bytes (NOT a re-parsed JSON form).
    2. Enforce IN-01 body-size precheck (1 MB).
    3. Look up the per-provider verifier; 404 on unknown provider.
    4. Best-effort JSON-parse the body so the tenant resolver and
       contract-owned provider hooks have a dict to inspect.
       Malformed JSON does NOT immediately reject — the verifier still
       runs first so an attacker cannot probe the JSON-validity oracle.
    5. Resolve runtime from `request.app.state.integration_runtime`
       (with legacy aliases as a compatibility bridge), then call the
       tenant resolver to map the (provider, installation_id) pair to
       a tenant. The outcome is captured but the rejection (if any) is
       deferred until AFTER signature verification — same security
       posture as before IN-08: signature failure first, then tenant.
    6. Resolve the route's contract-owned secret loader and invoke it
       uniformly. Most routes resolve ``provider_installations.secret_ref``
       for the exact installation; App-scoped providers own their loader.
       Loader failures and malformed results fail closed with 503.
    7. Run the verifier; on any `WebhookVerificationError` return 401
       + structured error + metric increment.
    8. Enforce the resolver outcome: `UnknownInstallation` → 401,
       `PayloadMissing` → 400. On `Resolved`, hand off to
       `ingestion.core.ingest()` under the resolved tenant.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Mapping

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.http_headers import safe_headers
from services.ingest.ingestion.core import (
    IngestResult,
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    ingest,
)
from services.ingest.ingestion.feature_flags import (
    SHADOW_WRITE_ENABLED,
)
from services.ingest.ingestion.feature_flags.traffic_signal import (
    maybe_emit_traffic_signal,
)
from services.ingest.ingestion.handlers import HandlerNotFound
from services.ingest.ingestion.kafka.flush_batcher import coalesced_flush
from services.ingest.ingestion.raw_tier.s3 import compute_content_hash
from services.ingest.ingestion.shadow_write import (
    CUTOVER_FLUSH_TIMEOUT_SEC,
    shadow_write_raw,
)
from services.app.webhooks import metrics
from services.app.webhooks.signatures import verifier_for_provider
from services.app.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)
from services.app.webhooks.verifier import (
    Secret,
    VerifiedContext,
    WebhookVerificationError,
)
from services.ingest.source_contract import (
    WEBHOOK_INGRESS_CATALOG,
    WebhookIngressDefinition,
    build_webhook_ingress_metadata,
    resolve_callable_reference,
    resolve_webhook_secret_loader,
    resolve_webhook_verified_pre_tenant_handler,
    resolve_webhook_verified_tenant_handler,
    webhook_ingress_definition,
)


log = structlog.get_logger("webhooks.router")


@dataclass(frozen=True, slots=True)
class WebhookRuntime:
    """Runtime dependencies consumed by the webhook ingress router.

    The canonical source is ``app.state.integration_runtime``. Legacy
    aliases remain supported while tests and older mounted apps still wire
    ``app.state.tenant_resolver`` / ``app.state.tenant_flags`` directly.
    """

    pool: Any | None
    secret_store: Any | None
    tenant_resolver: Any | None
    tenant_flags: Any | None
    kafka_producer: Any | None
    s3_raw_client: Any | None
    github_client: Any | None
    github_replay_cache: Any | None
    record_failure: Any = metrics.record_failure


@dataclass(frozen=True, slots=True)
class WebhookAuthContext:
    runtime: WebhookRuntime
    outcome: Any
    tenant_id: Any | None
    verified: VerifiedContext


@dataclass(frozen=True, slots=True)
class WebhookCutoverDecision:
    flag_enabled: bool
    response: JSONResponse | None


def _webhook_runtime(request: Request) -> WebhookRuntime:
    state = request.app.state
    integration_runtime = getattr(state, "integration_runtime", None)

    def runtime_attr(name: str) -> Any | None:
        if integration_runtime is not None:
            value = getattr(integration_runtime, name, None)
            if value is not None:
                return value
        return getattr(state, name, None)

    return WebhookRuntime(
        pool=runtime_attr("pool"),
        secret_store=runtime_attr("secret_store"),
        tenant_resolver=runtime_attr("tenant_resolver"),
        tenant_flags=runtime_attr("tenant_flags"),
        kafka_producer=runtime_attr("kafka_producer"),
        s3_raw_client=runtime_attr("s3_raw_client"),
        github_client=runtime_attr("github_client"),
        github_replay_cache=runtime_attr("github_replay_cache"),
        record_failure=metrics.record_failure,
    )


# The provisioner creates every ingestion topic with KAFKA_TOPIC_PARTITIONS
# partitions (default 12 — see scripts/provision_kafka_topics.py). The
# traffic-signal partition lookup MUST use the same count or it computes a
# partition the message never lands on. Previously hardcoded to 32, which
# drifted from the provisioned 12.
_DEFAULT_NUM_PARTITIONS = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", "12"))


def _kafka_partition_for_tenant(
    tenant_id: Any,
    *,
    num_partitions: int | None = None,
) -> int:
    """M-Load: explicit murmur2 partition match with librdkafka.

    librdkafka's default `murmur2_random` partitioner uses MurmurHash2
    32-bit with seed `0x9747b28c`, ANDs with `0x7fffffff` (positive
    int32), and mods by `num_partitions`. With `signed=False`, mmh3
    returns unsigned uint32 which is already positive; the mask is a
    no-op but kept conceptually for parity with the librdkafka source.

    `num_partitions` defaults to the provisioned topic partition count
    (`KAFKA_TOPIC_PARTITIONS`, default 12). Callers/tests may override.

    The match between this function's output and the actual landing
    partition is verified by
    `test_kafka_partition_lookup_matches_actual_landing_partition`
    (M-Load Phase 1 test). librdkafka version must match the partitioner
    algorithm assumed here; if a future librdkafka switches
    partitioners, that test catches the drift.
    """
    import mmh3

    if num_partitions is None:
        num_partitions = _DEFAULT_NUM_PARTITIONS
    key_bytes = str(tenant_id).encode("utf-8")
    h = mmh3.hash(key_bytes, seed=0x9747B28C, signed=False)
    return (h & 0x7FFFFFFF) % num_partitions


async def _attempt_kafka_path(
    request: Request,
    *,
    runtime: WebhookRuntime,
    provider: str,
    source: str,
    tenant_id: Any,
    raw_body: bytes,
    payload: Mapping[str, Any] | None,
) -> bool:
    """M5.3 cutover try: S3 PutIfAbsent → publish to `ingestion.raw` →
    emit 1% traffic signal (LLD §11.3). Returns True on full success,
    False on any failure (caller MUST fall back to inline `ingest()`).

    Three-place documentation for the fallback semantic (per the
    M5.3 reminder pattern):
      [1] This function's docstring (the contract).
      [2] The call site in `build_webhooks_router` (where the
          fallback decision is made + the metric is incremented).
      [3] `services/app/webhooks/metrics.py::_kafka_path_outcomes` (the
          operator-visible counter with the smoke-detector semantic).

    Graceful degradation, NOT gate-relaxation: when the cutover path
    fails, the user-visible response is still 200/201 from inline
    ingest(). The customer never sees the Kafka outage; the
    `fallback` metric is the only signal an operator sees that
    cutover connectivity is degraded.
    """
    kafka_producer = runtime.kafka_producer
    s3_client = runtime.s3_raw_client
    if kafka_producer is None or s3_client is None:
        # Cutover requires both S3 + Kafka wired. A missing dep at
        # this point is a deployment misconfiguration (the flag
        # being TRUE without the deps wired). Loud log + fall back
        # to inline so the customer is not impacted; operator sees
        # the failure via the fallback metric.
        log.error(
            "router.cutover_deps_missing",
            provider=provider,
            has_kafka=kafka_producer is not None,
            has_s3=s3_client is not None,
        )
        return False
    try:
        ingress_metadata = build_webhook_ingress_metadata(
            provider,
            request.headers,
            payload,
        )

        await shadow_write_raw(
            tenant_id=tenant_id,
            source=source,  # type: ignore[arg-type]  — runtime checked
            ingress_kind="webhook",
            raw_body=raw_body,
            s3_client=s3_client,
            kafka_producer=kafka_producer,
            ingress_metadata=ingress_metadata,
        )
        # 1% deterministic-hash traffic signal (LLD §11.3). Never
        # propagates per its own prime directive (`traffic_signal.py`).
        await maybe_emit_traffic_signal(
            tenant_id=tenant_id,
            source=source,
            ingress_kind="webhook",
            raw_partition=_kafka_partition_for_tenant(tenant_id),
            content_hash=compute_content_hash(raw_body).encode("ascii"),
            kafka_producer=kafka_producer,
        )
        # CRITICAL (request/response gateway): `produce()` only enqueues
        # into librdkafka's LOCAL buffer and returns before broker-ack.
        # Unlike the always-on workers, an HTTP handler has nothing
        # driving the delivery queue — a produced message would sit in
        # the local buffer indefinitely and never land on `ingestion.raw`.
        # Flush so we only return the 202 once the event is DURABLY on the
        # broker (the correct webhook semantic: don't ack the provider and
        # then lose the event). Bounded by CUTOVER_FLUSH_TIMEOUT_SEC so a slow
        # broker trips the inline fallback quickly rather than stacking a long
        # wait on the synchronous request. On incomplete flush, return False so
        # the caller falls back to inline ingest() — idempotent via S3
        # PutIfAbsent + observation-layer dedup, so a late-delivered duplicate
        # is harmless. Mirrors the Notion handler's flush.
        remaining = await coalesced_flush(
            kafka_producer,
            timeout_seconds=CUTOVER_FLUSH_TIMEOUT_SEC,
        )
        if remaining:
            log.warning(
                "router.kafka_path_flush_incomplete",
                provider=provider,
                remaining=remaining,
            )
            return False
        return True
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "router.kafka_path_failed",
            provider=provider,
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )
        return False


async def _maybe_shadow_write_webhook(
    request: Request,
    *,
    runtime: WebhookRuntime,
    provider: str,
    tenant_id: Any,
    raw_body: bytes,
    payload: Mapping[str, Any] | None,
) -> None:
    """Shadow-write helper for the webhook router. PRIME DIRECTIVE
    (M2 work order): a failure here MUST NOT propagate.

    Caller guarantees: tenant is resolved, signature verified, inline
    ingest() succeeded. Any exception thrown by S3 / Kafka / flag-read
    is caught and logged inline; the caller's 200/201 response is
    unaffected.

    No-ops cleanly when:
      - provider's contract does not declare inline shadow writing.
      - runtime.kafka_producer or runtime.s3_raw_client is unset
        (gateway-config: the lifespan handler hasn't wired the
        shadow deps; pre-M2 deployments).
      - runtime.tenant_flags reports
        ingestion.shadow_write_enabled=False for this tenant.

    Per LLD §11 (per-tenant flag) + M2 §M2.1.
    """
    try:
        ingress = webhook_ingress_definition(provider)
        source = ingress.source_id
        if not ingress.shadow_write_enabled or source is None:
            return

        kafka_producer = runtime.kafka_producer
        s3_client = runtime.s3_raw_client
        tenant_flags = runtime.tenant_flags

        if kafka_producer is None or s3_client is None:
            # Shadow deps not wired — silent skip. Pre-M2 deployments
            # and unit tests that don't exercise shadow path hit this.
            return

        if tenant_flags is not None:
            enabled = await tenant_flags.get_bool(
                tenant_id,
                SHADOW_WRITE_ENABLED,
                default=True,
            )
            if not enabled:
                return

        # Per-provider hints come from the immutable webhook contract.
        # Resolution remains lazy so unwired shadow paths stay cheap.
        ingress_metadata = build_webhook_ingress_metadata(
            provider,
            request.headers,
            payload,
        )

        await shadow_write_raw(
            tenant_id=tenant_id,
            source=source,  # type: ignore[arg-type]  — runtime checked
            ingress_kind="webhook",
            raw_body=raw_body,
            s3_client=s3_client,
            kafka_producer=kafka_producer,
            ingress_metadata=ingress_metadata,
        )
    except Exception as exc:  # noqa: BLE001
        # M2 prime directive: never propagate. log + metric and return.
        log.warning(
            "shadow_path.failure",
            provider=provider,
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )


_WEBHOOK_RETRY_AFTER_SECONDS = "30"


def _safe_public_webhook_response(
    *,
    provider: str,
    code: str,
    message: str,
    status_code: int,
) -> JSONResponse:
    headers = (
        {"Retry-After": _WEBHOOK_RETRY_AFTER_SECONDS}
        if status_code in {500, 502, 503, 504}
        else None
    )
    return JSONResponse(
        {
            "code": code,
            "message": message,
            "context": {"provider": provider},
        },
        status_code=status_code,
        headers=headers,
    )


def _public_payload_rejected_response(provider: str) -> JSONResponse:
    return _safe_public_webhook_response(
        provider=provider,
        code="webhook_payload_rejected",
        message="webhook payload rejected",
        status_code=400,
    )


def _public_processing_unavailable_response(provider: str) -> JSONResponse:
    return _safe_public_webhook_response(
        provider=provider,
        code="webhook_processing_unavailable",
        message="webhook processing temporarily unavailable",
        status_code=503,
    )


def _log_public_ingest_error(
    *,
    provider: str,
    status_code: int,
    exc: BaseException,
) -> None:
    context = getattr(exc, "context", None)
    log.warning(
        "webhook_inline_ingest_failed",
        provider=provider,
        status_code=status_code,
        error_type=type(exc).__name__,
        error_code=getattr(exc, "code", None),
        recoverable=getattr(exc, "recoverable", False),
        context_keys=sorted(context) if isinstance(context, dict) else [],
    )


def _err_response(
    err: WebhookVerificationError,
    status_code: int = 401,
) -> JSONResponse:
    """Render a verification error as a 401 with structured context.

    FR-016: the body and candidate signature are NOT included in the
    response (or in any structured log we emit). The error's
    `to_dict()` shape is `{code, message, context}` with `provider`
    and `reason` always populated.
    """
    metrics.record_failure(err.provider, err.reason)
    log.info(
        "webhook_verification_failed",
        provider=err.provider,
        reason=err.reason,
        code=err.code,
    )
    return JSONResponse(err.to_dict(), status_code=status_code)


def _is_discord_ping(payload: Mapping[str, Any] | None) -> bool:
    """Detect Discord's interaction PING (type=1)."""
    return isinstance(payload, dict) and payload.get("type") == 1


def _safe_json_loads(raw: bytes) -> dict[str, Any] | None:
    """Best-effort JSON parse. Returns None for non-JSON or non-object
    bodies; the caller treats `None` as "tenant indeterminate" and
    defers any rejection until after signature verification."""
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


async def _process_verified_webhook_unit(
    request: Request,
    *,
    runtime: WebhookRuntime,
    provider: str,
    ingress: WebhookIngressDefinition,
    tenant_id: Any,
    unit_payload: dict[str, Any],
) -> int:
    """Process one contract-hook payload through the generic ingestion tail.

    Returns the HTTP status the unit produced (202 = Kafka cutover, 200 =
    inline). Raises `ValidationError` / `CompanyOSError` on an inline ingest
    rejection so the contract-owned provider policy can choose its batch
    semantics.
    """
    assert ingress.source_id is not None
    unit_raw = json.dumps(unit_payload, separators=(",", ":")).encode("utf-8")

    flag_enabled = False
    tenant_flags = runtime.tenant_flags
    if tenant_flags is not None:
        flag_enabled = await tenant_flags.kafka_path_enabled(tenant_id)

    if flag_enabled:
        succeeded = await _attempt_kafka_path(
            request,
            runtime=runtime,
            provider=provider,
            source=ingress.source_id,
            tenant_id=tenant_id,
            raw_body=unit_raw,
            payload=unit_payload,
        )
        if succeeded:
            metrics.record_kafka_path_outcome(provider, "success")
            return 202
        metrics.record_kafka_path_outcome(provider, "fallback")
        log.warning(
            "router.kafka_path_fallback_to_inline",
            provider=provider,
            tenant_id=str(tenant_id),
        )

    deps = _deps(request)
    await ingest(
        ingress.channel,
        unit_payload,
        pool=deps.pool,
        tenant_id=tenant_id,
        actor_repo=deps.actor_repo,
        alias_repo=deps.alias_repo,
        embedder=deps.embedder,
        request_headers=safe_headers(request.headers),
    )
    # Mirror the generic tail: shadow-write only when NOT on the cutover path.
    if not flag_enabled:
        await _maybe_shadow_write_webhook(
            request,
            runtime=runtime,
            provider=provider,
            tenant_id=tenant_id,
            raw_body=unit_raw,
            payload=unit_payload,
        )
    return 200


def _unknown_provider_response(provider: str) -> JSONResponse:
    return JSONResponse(
        {
            "code": "unknown_provider",
            "message": f"no webhook verifier registered for {provider!r}",
            "context": {"provider": provider},
        },
        status_code=404,
    )


def _payload_too_large_response(provider: str) -> JSONResponse:
    return JSONResponse(
        {
            "code": "payload_too_large",
            "message": "payload exceeds maximum size",
            "context": {
                "provider": provider,
                "max_bytes": MAX_PAYLOAD_BYTES,
            },
        },
        status_code=413,
    )


def _tenant_resolver_missing_response(provider: str) -> JSONResponse:
    log.error("webhook_router_tenant_resolver_missing", provider=provider)
    return JSONResponse(
        {
            "code": "service_unavailable",
            "message": "tenant resolver not initialized",
            "context": {"provider": provider},
        },
        status_code=503,
    )


def _unexpected_verifier_error_response(provider: str, exc: Exception) -> JSONResponse:
    log.error(
        "webhook_verifier_unexpected_error",
        provider=provider,
        error_type=type(exc).__name__,
    )
    metrics.record_failure(provider, "signature_mismatch")
    return JSONResponse(
        {
            "code": "webhook_verification_failed",
            "message": "verifier raised unexpected error",
            "context": {
                "provider": provider,
                "reason": "signature_mismatch",
            },
        },
        status_code=401,
    )


async def _read_webhook_body(
    request: Request,
    *,
    provider: str,
) -> tuple[bytes | None, dict[str, Any] | None, JSONResponse | None]:
    raw = await request.body()
    if len(raw) > MAX_PAYLOAD_BYTES:
        return None, None, _payload_too_large_response(provider)
    return raw, _safe_json_loads(raw), None


async def _resolve_and_verify_webhook(
    request: Request,
    *,
    provider: str,
    ingress: WebhookIngressDefinition,
    subpath: str,
    payload: Mapping[str, Any] | None,
    raw: bytes,
    verifier: Any,
) -> WebhookAuthContext | JSONResponse:
    runtime = _webhook_runtime(request)

    tenant_resolver = runtime.tenant_resolver
    if tenant_resolver is None:
        return _tenant_resolver_missing_response(provider)

    # R3: pass the URL subpath so per-install-endpoint providers (Ashby)
    # can resolve the tenant from `/webhooks/{provider}/{installId}`.
    outcome = await tenant_resolver.resolve(
        provider,
        payload or {},
        dict(request.headers),
        subpath=subpath,
    )
    tenant_id = outcome.tenant_id if isinstance(outcome, Resolved) else None

    installation_row_id = (
        outcome.installation_row_id if isinstance(outcome, Resolved) else None
    )
    try:
        secret_loader = resolve_webhook_secret_loader(ingress.route_id)
        loaded_secrets = await secret_loader(
            provider,
            tenant_id,
            installation_row_id=installation_row_id,
            app_state=request.app.state,
        )
    except Exception as exc:  # noqa: BLE001 - secret resolution fails closed
        log.error(
            "webhook_secret_loader_failed",
            provider=provider,
            error_type=type(exc).__name__,
        )
        return _public_processing_unavailable_response(provider)
    if (
        not isinstance(loaded_secrets, Sequence)
        or isinstance(loaded_secrets, (str, bytes, bytearray))
        or any(not isinstance(secret, Secret) for secret in loaded_secrets)
    ):
        log.error(
            "webhook_secret_loader_invalid_result",
            provider=provider,
            result_type=type(loaded_secrets).__name__,
        )
        return _public_processing_unavailable_response(provider)
    secrets = tuple(loaded_secrets)
    try:
        verified = await verifier(
            body=raw,
            headers=request.headers,
            secrets=secrets,
            now=time.time(),
        )
    except WebhookVerificationError as exc:
        return _err_response(exc)
    except Exception as exc:  # pragma: no cover — defensive
        return _unexpected_verifier_error_response(provider, exc)

    return WebhookAuthContext(
        runtime=runtime,
        outcome=outcome,
        tenant_id=tenant_id,
        verified=verified,
    )


async def _verified_pre_tenant_response(
    request: Request,
    *,
    provider: str,
    ingress: WebhookIngressDefinition,
    runtime: WebhookRuntime,
    payload: Mapping[str, Any] | None,
) -> JSONResponse | None:
    if (
        ingress.acknowledgement_policy == "synchronous_provider_response"
        and _is_discord_ping(payload)
    ):
        return JSONResponse({"type": 1}, status_code=200)

    try:
        handler = resolve_webhook_verified_pre_tenant_handler(provider)
        if handler is None:
            return None
        response = await handler(
            request=request,
            runtime=runtime,
            payload=payload,
        )
    except Exception as exc:  # noqa: BLE001 - contract hook fails closed
        log.error(
            "webhook_verified_pre_tenant_handler_failed",
            provider=provider,
            error_type=type(exc).__name__,
        )
        return _public_processing_unavailable_response(provider)
    if response is not None and not isinstance(response, JSONResponse):
        log.error(
            "webhook_verified_pre_tenant_handler_invalid_response",
            provider=provider,
            response_type=type(response).__name__,
        )
        return _public_processing_unavailable_response(provider)
    return response


def _resolver_outcome_rejection(
    outcome: Any,
    *,
    provider: str,
    tenant_id: Any | None,
) -> JSONResponse | None:
    if isinstance(outcome, UnknownInstallation):
        err = WebhookVerificationError(
            "unknown_installation",
            "no enabled installation matches the supplied identifier",
            provider=outcome.provider,
        )
        return _err_response(err, status_code=401)
    if isinstance(outcome, PayloadMissing):
        metrics.record_failure(provider, "tenant_not_resolved")
        log.info(
            "webhook_payload_missing_identifier",
            provider=outcome.provider,
        )
        return JSONResponse(
            {
                "code": "payload_missing",
                "message": "request did not carry a parseable installation identifier",
                "context": {"provider": outcome.provider},
            },
            status_code=400,
        )
    if tenant_id is None:  # pragma: no cover — defensive
        err = WebhookVerificationError(
            "tenant_not_resolved",
            "verified webhook could not be mapped to a tenant",
            provider=provider,
        )
        return _err_response(err)
    return None


async def _provider_verified_response(
    request: Request,
    *,
    provider: str,
    ingress: WebhookIngressDefinition,
    runtime: WebhookRuntime,
    outcome: Any,
    tenant_id: Any,
    payload: Mapping[str, Any] | None,
    verified: VerifiedContext,
) -> JSONResponse | None:
    if ingress.handler_mode == "dedicated":
        assert ingress.dedicated_handler_binding is not None
        handler = resolve_callable_reference(ingress.dedicated_handler_binding)
        return await handler(
            request=request,
            outcome=outcome,
            payload=payload or {},
        )

    async def process_unit(
        *,
        tenant_id: Any,
        payload: dict[str, Any],
    ) -> int:
        return await _process_verified_webhook_unit(
            request,
            runtime=runtime,
            provider=provider,
            ingress=ingress,
            tenant_id=tenant_id,
            unit_payload=payload,
        )

    try:
        handler = resolve_webhook_verified_tenant_handler(provider)
        if handler is not None:
            response = await handler(
                request=request,
                runtime=runtime,
                outcome=outcome,
                tenant_id=tenant_id,
                payload=payload,
                verified=verified,
                process_unit=process_unit,
            )
        else:
            response = None
    except Exception as exc:  # noqa: BLE001 - contract hook fails closed
        log.error(
            "webhook_verified_tenant_handler_failed",
            provider=provider,
            error_type=type(exc).__name__,
        )
        return _public_processing_unavailable_response(provider)
    if handler is not None:
        if response is not None and not isinstance(response, JSONResponse):
            log.error(
                "webhook_verified_tenant_handler_invalid_response",
                provider=provider,
                response_type=type(response).__name__,
            )
            return _public_processing_unavailable_response(provider)
        if response is not None:
            return response

    return None


async def _kafka_cutover_response(
    request: Request,
    *,
    provider: str,
    ingress: WebhookIngressDefinition,
    runtime: WebhookRuntime,
    tenant_id: Any,
    raw: bytes,
    payload: Mapping[str, Any] | None,
    verified: VerifiedContext,
) -> WebhookCutoverDecision:
    flag_enabled = False
    cutover_source = ingress.source_id
    if ingress.kafka_cutover_enabled and runtime.tenant_flags is not None:
        flag_enabled = await runtime.tenant_flags.kafka_path_enabled(tenant_id)

    if not flag_enabled or not ingress.kafka_cutover_enabled or cutover_source is None:
        return WebhookCutoverDecision(flag_enabled=flag_enabled, response=None)

    succeeded = await _attempt_kafka_path(
        request,
        runtime=runtime,
        provider=provider,
        source=cutover_source,
        tenant_id=tenant_id,
        raw_body=raw,
        payload=payload,
    )
    if succeeded:
        metrics.record_kafka_path_outcome(provider, "success")
        return WebhookCutoverDecision(
            flag_enabled=True,
            response=JSONResponse(
                {
                    "status": "accepted",
                    "secret_label": verified.secret_label,
                },
                status_code=202,
                headers={
                    "X-Secret-Label": verified.secret_label or "",
                },
            ),
        )

    metrics.record_kafka_path_outcome(provider, "fallback")
    log.warning(
        "router.kafka_path_fallback_to_inline",
        provider=provider,
        tenant_id=str(tenant_id),
    )
    return WebhookCutoverDecision(flag_enabled=True, response=None)


async def _inline_ingest_response(
    request: Request,
    *,
    provider: str,
    ingress: WebhookIngressDefinition,
    runtime: WebhookRuntime,
    tenant_id: Any,
    raw: bytes,
    payload: Mapping[str, Any] | None,
    verified: VerifiedContext,
    suppress_shadow_write: bool,
) -> JSONResponse:
    channel = ingress.channel
    if payload is None:
        try:
            payload = json.loads(verified.body)
        except json.JSONDecodeError:
            return _public_payload_rejected_response(provider)

    deps = _deps(request)
    try:
        result: IngestResult = await ingest(
            channel,
            payload,
            pool=deps.pool,
            tenant_id=tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
            request_headers=safe_headers(request.headers),
        )
    except HandlerNotFound:
        log.error(
            "webhook_inline_ingest_handler_missing",
            provider=provider,
            channel=channel,
        )
        return _public_processing_unavailable_response(provider)
    except PayloadTooLarge:
        return JSONResponse(
            {
                "code": "payload_too_large",
                "message": "payload exceeds maximum size",
                "context": {"provider": provider},
            },
            status_code=413,
        )
    except ValidationError as exc:
        _log_public_ingest_error(provider=provider, status_code=400, exc=exc)
        return _public_payload_rejected_response(provider)
    except CompanyOSError as exc:
        if exc.recoverable:
            _log_public_ingest_error(provider=provider, status_code=503, exc=exc)
            return _public_processing_unavailable_response(provider)
        _log_public_ingest_error(provider=provider, status_code=400, exc=exc)
        return _public_payload_rejected_response(provider)

    if not suppress_shadow_write:
        await _maybe_shadow_write_webhook(
            request,
            runtime=runtime,
            provider=provider,
            tenant_id=tenant_id,
            raw_body=raw,
            payload=payload,
        )

    substrate_headers = {
        "X-Observation-Id": str(result.observation.id),
        "X-Deduped": "true" if result.deduped else "false",
        "X-Secret-Label": verified.secret_label or "",
    }
    if result.trigger_queue_id is not None:
        substrate_headers["X-Trigger-Queue-Id"] = str(result.trigger_queue_id)

    if (
        ingress.acknowledgement_policy == "synchronous_provider_response"
        and isinstance(payload, dict)
        and payload.get("type") == 2
    ):
        return JSONResponse(
            {
                "type": 4,
                "data": {
                    "content": "Got it — your question is recorded in Fyralis. (Follow-up content ships in IN-13.)",
                    "flags": 64,  # EPHEMERAL — only the invoker sees this
                },
            },
            status_code=200,
            headers=substrate_headers,
        )

    return JSONResponse(
        {
            "observation_id": str(result.observation.id),
            "deduped": result.deduped,
            "trigger_queue_id": (
                str(result.trigger_queue_id) if result.trigger_queue_id else None
            ),
            "secret_label": verified.secret_label,
        },
        status_code=200 if result.deduped else 201,
    )


async def _receive_webhook(
    provider: str,
    request: Request,
    *,
    subpath: str = "",
) -> JSONResponse:
    try:
        ingress = webhook_ingress_definition(provider)
    except KeyError:
        return _unknown_provider_response(provider)
    verifier = verifier_for_provider(provider)
    if verifier is None:  # pragma: no cover - catalog/runtime guard
        return _unknown_provider_response(provider)

    raw, payload, read_error = await _read_webhook_body(request, provider=provider)
    if read_error is not None:
        return read_error
    assert raw is not None  # for type checkers; read_error handled above.

    handshake_binding = ingress.verification_handshake_binding
    handshake_handler_binding = ingress.verification_handshake_handler_binding
    if handshake_binding is not None and handshake_handler_binding is not None:
        is_handshake = resolve_callable_reference(handshake_binding)
        if is_handshake(payload):
            handshake_handler = resolve_callable_reference(handshake_handler_binding)
            return handshake_handler(payload)

    auth = await _resolve_and_verify_webhook(
        request,
        provider=provider,
        ingress=ingress,
        subpath=subpath,
        payload=payload,
        raw=raw,
        verifier=verifier,
    )
    if isinstance(auth, JSONResponse):
        return auth

    response = await _verified_pre_tenant_response(
        request,
        provider=provider,
        ingress=ingress,
        runtime=auth.runtime,
        payload=payload,
    )
    if response is not None:
        return response

    response = _resolver_outcome_rejection(
        auth.outcome,
        provider=provider,
        tenant_id=auth.tenant_id,
    )
    if response is not None:
        return response

    response = await _provider_verified_response(
        request,
        provider=provider,
        ingress=ingress,
        runtime=auth.runtime,
        outcome=auth.outcome,
        tenant_id=auth.tenant_id,
        payload=payload,
        verified=auth.verified,
    )
    if response is not None:
        return response

    cutover = await _kafka_cutover_response(
        request,
        provider=provider,
        ingress=ingress,
        runtime=auth.runtime,
        tenant_id=auth.tenant_id,
        raw=raw,
        payload=payload,
        verified=auth.verified,
    )
    if cutover.response is not None:
        return cutover.response

    return await _inline_ingest_response(
        request,
        provider=provider,
        ingress=ingress,
        runtime=auth.runtime,
        tenant_id=auth.tenant_id,
        raw=raw,
        payload=payload,
        verified=auth.verified,
        suppress_shadow_write=cutover.flag_enabled,
    )


def build_webhooks_router() -> APIRouter:
    """Create exactly the webhook routes declared by the source contract.

    The router is stateless — all deps are resolved off the gateway
    runtime attached to `request.app.state`, so tests can construct the
    gateway app and exercise the router without further wiring. Notably,
    `app.state.integration_runtime.tenant_resolver` is the DB-backed
    resolver wired by gateway startup (see
    `services/app/gateway/state_wiring.py::wire_integration_runtime_state`).
    """
    router = APIRouter(prefix="/webhooks", tags=["webhooks"])

    def endpoint_for(
        ingress: WebhookIngressDefinition,
    ) -> Any:
        async def receive(request: Request) -> JSONResponse:
            route_prefix = f"/webhooks/{ingress.route_id}"
            request_path = str(request.scope.get("path", ""))
            subpath = (
                request_path[len(route_prefix) :].removeprefix("/")
                if request_path.startswith(route_prefix)
                else ""
            )
            return await _receive_webhook(
                ingress.route_id,
                request,
                subpath=subpath,
            )

        # Stable, unique route names make the mounted inventory and OpenAPI
        # output deterministic without creating another source registry.
        receive.__name__ = f"receive_{ingress.route_id}_webhook"
        return receive

    for ingress in WEBHOOK_INGRESS_CATALOG.values():
        router.add_api_route(
            ingress.route_path.removeprefix("/webhooks"),
            endpoint_for(ingress),
            methods=("POST",),
            name=f"receive_{ingress.route_id}_webhook",
        )

    return router


def _deps(request: Request) -> Any:
    """Resolve gateway deps off the app state.

    Lazy lookup so the router can be mounted before the lifespan
    handler wires deps (the existing gateway pattern).
    """
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError(
            "gateway deps not initialised — webhook router requires "
            "build_app() lifespan to have completed"
        )
    return deps


__all__ = ["build_webhooks_router"]
