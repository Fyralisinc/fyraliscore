"""services/app/webhooks/router.py — FastAPI router for /webhooks/{provider}/...

Mounted by `services/app/gateway/main.py`. The Bearer middleware in the
gateway skips this path prefix (see `_PUBLIC_PATH_PREFIXES`), so the
only authentication is the cryptographic signature check below.

Request flow:

    1. Capture raw body bytes (NOT a re-parsed JSON form).
    2. Enforce IN-01 body-size precheck (1 MB).
    3. Look up the per-provider verifier; 404 on unknown provider.
    4. Best-effort JSON-parse the body so the tenant resolver and the
       Slack URL-verification handshake have a dict to inspect.
       Malformed JSON does NOT immediately reject — the verifier still
       runs first so an attacker cannot probe the JSON-validity oracle.
    5. Resolve runtime from `request.app.state.integration_runtime`
       (with legacy aliases as a compatibility bridge), then call the
       tenant resolver to map the (provider, installation_id) pair to
       a tenant. The outcome is captured but the rejection (if any) is
       deferred until AFTER signature verification — same security
       posture as before IN-08: signature failure first, then tenant.
    6. Load secrets via `await load_secrets(provider, tenant_id,
       app_state=request.app.state)`. With IN-08, this resolves
       `provider_installations.secret_ref` through the envelope-
       encrypted secret store; the env-var path is dev-only.
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
from services.ingest.integrations.notion import webhook as notion_webhook
from services.app.webhooks import metrics
from services.app.webhooks.signatures import VERIFIERS
from services.app.webhooks.secrets import load_secrets
from services.app.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)
from services.app.webhooks.verifier import (
    VerifiedContext,
    Verifier,
    WebhookVerificationError,
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
    )


# Providers whose webhook bodies belong on the new ingestion data
# plane. linear/stripe ingestion stays inline-only — they're not in
# the source enum (LLD §1 / RawEnvelope: slack|github|discord|gmail).
# Gmail enters via Pub/Sub, not this webhook router (see M2.2).
_PROVIDER_TO_SHADOW_SOURCE: dict[str, str] = {
    "slack": "slack",
    "github": "github",
    "discord": "discord",
    "jira": "jira",
    # Finance sources — HMAC-signed webhooks route onto the data plane.
    "mercury": "mercury",
    "quickbooks": "quickbooks",
    # IN-GRAFANA: Grafana Alerting webhook (HMAC X-Grafana-Alerting-Signature)
    # routes onto the data plane.
    "grafana": "grafana",
    # IN-FIN2: Brex/Ramp/Gusto/Deel — HMAC-signed finance webhooks route onto
    # the data plane (Bearer archetype: brex, deel; OAuth archetype: ramp, gusto).
    "brex": "brex",
    "ramp": "ramp",
    "gusto": "gusto",
    "deel": "deel",
    # IN-FF/IN-MIRO/IN-FIGMA: Fireflies/Miro/Figma — HMAC-signed webhooks
    # (Brex archetype) route onto the data plane.
    "fireflies": "fireflies",
    "miro": "miro",
    "figma": "figma",
    # IN-PEOPLE/IN-RECRUITING: HiBob + Ashby — HMAC-signed webhooks route onto
    # the data plane. (LinkedIn is poll-only — no webhook — so it is absent.)
    "hibob": "hibob",
    "ashby": "ashby",
}

# M5.3 — providers whose `ingestion.kafka_path_enabled=TRUE` activates
# the cutover (skip inline `ingest()`, publish to Kafka, return 202).
# Discord interactions require a specific synchronous response shape
# (CHANNEL_MESSAGE_WITH_SOURCE; see the discord type-2 branch below);
# the 202 contract doesn't fit that shape, so discord webhooks stay
# on the inline path regardless of the flag. M5.4 documents this as
# a deferral — a future work-unit can wire discord cutover once the
# response-shape question is resolved.
_CUTOVER_ENABLED_PROVIDERS: dict[str, str] = {
    "slack": "slack",
    "github": "github",
    # IN-17: Jira webhooks route through the full pipeline (the 202 cutover
    # contract fits — no synchronous-response-shape constraint like Discord).
    "jira": "jira",
    # Finance sources: Mercury + QuickBooks webhooks fit the 202 cutover
    # contract (no synchronous-response-shape constraint), so they activate
    # the full pipeline once the tenant's kafka_path_enabled flag is TRUE.
    "mercury": "mercury",
    "quickbooks": "quickbooks",
    # IN-GRAFANA: Grafana Alerting webhooks fit the 202 cutover contract.
    "grafana": "grafana",
    # IN-FIN2: Brex/Ramp/Gusto/Deel finance webhooks fit the 202 cutover
    # contract (no synchronous-response-shape constraint), so they activate the
    # full pipeline once the tenant's kafka_path_enabled flag is TRUE.
    "brex": "brex",
    "ramp": "ramp",
    "gusto": "gusto",
    "deel": "deel",
    # IN-FF/IN-MIRO/IN-FIGMA: Fireflies/Miro/Figma webhooks fit the 202 cutover
    # contract, so they activate the full pipeline once the tenant's
    # kafka_path_enabled flag is TRUE.
    "fireflies": "fireflies",
    "miro": "miro",
    "figma": "figma",
    # IN-PEOPLE/IN-RECRUITING: HiBob + Ashby webhooks fit the 202 cutover
    # contract, so they activate the full pipeline once the tenant's
    # kafka_path_enabled flag is TRUE. (LinkedIn is poll-only — absent here.)
    "hibob": "hibob",
    "ashby": "ashby",
}


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
        ingress_metadata: dict[str, Any] = {
            "event_type": _event_type_for(provider, request, payload),
        }
        if provider == "github":
            delivery_id = _github_delivery_id(request.headers)
            if delivery_id:
                ingress_metadata["delivery_id"] = delivery_id

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
      - provider is not in the shadow-source map (linear/stripe).
      - runtime.kafka_producer or runtime.s3_raw_client is unset
        (gateway-config: the lifespan handler hasn't wired the
        shadow deps; pre-M2 deployments).
      - runtime.tenant_flags reports
        ingestion.shadow_write_enabled=False for this tenant.

    Per LLD §11 (per-tenant flag) + M2 §M2.1.
    """
    try:
        source = _PROVIDER_TO_SHADOW_SOURCE.get(provider)
        if source is None:
            return  # linear / stripe / future providers — not in scope

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

        # Per-provider hints — populated lazily to keep the unwired
        # paths cheap. The hints are best-effort; the normalizer
        # treats them as advisory.
        ingress_metadata: dict[str, Any] = {
            "event_type": _event_type_for(provider, request, payload)
        }
        if provider == "github":
            delivery_id = _github_delivery_id(request.headers)
            if delivery_id:
                ingress_metadata["delivery_id"] = delivery_id

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


def _event_type_for(
    provider: str,
    request: Request,
    payload: Mapping[str, Any] | None,
) -> str:
    """Best-effort event-type extraction for shadow ingress_metadata."""
    if provider == "github":
        return _github_event_type(request.headers) or "unknown"
    if provider == "slack" and isinstance(payload, dict):
        event = payload.get("event")
        if isinstance(event, dict):
            etype = event.get("type")
            if isinstance(etype, str):
                return etype
    if provider == "discord" and isinstance(payload, dict):
        # Discord interaction type is an int per their docs.
        itype = payload.get("type")
        if isinstance(itype, int):
            return f"interaction:{itype}"
    return "unknown"


# Channels in CHANNEL_TRUST_MAP are keyed differently per provider; the
# router maps from provider → channel name once, here, so the
# verification layer and the ingestion handler registry stay aligned.
_PROVIDER_CHANNEL: dict[str, str] = {
    "slack": "slack:message",
    "github": "github:webhook",
    "linear": "linear:webhook",
    "stripe": "stripe:webhook",
    "discord": "discord:interaction",
    # IN-17: inline-ingest fallback channel (used only when the tenant's
    # kafka_path_enabled flag is off; otherwise the cutover 202 path runs).
    "jira": "jira:issue",
    # Finance sources — inline-ingest fallback channels (cutover 202 path runs
    # when kafka_path_enabled is TRUE).
    "mercury": "mercury:transaction",
    "quickbooks": "quickbooks:object",
    # IN-GRAFANA: the webhook delivers alert groups -> the `grafana:alert`
    # channel (inline-ingest fallback when kafka_path_enabled is off).
    "grafana": "grafana:alert",
    # IN-FIN2: finance webhook channels (inline-ingest fallback when
    # kafka_path_enabled is off). Must match each handler's @register(...).
    "brex": "brex:transaction",
    "ramp": "ramp:transaction",
    "gusto": "gusto:object",
    "deel": "deel:payment",
    # IN-FF/IN-MIRO/IN-FIGMA: webhook channels (inline-ingest fallback when
    # kafka_path_enabled is off). Must match each handler's @register(...).
    "fireflies": "fireflies:transcript",
    "miro": "miro:item",
    "figma": "figma:event",
    # IN-PEOPLE/IN-RECRUITING: webhook channels (inline-ingest fallback when
    # kafka_path_enabled is off). Must match each handler's @register(_CHANNEL):
    # handlers/hibob.py _CHANNEL="hibob:object", handlers/ashby.py
    # _CHANNEL="ashby:object". (LinkedIn is poll-only — no inline webhook channel.)
    "hibob": "hibob:object",
    "ashby": "ashby:object",
}


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


def _is_slack_url_verification(
    payload: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Detect Slack's one-time `url_verification` handshake. Returns
    the payload when matched, else None."""
    if not isinstance(payload, dict):
        return None
    if payload.get("type") == "url_verification":
        return payload
    return None


def _is_discord_ping(payload: Mapping[str, Any] | None) -> bool:
    """Detect Discord's interaction PING (type=1)."""
    return isinstance(payload, dict) and payload.get("type") == 1


def _is_github_ping(headers: Mapping[str, str]) -> bool:
    """IN-13: Detect GitHub's `ping` event. The event type is in the
    `X-GitHub-Event` header (not the body), so we check headers."""
    event = headers.get("X-GitHub-Event") or headers.get("x-github-event")
    return event == "ping"


def _github_event_type(headers: Mapping[str, str]) -> str | None:
    return headers.get("X-GitHub-Event") or headers.get("x-github-event")


def _github_delivery_id(headers: Mapping[str, str]) -> str | None:
    return headers.get("X-GitHub-Delivery") or headers.get("x-github-delivery")


def _github_installation_id_from_payload(
    payload: Mapping[str, Any] | None,
) -> str | None:
    """Mirror of tenant_resolver._extract_github: read `installation.id`."""
    if not isinstance(payload, dict):
        return None
    inst = payload.get("installation")
    if not isinstance(inst, Mapping):
        return None
    iid = inst.get("id")
    if iid is None:
        return None
    if isinstance(iid, bool):
        return None
    if isinstance(iid, (int, str)):
        s = str(iid).strip()
        return s or None
    return None


def _github_repo_full_name(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    repo = payload.get("repository")
    if isinstance(repo, Mapping):
        full = repo.get("full_name")
        if isinstance(full, str) and full:
            return full
    return None


async def _load_github_selected_repositories(
    pool: Any,
    installation_row_id: Any,
) -> list[str] | None:
    """Read `selected_repositories` for an installation. Returns:
    - list[str]: explicit selection (delivery must match)
    - None:       all-repositories mode (no filter)
    - []:         empty selection (every delivery is filtered out)
    """
    if pool is None or installation_row_id is None:
        return None
    row = await pool.fetchrow(
        """
        SELECT selected_repositories
          FROM provider_installations
         WHERE id = $1
        """,
        installation_row_id,
    )
    if row is None:
        return None
    raw = row["selected_repositories"]
    if raw is None:
        return None
    # asyncpg may return JSONB as already-decoded list or as a JSON
    # string depending on codec registration.
    if isinstance(raw, list):
        return [str(x) for x in raw if isinstance(x, str)]
    try:
        import json as _json

        parsed = _json.loads(raw)
    except Exception:  # noqa: BLE001
        return None
    if isinstance(parsed, list):
        return [str(x) for x in parsed if isinstance(x, str)]
    return None


def _slack_lifecycle_event(payload: Mapping[str, Any] | None) -> str | None:
    """Detect Slack installation-lifecycle events. Returns the event
    type string when matched (`'app_uninstalled'` | `'tokens_revoked'`),
    else None. IN-08 US4: these route to the uninstall handler instead
    of ingestion."""
    if not isinstance(payload, dict):
        return None
    event = payload.get("event")
    if isinstance(event, dict):
        t = event.get("type")
        if t in ("app_uninstalled", "tokens_revoked"):
            return t
    return None


async def _handle_github_lifecycle(
    *,
    request: Request,
    runtime: WebhookRuntime,
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
    installation_id: str | None,
) -> JSONResponse:
    """IN-13: dispatch a verified, tenant-resolved GitHub lifecycle
    event (installation, installation_repositories) to
    `services.ingest.integrations.github.lifecycle.dispatch` and return its
    JSON body with HTTP 200.
    """
    pool = runtime.pool
    if pool is None or installation_id is None:
        log.error(
            "github_lifecycle_deps_missing",
            has_pool=pool is not None,
            has_installation_id=installation_id is not None,
        )
        return JSONResponse({"handled": event_type}, status_code=200)

    github_client = runtime.github_client
    cache_dict = None
    if github_client is not None:
        cache_dict = getattr(github_client, "_installation_tokens", None)

    tenant_resolver = runtime.tenant_resolver

    try:
        from services.ingest.integrations.github.lifecycle import dispatch

        body = await dispatch(
            event_type=event_type,
            payload=payload,
            tenant_id=outcome.tenant_id,
            installation_row_id=outcome.installation_row_id,
            installation_id=installation_id,
            pool=pool,
            installation_token_cache=cache_dict,
            tenant_resolver=tenant_resolver,
        )
    except Exception as exc:  # noqa: BLE001
        # Don't 500 on lifecycle dispatch failure; GitHub will retry.
        # Log loud and return a 200 so the retry budget closes out.
        log.error(
            "github_lifecycle_dispatch_failed",
            event_type=event_type,
            error_type=type(exc).__name__,
        )
        return JSONResponse(
            {"handled": event_type, "error": "dispatch_failed"},
            status_code=200,
        )

    return JSONResponse(body, status_code=200)


async def _handle_slack_lifecycle(
    request: Request,
    runtime: WebhookRuntime,
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
) -> JSONResponse:
    """Run the Slack uninstall flow for a verified, tenant-resolved
    webhook. Returns 200 with `{handled: <event_type>}` so Slack's
    retry budget closes out cleanly."""
    from services.ingest.integrations.slack import uninstall as slack_uninstall

    team_id = payload.get("team_id") if isinstance(payload, dict) else None
    if not isinstance(team_id, str):
        # The resolver already matched the team; this should never
        # happen, but defensively close the request out.
        return JSONResponse({"handled": event_type}, status_code=200)

    pool = runtime.pool
    secret_store = runtime.secret_store
    tenant_resolver = runtime.tenant_resolver
    if pool is None or secret_store is None or tenant_resolver is None:
        log.error(
            "slack_uninstall_deps_missing",
            has_pool=pool is not None,
            has_secret_store=secret_store is not None,
            has_tenant_resolver=tenant_resolver is not None,
        )
        return JSONResponse({"handled": event_type}, status_code=200)

    handler = (
        slack_uninstall.handle_app_uninstalled
        if event_type == "app_uninstalled"
        else slack_uninstall.handle_tokens_revoked
    )
    await handler(
        pool,
        secret_store,
        tenant_resolver,
        outcome.tenant_id,
        outcome.installation_row_id,
        team_id,
    )
    return JSONResponse({"handled": event_type}, status_code=200)


def _safe_json_loads(raw: bytes) -> dict[str, Any] | None:
    """Best-effort JSON parse. Returns None for non-JSON or non-object
    bodies; the caller treats `None` as "tenant indeterminate" and
    defers any rejection until after signature verification."""
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


# =====================================================================
# QuickBooks multi-tenant fan-out (R1, CRITICAL #7)
# =====================================================================
#
# A single Intuit webhook delivery batches `eventNotifications[]`, and EACH
# notification carries its OWN `realmId` — a realm is a connected QuickBooks
# company, and each company maps to a DIFFERENT Fyralis tenant. Within a
# notification, `dataChangeEvent.entities[]` lists MULTIPLE changed entities.
#
# The generic single-resolve→single-ingest tail resolves one tenant (from
# `eventNotifications[0].realmId`) and runs the handler once, so every
# notification past the first realm — and every entity past the first — is
# silently dropped. This fans the delivery out into one ingest per
# `(realmId, entity)` unit, each resolved to ITS realm's tenant.
#
# Security: `intuit-signature` is an APP-level secret (one verifier token per
# Intuit app, shared across every connected realm), so the single up-front
# signature verification in `receive` already authenticates the whole batch —
# no per-unit re-verification is needed. (Spec R1: "intuit-signature already
# verifies multi-realm".)
#
# Safety / gate-invariance: each unit is re-serialised as a FLAT single-entity
# payload `{realmId, name, id, operation, lastUpdated}` — the exact shape the
# QBO handler's "flattened webhook" branch already accepts, producing a draft
# BYTE-IDENTICAL to today's `eventNotifications[0]` path. So a single-realm/
# single-entity delivery (what the all-25 gate sends) fans out to exactly one
# unit and yields the same observation + the same 202 cutover status as before.


def _qbo_fanout_units(
    payload: Mapping[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """Split a QuickBooks `eventNotifications` delivery into
    `(realm_id, flat_single_entity_payload)` units.

    Returns one unit per (notification realm × changed entity). Drops
    notifications/entities that carry no realmId or are malformed. Returns
    `[]` when nothing resolvable is present (caller surfaces a 400).
    """
    notifications = payload.get("eventNotifications")
    if not isinstance(notifications, list):
        return []
    units: list[tuple[str, dict[str, Any]]] = []
    for notif in notifications:
        if not isinstance(notif, dict):
            continue
        realm_id = str(notif.get("realmId") or "")
        if not realm_id:
            continue
        dce = notif.get("dataChangeEvent")
        ents = dce.get("entities") if isinstance(dce, dict) else None
        if not isinstance(ents, list):
            continue
        for ent in ents:
            if not isinstance(ent, dict):
                continue
            units.append(
                (
                    realm_id,
                    {
                        "realmId": realm_id,
                        "name": ent.get("name"),
                        "id": ent.get("id"),
                        "operation": ent.get("operation"),
                        "lastUpdated": ent.get("lastUpdated"),
                    },
                )
            )
    return units


async def _process_qbo_unit(
    request: Request,
    *,
    runtime: WebhookRuntime,
    tenant_id: Any,
    unit_payload: dict[str, Any],
) -> int:
    """Process ONE resolved (realm, entity) QuickBooks unit through the same
    cutover-or-inline decision the generic tail uses for a single delivery.

    Returns the HTTP status the unit produced (202 = Kafka cutover, 200 =
    inline). Raises `ValidationError` / `CompanyOSError` on an inline ingest
    rejection (the caller logs + skips the offending unit).
    """
    unit_raw = json.dumps(unit_payload, separators=(",", ":")).encode("utf-8")

    flag_enabled = False
    tenant_flags = runtime.tenant_flags
    if tenant_flags is not None:
        flag_enabled = await tenant_flags.kafka_path_enabled(tenant_id)

    if flag_enabled:
        succeeded = await _attempt_kafka_path(
            request,
            runtime=runtime,
            provider="quickbooks",
            source="quickbooks",
            tenant_id=tenant_id,
            raw_body=unit_raw,
            payload=unit_payload,
        )
        if succeeded:
            metrics.record_kafka_path_outcome("quickbooks", "success")
            return 202
        metrics.record_kafka_path_outcome("quickbooks", "fallback")
        log.warning(
            "router.kafka_path_fallback_to_inline",
            provider="quickbooks",
            tenant_id=str(tenant_id),
        )

    deps = _deps(request)
    await ingest(
        "quickbooks:object",
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
            provider="quickbooks",
            tenant_id=tenant_id,
            raw_body=unit_raw,
            payload=unit_payload,
        )
    return 200


async def _ingest_quickbooks_fanout(
    request: Request,
    *,
    runtime: WebhookRuntime,
    payload: Mapping[str, Any],
    secret_label: str | None,
) -> JSONResponse:
    """Fan a verified QuickBooks `eventNotifications` delivery out into one
    ingest per `(realmId, entity)`, each resolved to its realm's tenant.

    Caller guarantees: signature already verified (app-level intuit-signature).
    """
    units = _qbo_fanout_units(payload)
    if not units:
        return JSONResponse(
            {
                "code": "validation_error",
                "message": "quickbooks webhook carried no (realm, entity) units",
                "context": {"provider": "quickbooks"},
            },
            status_code=400,
        )

    resolver = runtime.tenant_resolver
    headers = dict(request.headers)
    statuses: set[int] = set()
    ingested = 0
    unknown_realms = 0
    realm_tenant: dict[str, Any] = {}

    for realm_id, unit_payload in units:
        tenant_id = realm_tenant.get(realm_id)
        if tenant_id is None:
            # Resolve THIS realm to its own tenant. `_extract_quickbooks`
            # reads a top-level `realmId`, so a minimal `{realmId}` resolves.
            outcome = await resolver.resolve(
                "quickbooks",
                {"realmId": realm_id},
                headers,
            )
            if not isinstance(outcome, Resolved):
                unknown_realms += 1
                # Never log the realmId verbatim (resolver FR-015 posture).
                log.warning("qbo_fanout_unknown_realm", provider="quickbooks")
                continue
            tenant_id = outcome.tenant_id
            realm_tenant[realm_id] = tenant_id
        try:
            status = await _process_qbo_unit(
                request,
                runtime=runtime,
                tenant_id=tenant_id,
                unit_payload=unit_payload,
            )
        except (ValidationError, CompanyOSError) as exc:
            # One malformed entity must not sink the rest of the batch.
            log.warning(
                "qbo_fanout_unit_rejected",
                provider="quickbooks",
                code=getattr(exc, "code", "error"),
            )
            continue
        statuses.add(status)
        ingested += 1

    if ingested == 0:
        # Every realm unknown/disabled (or every unit rejected) — surface as
        # an auth failure so the provider retries + ops notices, matching the
        # generic UnknownInstallation mapping.
        err = WebhookVerificationError(
            "unknown_installation",
            "no enabled installation matched any realm in the delivery",
            provider="quickbooks",
        )
        return _err_response(err, status_code=401)

    # 202 if any unit took the Kafka cutover path (the gate asserts {202} for
    # QBO when kafka_path_enabled is TRUE); otherwise 200 (inline).
    status_code = 202 if 202 in statuses else 200
    return JSONResponse(
        {
            "status": "accepted",
            "units": ingested,
            "unknown_realms": unknown_realms,
            "secret_label": secret_label,
        },
        status_code=status_code,
        headers={"X-Secret-Label": secret_label or ""},
    )


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
    subpath: str,
    payload: Mapping[str, Any] | None,
    raw: bytes,
    verifier: Verifier,
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

    secrets = await load_secrets(provider, tenant_id, app_state=request.app.state)
    try:
        verified = await verifier.verify(
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
    runtime: WebhookRuntime,
    payload: Mapping[str, Any] | None,
    slack_url_verification: Mapping[str, Any] | None,
) -> JSONResponse | None:
    if slack_url_verification is not None:
        challenge = slack_url_verification.get("challenge", "")
        return JSONResponse({"challenge": challenge}, status_code=200)
    if provider == "discord" and _is_discord_ping(payload):
        return JSONResponse({"type": 1}, status_code=200)

    # IN-13 FR-022: GitHub bootstrap pings may arrive before installation rows.
    if provider == "github" and _is_github_ping(request.headers):
        try:
            from services.ingest.integrations.github import metrics as gh_metrics

            gh_metrics.record_webhook_verified(result="ok")
        except Exception:  # noqa: BLE001
            pass
        log.info(
            "github_webhook_ping",
            event_type="ping",
            delivery_id=_github_delivery_id(request.headers),
        )
        return JSONResponse({"handled": "ping"}, status_code=200)

    if provider != "github":
        return None

    replay_cache = runtime.github_replay_cache
    github_installation_id = _github_installation_id_from_payload(payload)
    delivery_id = _github_delivery_id(request.headers)
    if replay_cache is None or github_installation_id is None or delivery_id is None:
        return None
    if not replay_cache.seen(github_installation_id, delivery_id):
        return None

    try:
        from services.ingest.integrations.github import metrics as gh_metrics

        gh_metrics.record_replay_dropped()
    except Exception:  # noqa: BLE001
        pass
    log.info("github_webhook_replay_dropped", delivery_id=delivery_id)
    return JSONResponse({"handled": "replay"}, status_code=200)


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
    runtime: WebhookRuntime,
    outcome: Any,
    tenant_id: Any,
    payload: Mapping[str, Any] | None,
    verified: VerifiedContext,
) -> JSONResponse | None:
    if provider == "notion":
        return await notion_webhook.handle_notion_event(
            request=request,
            outcome=outcome,
            payload=payload or {},
        )

    if provider == "slack":
        slack_lifecycle = _slack_lifecycle_event(payload)
        if slack_lifecycle is not None:
            return await _handle_slack_lifecycle(
                request,
                runtime,
                outcome,
                payload or {},
                slack_lifecycle,
            )

    if provider == "github":
        event_type = _github_event_type(request.headers)
        github_installation_id = _github_installation_id_from_payload(payload)
        if event_type in ("installation", "installation_repositories"):
            return await _handle_github_lifecycle(
                request=request,
                runtime=runtime,
                outcome=outcome,
                payload=payload or {},
                event_type=event_type,
                installation_id=github_installation_id,
            )

        selected = await _load_github_selected_repositories(
            runtime.pool,
            outcome.installation_row_id,
        )
        if selected is not None:
            repo_full = _github_repo_full_name(payload)
            if repo_full is None or repo_full not in selected:
                try:
                    from services.ingest.integrations.github import (
                        metrics as gh_metrics,
                    )

                    gh_metrics.record_filtered_repo(reason="not_selected")
                except Exception:  # noqa: BLE001
                    pass
                log.info(
                    "github_webhook_filtered_repo",
                    event_type=event_type,
                    repo_full_name=repo_full,
                )
                return JSONResponse(
                    {"handled": "filtered_repo"},
                    status_code=200,
                )

    if provider == "quickbooks" and isinstance(
        (payload or {}).get("eventNotifications"),
        list,
    ):
        return await _ingest_quickbooks_fanout(
            request,
            runtime=runtime,
            payload=payload or {},
            secret_label=verified.secret_label,
        )

    return None


async def _kafka_cutover_response(
    request: Request,
    *,
    provider: str,
    runtime: WebhookRuntime,
    tenant_id: Any,
    raw: bytes,
    payload: Mapping[str, Any] | None,
    verified: VerifiedContext,
) -> WebhookCutoverDecision:
    flag_enabled = False
    cutover_source = _CUTOVER_ENABLED_PROVIDERS.get(provider)
    if cutover_source is not None and runtime.tenant_flags is not None:
        flag_enabled = await runtime.tenant_flags.kafka_path_enabled(tenant_id)

    if not flag_enabled or cutover_source is None:
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
    runtime: WebhookRuntime,
    tenant_id: Any,
    raw: bytes,
    payload: Mapping[str, Any] | None,
    verified: VerifiedContext,
    suppress_shadow_write: bool,
) -> JSONResponse:
    channel = _PROVIDER_CHANNEL[provider]
    if payload is None:
        try:
            payload = json.loads(verified.body)
        except json.JSONDecodeError:
            return JSONResponse(
                {
                    "code": "invalid_json",
                    "message": "verified body is not valid JSON",
                    "context": {"provider": provider},
                },
                status_code=400,
            )

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
        return JSONResponse(
            {
                "code": "handler_not_found",
                "message": f"no ingestion handler for channel {channel!r}",
                "context": {"provider": provider, "channel": channel},
            },
            status_code=501,
        )
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
        return JSONResponse(
            {"code": exc.code, "message": exc.message, "context": exc.context},
            status_code=400,
        )
    except CompanyOSError as exc:
        return JSONResponse(
            {"code": exc.code, "message": exc.message, "context": exc.context},
            status_code=400,
        )

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

    if provider == "discord" and isinstance(payload, dict) and payload.get("type") == 2:
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
    verifier = VERIFIERS.get(provider)
    if verifier is None:
        return _unknown_provider_response(provider)

    raw, payload, read_error = await _read_webhook_body(request, provider=provider)
    if read_error is not None:
        return read_error
    assert raw is not None  # for type checkers; read_error handled above.

    slack_url_verification = (
        _is_slack_url_verification(payload) if provider == "slack" else None
    )

    if provider == "notion" and notion_webhook.is_verification_handshake(payload):
        return notion_webhook.handle_verification_handshake(payload)

    auth = await _resolve_and_verify_webhook(
        request,
        provider=provider,
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
        runtime=auth.runtime,
        payload=payload,
        slack_url_verification=slack_url_verification,
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
        runtime=auth.runtime,
        tenant_id=auth.tenant_id,
        raw=raw,
        payload=payload,
        verified=auth.verified,
        suppress_shadow_write=cutover.flag_enabled,
    )


def build_webhooks_router() -> APIRouter:
    """Create the FastAPI router. Mounted at the app root by the
    gateway so paths read as `/webhooks/{provider}/{subpath:path}`.

    The router is stateless — all deps are resolved off the gateway
    runtime attached to `request.app.state`, so tests can construct the
    gateway app and exercise the router without further wiring. Notably,
    `app.state.integration_runtime.tenant_resolver` is the DB-backed
    resolver wired by gateway startup (see
    `services/app/gateway/state_wiring.py::wire_integration_runtime_state`).
    """
    router = APIRouter(prefix="/webhooks", tags=["webhooks"])

    # Register BOTH the bare `/webhooks/{provider}` and the
    # `/webhooks/{provider}/{subpath}` forms on the same handler. GitHub (and
    # other senders) do NOT follow 3xx on webhook delivery — they treat a
    # redirect as a failed delivery — so we must NOT rely on Starlette's
    # trailing-slash 307 (`/webhooks/github` → `/webhooks/github/`). Both
    # forms now route directly to verification (no redirect). subpath="" for
    # the bare form.
    @router.post("/{provider}")
    @router.post("/{provider}/{subpath:path}")
    async def receive(
        provider: str,
        request: Request,
        subpath: str = "",
    ) -> JSONResponse:
        return await _receive_webhook(provider, request, subpath=subpath)

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
