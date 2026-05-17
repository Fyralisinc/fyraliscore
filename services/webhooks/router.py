"""services/webhooks/router.py — FastAPI router for /webhooks/{provider}/...

Mounted by `services/gateway/main.py`. The Bearer middleware in the
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
    5. Call `request.app.state.tenant_resolver.resolve(provider, payload,
       headers)` to map the (provider, installation_id) pair to a
       tenant. The outcome is captured but the rejection (if any) is
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
import time
from typing import Any, Mapping

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from services.ingestion.core import (
    IngestResult,
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    ingest,
)
from services.ingestion.feature_flags import SHADOW_WRITE_ENABLED
from services.ingestion.handlers import HandlerNotFound
from services.ingestion.shadow_write import shadow_write_raw
from services.webhooks import metrics
from services.webhooks.signatures import VERIFIERS
from services.webhooks.secrets import load_secrets
from services.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)
from services.webhooks.verifier import WebhookVerificationError


log = structlog.get_logger("webhooks.router")


# Providers whose webhook bodies belong on the new ingestion data
# plane. linear/stripe ingestion stays inline-only — they're not in
# the source enum (LLD §1 / RawEnvelope: slack|github|discord|gmail).
# Gmail enters via Pub/Sub, not this webhook router (see M2.2).
_PROVIDER_TO_SHADOW_SOURCE: dict[str, str] = {
    "slack": "slack",
    "github": "github",
    "discord": "discord",
}


async def _maybe_shadow_write_webhook(
    request: Request,
    *,
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
      - app.state.kafka_producer or app.state.s3_raw_client is unset
        (gateway-config: the lifespan handler hasn't wired the
        shadow deps; pre-M2 deployments).
      - app.state.tenant_flags reports
        ingestion.shadow_write_enabled=False for this tenant.

    Per LLD §11 (per-tenant flag) + M2 §M2.1.
    """
    try:
        source = _PROVIDER_TO_SHADOW_SOURCE.get(provider)
        if source is None:
            return  # linear / stripe / future providers — not in scope

        kafka_producer = getattr(request.app.state, "kafka_producer", None)
        s3_client = getattr(request.app.state, "s3_raw_client", None)
        tenant_flags = getattr(request.app.state, "tenant_flags", None)

        if kafka_producer is None or s3_client is None:
            # Shadow deps not wired — silent skip. Pre-M2 deployments
            # and unit tests that don't exercise shadow path hit this.
            return

        if tenant_flags is not None:
            enabled = await tenant_flags.get_bool(
                tenant_id, SHADOW_WRITE_ENABLED, default=True,
            )
            if not enabled:
                return

        # Per-provider hints — populated lazily to keep the unwired
        # paths cheap. The hints are best-effort; the normalizer
        # treats them as advisory.
        ingress_metadata: dict[str, Any] = {"event_type": _event_type_for(provider, request, payload)}
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


def _is_slack_url_verification(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
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
    return (
        headers.get("X-GitHub-Delivery")
        or headers.get("x-github-delivery")
    )


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
    pool: Any, installation_row_id: Any,
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
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
    installation_id: str | None,
) -> JSONResponse:
    """IN-13: dispatch a verified, tenant-resolved GitHub lifecycle
    event (installation, installation_repositories) to
    `services.integrations.github.lifecycle.dispatch` and return its
    JSON body with HTTP 200.
    """
    pool = getattr(request.app.state, "pool", None)
    if pool is None or installation_id is None:
        log.error(
            "github_lifecycle_deps_missing",
            has_pool=pool is not None,
            has_installation_id=installation_id is not None,
        )
        return JSONResponse({"handled": event_type}, status_code=200)

    github_client = getattr(request.app.state, "github_client", None)
    cache_dict = None
    if github_client is not None:
        cache_dict = getattr(github_client, "_installation_tokens", None)

    tenant_resolver = getattr(request.app.state, "tenant_resolver", None)

    try:
        from services.integrations.github.lifecycle import dispatch
        from lib.shared.errors import ValidationError as _ValidationError
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
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
) -> JSONResponse:
    """Run the Slack uninstall flow for a verified, tenant-resolved
    webhook. Returns 200 with `{handled: <event_type>}` so Slack's
    retry budget closes out cleanly."""
    from services.integrations.slack import uninstall as slack_uninstall

    team_id = (
        payload.get("team_id")
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(team_id, str):
        # The resolver already matched the team; this should never
        # happen, but defensively close the request out.
        return JSONResponse({"handled": event_type}, status_code=200)

    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    tenant_resolver = getattr(request.app.state, "tenant_resolver", None)
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


def build_webhooks_router() -> APIRouter:
    """Create the FastAPI router. Mounted at the app root by the
    gateway so paths read as `/webhooks/{provider}/{subpath:path}`.

    The router is stateless — all deps are resolved off `request.app.state`
    so tests can construct the gateway app and exercise the router
    without further wiring. Notably, `app.state.tenant_resolver` is
    the IN-07 DB-backed resolver wired by IN-08 (see
    `services/gateway/main.py::_wire_in08_state`).
    """
    router = APIRouter(prefix="/webhooks", tags=["webhooks"])

    @router.post("/{provider}/{subpath:path}")
    async def receive(
        provider: str,
        subpath: str,
        request: Request,
    ) -> JSONResponse:
        verifier = VERIFIERS.get(provider)
        if verifier is None:
            return JSONResponse(
                {
                    "code": "unknown_provider",
                    "message": f"no webhook verifier registered for {provider!r}",
                    "context": {"provider": provider},
                },
                status_code=404,
            )

        # Step 1+2: capture raw body bytes; enforce size precheck.
        raw = await request.body()
        if len(raw) > MAX_PAYLOAD_BYTES:
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

        # Step 3 + 4: best-effort JSON parse so the resolver and the
        # Slack URL-verification handshake have a dict to inspect.
        payload = _safe_json_loads(raw)
        slack_uv = (
            _is_slack_url_verification(payload) if provider == "slack" else None
        )

        # Step 5: resolve tenant via the IN-07 DB-backed resolver.
        # `payload or {}` keeps the API contract clean for Stripe
        # (header-only id extraction) and for malformed bodies.
        tenant_resolver = getattr(request.app.state, "tenant_resolver", None)
        if tenant_resolver is None:
            # Gateway misconfiguration — fail loud rather than silently
            # falling back to the legacy env-var resolver. The
            # `_wire_in08_state` lifespan hook is the single chokepoint
            # that populates this attribute.
            log.error("webhook_router_tenant_resolver_missing", provider=provider)
            return JSONResponse(
                {
                    "code": "service_unavailable",
                    "message": "tenant resolver not initialized",
                    "context": {"provider": provider},
                },
                status_code=503,
            )
        outcome = await tenant_resolver.resolve(
            provider, payload or {}, dict(request.headers),
        )
        tenant_id_uuid = (
            outcome.tenant_id if isinstance(outcome, Resolved) else None
        )

        # Step 6: load secrets — DB-backed via the IN-08 secret store.
        # The verifier itself raises `secret_not_configured` when the
        # list is empty, which keeps the rejection reason consistent.
        secrets = await load_secrets(
            provider, tenant_id_uuid, app_state=request.app.state,
        )

        # Step 7: verify.
        try:
            ctx = await verifier.verify(
                body=raw,
                headers=request.headers,
                secrets=secrets,
                now=time.time(),
            )
        except WebhookVerificationError as e:
            return _err_response(e)
        except Exception as e:  # pragma: no cover — defensive
            log.error(
                "webhook_verifier_unexpected_error",
                provider=provider,
                error_type=type(e).__name__,
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

        # Step 8a: provider-specific verified-handshake responses.
        # These bypass the tenant-rejection enforcement because the
        # handshake itself does not name a tenant.
        if slack_uv is not None:
            challenge = slack_uv.get("challenge", "")
            return JSONResponse({"challenge": challenge}, status_code=200)
        if provider == "discord" and _is_discord_ping(payload):
            return JSONResponse({"type": 1}, status_code=200)
        # IN-13 FR-022: GitHub `ping` event. Handled BEFORE unknown-
        # installation enforcement because the bootstrap ping may
        # arrive before any customer has installed.
        if provider == "github" and _is_github_ping(request.headers):
            try:
                from services.integrations.github import metrics as gh_metrics
                gh_metrics.record_webhook_verified(result="ok")
            except Exception:  # noqa: BLE001
                pass
            log.info(
                "github_webhook_ping",
                event_type="ping",
                delivery_id=_github_delivery_id(request.headers),
            )
            return JSONResponse({"handled": "ping"}, status_code=200)

        # IN-13 FR-008b + Clarifications Q4: replay-cache check runs
        # AFTER signature verification AND BEFORE tenant-resolution
        # outcome enforcement. Defense-in-depth — observation-layer
        # dedup is the correctness backstop.
        if provider == "github":
            replay_cache = getattr(
                request.app.state, "github_replay_cache", None,
            )
            github_installation_id = _github_installation_id_from_payload(
                payload,
            )
            delivery_id = _github_delivery_id(request.headers)
            if (
                replay_cache is not None
                and github_installation_id is not None
                and delivery_id is not None
            ):
                if replay_cache.seen(github_installation_id, delivery_id):
                    try:
                        from services.integrations.github import (
                            metrics as gh_metrics,
                        )
                        gh_metrics.record_replay_dropped()
                    except Exception:  # noqa: BLE001
                        pass
                    log.info(
                        "github_webhook_replay_dropped",
                        delivery_id=delivery_id,
                    )
                    return JSONResponse(
                        {"handled": "replay"}, status_code=200,
                    )

        # Step 8b: enforce resolver outcome — deferred until AFTER
        # signature verification so an attacker probing tenant ids
        # sees signature failures first (FR-023, IN-07 SC-008).
        if isinstance(outcome, UnknownInstallation):
            err = WebhookVerificationError(
                "unknown_installation",
                "no enabled installation matches the supplied identifier",
                provider=outcome.provider,
            )
            return _err_response(err, status_code=401)
        if isinstance(outcome, PayloadMissing):
            # PayloadMissing is a client-side defect (bad request) rather
            # than an auth failure — return 400, matching IN-07 mapping.
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

        # outcome is Resolved at this point — tenant_id_uuid is set.
        if tenant_id_uuid is None:  # pragma: no cover — defensive
            err = WebhookVerificationError(
                "tenant_not_resolved",
                "verified webhook could not be mapped to a tenant",
                provider=provider,
            )
            return _err_response(err)

        # IN-08 US4: dispatch Slack lifecycle events (app_uninstalled /
        # tokens_revoked) to the uninstall handler BEFORE ingestion.
        # These events disable the installation + zeroize secret
        # material; they do NOT produce an Observation.
        if provider == "slack":
            slack_lifecycle = _slack_lifecycle_event(payload)
            if slack_lifecycle is not None:
                return await _handle_slack_lifecycle(
                    request,
                    outcome,
                    payload,
                    slack_lifecycle,
                )

        # IN-13 US4 + US5: dispatch GitHub lifecycle events
        # (installation, installation_repositories) BEFORE ingestion;
        # then enforce per-installation `selected_repositories` allowlist
        # for non-lifecycle events.
        if provider == "github":
            event_type = _github_event_type(request.headers)
            github_installation_id = _github_installation_id_from_payload(
                payload,
            )

            if event_type in ("installation", "installation_repositories"):
                return await _handle_github_lifecycle(
                    request=request,
                    outcome=outcome,
                    payload=payload or {},
                    event_type=event_type,
                    installation_id=github_installation_id,
                )

            # Repo filter: only applies when the installation pinned an
            # explicit list. NULL = "all repositories" (no filter).
            pool = getattr(request.app.state, "pool", None)
            selected = await _load_github_selected_repositories(
                pool, outcome.installation_row_id,
            )
            if selected is not None:
                repo_full = _github_repo_full_name(payload)
                if repo_full is None or repo_full not in selected:
                    try:
                        from services.integrations.github import (
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
                        {"handled": "filtered_repo"}, status_code=200,
                    )

        # Step 9: ingest. Use the already-parsed payload when possible
        # to save a re-decode; fall back to re-parse for paths where
        # the payload didn't reach JSON earlier (shouldn't happen now).
        channel = _PROVIDER_CHANNEL[provider]
        if payload is None:
            try:
                payload = json.loads(ctx.body)
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
                tenant_id=tenant_id_uuid,
                actor_repo=deps.actor_repo,
                alias_repo=deps.alias_repo,
                embedder=deps.embedder,
                request_headers=dict(request.headers),
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
        except ValidationError as e:
            return JSONResponse(
                {"code": e.code, "message": e.message, "context": e.context},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"code": e.code, "message": e.message, "context": e.context},
                status_code=400,
            )

        # ---- M2.1 Shadow path ----
        # After successful inline ingest(), before the 200/201 response.
        # Best-effort; failures are caught inside the helper and logged.
        # PRIME DIRECTIVE (M2 work order §M2.1): never propagate — the
        # inline `ingest()` already succeeded and the user-visible
        # response must not be reordered or blocked by Kafka/S3 issues.
        await _maybe_shadow_write_webhook(
            request,
            provider=provider,
            tenant_id=tenant_id_uuid,
            raw_body=raw,
            payload=payload,
        )

        # Discord interactions require a specific response shape
        # (https://discord.com/developers/docs/interactions/receiving-and-responding).
        # The substrate's generic ingestion shape is invisible to Discord;
        # without a recognised `type` field the client UI renders
        # "The application didn't respond in time" even though we
        # returned 200/201 within the deadline. For type=2
        # ApplicationCommand we emit a CHANNEL_MESSAGE_WITH_SOURCE
        # response with an ephemeral confirmation so the user sees an
        # acknowledgement instead of an error. The real follow-up
        # message with Fyralis content lands in IN-13.
        # Headers expose the substrate metadata for tests / debugging
        # without leaking it into Discord's channel.
        substrate_headers = {
            "X-Observation-Id": str(result.observation.id),
            "X-Deduped": "true" if result.deduped else "false",
            "X-Secret-Label": ctx.secret_label or "",
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
                    str(result.trigger_queue_id)
                    if result.trigger_queue_id
                    else None
                ),
                "secret_label": ctx.secret_label,
            },
            status_code=200 if result.deduped else 201,
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
