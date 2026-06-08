"""
lib/shared/errors.py — shared error types with structured context.

Every error carries a `context: dict[str, Any]` dictionary. The
context is what downstream log emitters and retry machinery read.
Carrying context structurally (not only in the message) is how we
build uniform observability across services.
"""
from __future__ import annotations

from typing import Any


class CompanyOSError(Exception):
    """Root of every domain-level exception. Never raised directly."""

    default_code: str = "company_os_error"

    def __init__(self, message: str, **context: Any) -> None:
        super().__init__(message)
        self.message = message
        self.context: dict[str, Any] = dict(context)

    @property
    def code(self) -> str:
        return getattr(self, "_code", self.default_code)

    @property
    def recoverable(self) -> bool:
        """True when retrying the same operation later may succeed —
        rate limits, upstream 5xx, a temporarily-suspended install.

        Backfill (`services/ingestion/workflows/shard_fetch.py`) parks a
        shard (leaves it `in_progress` for the orphan-scan to retry)
        instead of terminal-failing it when the raised error is
        recoverable. Defaults False (fail-fast); subclasses opt in.
        """
        return getattr(self, "_recoverable", False)

    def to_dict(self) -> dict[str, Any]:
        """
        Serialisable form used by structured loggers, HTTP error
        responses, and the Think failure ledger.
        """
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.message!r}, context={self.context!r})"


# ---------------------------------------------------------------------
# Validation & invariants
# ---------------------------------------------------------------------

class ValidationError(CompanyOSError):
    """A payload failed schema or field validation. 4xx-class."""
    default_code = "validation_error"


class InvariantViolation(CompanyOSError):
    """
    A domain invariant (C1-C10, G1-G4, per spec §3) was violated.
    Raised at INSERT/transition time by services/domain/acts/invariants.py
    and by the Think validator.
    """
    default_code = "invariant_violation"

    def __init__(
        self,
        invariant: str,
        message: str,
        **context: Any,
    ) -> None:
        super().__init__(message, invariant=invariant, **context)
        self.invariant = invariant


# ---------------------------------------------------------------------
# Schema / storage
# ---------------------------------------------------------------------

class SchemaDriftError(CompanyOSError):
    """
    Live database diverges from SCHEMA-LOCK.md. Raised by
    scripts/check_schema_drift.py when run in fail-fast mode from
    inside a service (e.g. at startup).
    """
    default_code = "schema_drift"


# ---------------------------------------------------------------------
# Trust / calibration / falsifier
# ---------------------------------------------------------------------

class TrustTierError(CompanyOSError):
    """
    An operation required a minimum trust tier that the present
    signal did not satisfy. E.g. Commitment transition to
    `doneverified` with a non-authoritative resolved_by_event.
    """
    default_code = "trust_tier_error"

    def __init__(
        self,
        required: str,
        actual: str,
        message: str | None = None,
        **context: Any,
    ) -> None:
        super().__init__(
            message or f"required trust tier {required}; got {actual}",
            required=required,
            actual=actual,
            **context,
        )
        self.required = required
        self.actual = actual


class FalsifierInadequateError(CompanyOSError):
    """
    A Model with confidence > 0.7 was proposed without an adequate
    falsifier per spec §10 is_adequate_falsifier. See S2.1.
    """
    default_code = "falsifier_inadequate"

    def __init__(
        self,
        reason: str,
        falsifier: Any | None = None,
        **context: Any,
    ) -> None:
        super().__init__(reason, falsifier=falsifier, **context)
        self.reason = reason
        self.falsifier = falsifier


class MalformedFalsifierError(CompanyOSError):
    """
    A falsifier payload is structurally invalid — it has the right
    `kind` but at least one field cannot be parsed (e.g.
    `within_window` does not match either the ISO-8601 duration or
    human-readable grammar; `evaluate_at` is not a parseable
    timestamp; `check` does not match the prediction-deadline
    grammar).

    Distinct from `FalsifierInadequateError`, which signals a falsifier
    that is well-formed but too vague (pattern < 20 chars, missing
    `within_window`, etc.). Inadequate is a content-quality judgment;
    malformed is a parser failure.

    Surfacing this as a separate class lets the validator log a
    distinct `failure_reason='malformed_falsifier'` for observability,
    and lets call sites that want to repair the input (e.g. retry the
    LLM with a remediation hint) branch on the type rather than
    string-matching the message.
    """
    default_code = "falsifier_malformed"

    def __init__(
        self,
        reason: str,
        falsifier: Any | None = None,
        field: str | None = None,
        value: Any | None = None,
        **context: Any,
    ) -> None:
        super().__init__(
            reason, falsifier=falsifier, field=field, value=value, **context,
        )
        self.reason = reason
        self.falsifier = falsifier
        self.field = field
        self.value = value


class CalibrationMissingError(CompanyOSError):
    """
    A confidence adjustment was attempted but no calibration offset
    exists for the (actor, proposition_kind) pair and no cold-start
    default is configured. Typically raised during Think.validate.
    """
    default_code = "calibration_missing"

    def __init__(
        self,
        actor_id: Any,
        proposition_kind: str,
        **context: Any,
    ) -> None:
        super().__init__(
            f"no calibration offset for actor={actor_id} "
            f"proposition_kind={proposition_kind}",
            actor_id=str(actor_id),
            proposition_kind=proposition_kind,
            **context,
        )
        self.actor_id = actor_id
        self.proposition_kind = proposition_kind


# ---------------------------------------------------------------------
# Webhook tenant resolution (services/app/webhooks/tenant_resolver.py)
# ---------------------------------------------------------------------

class InstallationConflictError(CompanyOSError):
    """
    Admin attempted to register a (provider, installation_id) pair
    that already exists. Uniqueness is enforced by the UNIQUE
    constraint on provider_installations; this is the structured
    surface for the asyncpg.UniqueViolationError that bubbles up.
    """
    default_code = "installation_conflict"


class InstallationNotFoundError(CompanyOSError):
    """
    Admin attempted to disable / re-enable / update-secret-ref an
    installation row by id and the row did not exist. Distinct from
    the resolver's UnknownInstallation outcome (which deliberately
    does not leak existence).
    """
    default_code = "installation_not_found"


# ---------------------------------------------------------------------
# Secret store (lib/shared/secrets/)
# ---------------------------------------------------------------------

class SecretStoreError(CompanyOSError):
    """
    Backend-level failure in the envelope-encrypted secret store
    (DB unavailable, Fernet KEK invalid, ciphertext decrypt failed).
    Maps to HTTP 503 at API boundaries.
    """
    default_code = "secret_store_unavailable"


class SecretNotFoundError(CompanyOSError):
    """
    A `secret_ref` lookup returned zero rows for the given tenant.
    Distinct from SecretStoreError: the backend is healthy, the ref
    simply does not exist for this tenant. Webhook signature paths
    treat this as `unknown_installation` rather than 5xx so existence
    of refs cannot be probed across tenant boundaries.
    """
    default_code = "secret_not_found"


# ---------------------------------------------------------------------
# OAuth install flow (services/ingest/integrations/slack/oauth.py)
# ---------------------------------------------------------------------

class StateTokenInvalidError(CompanyOSError):
    """
    The OAuth callback's state token failed verification. The `reason`
    context field discriminates the failure mode: `state_invalid`
    (HMAC mismatch, malformed payload, or unknown nonce),
    `state_expired` (nonce known but past `expires_at`), or
    `state_consumed` (nonce known and already consumed). The HTTP
    status set by the handler is 400 for all three; the redirect's
    `reason` query param exposes the specific code.
    """
    default_code = "state_token_invalid"

    def __init__(self, reason: str, message: str, **context: Any) -> None:
        super().__init__(message, reason=reason, **context)
        self.reason = reason


class InstallationCollisionError(CompanyOSError):
    """
    OAuth callback attempted to bind a Slack `team_id` to a tenant
    that differs from the tenant already owning the
    `provider_installations` row for `(slack, team_id)`. Slack
    workspaces are not multi-tenant on the Fyralis side; the request
    fails closed with HTTP 409 and the foreign tenant identity is
    NEVER disclosed across the boundary (no log line carries either
    `team_id` or the conflicting `tenant_id`).

    Reused verbatim by IN-09 for Discord guild collisions.
    """
    default_code = "installation_collision"


class DiscordOAuthError(CompanyOSError):
    """
    Discord OAuth install/callback failure surface (IN-09).

    Stable `code` values consumed by the UI shell + audit log:
      - discord_oauth_token_exchange_failed: POST /oauth2/token non-2xx
      - discord_oauth_missing_guild: bot-scope response lacked guild.id
      - discord_command_registration_failed: POST /applications/.../commands 4xx

    `context` carries `{tenant_id, http_status?, discord_error_code?}`.
    `guild_id` is intentionally elided from context per FR-005/SC-006.
    """
    default_code = "discord_oauth_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class DiscordApiError(CompanyOSError):
    """
    Outbound Discord REST call failure (IN-09).

    Stable `code` values:
      - discord_api_unauthorized: 401 (or 403 code=50001) — chokepoint already fired
      - discord_api_rate_limited: retry budget exhausted (≤3 attempts / ≤30s wall)
      - discord_secret_unavailable: bot token not in secret store (orphan installation)
      - discord_api_error: other terminal 4xx/5xx

    `context` carries `{tenant_id, http_status?, attempts?, total_wall_seconds?}`.
    `guild_id` is intentionally elided from context per FR-005/SC-006.
    """
    default_code = "discord_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class GithubJWTError(CompanyOSError):
    """
    Failure to mint a GitHub App JWT (IN-13).

    Stable `reason` values carried on `context`:
      - no_app_id: GITHUB_APP_ID env var missing
      - no_private_key: neither GITHUB_APP_PRIVATE_KEY nor
        GITHUB_APP_PRIVATE_KEY_PATH is set
      - conflicting_keys: both env vars set (operator misconfig)
      - malformed_key: PEM parse failure
      - io_error: GITHUB_APP_PRIVATE_KEY_PATH cannot be read
    """
    default_code = "github_jwt_error"

    def __init__(self, reason: str, message: str, **context: Any) -> None:
        super().__init__(message, reason=reason, **context)
        self.reason = reason


class GithubOAuthError(CompanyOSError):
    """
    GitHub App OAuth install/callback failure surface (IN-13).

    Stable `code` values consumed by the UI shell + audit log:
      - github_oauth_token_mint_failed: POST /app/installations/.../access_tokens non-2xx
      - github_oauth_missing_installation_id: callback query lacked installation_id
      - github_oauth_repository_fetch_failed: GET /installation/repositories returned non-2xx

    `context` carries `{tenant_id, http_status?, github_error_code?}`.
    `installation_id` is hashed via `installation_id_hash` per FR-016.
    """
    default_code = "github_oauth_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class GithubApiError(CompanyOSError):
    """
    Outbound GitHub REST call failure (IN-13).

    Stable `code` values:
      - github_api_unauthorized: 401 Bad credentials — chokepoint already fired
      - github_api_not_found: 404 with apps-not-found doc_url — chokepoint already fired
      - github_api_rate_limited: 429 with retry budget exhausted
      - github_api_error: other terminal 4xx/5xx
      - github_jwt_unavailable: App JWT could not be minted (delegated GithubJWTError)

    `context` carries `{tenant_id, http_status?, attempts?, installation_id_hash?}`.
    Raw `installation_id` is NEVER placed on context (FR-016 / SC-008).
    """
    default_code = "github_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        recoverable: bool = False,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code
        # Rate limits / 5xx are transient; the backfill parks the shard
        # and retries rather than terminal-failing it (IN-13 hardening).
        self._recoverable = recoverable


class NotionOAuthError(CompanyOSError):
    """
    Notion OAuth install/callback failure surface (IN-14).

    Stable `code` values consumed by the UI shell + audit log:
      - notion_oauth_token_exchange_failed: POST /v1/oauth/token non-2xx
      - notion_oauth_missing_workspace_id: token response lacked workspace_id
      - notion_oauth_unconfigured: NOTION_CLIENT_ID / _SECRET / _REDIRECT_URI unset

    `context` carries `{tenant_id, http_status?, notion_error?}`. The raw
    workspace_id is hashed (`workspace_id_hash`) before it touches logs.
    """
    default_code = "notion_oauth_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class NotionApiError(CompanyOSError):
    """
    Outbound Notion REST call failure (IN-14).

    Stable `code` values:
      - notion_api_unauthorized: 401 — bot token revoked / integration removed
      - notion_api_not_found: 404 — object no longer accessible
      - notion_api_rate_limited: 429 with retry budget exhausted
      - notion_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, notion_code?, retry_after?}`. No bot
    token is ever placed on context.
    """
    default_code = "notion_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class JiraApiError(CompanyOSError):
    """
    Outbound Jira Cloud REST call failure (IN-17).

    Stable `code` values:
      - jira_api_unauthorized: 401/403 — API token rejected / no permission
      - jira_api_not_found: 404 — project/issue no longer accessible
      - jira_api_rate_limited: 429 with retry budget exhausted
      - jira_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The API token
    and the Basic-auth header are NEVER placed on context.
    """
    default_code = "jira_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class MercuryApiError(CompanyOSError):
    """
    Outbound Mercury banking REST call failure (finance source).

    Stable `code` values:
      - mercury_api_unauthorized: 401/403 — token rejected / insufficient scope
      - mercury_api_not_found: 404 — account/resource not visible to the token
      - mercury_api_rate_limited: 429 with retry budget exhausted
      - mercury_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The API token is
    NEVER placed on context.
    """
    default_code = "mercury_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class QuickBooksApiError(CompanyOSError):
    """
    Outbound QuickBooks Online REST call failure (finance source).

    Stable `code` values:
      - quickbooks_api_unauthorized: 401/403 — access token expired / no scope
        (the caller may need to refresh via the rotating refresh token)
      - quickbooks_api_not_found: 404 — entity/realm not visible
      - quickbooks_api_rate_limited: 429 (10 req/s, 120/min batch per realm)
      - quickbooks_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The access/refresh
    tokens are NEVER placed on context.
    """
    default_code = "quickbooks_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class GrafanaApiError(CompanyOSError):
    """
    Outbound Grafana HTTP API call failure (IN-GRAFANA).

    Stable `code` values:
      - grafana_api_unauthorized: 401/403 — service-account token rejected /
        insufficient role (needs `annotations:read`)
      - grafana_api_not_found: 404 — endpoint/org not visible to the token
      - grafana_api_rate_limited: 429 with retry budget exhausted
      - grafana_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The service-account
    token and the Authorization header are NEVER placed on context.
    """
    default_code = "grafana_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class TelegramApiError(CompanyOSError):
    """
    Outbound Telegram MTProto API call failure (IN-TELEGRAM).

    Telegram uses the MTProto user-account API (not the Bot API): backfill via
    messages.getHistory, live via a persistent updates connection. There is no
    HTTP status code at the protocol level — these map MTProto RPC errors.

    Stable `code` values:
      - telegram_api_flood_wait: FLOOD_WAIT (RPC error 420) — caller must wait
        the server-returned `seconds` (carried on context["retry_after"]).
      - telegram_api_unauthorized: AUTH_KEY_* / SESSION_REVOKED — the persisted
        session was invalidated; the install must re-authenticate.
      - telegram_api_not_found: peer/dialog not found or inaccessible.
      - telegram_api_error: other terminal RPC errors / transport failures.

    `context` carries `{retry_after?, peer?, method?}`. The session string /
    auth_key and api_hash are NEVER placed on context.
    """
    default_code = "telegram_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class BrexApiError(CompanyOSError):
    """
    Outbound Brex REST call failure (finance source — Bearer/Mercury archetype).

    Brex uses a long-lived API token (`Authorization: Bearer {token}`, no
    refresh). Mirrors MercuryApiError.

    Stable `code` values:
      - brex_api_unauthorized: 401/403 — token rejected / insufficient scope
      - brex_api_not_found: 404 — account/resource not visible to the token
      - brex_api_rate_limited: 429 with retry budget exhausted
      - brex_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The API token is
    NEVER placed on context.
    """
    default_code = "brex_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class RampApiError(CompanyOSError):
    """
    Outbound Ramp REST call failure (finance source — OAuth/QuickBooks archetype).

    Ramp uses an OAuth2 access token (+ rotating refresh token). Mirrors
    QuickBooksApiError.

    Stable `code` values:
      - ramp_api_unauthorized: 401/403 — access token expired / no scope
        (the caller may need to refresh via the rotating refresh token)
      - ramp_api_not_found: 404 — entity/business not visible
      - ramp_api_rate_limited: 429 with retry budget exhausted
      - ramp_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The access/refresh
    tokens are NEVER placed on context.
    """
    default_code = "ramp_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class GustoApiError(CompanyOSError):
    """
    Outbound Gusto REST call failure (finance source — OAuth/QuickBooks archetype).

    Gusto uses an OAuth2 access token (+ rotating refresh token); scope id is
    `company_uuid`. Mirrors QuickBooksApiError.

    Stable `code` values:
      - gusto_api_unauthorized: 401/403 — access token expired / no scope
        (the caller may need to refresh via the rotating refresh token)
      - gusto_api_not_found: 404 — entity/company not visible
      - gusto_api_rate_limited: 429 with retry budget exhausted
      - gusto_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The access/refresh
    tokens are NEVER placed on context.
    """
    default_code = "gusto_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class DeelApiError(CompanyOSError):
    """
    Outbound Deel REST call failure (finance source — Bearer/Mercury archetype).

    Deel uses a long-lived API token (`Authorization: Bearer {token}`, no
    refresh). Mirrors MercuryApiError.

    Stable `code` values:
      - deel_api_unauthorized: 401/403 — token rejected / insufficient scope
      - deel_api_not_found: 404 — contract/resource not visible to the token
      - deel_api_rate_limited: 429 with retry budget exhausted
      - deel_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The API token is
    NEVER placed on context.
    """
    default_code = "deel_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class FirefliesApiError(CompanyOSError):
    """
    Outbound Fireflies.ai call failure (comms source — Bearer/token archetype).

    Fireflies uses a long-lived API token (`Authorization: Bearer {token}`). The
    real API is GraphQL; this mirrors the Brex Bearer error shape.

    Stable `code` values:
      - fireflies_api_unauthorized: 401/403 — token rejected / insufficient scope
      - fireflies_api_not_found: 404 — transcript/workspace not visible
      - fireflies_api_rate_limited: 429 with retry budget exhausted
      - fireflies_api_error: other terminal 4xx/5xx (or a GraphQL error extension)

    `context` carries `{http_status?, retry_after?, path?}`. The API token is
    NEVER placed on context.
    """
    default_code = "fireflies_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class MiroApiError(CompanyOSError):
    """
    Outbound Miro REST call failure (design source — Bearer/token archetype).

    Miro uses an org-app Bearer token. Mirrors BrexApiError.

    Stable `code` values:
      - miro_api_unauthorized: 401/403 — token rejected / insufficient scope
      - miro_api_not_found: 404 — board/item not visible to the token
      - miro_api_rate_limited: 429 with retry budget exhausted
      - miro_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The API token is
    NEVER placed on context.
    """
    default_code = "miro_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class FigmaApiError(CompanyOSError):
    """
    Outbound Figma REST call failure (design source — Bearer/token archetype).

    Figma uses a team/org Bearer token. Mirrors BrexApiError. (Note: Figma's
    webhook verifier is passcode-in-body, not HMAC — see signatures/figma.py.)

    Stable `code` values:
      - figma_api_unauthorized: 401/403 — token rejected / insufficient scope
      - figma_api_not_found: 404 — file/event not visible to the token
      - figma_api_rate_limited: 429 with retry budget exhausted
      - figma_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The API token is
    NEVER placed on context.
    """
    default_code = "figma_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class SignalApiError(CompanyOSError):
    """
    Signal linked-device session failure (comms source — gateway/session
    archetype). Mirrors the Telegram gateway error shape; NOT a Bearer/OAuth
    REST call. Coverage is own/linked-account only.

    Stable `code` values:
      - signal_api_unauthorized: linked-device session rejected / unlinked
      - signal_api_not_found: thread/message not visible to the linked device
      - signal_api_rate_limited: send/receive throttled with retry budget exhausted
      - signal_api_error: other terminal session/transport failure

    `context` carries `{retry_after?, thread_id?}`. Session keys are NEVER placed
    on context.
    """
    default_code = "signal_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class AwsApiError(CompanyOSError):
    """
    Outbound AWS API call failure (infra source — IAM/SigV4 archetype).

    AWS calls are SigV4-signed (CloudTrail/CloudWatch/SQS). Mirrors the generic
    finance error shape; auth is IAM credentials, not a Bearer token.

    Stable `code` values:
      - aws_api_unauthorized: 401/403 — credentials rejected / no IAM permission
      - aws_api_not_found: 404 — account/region/resource not visible
      - aws_api_rate_limited: 429/Throttling with retry budget exhausted
      - aws_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, account_id?, region?}`. IAM
    secret keys are NEVER placed on context.
    """
    default_code = "aws_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class CartaApiError(CompanyOSError):
    """
    Outbound Carta REST call failure (cap-table source — OAuth/QuickBooks
    archetype). Carta uses an OAuth2 access token (+ rotating refresh token);
    scope id is `firm_id`. Mirrors QuickBooksApiError. Poll-only (no webhook).

    Stable `code` values:
      - carta_api_unauthorized: 401/403 — access token expired / no scope
        (the caller may need to refresh via the rotating refresh token)
      - carta_api_not_found: 404 — entity/firm not visible
      - carta_api_rate_limited: 429 with retry budget exhausted
      - carta_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The access/refresh
    tokens are NEVER placed on context.
    """
    default_code = "carta_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class HibobApiError(CompanyOSError):
    """
    Outbound HiBob (People/HR) REST call failure (IN-PEOPLE, Gusto-structure /
    Brex-auth archetype). HiBob authenticates with a service user: HTTP Basic
    `base64(service_user_id:token)` — NOT OAuth, no refresh. Mirrors BrexApiError.

    Stable `code` values:
      - hibob_api_unauthorized: 401/403 — service-user credential rejected /
        insufficient scope
      - hibob_api_not_found: 404 — employee/entity not visible to the credential
      - hibob_api_rate_limited: 429 with retry budget exhausted
      - hibob_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The service-user
    token and the Basic-auth header are NEVER placed on context.
    """
    default_code = "hibob_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class AshbyApiError(CompanyOSError):
    """
    Outbound Ashby (Recruiting ATS) RPC call failure (IN-PEOPLE, Gusto-structure
    archetype). Ashby authenticates with an API key as the Basic username + empty
    password (`base64("KEY:")`); RPC POST `/CATEGORY.list|.info`. Mirrors
    BrexApiError.

    Stable `code` values:
      - ashby_api_unauthorized: 401/403 — API key rejected / insufficient scope
      - ashby_api_not_found: 404 — entity not visible to the key
      - ashby_api_rate_limited: 429 with retry budget exhausted
      - ashby_api_error: other terminal 4xx/5xx (or an RPC `errors` extension)

    `context` carries `{http_status?, retry_after?, path?}`. The API key and the
    Basic-auth header are NEVER placed on context.
    """
    default_code = "ashby_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


class LinkedinApiError(CompanyOSError):
    """
    Outbound LinkedIn (Recruiting) REST call failure (IN-PEOPLE, Carta-structure
    archetype). LinkedIn uses OAuth2; the organization-scoped recruitment APIs are
    PARTNER-GATED (invite-only). Poll-only live edge (no webhook). Mirrors
    CartaApiError.

    Stable `code` values:
      - linkedin_api_unauthorized: 401/403 — access token expired / no scope /
        partner entitlement not granted
      - linkedin_api_not_found: 404 — organization/entity not visible
      - linkedin_api_rate_limited: 429 with retry budget exhausted
      - linkedin_api_error: other terminal 4xx/5xx

    `context` carries `{http_status?, retry_after?, path?}`. The access/refresh
    tokens are NEVER placed on context.
    """
    default_code = "linkedin_api_error"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        context: dict[str, Any] | None = None,
        **extra: Any,
    ) -> None:
        merged = dict(context or {})
        merged.update(extra)
        super().__init__(message, **merged)
        if code is not None:
            self._code = code


__all__ = [
    "CompanyOSError",
    "ValidationError",
    "InvariantViolation",
    "SchemaDriftError",
    "TrustTierError",
    "FalsifierInadequateError",
    "MalformedFalsifierError",
    "CalibrationMissingError",
    "InstallationConflictError",
    "InstallationNotFoundError",
    "SecretStoreError",
    "SecretNotFoundError",
    "StateTokenInvalidError",
    "InstallationCollisionError",
    "DiscordOAuthError",
    "DiscordApiError",
    "GithubJWTError",
    "GithubOAuthError",
    "GithubApiError",
    "NotionOAuthError",
    "NotionApiError",
    "JiraApiError",
    "MercuryApiError",
    "GrafanaApiError",
    "QuickBooksApiError",
    "TelegramApiError",
    "BrexApiError",
    "RampApiError",
    "GustoApiError",
    "DeelApiError",
    "FirefliesApiError",
    "MiroApiError",
    "FigmaApiError",
    "SignalApiError",
    "AwsApiError",
    "CartaApiError",
    "HibobApiError",
    "AshbyApiError",
    "LinkedinApiError",
]
